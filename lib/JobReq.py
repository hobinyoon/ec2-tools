import base64
import boto3
import botocore
import json
import os
import pprint
import sys
import threading
import time
import traceback

sys.path.insert(0, "%s/util" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "..")
import ReqSpotInsts
import LaunchOnDemandInsts

import JobContOutput


# Note: no graceful termination

_thr_poll = None


def PollBackground(jr_q):
	_Init()

	global _thr_poll
	_thr_poll = threading.Thread(target=_Poll, args=[jr_q])
	_thr_poll.daemon = True
	_thr_poll.start()


sqs_q_name = "mutant-jobs-requested"

def DeleteQ():
	_Init()

	Cons.P("\nDeleting the job request queue so that requests don't reappear next time the job controller starts ...")
	try:
		q = _sqs.get_queue_by_name(
				QueueName = sqs_q_name,
				)
		r = _bc.delete_queue(QueueUrl = q._url)
		Cons.P(pprint.pformat(r, indent=2))
	except botocore.exceptions.ClientError as e:
		if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
			Cons.P("No such queue exists.")
		else:
			raise e


def DeleteMsg(msg_receipt_handle):
	JobContOutput.P("Deleting the job request message ...")
	#Cons.P("  receipt_handle: %s" % msg_receipt_handle)
	try:
		response = _bc.delete_message(
				QueueUrl = _q._url,
				ReceiptHandle = msg_receipt_handle
				)
		#Cons.P(pprint.pformat(response, indent=2))
	except botocore.exceptions.ClientError as e:
		if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
			Cons.P("No such queue exists.")
		else:
			raise e


def Process(msg, job_controller_gm_q):
	# Note: May want some admission control here, like one based on how many free
	# instance slots are available.

	job_id = time.strftime("%y%m%d-%H%M%S")
	JobContOutput.P("Got a job request msg. job_id:%s attrs:\n%s"
			% (job_id, Util.Indent(pprint.pformat(msg.msg_body), 2)))

	# Pass these as the init script parameters. Decided not to use EC2 tag
	# for these, due to its limited numbers.
	#   http://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/allocation-tag-restrictions.html

	# Cassandra cluster name. It's ok for multiple clusters to have the same
	# cluster_name for Cassandra. It's ok for multiple clusters to have the
	# same name as long as they don't see each other through the gossip
	# protocol.  It's even okay to use the default one: test-cluster
	#msg.attrs["cass_cluster_name"] = "mutant"

	if "spot_req" in msg.msg_body:
		# Request spot instances
		ReqSpotInsts.Req(
				job_id = job_id
				, msg = msg
				, job_controller_gm_q = job_controller_gm_q
				)
	else:
		# Launch On-demand instances
		LaunchOnDemandInsts.Launch(
				job_id = job_id
				, msg = msg
				, job_controller_gm_q = job_controller_gm_q
				)

	# Sleep a bit to make each request has unique job_id
	time.sleep(1.1)

	# Delete the job request sqs msg dev nodes, so that they don't reappear.
	if msg.IsDevJob():
		DeleteMsg(msg.msg.receipt_handle)


sqs_region = "us-east-1"

_initialized = False
_bc = None
_sqs = None
_q = None

def _Init():
	global _initialized
	if _initialized == False:
		global _bc, _sqs, _q
		_bc = boto3.client("sqs", region_name = sqs_region)
		_sqs = boto3.resource("sqs", region_name = sqs_region)
		_initialized = True
		_q = _GetQ()


def _Poll(jr_q):
	while True:
		try:
			msgs = _q.receive_messages(
					#AttributeNames=[
					#	'Policy'|'VisibilityTimeout'|'MaximumMessageSize'|'MessageRetentionPeriod'|'ApproximateNumberOfMessages'|'ApproximateNumberOfMessagesNotVisible'|'CreatedTimestamp'|'LastModifiedTimestamp'|'QueueArn'|'ApproximateNumberOfMessagesDelayed'|'DelaySeconds'|'ReceiveMessageWaitTimeSeconds'|'RedrivePolicy',
					#	],
					MessageAttributeNames=["All"],
					MaxNumberOfMessages=1,

					# Should be bigger than one experiment duration so that another
					# of the same experiment doesn't get picked up while one is
					# running.
					#
					# 1 hour. If something goes wrong within the first hour, re-execute.
					# FYI, the maximum you can set is 12 hours.
					# http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html
					# Need to be extremely careful not to spend a lot of money by an
					# error in the experiment script overnight.
					VisibilityTimeout=3600,

					# Note: If this can be 0 or a faction of second, then job req
					# messages doen't have to be polled in a separate thread, and the
					# main loop can use a single thread.
					WaitTimeSeconds=5
					)
			for m in msgs:
				# put the job completion msg. Wait when the queue is full.
				jr_q.put(Msg(m), block=True, timeout=None)
		except botocore.exceptions.EndpointConnectionError as e:
			# Could not connect to the endpoint URL: "https://queue.amazonaws.com/"
			Cons.P("%s\n%s" % (e, traceback.format_exc()))
			os._exit(1)
		except Exception as e:
			Cons.P("%s\n%s" % (e, traceback.format_exc()))
			# http://stackoverflow.com/questions/1489669/how-to-exit-the-entire-application-from-a-python-thread
			os._exit(1)


class Msg:
	def __init__(self, msg):
		self.msg = msg

		if msg.receipt_handle is None:
			raise RuntimeError("Unexpected")
		if msg.message_attributes is not None:
			raise RuntimeError("Unexpected")

		self.msg_body = json.loads(msg.body)

	def IsDevJob(self):
		if "server" in self.msg_body:
			return self.msg_body["server"]["init_script"].endswith("-dev")
		elif "client" in self.msg_body:
			return self.msg_body["client"]["init_script"].endswith("-dev")
		else:
			raise RuntimeError("Unexpected")

	# To pass to the ec2 instances
	def Serialize(self, extra_options):
		d = self.msg_body.copy()
		d["sqs_msg_receipt_handle"] = self.msg.receipt_handle
		d["extra"] = extra_options
		return base64.b64encode(json.dumps(d))

	def __str__(self):
		return pprint.pformat(self.msg_body)


def _GetQ():
	# Get the queue. Create one if not exists.
	try:
		queue = _sqs.get_queue_by_name(
				QueueName = sqs_q_name,
				# QueueOwnerAWSAccountId='string'
				)
		#Cons.P(pprint.pformat(vars(queue), indent=2))
		#{ '_url': 'https://queue.amazonaws.com/998754746880/mutant-exps',
		#		  'meta': ResourceMeta('sqs', identifiers=[u'url'])}
		return queue
	except botocore.exceptions.ClientError as e:
		#Cons.P(pprint.pformat(e, indent=2))
		#Cons.P(pprint.pformat(vars(e), indent=2))
		if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
			pass
		else:
			raise e

	Cons.Pnnl("The queue doesn't exists. Creating one ")
	while True:
		response = None
		try:
			response = _bc.create_queue(QueueName = sqs_q_name)
			# Default message retention period is 4 days.
			print ""
			break
		except botocore.exceptions.ClientError as e:
			# When calling the CreateQueue operation: You must wait 60 seconds after
			# deleting a queue before you can create another with the same name.
			# It doesn't give me how much more you need to wait. Polling until succeed.
			if e.response["Error"]["Code"] == "AWS.SimpleQueueService.QueueDeletedRecently":
				sys.stdout.write(".")
				sys.stdout.flush()
				time.sleep(1)
			else:
				raise e

	return _sqs.get_queue_by_name(QueueName = sqs_q_name)
