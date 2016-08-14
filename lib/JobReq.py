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
import RunAndMonitorEc2Inst

import JobControllerLog


# Note: no graceful termination

_thr_poll = None


def PollBackground(jr_q):
	_Init()

	global _thr_poll
	_thr_poll = threading.Thread(target=_Poll, args=[jr_q])
	_thr_poll.daemon = True
	_thr_poll.start()


# TODO: Check if other places have hard-coded this
sqs_q_name = "mutants-jobs-requested"

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
	Cons.P("Deleting the job request message ...")
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


def Process(req, job_controller_gm_q):
	# Note: May want some admission control here, like one based on how many free
	# instance slots are available.

	job_id = time.strftime("%y%m%d-%H%M%S")
	JobControllerLog.P("\nGot a job request msg. job_id:%s attrs:\n%s"
			% (job_id, Util.Indent(pprint.pformat(req.attrs), 2)))
	req.attrs["job_id"] = job_id

	# Pass these as the init script parameters. Decided not to use EC2 tag
	# for these, due to its limitations.
	#   http://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/allocation-tag-restrictions.html
	jr_sqs_url = req.msg.queue_url
	jr_sqs_msg_receipt_handle = req.msg.receipt_handle

	# Get job controller parameters and delete them from the attrs
	jc_params = json.loads(req.attrs["job_controller_params"])
	req.attrs.pop("job_controller_params", None)

	# Cassandra cluster name. It's ok for multiple clusters to have the same
	# cluster_name for Cassandra. It's ok for multiple clusters to have the
	# same name as long as they don't see each other through the gossip
	# protocol.  It's even okay to use the default one: test-cluster
	#req.attrs["cass_cluster_name"] = "mutants"

	ReqSpotInsts.Req(
			region_spot_req = jc_params["region_spot_req"]
			, ami_name = jc_params.get("ami_name", "mutants-server")
			, tags = req.attrs
			, jr_sqs_url = jr_sqs_url
			, jr_sqs_msg_receipt_handle = jr_sqs_msg_receipt_handle
			, job_controller_gm_q = job_controller_gm_q
			)
	# On-demand instances are too expensive.
	#RunAndMonitorEc2Inst.Run()

	# Sleep a bit to make each request has unique job_id
	time.sleep(1.1)

	# Delete the job request msg for non-"mutants-server" nodes, e.g.,
	# mutants-dev nodes, so that they don't reappear.
	if req.attrs["init_script"] not in ["mutants-server"]:
		DeleteMsg(jr_sqs_msg_receipt_handle)


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

					# TODO: If this can be 0 or a faction of second, then this doesn't have to be in a separate thread.
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
	msg_body = "mutants-exp-req"

	def __init__(self, msg):
		if msg.body != Msg.msg_body:
			raise RuntimeError("Unexpected. msg.body=[%s]" % msg.body)
		if msg.receipt_handle is None:
			raise RuntimeError("Unexpected")
		if msg.message_attributes is None:
			raise RuntimeError("Unexpected")

		self.attrs = {}
		for k, v in msg.message_attributes.iteritems():
			if v["DataType"] != "String":
				raise RuntimeError("Unexpected")
			v1 = v["StringValue"]
			self.attrs[k] = v1
			#Cons.P("  %s: %s" % (k, v1))

		self.msg = msg


def _GetQ():
	# Get the queue. Create one if not exists.
	try:
		queue = _sqs.get_queue_by_name(
				QueueName = sqs_q_name,
				# QueueOwnerAWSAccountId='string'
				)
		#Cons.P(pprint.pformat(vars(queue), indent=2))
		#{ '_url': 'https://queue.amazonaws.com/998754746880/mutants-exps',
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
