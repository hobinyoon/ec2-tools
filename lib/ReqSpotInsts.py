import base64
import botocore
import concurrent.futures
import datetime
import os
import pprint
import sys
import threading
import time
import traceback

sys.path.insert(0, "%s/util" % os.path.dirname(__file__))
import Cons
import Util

import BotoClient
import Conf
import Ec2Region
import JobContOutput
import SpotPrice


_dn_run = "%s/../.run" % os.path.dirname(__file__)


# http://masnun.com/2016/03/29/python-a-quick-introduction-to-the-concurrent-futures-module.html
_tpe = concurrent.futures.ThreadPoolExecutor(max_workers=10)


def Req(job_id, msg, job_controller_gm_q):
	Util.MkDirs(_dn_run)
	_tpe.submit(_Req, job_id, msg, job_controller_gm_q)


class _Req:
	def __init__(self, job_id, req_msg, job_controller_gm_q):
		try:
			self.job_id = job_id
			self.log = Log(job_id)
			self.log.P("req_msg: %s" % req_msg)
			self.req_msg = req_msg

			# Get the AZ with the most stable last-2-day spot price
			self.region = req_msg.msg_body["region"]
			self.inst_type = req_msg.msg_body["spot_req"]["inst_type"]
			self.az = SpotPrice.MostStableAz(self.region, self.inst_type)
			self.spot_max_price = req_msg.msg_body["spot_req"]["max_price"]
			self.num_nodes = 1
			if "server" in self.req_msg.msg_body:
				self.num_nodes += int(self.req_msg.msg_body["server"]["num_nodes"])

			self.spot_req_infos = _Req.SpotReqInfos()

			self.inst_check_thr_started = False
			self.inst_check_thr_cnt_lock = threading.Lock()

			self.job_controller_gm_q = job_controller_gm_q

			self._ReqSpotInstClient()
			self._ReqSpotInstServer()
			self._KeepCheckingSpotReq()

			# Join the inst checking thread
			self.thread_inst_check.join()
		except Exception as e:
			self.log.P("%s\n%s" % (e, traceback.format_exc()), output="both")
			os._exit(1)

	class SpotReqInfos:
		class Req:
			def __init__(self, name, spot_req_id):
				# name can be client, server0, server1, ...
				self.name = name
				self.spot_req_id = spot_req_id
				# inst_id is set when the spot request is fulfilled
				self.inst_id = None
				self.inst_tagged = False

		def __init__(self):
			self.lock = threading.Lock()
			self.by_sr_id = {}
			self.by_inst_id = {}
			# Not sure by_type will be needed

		def Add(self, name, spot_req_id):
			with self.lock:
				name1 = name
				if name == "server":
					num_servers = 0
					for sr_id, r in self.by_sr_id.iteritems():
						if r.name.startswith("server"):
							num_servers += 1
					name1 = "%s%d" % (name, num_servers)
				r = _Req.SpotReqInfos.Req(name1, spot_req_id)
				self.by_sr_id[spot_req_id] = r

		def SpotReqIds(self):
			with self.lock:
				return self.by_sr_id.keys()

		def SetSpotReqFulfilled(self, spot_req_id, inst_id):
			with self.lock:
				r = self.by_sr_id[spot_req_id]
				r.inst_id = inst_id
				self.by_inst_id[inst_id] = r

		def AllFulfilled(self):
			with self.lock:
				for sr_id, r in self.by_sr_id.iteritems():
					if r.inst_id is None:
						return False
				return True

		def GetFulfilledInstIDs(self):
			with self.lock:
				return self.by_inst_id.keys()

		def IsInstTagged(self, inst_id):
			with self.lock:
				r = self.by_inst_id[inst_id]
				return r.inst_tagged

		def SetInstTagged(self, inst_id):
			with self.lock:
				r = self.by_inst_id[inst_id]
				r.inst_tagged = True

		def NodeName(self, inst_id):
			with self.lock:
				r = self.by_inst_id[inst_id]
				return r.name

		def NodeNameShort(self, inst_id):
			with self.lock:
				r = self.by_inst_id[inst_id]
				return r.name.replace("client", "c").replace("server", "s")


	# This is run as root. Some of them need to be run by the user ubuntu
	#   http://unix.stackexchange.com/questions/4342/how-do-i-get-sudo-u-user-to-use-the-users-env
	user_data = \
"""#!/bin/bash
rm -rf /home/ubuntu/work/mutants
sudo -i -u ubuntu bash -c 'mkdir -p /home/ubuntu/work/mutants'
sudo -i -u ubuntu bash -c 'git clone https://github.com/hobinyoon/mutants-ec2-tools.git /home/ubuntu/work/mutants/ec2-tools'
sudo -i -u ubuntu /home/ubuntu/work/mutants/ec2-tools/lib/ec2-init.py {0}
"""
	# TODO: may want to pass this to ec2-init.py. we'll see.
	# jr_sqs_url = msg.msg.queue_url

	def _ReqSpotInstClient(self):
		if "client" not in self.req_msg.msg_body:
			return

		user_data = _Req.user_data.format(self.req_msg.Serialize({"job_id": self.job_id, "type": "client"}))
		ami_name = self.req_msg.msg_body["client"]["ami_name"]

		ls = {'ImageId': GetLatestAmiId(self.region, ami_name)
				#, 'KeyName': 'string'
				, 'SecurityGroups': ["cass-server"]
				, 'UserData': base64.b64encode(user_data)
				#, 'AddressingType': 'string'
				, 'InstanceType': self.inst_type
				# c3.8xlarge doesn't support EBS optimized
				, 'EbsOptimized': (False if self.inst_type == "c3.8xlarge" else True)
				, 'Placement': {'AvailabilityZone': self.az}
				}

		self.log.P(Util.FileLine())

		while True:
			try:
				r = BotoClient.Get(self.region).request_spot_instances(
						SpotPrice = str(self.spot_max_price),
						#ClientToken='string',
						InstanceCount = 1,
						Type = "one-time",
						#ValidFrom=datetime(2015, 1, 1),
						#ValidUntil=datetime(2015, 1, 1),
						#LaunchGroup='string',
						#AvailabilityZoneGroup='string',

						# https://aws.amazon.com/blogs/aws/new-ec2-spot-blocks-for-defined-duration-workloads/
						#BlockDurationMinutes=123,

						LaunchSpecification = ls,
						)
				self.log.P("SpotInstReqResp: %s" % pprint.pformat(r))
				if len(r["SpotInstanceRequests"]) != 1:
					raise RuntimeError("len(r[\"SpotInstanceRequests\"])=%d" % len(r["SpotInstanceRequests"]))

				spot_req_id = r["SpotInstanceRequests"][0]["SpotInstanceRequestId"]
				self.spot_req_infos.Add("client", spot_req_id)
				self.log.P("spot_req_id client: %s" % spot_req_id, output="both")
				break
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "RequestLimitExceeded":
					self.log.P("region=%s error=%s" % (self.region, e), output="both")
					time.sleep(5)
				else:
					raise e

	def _ReqSpotInstServer(self):
		if "server" not in self.req_msg.msg_body:
			return

		user_data = _Req.user_data.format(self.req_msg.Serialize({"job_id": self.job_id, "type": "server"}))
		req_msg_server = self.req_msg.msg_body["server"]
		ami_name = self.req_msg_server["ami_name"]
		server_num_nodes = self.req_msg_server["num_nodes"]

		block_dev_mappings = []
		for b in self.req_msg_server["block_storage_devs"]:
			block_dev_mappings.append({
				"DeviceName": "/dev/sd%s" % b["DeviceName"]
				, "Ebs": {
					"VolumeSize": b["VolumeSize"]
					, "DeleteOnTermination": True
					, "VolumeType": b["VolumeType"]
					}
				})

		ls = {'ImageId': GetLatestAmiId(self.region, ami_name)
				#, 'KeyName': 'string'
				, 'SecurityGroups': ["cass-server"]
				, 'UserData': base64.b64encode(user_data)
				#, 'AddressingType': 'string'
				, 'InstanceType': self.inst_type
				, 'EbsOptimized': True
				, 'Placement': {'AvailabilityZone': self.az}
				, 'BlockDeviceMappings': block_dev_mappings
				}

		while True:
			try:
				r = BotoClient.Get(self.region).request_spot_instances(
						SpotPrice = str(self.spot_max_price),
						InstanceCount = int(server_num_nodes),
						Type = "one-time",
						LaunchSpecification = ls,
						)
				self.log.P("SpotInstReqResp: %s" % pprint.pformat(r))

				for e in r["SpotInstanceRequests"]:
					spot_req_id = e["SpotInstanceRequestId"]
					self.spot_req_infos.Add("server", spot_req_id)
					self.log.P("spot_req_id server: %s" % spot_req_id, output="both")
				break
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "RequestLimitExceeded":
					self.log.P("region=%s error=%s" % (self.region, e), output="both")
					time.sleep(5)
				else:
					raise e

	def _KeepCheckingSpotReq(self):
		r = None
		while True:
			while True:
				try:
					r = BotoClient.Get(self.region).describe_spot_instance_requests(
							SpotInstanceRequestIds = self.spot_req_infos.SpotReqIds())
					#self.log.P(Util.Indent(pprint.pformat(r, indent=2, width=100), 2), output="both")
					break
				except botocore.exceptions.ClientError as e:
					if e.response["Error"]["Code"] == "InvalidSpotInstanceRequestID.NotFound":
						self.log.P("region=%s error=%s" % (self.region, e), output="both")
						time.sleep(1)
					else:
						raise e

			for e in r["SpotInstanceRequests"]:
				spot_req_id = e["SpotInstanceRequestId"]
				status = e["Status"]["Code"]

				if status != "fulfilled":
					self.log.P("spot_req_id=%s status=%s" % (spot_req_id, status))
					continue

				inst_id = e["InstanceId"]
				self.log.P("spot_req_id=%s status=%s inst_id=%s" % (spot_req_id, status, inst_id))
				self.spot_req_infos.SetSpotReqFulfilled(spot_req_id, inst_id)

				# A "fulfilled" request needs to move on to the next round right away,
				# so that the instance can be "tagged" before cloud-init script is run
				# on the node. So, this and the next function need to overlap.
				self._StartInstCheckingThread()

			# We are done with describe_spot_instance_requests()
			if self.spot_req_infos.AllFulfilled():
				return
			time.sleep(2)


	def _StartInstCheckingThread(self):
		with self.inst_check_thr_cnt_lock:
			if self.inst_check_thr_started:
				return
			self.inst_check_thr_started = True

			self.thread_inst_check = threading.Thread(target=self._KeepCheckingInstAndTag)
			self.thread_inst_check.daemon = True
			self.thread_inst_check.start()


	def _KeepCheckingInstAndTag(self):
		try:
			while True:
				r = None
				while True:
					try:
						r = BotoClient.Get(self.region).describe_instances(InstanceIds=self.spot_req_infos.GetFulfilledInstIDs())
						# Note: describe_instances() returns StateReason, while
						# describe_instance_status() doesn't.
						break
					except botocore.exceptions.ClientError as e:
						if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
							self.log.P("region=%s error=%s" % (self.region, e), output="both")
							time.sleep(1)
						else:
							raise e

				num_terminated_or_running = 0

				for e in r["Reservations"]:
					for e1 in e["Instances"]:
						inst_id = e1["InstanceId"]
						state = e1["State"]["Name"]
						self.log.P("inst_id=%s state=%s" % (inst_id, state))
						if state == "terminated":
							num_terminated_or_running += 1
						elif state == "running":
							num_terminated_or_running += 1
							pub_ip = e1["PublicIpAddress"]

							# Make region-ipaddr files. Helpful for dev. Match them with the
							# hostnames. Examples:
							#   ssh-us-east-1-c
							#   ssh-us-east-1-s0
							#   ssh-us-east-1-s1
							fn = "%s/%s-%s" % (_dn_run, self.region, self.spot_req_infos.NodeNameShort(inst_id))
							with open(fn, "w") as fo:
								fo.write(pub_ip)

						elif state == "pending":
							self._TagInst(inst_id)

				# We are done here
				if num_terminated_or_running == self.num_nodes:
					msg = "job_id: %s. %d instances are created." % (self.job_id, self.num_nodes)
					self.job_controller_gm_q.put(msg, block=True, timeout=None)
					return

				time.sleep(2)
		except Exception as e:
			self.log.P("%s\n%s" % (e, traceback.format_exc()), output="both")
			os._exit(1)


	def _TagInst(self, inst_id):
		if self.spot_req_infos.IsInstTagged(inst_id):
			return

		# Go with minimal tags for now.
		tags = {
				"job_id": self.job_id
				, "name": self.spot_req_infos.NodeName(inst_id)
				# Note: node expiration time can be added here for auto cleaning. dev
				# nodes don't have them.
				}

		tags_boto = []
		for k, v in tags.iteritems():
			tags_boto.append({"Key": k, "Value": v})

		while True:
			try:
				BotoClient.Get(self.region).create_tags(Resources=[inst_id], Tags=tags_boto)
				self.spot_req_infos.SetInstTagged(inst_id)
				return
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
					self.log.P("region=%s error=%s" % (self.region, e), output="both")
					time.sleep(1)
				elif e.response["Error"]["Code"] == "RequestLimitExceeded":
					self.log.P("region=%s error=%s" % (self.region, e), output="both")
					time.sleep(5)
				else:
					raise e


class Log:
	def __init__(self, job_id):
		dn = "%s/../.log/job-req" % os.path.dirname(__file__)
		if not os.path.isdir(dn):
			Util.MkDirs(dn)

		self.fo_lock = threading.Lock()
		self.fo = open("%s/%s" % (dn, job_id), "a")
		self.P("job_id: %s (for describing and terminating the nodes in the job)" % job_id)

	def P(self, msg, output = "log"):
		with self.fo_lock:
			if output == "log":
				self.fo.write("%s\n" % msg)
			elif output == "both":
				self.fo.write("%s\n" % msg)
				JobContOutput.P(msg)
			else:
				raise RuntimeError("Unexpected output: %s" % output)

	def Pnnl(self, msg, output = "log"):
		with self.fo_lock:
			if output == "log":
				self.fo.write("%s" % msg)
			elif output == "both":
				self.fo.write("%s" % msg)
				JobContOutput.Pnnl(msg)
			else:
				raise RuntimeError("Unexpected output: %s" % output)


def GetLatestAmiId(region, name):
	return Conf.Get()["region_ami"][name][region]
