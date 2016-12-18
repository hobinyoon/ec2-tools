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


# http://masnun.com/2016/03/29/python-a-quick-introduction-to-the-concurrent-futures-module.html
_tpe = concurrent.futures.ThreadPoolExecutor(max_workers=10)


def Launch(job_id, msg, job_controller_gm_q):
	_tpe.submit(_Req, job_id, msg, job_controller_gm_q)


class _Req:
	def __init__(self, job_id, req_msg, job_controller_gm_q):
		try:
			self.job_id = job_id
			self.log = Log(job_id)
			self.log.P("req_msg: %s" % req_msg)
			self.req_msg = req_msg

			# Launch instances in the AZ with the most stable last-2-day spot price.
			# Might be better than randomly placing them.
			self.region = req_msg.msg_body["region"]
			self.inst_type = req_msg.msg_body["inst_type"]
			if True:
				self.az = SpotPrice.MostStableAz(self.region, self.inst_type)
			else:
				# 161108-135339: An error occurred (InsufficientInstanceCapacity) when
				# calling the RunInstances operation (reached max retries: 4): We
				# currently do not have sufficient c3.2xlarge capacity in the
				# Availability Zone you requested (us-east-1a). Our system will be
				# working on provisioning additional capacity. You can currently get
				# c3.2xlarge capacity by not specifying an Availability Zone in your
				# request or choosing us-east-1c.
				self.az = "us-east-1c"

			# One client and multiple servers
			self.num_nodes = 1
			if "server" in self.req_msg.msg_body:
				self.num_nodes += int(self.req_msg.msg_body["server"]["num_nodes"])

			self.inst_check_thr_started = False
			self.inst_check_thr_cnt_lock = threading.Lock()

			self.job_controller_gm_q = job_controller_gm_q

			self.inst_info = InstInfo()

			self._LaunchServers()
			self._LaunchClient()

			# Join the inst checking thread
			self.thread_inst_check.join()
		except Exception as e:
			self.log.P("%s\n%s" % (e, traceback.format_exc()), output="both")
			os._exit(1)

	# This is run as root. Some of them need to be run by the user ubuntu
	#   http://unix.stackexchange.com/questions/4342/how-do-i-get-sudo-u-user-to-use-the-users-env
	user_data = \
"""#!/bin/bash
sudo -i -u ubuntu bash -c 'git clone https://github.com/hobinyoon/mutant-ec2-tools.git /home/ubuntu/work/mutant/ec2-tools'
sudo -i -u ubuntu /home/ubuntu/work/mutant/ec2-tools/lib/ec2-init.py {0}
"""
#rm -rf /home/ubuntu/work/mutant
#sudo -i -u ubuntu bash -c 'mkdir -p /home/ubuntu/work/mutant'

	def _LaunchClient(self):
		if "client" not in self.req_msg.msg_body:
			return

		user_data = _Req.user_data.format(self.req_msg.Serialize({"job_id": self.job_id, "type": "client"}))
		ami_name = self.req_msg.msg_body["client"]["ami_name"]
		self.log.P(Util.FileLine())

		while True:
			try:
				r = BotoClient.Get(self.region).run_instances(
						ImageId = GetLatestAmiId(self.region, ami_name)
						, MinCount=1
						, MaxCount=1
						, SecurityGroups=["mutant-server"]
						, EbsOptimized=True
						, InstanceType = self.inst_type
						, Placement={"AvailabilityZone": self.az}
						, UserData=user_data
						, InstanceInitiatedShutdownBehavior='terminate'
						)
				self.log.P("run_instances response: %s" % pprint.pformat(r))

				if len(r["Instances"]) != 1:
					raise RuntimeError("len(r[\"Instances\"])=%d" % len(r["Instances"]))
				client_inst_id = r["Instances"][0]["InstanceId"]
				self.inst_info.Add("client", [client_inst_id])
				self.log.P("client inst_id: %s" % client_inst_id, output="both")

				self._StartInstCheckingThread()
				break
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "RequestLimitExceeded":
					self.log.P("region=%s error=%s" % (self.region, e), output="both")
					time.sleep(5)
				else:
					raise e

	def _LaunchServers(self):
		if "server" not in self.req_msg.msg_body:
			return

		user_data = _Req.user_data.format(self.req_msg.Serialize({"job_id": self.job_id, "type": "server"}))
		req_msg_server = self.req_msg.msg_body["server"]
		ami_name = req_msg_server["ami_name"]
		server_num_nodes = int(req_msg_server["num_nodes"])
		self.log.P(Util.FileLine())

		block_dev_mappings = []
		for b in req_msg_server["block_storage_devs"]:
			block_dev_mappings.append({
				"DeviceName": "/dev/sd%s" % b["DeviceName"]
				, "Ebs": {
					"VolumeSize": b["VolumeSize"]
					, "DeleteOnTermination": True
					, "VolumeType": b["VolumeType"]
					}
				})

		while True:
			try:
				r = BotoClient.Get(self.region).run_instances(
						ImageId = GetLatestAmiId(self.region, ami_name)
						, MinCount=server_num_nodes
						, MaxCount=server_num_nodes
						, SecurityGroups=["mutant-server"]
						, EbsOptimized=True
						, InstanceType = self.inst_type
						, Placement={"AvailabilityZone": self.az}
						, BlockDeviceMappings = block_dev_mappings
						, UserData=user_data
						, InstanceInitiatedShutdownBehavior='terminate'
						)
				self.log.P("run_instances response: %s" % pprint.pformat(r))

				if len(r["Instances"]) != server_num_nodes:
					raise RuntimeError("len(r[\"Instances\"])=%d" % len(r["Instances"]))
				server_inst_ids = []
				for i in r["Instances"]:
					server_inst_ids.append(i["InstanceId"])
				self.inst_info.Add("server", server_inst_ids)
				self.log.P("server inst_id(s): %s" % " ".join(server_inst_ids), output="both")

				self._StartInstCheckingThread()
				break
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "RequestLimitExceeded":
					self.log.P("region=%s error=%s" % (self.region, e), output="both")
					time.sleep(5)
				else:
					raise e


	# This is called as soon as you know the first inst_id
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
						r = BotoClient.Get(self.region).describe_instances(InstanceIds=self.inst_info.GetAllInstIds())
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
						elif state == "pending":
							self._TagInst(inst_id)

				# We are done here
				if num_terminated_or_running == self.num_nodes:
					msg = "job_id: %s. %d instances are created." % (self.job_id, self.num_nodes)
					self.job_controller_gm_q.put(msg, block=True, timeout=None)
					return

				time.sleep(1)
		except Exception as e:
			self.log.P("%s\n%s" % (e, traceback.format_exc()), output="both")
			os._exit(1)


	def _TagInst(self, inst_id):
		if self.inst_info.IsInstTagged(inst_id):
			return

		# Go with minimal tags for now. The numbers are limited, like 10.
		tags = {
				"job_id": self.job_id
				, "name": self.inst_info.NodeName(inst_id)
				# Note: node expiration time can be added here for auto cleaning when
				# needed later. dev nodes don't have them.
				}

		tags_boto = []
		for k, v in tags.iteritems():
			tags_boto.append({"Key": k, "Value": v})

		while True:
			try:
				BotoClient.Get(self.region).create_tags(Resources=[inst_id], Tags=tags_boto)
				self.inst_info.SetInstTagged(inst_id)
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


class InstInfo:
	class E:
		def __init__(self, name, inst_id):
			# names are "client", "server0", "server1", ...
			self.name = name
			self.inst_id = inst_id
			self.inst_tagged = False

	def __init__(self):
		self.lock = threading.Lock()
		self.by_inst_id = {}

	def Add(self, name, inst_ids):
		with self.lock:
			if name == "server":
				for i in range(len(inst_ids)):
					name1 = "%s%d" % (name, i)
					inst_id = inst_ids[i]
					e = InstInfo.E(name1, inst_id)
					self.by_inst_id[inst_id] = e
			else:
				if len(inst_ids) != 1:
					raise RuntimeError("Unexpected: %d" % len(inst_ids))
				inst_id = inst_ids[0]
				e = InstInfo.E(name, inst_id)
				self.by_inst_id[inst_id] = e

	def GetAllInstIds(self):
		with self.lock:
			keys = []
			for k, v in self.by_inst_id.iteritems():
				keys.append(k)
			return keys

	def IsInstTagged(self, inst_id):
		with self.lock:
			e = self.by_inst_id[inst_id]
			return e.inst_tagged

	def NodeName(self, inst_id):
		with self.lock:
			e = self.by_inst_id[inst_id]
			return e.name

	def SetInstTagged(self, inst_id):
		with self.lock:
			e = self.by_inst_id[inst_id]
			e.inst_tagged = True


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
