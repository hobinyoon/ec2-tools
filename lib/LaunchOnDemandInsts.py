import base64
import botocore
import json
import os
import pprint
import sys
import time
import zlib

sys.path.insert(0, "%s/util" % os.path.dirname(__file__))
import Cons

import BotoClient
import Conf
import SpotPrice


def Launch(params):
	r = _Req(params)


class _Req:
	def __init__(self, params):
		self.job_id = time.strftime("%y%m%d-%H%M%S")
		self.params = params

		# Launch instances in the AZ with the most stable last-2-day spot price.
		# Might be better than randomly placing them.
		if True:
			self.az = SpotPrice.MostStableAz(self.params["region"], self.params["inst_type"])
		else:
			# 161108-135339: An error occurred (InsufficientInstanceCapacity) when
			# calling the RunInstances operation (reached max retries: 4): We
			# currently do not have sufficient c3.2xlarge capacity in the
			# Availability Zone you requested (us-east-1a). Our system will be
			# working on provisioning additional capacity. You can currently get
			# c3.2xlarge capacity by not specifying an Availability Zone in your
			# request or choosing us-east-1c.
			self.az = "us-east-1c"

		self.inst_info = InstInfo()

		self._LaunchNode()

	def _LaunchNode(self):
		self.params["extra"] = {"job_id": self.job_id, "type": "server"}

		# This is run as root. Some of them need to be run by the user ubuntu
		#   http://unix.stackexchange.com/questions/4342/how-do-i-get-sudo-u-user-to-use-the-users-env
		user_data = \
"""#!/bin/bash
sudo rm -rf /home/ubuntu/work/mutant/ec2-tools
sudo -i -u ubuntu bash -c 'git clone https://github.com/hobinyoon/mutant-ec2-tools.git /home/ubuntu/work/mutant/ec2-tools'
sudo -i -u ubuntu /home/ubuntu/work/mutant/ec2-tools/lib/ec2-init.py {0}
"""
		# User data is limited to 16384 bytes. zlib has a really good compression rate.
		#user_data = user_data.format(base64.b64encode(json.dumps(self.params)))
		user_data = user_data.format(base64.b64encode(zlib.compress(json.dumps(self.params))))

		# Useful for dev, so that you don't need to launch a new instance every time you test something.
		if False:
			Cons.P("user_data=[%s]" % user_data)
			sys.exit(0)

		block_dev_mappings = []
		for b in self.params["block_storage_devs"]:
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
				r = BotoClient.Get(self.params["region"]).run_instances(
						ImageId = GetLatestAmiId(self.params["region"], self.params["ami_name"])
						, MinCount=1
						, MaxCount=1
						, SecurityGroups=["mutant-server"]
						, EbsOptimized=True
						, InstanceType = self.params["inst_type"]
						, Placement={"AvailabilityZone": self.az}
						, BlockDeviceMappings = block_dev_mappings
						, UserData=user_data
						, InstanceInitiatedShutdownBehavior='terminate'
						)
				Cons.P("run_instances response: %s" % pprint.pformat(r))

				if len(r["Instances"]) != 1:
					raise RuntimeError("len(r[\"Instances\"])=%d" % len(r["Instances"]))
				server_inst_ids = []
				for i in r["Instances"]:
					server_inst_ids.append(i["InstanceId"])
				self.inst_info.Add("server", server_inst_ids)
				Cons.P("server inst_id(s): %s" % " ".join(server_inst_ids))

				self._KeepCheckingInstAndTag()
				break
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "RequestLimitExceeded":
					Cons.P("region=%s error=%s" % (self.params["region"], e))
					time.sleep(5)
				else:
					raise e


	def _KeepCheckingInstAndTag(self):
		while True:
			r = None
			while True:
				try:
					r = BotoClient.Get(self.params["region"]).describe_instances(InstanceIds=self.inst_info.GetAllInstIds())
					break
				except botocore.exceptions.ClientError as e:
					if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
						Cons.P("region=%s error=%s" % (self.params["region"], e))
						time.sleep(1)
					else:
						raise e

			num_running = 0

			for e in r["Reservations"]:
				for e1 in e["Instances"]:
					inst_id = e1["InstanceId"]
					state = e1["State"]["Name"]
					Cons.P("inst_id=%s state=%s" % (inst_id, state))
					if state in ["shutting-down", "terminated"]:
						raise RuntimeError("Unexpected: %s" % pprint.pformat(e1))
					elif state == "running":
						num_running += 1
						pub_ip = e1["PublicIpAddress"]
					elif state == "pending":
						self._TagInst(inst_id)

			if num_running == 1:
				Cons.P("job_id: %s. %d instances are created." % (self.job_id, num_running))
				return

			time.sleep(1)


	def _TagInst(self, inst_id):
		if self.inst_info.IsInstTagged(inst_id):
			return

		# Go with minimal tags for now. The numbers are limited, like 10.
		tags = {"job_id": self.job_id}

		tags_boto = []
		for k, v in tags.iteritems():
			tags_boto.append({"Key": k, "Value": v})

		while True:
			try:
				BotoClient.Get(self.params["region"]).create_tags(Resources=[inst_id], Tags=tags_boto)
				self.inst_info.SetInstTagged(inst_id)
				return
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
					Cons.P("region=%s error=%s" % (self.params["region"], e))
					time.sleep(1)
				elif e.response["Error"]["Code"] == "RequestLimitExceeded":
					Cons.P("region=%s error=%s" % (self.params["region"], e))
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
		self.by_inst_id = {}

	def Add(self, name, inst_ids):
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
		keys = []
		for k, v in self.by_inst_id.iteritems():
			keys.append(k)
		return keys

	def IsInstTagged(self, inst_id):
		e = self.by_inst_id[inst_id]
		return e.inst_tagged

	def NodeName(self, inst_id):
		e = self.by_inst_id[inst_id]
		return e.name

	def SetInstTagged(self, inst_id):
		e = self.by_inst_id[inst_id]
		e.inst_tagged = True


def GetLatestAmiId(region, name):
	return Conf.Get()["region_ami"][name][region]
