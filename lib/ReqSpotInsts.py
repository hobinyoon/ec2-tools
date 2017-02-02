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
import Util

import BotoClient
import Conf
import SpotPrice


def Req(params):
	r = _Req(params)


class _Req:
	def __init__(self, params):
		self.job_id = time.strftime("%y%m%d-%H%M%S")
		Cons.P(pprint.pformat(params))
		self.params = params
		# Get the AZ with the most stable last-2-day spot price
		self.az = SpotPrice.MostStableAz(self.params["region"], self.params["inst_type"])
		self.spot_req_infos = _Req.SpotReqInfos()
		self._ReqSpotInst()

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
			self.by_sr_id = {}
			self.by_inst_id = {}
			# Not sure by_type will be needed

		def Add(self, name, spot_req_id):
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
			return self.by_sr_id.keys()

		def SetSpotReqFulfilled(self, spot_req_id, inst_id):
			r = self.by_sr_id[spot_req_id]
			r.inst_id = inst_id
			self.by_inst_id[inst_id] = r

		def GetFulfilledInstIDs(self):
			return self.by_inst_id.keys()

		def IsInstTagged(self, inst_id):
			if inst_id not in self.by_inst_id:
				return False
			r = self.by_inst_id[inst_id]
			return r.inst_tagged

		def SetInstTagged(self, inst_id):
			r = self.by_inst_id[inst_id]
			r.inst_tagged = True

		def NodeName(self, inst_id):
			r = self.by_inst_id[inst_id]
			return r.name

	def _ReqSpotInst(self):
		self.params["extra"] = {"job_id": self.job_id, "type": "server"}

		# This is run as root. Some of them need to be run by the user ubuntu
		#   http://unix.stackexchange.com/questions/4342/how-do-i-get-sudo-u-user-to-use-the-users-env
		user_data = \
"""#!/bin/bash
sudo rm -rf /home/ubuntu/work/mutant/ec2-tools
sudo -i -u ubuntu bash -c 'git clone https://github.com/hobinyoon/mutant-ec2-tools.git /home/ubuntu/work/mutant/ec2-tools'
sudo -i -u ubuntu /home/ubuntu/work/mutant/ec2-tools/lib/ec2-init.py {0}
"""
		user_data = user_data.format(base64.b64encode(zlib.compress(json.dumps(self.params))))

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

		ls = {'ImageId': GetLatestAmiId(self.params["region"], self.params["ami_name"])
				#, 'KeyName': 'string'
				, 'SecurityGroups': ["mutant-server"]
				, 'UserData': base64.b64encode(user_data)
				#, 'AddressingType': 'string'
				, 'InstanceType': self.params["inst_type"]
				, 'EbsOptimized': True
				, 'Placement': {'AvailabilityZone': self.az}
				, 'BlockDeviceMappings': block_dev_mappings
				}

		while True:
			try:
				r = BotoClient.Get(self.params["region"]).request_spot_instances(
						SpotPrice = str(self.params["spot_req_max_price"]),
						InstanceCount = 1,
						Type = "one-time",
						LaunchSpecification = ls,
						)
				Cons.P("SpotInstReqResp: %s" % pprint.pformat(r))

				for e in r["SpotInstanceRequests"]:
					spot_req_id = e["SpotInstanceRequestId"]
					self.spot_req_infos.Add("server", spot_req_id)
					Cons.P("spot_req_id server: %s" % spot_req_id)
				break
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "RequestLimitExceeded":
					Cons.P("region=%s error=%s" % (self.params["region"], e))
					time.sleep(5)
				else:
					raise e
		self._KeepCheckingSpotReq()

	def _KeepCheckingSpotReq(self):
		fulfilled = False
		while not fulfilled:
			try:
				r = BotoClient.Get(self.params["region"]).describe_spot_instance_requests(
						SpotInstanceRequestIds = self.spot_req_infos.SpotReqIds())
				#Cons.P(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))
				for e in r["SpotInstanceRequests"]:
					spot_req_id = e["SpotInstanceRequestId"]
					status = e["Status"]["Code"]
					if status != "fulfilled":
						Cons.P("spot_req_id=%s status=%s" % (spot_req_id, status))
						continue
					inst_id = e["InstanceId"]
					Cons.P("spot_req_id=%s status=%s inst_id=%s" % (spot_req_id, status, inst_id))
					self.spot_req_infos.SetSpotReqFulfilled(spot_req_id, inst_id)
					fulfilled = True
				if not filfilled:
					time.sleep(1)
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "InvalidSpotInstanceRequestID.NotFound":
					Cons.P("region=%s error=%s" % (self.params["region"], e))
					time.sleep(1)
				else:
					raise e
		self._KeepCheckingInstAndTag()

	def _KeepCheckingInstAndTag(self):
		while True:
			r = None
			while True:
				try:
					r = BotoClient.Get(self.params["region"]).describe_instances(InstanceIds=self.spot_req_infos.GetFulfilledInstIDs())
					# Note: describe_instances() returns StateReason, while
					# describe_instance_status() doesn't.
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

			if num_running >= 1:
				Cons.P("job_id: %s. %d instances are created." % (self.job_id, num_running))
				return

			time.sleep(1)


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
				BotoClient.Get(self.params["region"]).create_tags(Resources=[inst_id], Tags=tags_boto)
				self.spot_req_infos.SetInstTagged(inst_id)
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


def GetLatestAmiId(region, name):
	return Conf.Get()["region_ami"][name][region]
