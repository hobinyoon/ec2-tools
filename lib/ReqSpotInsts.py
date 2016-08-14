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
import Ec2Region
import SpotPrice


_dn_run = "%s/../.run" % os.path.dirname(__file__)


# This can also be used for querying 11 datacenters in parallel.
# http://masnun.com/2016/03/29/python-a-quick-introduction-to-the-concurrent-futures-module.html
_tpe = concurrent.futures.ThreadPoolExecutor(max_workers=11)


def Req(region_spot_req, ami_name, tags, jr_sqs_url, jr_sqs_msg_receipt_handle, job_controller_gm_q):
	Util.MkDirs(_dn_run)

	_tpe.submit(_Req, region_spot_req, ami_name, tags, jr_sqs_url, jr_sqs_msg_receipt_handle
			, job_controller_gm_q)


class _Req:
	def __init__(self, region_spot_req, ami_name, tags, jr_sqs_url, jr_sqs_msg_receipt_handle, job_controller_gm_q):
		log = None
		try:
			job_id = tags["job_id"]
			log = Log(job_id)
			log.P("job_id:%s (for describing and terminating the cluster)" % job_id)

			# Get AZ with the lowest last-2-day max spot price
			#   {region: [az_with_lowest_price, the_price] }
			region_az_lowest_max_spot_price = SpotPrice.GetTheLowestMaxPriceAZs(log, region_spot_req)

			rats = []
			for region, spot_req_params in region_spot_req.iteritems():
				# Spot requests are made to specific AZs, which has the lowest last-1-day
				# max price.
				az = region_az_lowest_max_spot_price[region][0]
				rats.append(ReqAndTag(log, region, spot_req_params, az, ami_name, tags, len(region_spot_req)
					, jr_sqs_url, jr_sqs_msg_receipt_handle
					))
			#for r, az_price in region_az_lowest_max_spot_price.iteritems():
			#	Cons.P("%s %s" % (r, az_price[0], az_price[1]))
			spot_prices_str = "\n".join("%s-%s" % (k, v[1]) for k, v in sorted(region_az_lowest_max_spot_price.items()))

			threads = []
			for rat in rats:
				t = threading.Thread(target=rat.Run)
				t.daemon = True
				threads.append(t)
				t.start()
			for t in threads:
				t.join()

			job_controller_gm_q.put("job_id: %s. %d instances are created. Spot prices:\n%s" \
					% (job_id, len(region_spot_req), spot_prices_str)
					, block=True, timeout=None)
		except Exception as e:
			log.P("%s\n%s" % (e, traceback.format_exc()), target="both")
			os._exit(1)


class Log:
	def __init__(self, job_id):
		dn = "%s/../.log/job-req" % os.path.dirname(__file__)
		if not os.path.isdir(dn):
			Util.MkDirs(dn)

		self.fo_lock = threading.Lock()
		self.fo = open("%s/%s" % (dn, job_id), "a")

	def P(self, msg, target = "log"):
		with self.fo_lock:
			if target == "log":
				self.fo.write("%s\n" % msg)
			elif target == "both":
				self.fo.write("%s\n" % msg)
				Cons.P(msg)
			else:
				raise RuntimeError("Unexpected target: %s" % target)

	def Pnnl(self, msg, target = "log"):
		with self.fo_lock:
			if target == "log":
				self.fo.write("%s" % msg)
			elif target == "both":
				self.fo.write("%s" % msg)
				Cons.Pnnl(msg)
			else:
				raise RuntimeError("Unexpected target: %s" % target)


class ReqAndTag():
	def __init__(self, log, region, spot_req_params, az, ami_name, tags, num_regions, jr_sqs_url, jr_sqs_msg_receipt_handle):
		self.log = log
		self.region = region
		self.az = az

		self.ami_name = ami_name

		self.inst_type = spot_req_params["inst_type"]
		self.max_price = spot_req_params["max_price"]

		self.tags = tags
		self.num_regions = num_regions

		self.jr_sqs_url = jr_sqs_url
		self.jr_sqs_msg_receipt_handle = jr_sqs_msg_receipt_handle

		self.inst_id = None

	def Run(self):
		try:
			self._ReqSpotInst()
			self._KeepCheckingSpotReq()
			self._KeepCheckingInst()
		except Exception as e:
			self.log.P("%s\nregion=%s\n%s" % (e, self.region, traceback.format_exc()), target="both")
			os._exit(1)

	def _ReqSpotInst(self):
		# This is run as root
		#
		# http://unix.stackexchange.com/questions/4342/how-do-i-get-sudo-u-user-to-use-the-users-env
		user_data = \
"""#!/bin/bash
cd /home/ubuntu/work
rm -rf /home/ubuntu/work/ec2-tools
sudo -i -u ubuntu bash -c 'git clone https://github.com/hobinyoon/ec2-tools.git /home/ubuntu/work/ec2-tools'
sudo -i -u ubuntu /home/ubuntu/work/ec2-tools/ec2-init.py {0} {1} {2} {3}
"""
		user_data = user_data.format(self.tags["init_script"], self.jr_sqs_url, self.jr_sqs_msg_receipt_handle, self.num_regions)

		ls = {'ImageId': Ec2Region.GetLatestAmiId(region = self.region, name = self.ami_name)
				#, 'KeyName': 'string'
				, 'SecurityGroups': ["cass-server"]
				, 'UserData': base64.b64encode(user_data)
				#, 'AddressingType': 'string'
				, 'InstanceType': self.inst_type
				, 'EbsOptimized': True
				, 'Placement': {'AvailabilityZone': self.az}
				}

		while True:
			try:
				r = BotoClient.Get(self.region).request_spot_instances(
						SpotPrice=str(self.max_price),
						#ClientToken='string',
						InstanceCount=1,
						Type='one-time',
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
				self.spot_req_id = r["SpotInstanceRequests"][0]["SpotInstanceRequestId"]
				self.log.P("region=%s spot_req_id=%s" % (self.region, self.spot_req_id))
				break
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "RequestLimitExceeded":
					self.log.P("region=%s error=%s" % (self.region, e))
					time.sleep(5)
				else:
					raise e

	def _KeepCheckingSpotReq(self):
		r = None
		while True:
			while True:
				try:
					r = BotoClient.Get(self.region).describe_spot_instance_requests(
							SpotInstanceRequestIds=[self.spot_req_id])
					break
				except botocore.exceptions.ClientError as e:
					if e.response["Error"]["Code"] == "InvalidSpotInstanceRequestID.NotFound":
						self.log.P("region=%s error=%s" % (self.region, e))
						time.sleep(1)
					else:
						raise e

			if len(r["SpotInstanceRequests"]) != 1:
				raise RuntimeError("len(r[\"SpotInstanceRequests\"])=%d" % len(r["SpotInstanceRequests"]))
			#self.log.P(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))

			status = r["SpotInstanceRequests"][0]["Status"]["Code"]
			self.log.P("region=%s status=%s" % (self.region, status))
			if status == "fulfilled":
				break
			time.sleep(2)

		# Get inst_id
		#self.log.P(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))
		self.inst_id = r["SpotInstanceRequests"][0]["InstanceId"]
		self.log.P("region=%s inst_id=%s" % (self.region, self.inst_id))

	def _KeepCheckingInst(self):
		state = None
		tagged = False

		while True:
			r = None
			while True:
				try:
					r = BotoClient.Get(self.region).describe_instances(InstanceIds=[self.inst_id])
					# Note: describe_instances() returns StateReason, while
					# describe_instance_status() doesn't.
					break
				except botocore.exceptions.ClientError as e:
					if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
						self.log.P("region=%s error=%s" % (self.region, e))
						time.sleep(1)
					else:
						raise e

			state = r["Reservations"][0]["Instances"][0]["State"]["Name"]
			self.log.P("region=%s state=%s" % (self.region, state))

			# Create tags
			if state == "pending" and tagged == False:
				tags_boto = []
				for k, v in self.tags.iteritems():
					tags_boto.append({"Key": k, "Value": v})

				while True:
					try:
						BotoClient.Get(self.region).create_tags(Resources = [self.inst_id], Tags = tags_boto)
						tagged = True
						break
					except botocore.exceptions.ClientError as e:
						if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
							self.log.P("region=%s error=%s" % (self.region, e))
							time.sleep(1)
						elif e.response["Error"]["Code"] == "RequestLimitExceeded":
							self.log.P("region=%s error=%s" % (self.region, e))
							time.sleep(5)
						else:
							raise e

			elif state in ["terminated", "running"]:
				break
			time.sleep(2)

		# Make sure everything is ok.
		if state == "running":
			r = BotoClient.Get(self.region).describe_instances(InstanceIds=[self.inst_id])
			state = r["Reservations"][0]["Instances"][0]["State"]["Name"]
			self.log.P("region=%s state=%s" % (self.region, state))

			# Make region-ipaddr files. Helpful with a single cluster. With multiple
			# clusters, the mapping gets mixed up.
			fn = "%s/%s" % (_dn_run, self.region)
			with open(fn, "w") as fo:
				fo.write(r["Reservations"][0]["Instances"][0]["PublicIpAddress"])


# Example of attaching an EBS
#def _RunEc2InstR3XlargeEbs():
#	response = boto_client.run_instances(
#			DryRun = False
#			, ImageId = "ami-1fc7d575"
#			, MinCount=1
#			, MaxCount=1
#			, SecurityGroups=["cass-server"]
#			, EbsOptimized=True
#			, InstanceType="r3.xlarge"
#			, BlockDeviceMappings=[
#				{
#					'DeviceName': '/dev/sdc',
#					'Ebs': {
#						'VolumeSize': 16384,
#						'DeleteOnTermination': True,
#						'VolumeType': 'gp2',
#						'Encrypted': False
#						},
#					},
#				],
#			)
#
#			# What's the defalt value, when not specified? Might be True. I see the
#			# Basic CloudWatch monitoring on the web console.
#			# Monitoring={
#			#     'Enabled': True|False
#			# },
#			#
#			# "stop" when not specified.
#			#   InstanceInitiatedShutdownBehavior='stop'|'terminate',
#	Cons.P("Response:")
#	Cons.P(Util.Indent(pprint.pformat(response, indent=2, width=100), 2))
#	if len(response["Instances"]) != 1:
#		raise RuntimeError("len(response[\"Instances\"])=%d" % len(response["Instances"]))
#	inst_id = response["Instances"][0]["InstanceId"]
#	return inst_id
