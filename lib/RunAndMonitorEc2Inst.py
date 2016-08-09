import boto3
import botocore
import datetime
import os
import pprint
import re
import sys
import threading
import time
import traceback

sys.path.insert(0, "%s/util" % os.path.dirname(__file__))
import Cons
import Util

import BotoClient
import Ec2Region


_threads = []
_dn_run = "%s/../.run" % os.path.dirname(__file__)
_job_id = None

_inst_type = None
_tags = None
_num_regions = None
_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None
_init_script = None


# TODO: region_inst_type. match ReqSpotAndMonitor
def Run(regions, inst_type, tags, jr_sqs_url, jr_sqs_msg_receipt_handle, init_script):
	Reset()

	Util.RunSubp("mkdir -p %s" % _dn_run, print_cmd = False)

	req_datetime = datetime.datetime.now()
	global _job_id
	_job_id = req_datetime.strftime("%y%m%d-%H%M%S")
	Cons.P("job_id:%s (for describing and terminating the cluster)" % _job_id)

	global _inst_type, _tags, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _init_script
	_inst_type = inst_type
	_tags = tags
	_tags["job_id"] = _job_id
	_num_regions = len(regions)
	_jr_sqs_url = jr_sqs_url
	_jr_sqs_msg_receipt_handle = jr_sqs_msg_receipt_handle
	_init_script = init_script

	rams = []
	for r in regions:
		rams.append(RunAndMonitor(r))

	for ram in rams:
		t = threading.Thread(target=ram.RunEc2Inst)
		t.daemon = True
		_threads.append(t)
		t.start()

	InstLaunchProgMon.Run()

	for t in _threads:
		t.join()


# This module can be called repeatedly
def Reset():
	global _threads, _job_id
	global _inst_type, _tags, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _init_script

	_threads = []
	_job_id = None

	_inst_type = None
	_tags = None
	_num_regions = None
	_jr_sqs_url = None
	_jr_sqs_msg_receipt_handle = None
	_init_script = None

	InstLaunchProgMon.Reset()


class RunAndMonitor():
	def __init__(self, az_or_region):
		if re.match(r".*[a-z]$", az_or_region):
			self.az = az_or_region
			self.region_name = self.az[:-1]
		else:
			self.az = None
			self.region_name = az_or_region
		self.ami_id = Ec2Region.GetLatestAmiId(self.region_name)


	def RunEc2Inst(self):
		try:
			# This is run as root
			user_data = \
"""#!/bin/bash
cd /home/ubuntu/work
rm -rf /home/ubuntu/work/acorn-tools
sudo -i -u ubuntu bash -c 'git clone https://github.com/hobinyoon/acorn-tools.git /home/ubuntu/work/acorn-tools'
sudo -i -u ubuntu /home/ubuntu/work/acorn-tools/ec2/ec2-init.py {0} {1} {2} {3}
"""
			user_data = user_data.format(_init_script, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _num_regions)

			placement = {}
			if self.az != None:
				placement['AvailabilityZone'] = self.az

			response = None
			while True:
				try:
					response = BotoClient.Get(self.region_name).run_instances(
							DryRun = False
							, ImageId = self.ami_id
							, MinCount=1
							, MaxCount=1
							, SecurityGroups=["cass-server"]
							, EbsOptimized=True
							, InstanceType = _inst_type
							, Placement=placement

							# User data is passed as a string. I don't see an option of specifying a file.
							, UserData=user_data

							, InstanceInitiatedShutdownBehavior='terminate'
							)
					break
				except botocore.exceptions.ClientError as e:
					if e.response["Error"]["Code"] == "RequestLimitExceeded":
						InstLaunchProgMon.Update(self.inst_id, e)
						# TODO
						Cons.P("%s. Retrying in 5 sec ..." % e)
						time.sleep(5)
					else:
						raise e

			#Cons.P("Response:")
			#Cons.P(Util.Indent(pprint.pformat(response, indent=2, width=100), 2))

			if len(response["Instances"]) != 1:
				raise RuntimeError("len(response[\"Instances\"])=%d" % len(response["Instances"]))
			self.inst_id = response["Instances"][0]["InstanceId"]
			#Cons.P("region=%s inst_id=%s" % (self.region_name, self.inst_id))
			InstLaunchProgMon.SetRegion(self.inst_id, self.region_name)

			self._KeepCheckingInst()
		except Exception as e:
			Cons.P("%s\nRegion=%s\n%s" % (e, self.region_name, traceback.format_exc()))
			os._exit(1)


	def _KeepCheckingInst(self):
		state = None
		tagged = False

		while True:
			r = None
			while True:
				try:
					r = BotoClient.Get(self.region_name).describe_instances(InstanceIds=[self.inst_id])
					# Note: describe_instances() returns StateReason, while
					# describe_instance_status() doesn't.
					break
				except botocore.exceptions.ClientError as e:
					# describe_instances() right after run_instances() fails sometimes.
					# Keep retrying.
					#   An error occurred (InvalidInstanceID.NotFound) when calling the
					#   DescribeInstances operation: The instance ID 'i-dbb11a47' does n ot
					#   exist
					if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
						InstLaunchProgMon.Update(self.inst_id, e)
						# TODO
						Cons.P("inst_id %s not found. retrying in 1 sec ..." % self.inst_id)
						time.sleep(1)
					else:
						raise e

			InstLaunchProgMon.Update(self.inst_id, r)
			state = r["Reservations"][0]["Instances"][0]["State"]["Name"]
			# Create tags
			if state == "pending" and tagged == False:
				tags_boto = []
				for k, v in _tags.iteritems():
					tags_boto.append({"Key": k, "Value": v})
					#Cons.P("[%s]=[%s]" %(k, v))

				while True:
					try:
						BotoClient.Get(self.region_name).create_tags(Resources = [self.inst_id], Tags = tags_boto)
						tagged = True
					except botocore.exceptions.ClientError as e:
						if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
							InstLaunchProgMon.Update(self.inst_id, e)
							# TODO
							Cons.P("inst_id %s not found. retrying in 1 sec ..." % self.inst_id)
							time.sleep(1)
						elif e.response["Error"]["Code"] == "RequestLimitExceeded":
							InstLaunchProgMon.Update(self.inst_id, e)
							Cons.P("%s. Retrying in 5 sec ..." % e)
							time.sleep(5)
						else:
							raise e

			elif state == "terminated" or state == "running":
				break
			time.sleep(1)

		# Make sure everything is ok.
		if state == "running":
			r = BotoClient.Get(self.region_name).describe_instances(InstanceIds=[self.inst_id])
			state = r["Reservations"][0]["Instances"][0]["State"]["Name"]
			InstLaunchProgMon.Update(self.inst_id, r)

			# Make region-ipaddr files
			fn = "%s/%s" % (_dn_run, self.region_name)
			with open(fn, "w") as fo:
				fo.write(r["Reservations"][0]["Instances"][0]["PublicIpAddress"])


class InstLaunchProgMon():
	progress = {}
	progress_lock = threading.Lock()

	class Entry():
		def __init__(self, region):
			self.region = region
			self.responses = []

		def AddResponse(self, response):
			self.responses.append(response)

	@staticmethod
	def Reset():
		with InstLaunchProgMon.progress_lock:
			InstLaunchProgMon.progress = {}

	@staticmethod
	def SetRegion(inst_id, region_name):
		with InstLaunchProgMon.progress_lock:
			InstLaunchProgMon.progress[inst_id] = InstLaunchProgMon.Entry(region_name)

	@staticmethod
	def Update(inst_id, response):
		with InstLaunchProgMon.progress_lock:
			InstLaunchProgMon.progress[inst_id].AddResponse(response)

	@staticmethod
	def Run():
		output_lines_written = 0
		while True:
			output = ""
			for k, v in InstLaunchProgMon.progress.iteritems():
				if len(output) > 0:
					output += "\n"
				inst_id = k
				output += ("%-15s %s" % (v.region, inst_id))
				prev_state = None
				same_state_cnt = 0
				for r in v.responses:
					state = r["Reservations"][0]["Instances"][0]["State"]["Name"]
					if state == "shutting-down":
						state_reason = response["Reservations"][0]["Instances"][0]["StateReason"]["Message"]
						state = "%s:%s" % (state, state_reason)

					if prev_state == None:
						output += (" %s" % state)
					elif prev_state != state:
						if same_state_cnt > 0:
							output += (" x%2d %s" % ((same_state_cnt + 1), state))
						else:
							output += (" %s" % state)
						same_state_cnt = 0
					else:
						same_state_cnt += 1
					prev_state = state

				if same_state_cnt > 0:
					output += (" x%2d" % (same_state_cnt + 1))

			# Clear prev output
			if output_lines_written > 0:
				for l in range(output_lines_written - 1):
					# Clear current line
					sys.stdout.write(chr(27) + "[2K")
					# Move up
					sys.stdout.write(chr(27) + "[1F")
				# Clear current line
				sys.stdout.write(chr(27) + "[2K")
				# Move the cursor to column 1
				sys.stdout.write(chr(27) + "[1G")

			#sys.stdout.write(output)
			# Sort them
			sys.stdout.write("\n".join(sorted(output.split("\n"))))
			sys.stdout.flush()
			output_lines_written = len(output.split("\n"))

			# Are we done?
			all_done = True
			for t in _threads:
				if t.is_alive():
					all_done = False
					break
			if all_done:
				break

			# Update status every so often
			time.sleep(0.1)
		print ""

		InstLaunchProgMon.DescInsts()

	@staticmethod
	def DescInsts():
		fmt = "%-15s %19s %10s %13s %15s %10s"
		Cons.P(Util.BuildHeader(fmt,
			"Placement:AvailabilityZone"
			" InstanceId"
			" InstanceType"
			" LaunchTime"
			#" PrivateIpAddress"
			" PublicIpAddress"
			" State:Name"
			#" Tags"
			))

		output = []
		for k, v in InstLaunchProgMon.progress.iteritems():
			if len(v.responses) == 0:
				continue
			r = v.responses[-1]["Reservations"][0]["Instances"][0]

			tags = {}
			if "Tags" in r:
				for t in r["Tags"]:
					tags[t["Key"]] = t["Value"]

			#Cons.P(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))
			output.append(fmt % (
				_Value(_Value(r, "Placement"), "AvailabilityZone")
				, _Value(r, "InstanceId")
				, _Value(r, "InstanceType")
				, _Value(r, "LaunchTime").strftime("%y%m%d-%H%M%S")
				#, _Value(r, "PrivateIpAddress")
				, _Value(r, "PublicIpAddress")
				, _Value(_Value(r, "State"), "Name")
				#, ",".join(["%s:%s" % (k, v) for (k, v) in sorted(tags.items())])
				))
		for o in sorted(output):
			Cons.P(o)


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""


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
