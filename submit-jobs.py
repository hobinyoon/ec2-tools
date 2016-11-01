#!/usr/bin/env python

import boto3
import botocore
import json
import os
import pprint
import sys
import types

sys.path.insert(0, "%s/lib/util" % os.path.dirname(__file__))
import Cons

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
import Ec2Region
import JobReq


def main(argv):
	job_list = []
	for k, v in globals().iteritems():
		if type(v) != types.FunctionType:
			continue
		if k.startswith("Job_"):
			job_list.append(k[4:])
	#Cons.P(job_list)

	if len(argv) != 2:
		Cons.P("Usage: %s job_name" % argv[0])
		Cons.P("  Jobs available: %s" % " ".join(job_list))
		sys.exit(1)

	job = "Job_" + argv[1]

	# http://stackoverflow.com/questions/3061/calling-a-function-of-a-module-from-a-string-with-the-functions-name-in-python
	globals()[job]()


# Get the queue. Create one if not exists.
_sqs = None
_sqs_q = None
def _GetQ():
	global _sqs
	if _sqs is None:
		_sqs = boto3.resource("sqs", region_name = JobReq.sqs_region)

	global _sqs_q
	if _sqs_q is None:
		_sqs_q = _sqs.get_queue_by_name(
				QueueName = JobReq.sqs_q_name,
				# QueueOwnerAWSAccountId='string'
				)
		#Cons.P(pprint.pformat(vars(_sqs_q), indent=2))
		#{ '_url': 'https://queue.amazonaws.com/998754746880/mutant-exps',
		#		  'meta': ResourceMeta('sqs', identifiers=[u'url'])}
	return _sqs_q


def Job_MutantDevS1C1():
	# Workload A: Update heavy workload
	ycsb_workload_a = {
			"workload_type": "a"
			, "params" : "-p recordcount=1000" \
					" -p operationcount=1" \
					" -p status.interval=1" \
					" -p fieldcount=10" \
					" -p fieldlength=100"
					# TODO: add threads and target later
					}

	# Workload D: Read latest workload
	ycsb_workload_d = {
			"workload_type": "d"
			, "params" : "-p recordcount=1000" \
					" -p operationcount=400000000" \
					" -p status.interval=1" \
					" -p fieldcount=10" \
					" -p fieldlength=100" \
					" -threads 100" \
					" -target 17000"
					}

	# Workload E: Short ranges
	ycsb_workload_e = {
			"workload_type": "e"
			, "params" : "-p recordcount=1000" \
					" -p operationcount=400000000" \
					" -p status.interval=1" \
					" -p fieldcount=10" \
					" -p fieldlength=100" \
					" -threads 100" \
					# Just taking a guess. It scans "zipian(100) with 0.99" records at
					# once. Not sure tell how many it is on average. Let's say 3.  Then
					# roughly 17000 / 3 = 5400, ignoring the write overhead.
					" -target 5000"
					}

	_EnqReq(
			{
				# The server and the client are in the same AZ.
				"region": "us-east-1"

				# Client uses the same instance type as the server, cause it generates
				# all requests for a cluster of servers.
				#
				#            vCPU ECU Memory (GiB) Instance Storage (GB) Linux/UNIX Usage
				# c3.2xlarge    8  28           15            2 x 80 SSD  $0.42  per Hour.
				# r3.xlarge     4  13         30.5            1 x 80 SSD  $0.333 per Hour
				#
				# r3 types don't need local SSD initialization. However, even after
				# init, it's still slower(86 MB/s) than c3 (372 MB/s).
				#
				# c3.2xlarge is a better fit for Cassandra with the YCSB "read latest"
				# workload. The workload is quite computation heavy.
				#
				# When "spot_req" is not specified, the job controller launches
				# on-demand instances.
				#
				#, "spot_req": {"max_price": 2.0}
				, "inst_type": "c3.2xlarge"

				, "server": {
					# Note: We'll see if the AMIs need to be separated by DBs. May want
					# to upgrade to 16.04.
					"init_script": "mutant-cassandra-server-dev"
					, "ami_name": "mutant-cassandra-server"
					, "num_nodes": "1"

					# The storage cost is higher than the VM spot price, which is around
					# $0.1/hour. You will want to be careful.
					#                             $/GB-month     $/hour
					#                                 size_in_GB
					#   EBS gp2                        0.100  80 0.0110
					#   EBS st1 (throughput optimized) 0.045 500 0.0308
					#   EBS sc1 (cold HDD)             0.025 500 0.0171
					#
					#   calc "0.100 *  80 / (365.25/12*24)"
					#   calc "0.045 * 500 / (365.25/12*24)"
					#   calc "0.025 * 500 / (365.25/12*24)"
					#
					# For st1 and sc1, 500 GB is the minimum size.
					, "block_storage_devs": [
						{"VolumeType": "gp2", "VolumeSize": 80, "DeviceName": "d"}
						#, {"VolumeType": "st1", "VolumeSize": 500, "DeviceName": "e"}
						#, {"VolumeType": "sc1", "VolumeSize": 500, "DeviceName": "f"}
						]

					# Pre-populate DB using the 50GB one stored in S3.
					, "pre_populate_db": "true"

					# TODO: may want to group by cassandra or rocks_db

					, "rocks_db": {
						}
					}

				, "client" : {
					"init_script": "mutant-cassandra-client-dev"
					, "ami_name": "mutant-client"
					, "ycsb": ycsb_workload_a
					, "terminate_cluster_when_done": "false"
					}
				}
			)


def Job_MutantDevS1():
	_EnqReq(
			{"region": "us-east-1"
				# Client uses the same instance type as the server, cause it generates
				# all requests for a cluster of servers.
				, "spot_req": {"inst_type": "c3.2xlarge", "max_price": 2.0}
				#, "spot_req": {"inst_type": "r3.xlarge", "max_price": 2.0}

				, "server": {
					# We'll see if the AMIs need to be separated by DBs.
					"init_script": "mutant-cassandra-server-dev"
					, "ami_name": "mutant-cassandra-server"
					, "num_nodes": "1"
					}
				}
			)


def Job_Castnet():
	_EnqReq(
			{"region": "us-east-1"
				# Client uses the same instance type as the server, cause it generates
				# all requests for a cluster of servers.
				#, "spot_req": {"inst_type": "c3.2xlarge", "max_price": 2.0}
				#            vCPU ECU Memory (GiB) Instance Storage (GB) Linux/UNIX Usage
				# c3.2xlarge    8  28           15            2 x 80 SSD   $0.42 per Hour

				, "spot_req": {"inst_type": "c3.8xlarge", "max_price": 8.0}
				#            vCPU ECU Memory (GiB) Instance Storage (GB) Linux/UNIX Usage
				# c3.2xlarge    8  28           15            2 x 80 SSD   $0.42 per Hour

				# The client needs to be in the same AZ.
				, "client" : {
					"init_script": "castnet-dev"
					# TODO: modify after moving some data
					, "ami_name": "castnet"
					}
				}
			)


def _EnqReq(attrs):
	with Cons.MT("Enq a request: "):
		# Need to make a copy so that an SQS message can be sent while attrs is being
		# modified.
		attrs = attrs.copy()
		Cons.P(pprint.pformat(attrs))

		# A Mutant job req has too many attributes - well over 10. Pack then with a
		# json format in the message body, not in the attributes.
		#
		# "You can attach up to ten attributes to each of your messages. The entire
		# message, including the body plus all names, types, and values, can be as
		# large as 256 KB (262,144 bytes)."
		# - https://aws.amazon.com/blogs/aws/simple-queue-service-message-attributes
		#
		# Seems like the msg body doesn't need to be based64 encoded.
		_GetQ().send_message(MessageBody=json.dumps(attrs))


if __name__ == "__main__":
	sys.exit(main(sys.argv))
