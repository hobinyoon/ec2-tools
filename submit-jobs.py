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
		#{ '_url': 'https://queue.amazonaws.com/998754746880/mutants-exps',
		#		  'meta': ResourceMeta('sqs', identifiers=[u'url'])}
	return _sqs_q


def Job_MutantsDevS1C1():
	_EnqReq(
			{"region": "us-east-1"
				# Client uses the same instance type as the server, cause it generates
				# all requests for a cluster of servers.
				, "spot_req": {"inst_type": "c3.2xlarge", "max_price": 1.0}
				#            vCPU ECU Memory (GiB) Instance Storage (GB) Linux/UNIX Usage
				# c3.2xlarge    8  28           15            2 x 80 SSD   $0.42 per Hour

				, "server": {
					# We'll see if the AMIs need to be separated by DBs.
					"init_script": "mutants-cassandra-server-dev"
					# TODO
					, "ami_name": "mutants-cassandra"
					, "num_nodes": "1"
					}

				# The client needs to be in the same AZ.
				, "client" : {
					"init_script": "mutants-cassandra-client-dev"
					, "ami_name": "mutants-cassandra"
					}
				}
			)


def _EnqReq(attrs):
	with Cons.MT("Enq a request: "):
		# Need to make a copy so that an SQS message can be sent while attrs is being
		# modified.
		attrs = attrs.copy()
		Cons.P(pprint.pformat(attrs))

		# A Mutants job req has too many attributes - well over 10. Pack then with
		# a json format in the message body, not in the attributes.
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
