#!/usr/bin/env python

import boto3
import botocore
import json
import os
import pprint
import sys

sys.path.insert(0, "%s/lib/util" % os.path.dirname(__file__))
import Cons

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
import Ec2Region
import JobReq


def main(argv):
	if len(argv) != 2:
		Cons.P("Usage: %s job_name" % argv[0])
		Cons.P("  E.g.: %s MutantsDevSingleServer" % argv[0])
		sys.exit(1)

	job = argv[1]

	bc = boto3.client("sqs", region_name = JobReq.sqs_region)
	sqs = boto3.resource("sqs", region_name = JobReq.sqs_region)
	q = GetQ(bc, sqs)

	# http://stackoverflow.com/questions/3061/calling-a-function-of-a-module-from-a-string-with-the-functions-name-in-python
	globals()[job](q)


# Get the queue. Create one if not exists.
def GetQ(bc, sqs):
	with Cons.MT("Getting the queue ..."):
		queue = sqs.get_queue_by_name(
				QueueName = JobReq.sqs_q_name,
				# QueueOwnerAWSAccountId='string'
				)
		#Cons.P(pprint.pformat(vars(queue), indent=2))
		#{ '_url': 'https://queue.amazonaws.com/998754746880/mutants-exps',
		#		  'meta': ResourceMeta('sqs', identifiers=[u'url'])}
		return queue


def MutantsDevSingleServer(q):
	req_attrs = {
			"init_script": "mutants-server-dev"
			, "ami_name": "mutants-server"
			, "region_spot_req": {
				"us-east-1": {"inst_type": "c3.2xlarge", "max_price": 1.0}
				#            vCPU ECU Memory (GiB) Instance Storage (GB) Linux/UNIX Usage
				# c3.2xlarge    8  28           15            2 x 80 SSD   $0.42 per Hour
				}
			}
	_EnqReq(q, req_attrs)


def _EnqReq(q, attrs):
	with Cons.MT("Enq a request: "):
		attrs = attrs.copy()
		Cons.P(pprint.pformat(attrs))

		jc_params = {}
		for k in attrs.keys():
			if k in ["region_spot_req", "ami_name"]:
				jc_params[k] = attrs[k]
				del attrs[k]
		#Cons.P(json.dumps(jc_params))

		msg_attrs = {}
		for k, v in attrs.iteritems():
			msg_attrs[k] = {"StringValue": v, "DataType": "String"}
		msg_attrs["job_controller_params"] = {"StringValue": json.dumps(jc_params), "DataType": "String"}

		q.send_message(MessageBody=JobReq.Msg.msg_body, MessageAttributes=msg_attrs)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
