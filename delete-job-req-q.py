#!/usr/bin/env python

import boto3
import botocore
import os
import pprint
import sys

sys.path.insert(0, "%s/lib/util" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
import JobReq


sqs_region = "us-east-1"

_bc = None
_sqs = None

def main(argv):
	global _bc, _sqs
	_bc = boto3.client("sqs", region_name = sqs_region)
	_sqs = boto3.resource("sqs", region_name = sqs_region)

	q = GetQ()
	if q is None:
		Cons.P("The queue doesn't exists")
	else:
		DeleteQ(q)


def GetQ():
	with Cons.MT("Getting the queue ..."):
		try:
			queue = _sqs.get_queue_by_name(
					QueueName = JobReq.sqs_q_name,
					# QueueOwnerAWSAccountId='string'
					)
			#Cons.P(pprint.pformat(vars(queue), indent=2))
			#{ '_url': 'https://queue.amazonaws.com/998754746880/mutants-exps',
			#		  'meta': ResourceMeta('sqs', identifiers=[u'url'])}
			return queue
		except botocore.exceptions.ClientError as e:
			if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
				return None
			else:
				raise e


def DeleteQ(q):
	with Cons.MT("Deleting queue ..."):
		response = _bc.delete_queue(QueueUrl = q._url)
		Cons.P(pprint.pformat(response, indent=2))


if __name__ == "__main__":
	sys.exit(main(sys.argv))
