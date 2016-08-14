#!/usr/bin/env python

import os
import Queue
import sys
import traceback

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
sys.path.insert(0, "%s/lib/util" % os.path.dirname(__file__))

import ClusterCleaner
import ClusterMonitor
import JobCompletion
import JobControllerLog
import JobReq


# TODO: Why does this uses quite a lot of CPU (5% on my macbook air) while
# waiting?


def main(argv):
	try:
		JobControllerLog.P("Starting ...")
		PollMsgs()
	except KeyboardInterrupt as e:
		JobControllerLog.P("\nGot a keyboard interrupt. Stopping ...")
	except Exception as e:
		JobControllerLog.P("\nGot an exception: %s\n%s" % (e, traceback.format_exc()))
	# Deleting the job request queue is useful for preventing the job request
	# reappearing

	# You can temporarily disable this for dev, but needs to be very careful. You
	# can easily spend $1000 a night.
	JobReq.DeleteQ()


# Not sure if a Queue is necessary when the maxsize is 1. Leave it for now.
_q_jr = Queue.Queue(maxsize=1)
_q_jc = Queue.Queue(maxsize=1)

# General message queue
_q_general_msg = Queue.Queue(maxsize=10)

def PollMsgs():
	JobReq.PollBackground(_q_jr)
	JobCompletion.PollBackground(_q_jc)

	while True:
		with ClusterMonitor.CM():
			# Blocked waiting until a request is available
			#
			# Interruptable get
			#   http://stackoverflow.com/questions/212797/keyboard-interruptable-blocking-queue-in-python
			while True:
				try:
					msg = _q_general_msg.get(timeout=0.01)
					break
				except Queue.Empty:
					pass

				try:
					msg = ClusterCleaner.Queue().get(timeout=0.01)
					break
				except Queue.Empty:
					pass

				try:
					msg = _q_jc.get(timeout=0.01)
					break
				except Queue.Empty:
					pass

				try:
					if ClusterMonitor.CanLaunchAnotherCluster():
						# TODO: I don't think the queue is needed here. Fetch one directly from the SQS queue.
						# Something like this
						#msg = JobReq.Get(timeout=0.01)
						msg = _q_jr.get(timeout=0.01)
						break
				except Queue.Empty:
					pass

		if isinstance(msg, str) or isinstance(msg, unicode):
			JobControllerLog.P("\nGot a message: %s" % msg)
		elif isinstance(msg, ClusterCleaner.Msg):
			ProcessClusterCleanReq(msg)
		elif isinstance(msg, JobReq.Msg):
			JobReq.Process(msg, _q_general_msg)
		elif isinstance(msg, JobCompletion.Msg):
			JobCompletion.Process(msg)
		else:
			raise RuntimeError("Unexpected type %s" % type(msg))


def ProcessClusterCleanReq(req):
	job_id = req.job_id
	JobControllerLog.P("\nGot a cluster clean request. job_id:%s" % job_id)
	JobCompletion.TermCluster(job_id)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
