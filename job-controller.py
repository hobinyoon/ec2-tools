#!/usr/bin/env python

import os
import Queue
import sys
import traceback

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
sys.path.insert(0, "%s/lib/util" % os.path.dirname(__file__))

# Implement later when needed
#import ClusterCleaner

import JobCompletion
import JobContOutput
import JobMonitor
import JobReq


def main(argv):
	try:
		# The first JobContOutput.P() triggers JobMonitor.Restart(), which starts
		# updating the job monitor status when no-one has output in the last 10
		# secs  If someone has output something during the time inteval, it
		# restarts again.
		JobContOutput.P("Starting ...")

		PollMsgs()
	except KeyboardInterrupt as e:
		JobContOutput.P("Got a keyboard interrupt. Stopping ...")
	except Exception as e:
		JobContOutput.P("Got an exception: %s\n%s" % (e, traceback.format_exc()))

	JobMonitor.Stop()

	# Deleting the job request queue is useful for preventing the job request
	# reappearing. You can temporarily disable this for dev, but should be very
	# careful. You can easily spend $1000 a night.
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
		# Had to poll on each of the queues with a timeout, since python doesn't
		# seem to have an interruptable get.
		# http://stackoverflow.com/questions/212797/keyboard-interruptable-blocking-queue-in-python
		#
		# The polling must be why this uses quite a bit of CPU (5% on my macbook
		# air).
		while True:
			try:
				msg = _q_general_msg.get(timeout=0.1)
				break
			except Queue.Empty:
				pass

			#try:
			#	msg = JobCleaner.Queue().get(timeout=0.1)
			#	break
			#except Queue.Empty:
			#	pass

			try:
				msg = _q_jc.get(timeout=0.1)
				break
			except Queue.Empty:
				pass

			try:
				msg = _q_jr.get(timeout=0.1)
				# If the msg is not being able to be processed, it can be put back to
				# SQS by changing the visibility timeout to 0.
				break
			except Queue.Empty:
				pass

		if isinstance(msg, str) or isinstance(msg, unicode):
			JobContOutput.P("Got a message: %s" % msg)
		#elif isinstance(msg, ClusterCleaner.Msg):
		#	ProcessClusterCleanReq(msg)
		elif isinstance(msg, JobReq.Msg):
			JobReq.Process(msg, _q_general_msg)
		elif isinstance(msg, JobCompletion.Msg):
			JobCompletion.Process(msg)
		else:
			raise RuntimeError("Unexpected type %s" % type(msg))


def ProcessClusterCleanReq(req):
	job_id = req.job_id
	JobContOutput.P("Got a cluster clean request. job_id:%s" % job_id)
	JobCompletion.TermCluster(job_id)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
