import datetime
import os
import Queue
import sys

sys.path.insert(0, "%s/util" % os.path.dirname(__file__))
import Cons


WAIT_TIME_BEFORE_CLEAN_UNDER11 = 6 * 60
WAIT_TIME_BEFORE_CLEAN_11 = 55 * 60

# First time we've seen a cluster with under 11 nodes and with 11 nodes
#   { job_id: datetime }
_jobid_first_time_under11 = {}
_jobid_first_time_11 = {}

# Cluster cleaner job queue
_q = Queue.Queue(maxsize=100)


def Queue():
	return _q


def Clean(jobid_inst):
	# jobid_inst: { job_id: {region: Inst} }

	for job_id, v in jobid_inst.iteritems():
		# Only clean acorn-server nodes. Dev nodes are not cleaned automatically.
		is_acorn_server = False
		for region, i in v.iteritems():
			if "init_script" in i.tags:
				if i.tags["init_script"] == "acorn-server":
					is_acorn_server = True
					break
		if not is_acorn_server:
			continue

		# Count only "running" instances.
		running_insts = []
		for region, i in v.iteritems():
			if i.state == "running":
				running_insts.append(i)
		if len(running_insts) == 0:
			continue

		if len(running_insts) < 11:
			if job_id not in _jobid_first_time_under11:
				_jobid_first_time_under11[job_id] = datetime.datetime.now()
				continue
			diff = (datetime.datetime.now() - _jobid_first_time_under11[job_id]).total_seconds()
			if diff > WAIT_TIME_BEFORE_CLEAN_UNDER11:
				Cons.P("Cluster (job_id %s, %d \"running\" nodes) has been there for %d seconds." \
						" Termination requested." % (job_id, len(running_insts), diff))
				_q.put(Msg(job_id), block=False)
				# Reset to give the job-controller some time to clean up the cluster
				_jobid_first_time_under11.pop(job_id, None)

		elif len(running_insts) == 11:
			_jobid_first_time_under11.pop(job_id, None)

			# Even with 11 nodes, the cluster can be in a state it doesn't make any
			# progress
			if job_id not in _jobid_first_time_11:
				_jobid_first_time_11[job_id] = datetime.datetime.now()
				continue
			diff = (datetime.datetime.now() - _jobid_first_time_11[job_id]).total_seconds()
			if diff > WAIT_TIME_BEFORE_CLEAN_11:
				Cons.P("Cluster (job_id %s, %d \"running\" nodes) has been there for %d seconds." \
						" Termination requested." % (job_id, len(running_insts), diff))
				_q.put(Msg(job_id), block=False)
				_jobid_first_time_11.pop(job_id, None)

		else:
			raise RuntimeError("Unexpected len(running_insts): %d" % len(running_insts))


class Msg():
	def __init__(self, job_id):
		self.job_id = job_id
