import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/util" % os.path.dirname(__file__))
import Cons
import Util

import BotoClient
import Ec2Region
import Ec2Util


def ByTags(tags, job_id_none_requested):
	Cons.P("Terminating running instances:")
	_TermInst.Init(job_id_none_requested)

	tis = []
	for r in Ec2Region.All():
		tis.append(_TermInst(r, tags))

	threads = []
	for ti in tis:
		t = threading.Thread(target=ti.Run)
		t.daemon = True
		threads.append(t)
		t.start()

	for t in threads:
		t.join()
	print ""

	Cons.P(_TermInst.Header())
	for ti in tis:
		ti.PrintResult()


class _TermInst:
	_job_id_none_requested = False
	_regions_processed = 0
	_regions_processed_lock = threading.Lock()

	@staticmethod
	def Init(job_id_none_requested=False, term_by_job_id_self_last=False):
		_TermInst._job_id_none_requested = job_id_none_requested
		_TermInst._term_by_job_id_self_last = term_by_job_id_self_last
		_TermInst._regions_processed = 0

	def __init__(self, region, tags):
		self.region = region
		self.tags = tags

	def Run(self):
		response = None
		if self.tags is None:
			response = BotoClient.Get(self.region).describe_instances()
		else:
			filters = []
			for k, v in self.tags.iteritems():
				# job_id:None can be specified to kill all instances without job_id
				if v == "None":
					continue
				d = {}
				d["Name"] = ("tag:%s" % k)
				d["Values"] = [v]
				filters.append(d)
			response = BotoClient.Get(self.region).describe_instances(Filters = filters)
		#Cons.P(pprint.pformat(response, indent=2, width=100))

		inst_ids_to_term_self = []
		inst_ids_to_term_others = []
		self.inst_ids_to_term = []

		for r in response["Reservations"]:
			for r1 in r["Instances"]:
				if "Name" in r1["State"]:
					# Terminate only running intances
					if r1["State"]["Name"] == "running":
						inst_id = r1["InstanceId"]
						if _TermInst._job_id_none_requested:
							#Cons.P(pprint.pformat(r1))
							if "Tags" not in r1:
								if inst_id == Ec2Util.InstId():
									inst_ids_to_term_self.append(inst_id)
								else:
									inst_ids_to_term_others.append(inst_id)
								self.inst_ids_to_term.append(inst_id)
						else:
							if inst_id == Ec2Util.InstId():
								inst_ids_to_term_self.append(inst_id)
							else:
								inst_ids_to_term_others.append(inst_id)
							self.inst_ids_to_term.append(inst_id)

		#Cons.P("There are %d instances to terminate." % len(self.inst_ids_to_term))
		#Cons.P(pprint.pformat(self.inst_ids_to_term, indent=2, width=100))

		if _TermInst._term_by_job_id_self_last:
			if len(inst_ids_to_term_others) > 0:
				self.term_inst_response = BotoClient.Get(self.region).terminate_instances(InstanceIds = inst_ids_to_term_others)

			if len(inst_ids_to_term_self) > 0:
				# Wait for others to terminate
				time.sleep(5)
				self.term_inst_response = BotoClient.Get(self.region).terminate_instances(InstanceIds = inst_ids_to_term_self)
		else:
			if len(self.inst_ids_to_term) > 0:
				self.term_inst_response = BotoClient.Get(self.region).terminate_instances(InstanceIds = self.inst_ids_to_term)

		# Note: below is not even reached when you kill yourself
		with _TermInst._regions_processed_lock:
			_TermInst._regions_processed += 1
			if _TermInst._regions_processed == 1:
				pass
			elif _TermInst._regions_processed % 6 == 1:
				#                        Terminating running instances:
				Cons.sys_stdout_write("\n                              ")
			Cons.sys_stdout_write(" %s" % self.region)

	_fmt = "%-15s %19s %13s %13s"

	@staticmethod
	def Header():
		return Util.BuildHeader(_TermInst._fmt,
		"Region"
		" InstanceId"
		" PrevState"
		" CurrState"
		)

	def PrintResult(self):
		if len(self.inst_ids_to_term) == 0:
			return

		#Cons.P(pprint.pformat(self.term_inst_response, indent=2, width=100))
		for ti in self.term_inst_response["TerminatingInstances"]:
			Cons.P(_TermInst._fmt % (
				self.region
				, ti["InstanceId"]
				, ti["PreviousState"]["Name"]
				, ti["CurrentState"]["Name"]
				))

			
def ByJobIdTermSelfLast():
	job_id = Ec2Util.JobId()
	Cons.P("Terminating running instances of job_id %s" % job_id)

	_TermInst.Init(term_by_job_id_self_last=True)

	tags = {}
	tags["job_id"] = job_id

	tis = []
	for r in Ec2Region.All():
		tis.append(_TermInst(r, tags))

	threads = []
	for ti in tis:
		t = threading.Thread(target=ti.Run)
		t.daemon = True
		threads.append(t)
		t.start()

	for t in threads:
		t.join()
	print ""

	Cons.P(_TermInst.Header())
	for ti in tis:
		ti.PrintResult()
