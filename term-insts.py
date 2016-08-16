#!/usr/bin/env python

import boto3
import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/lib/util" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
import BotoClient
import Ec2Region

_fmt = "%-15s %19s %13s %13s"
_regions_processed = 0
_regions_processed_lock = threading.Lock()


def RunTermInst(tags):
	threads = []

	sys.stdout.write("Terminating running instances:")
	sys.stdout.flush()

	tis = []
	for r in Ec2Region.All():
		tis.append(TermInst(r, tags))

	for ti in tis:
		t = threading.Thread(target=ti.Run)
		t.daemon = True
		threads.append(t)
		t.start()

	for t in threads:
		t.join()
	print ""

	Cons.P(Util.BuildHeader(_fmt,
		"Region"
		" InstanceId"
		" PrevState"
		" CurrState"
		))

	for ti in tis:
		ti.PrintResult()


class TermInst:
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

		self.inst_ids_to_term = []

		for r in response["Reservations"]:
			for r1 in r["Instances"]:
				if "Name" in r1["State"]:
					# Terminate only running intances
					if r1["State"]["Name"] == "running":
						if _job_id_none_requested:
							#Cons.P(pprint.pformat(r1))
							if "Tags" not in r1:
								self.inst_ids_to_term.append(r1["InstanceId"])
						else:
							self.inst_ids_to_term.append(r1["InstanceId"])

		#Cons.P("There are %d instances to terminate." % len(self.inst_ids_to_term))
		#Cons.P(pprint.pformat(self.inst_ids_to_term, indent=2, width=100))

		if len(self.inst_ids_to_term) > 0:
			self.term_inst_response = BotoClient.Get(self.region).terminate_instances(InstanceIds = self.inst_ids_to_term)

		with _regions_processed_lock:
			global _regions_processed
			_regions_processed += 1
			if _regions_processed == 1:
				pass
			elif _regions_processed % 6 == 1:
				#                        Terminating running instances:
				Cons.sys_stdout_write("\n                              ")
			Cons.sys_stdout_write(" %s" % self.region)

	def PrintResult(self):
		if len(self.inst_ids_to_term) == 0:
			return

		#Cons.P(pprint.pformat(self.term_inst_response, indent=2, width=100))
		for ti in self.term_inst_response["TerminatingInstances"]:
			Cons.P(_fmt % (
				self.region
				, ti["InstanceId"]
				, ti["PreviousState"]["Name"]
				, ti["CurrentState"]["Name"]
				))


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""


_job_id_none_requested = False

def main(argv):
	if len(argv) < 2:
		print "Usage: %s (all or tags in key:value pairs)" % argv[0]
		sys.exit(1)

	tags = None
	if argv[1] == "all":
		pass
	elif argv[1] == "job_id:None":
		global _job_id_none_requested
		_job_id_none_requested = True
	else:
		tags = {}
		for i in range(1, len(argv)):
			t = argv[i].split(":")
			if len(t) != 2:
				raise RuntimeError("Unexpected. argv[%d]=[%s]" % (i, argv[i]))
			tags[t[0]] = t[1]

	RunTermInst(tags)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
