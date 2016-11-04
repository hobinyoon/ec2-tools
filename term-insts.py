#!/usr/bin/env python

import os
import sys

sys.path.insert(0, "%s/lib/util" % os.path.dirname(__file__))
import Cons

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
import TermInst


def main(argv):
	if len(argv) < 2:
		print "Usage: %s (all or tags in key:value pairs)" % argv[0]
		sys.exit(1)

	tags = None
	job_id_none_requested = False
	if argv[1] == "all":
		pass
	elif argv[1] == "job_id:None":
		job_id_none_requested = True
	else:
		tags = {}
		for i in range(1, len(argv)):
			t = argv[i].split(":")
			if len(t) != 2:
				raise RuntimeError("Unexpected. argv[%d]=[%s]" % (i, argv[i]))
			tags[t[0]] = t[1]

	TermInst.ByTags(tags, job_id_none_requested)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
