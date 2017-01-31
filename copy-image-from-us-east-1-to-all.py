#!/usr/bin/env python

import os
import sys

sys.path.insert(0, "%s/lib/util" % os.path.dirname(__file__))
import Util

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
import Ec2Region


def main(argv):
	if len(argv) != 3:
		raise RuntimeError("Usage: %s ami-id-in-us-east-1 name\n" \
				"  E.g.: %s ami-01d12c17 mutant-server-170129-1600" \
				% (argv[0], argv[0]))

	region_ami = {"us-east-1": argv[1]}

	regions = Ec2Region.All()
	regions.remove("us-east-1")
	for r in regions:
		cmd = "aws ec2 copy-image" \
				" --source-image-id %s" \
				" --source-region us-east-1" \
				" --region %s" \
				" --name %s" \
				% (argv[1], r, argv[2])
		out = Util.RunSubp(cmd)
		found_ami_id = False
		for line in out.split("\n"):
			if "\"ImageId\": \"ami-" in line:
				t = line.split("\"ImageId\": \"")
				if len(t) != 2:
					raise RuntimeError("Unexpected line=[%s]" % line)
				# ami-a46623c4"
				# 012345678901
				region_ami[r] = t[1][0:11+1]
				found_ami_id = True
				break
		if not found_ami_id:
			raise RuntimeError("Unexpected output=[%s]" % out)

	print "{\n%s\n}" % ("\n, ".join(["\"%s\": \"%s\"" % (k, v) for (k, v) in sorted(region_ami.items())]))


if __name__ == "__main__":
	sys.exit(main(sys.argv))
