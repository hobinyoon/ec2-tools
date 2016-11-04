#!/usr/bin/env python

import boto3
import os
import sys

sys.path.insert(0, "%s/work/mutant/ec2-tools/lib/util" % os.path.expanduser("~"))
import Cons
import Util

sys.path.insert(0, "%s/work/mutant/ec2-tools/lib" % os.path.expanduser("~"))
import Ec2Util


def main(argv):
	# $ tree -d /mnt/local-ssd0/mutant/log
	# .
	# `-- 161031-214118
	#     :-- c
	#     :   :-- dstat
	#     :   `-- ycsb
	#     `-- s0 (copied from s0)
	#         :-- cassandra
	#         `-- dstat

	# For now, assume only one IP. Update when there are multiple IPs.
	if len(ServerIPs()) != 1:
		raise RuntimeError("Unexpected")
	s0_ip = ServerIPs()[0]

	# dstat logs are already in the log directory of the server
	cmd = "rsync -avP -e 'ssh -o \"StrictHostKeyChecking no\" -o \"UserKnownHostsFile /dev/null\"'" \
			" ubuntu@%s:/mnt/local-ssd0/mutant/log/%s/* /mnt/local-ssd0/mutant/log/%s/" \
			% (s0_ip, Ec2Util.JobId(), Ec2Util.JobId())
	Util.RunSubp(cmd, measure_time=True)

	# Get Cassandra logs too
	dn = "/mnt/local-ssd0/mutant/log/%s/s0/cassandra" % Ec2Util.JobId()
	Util.MkDirs(dn)
	cmd = "rsync -avP -e 'ssh -o \"StrictHostKeyChecking no\" -o \"UserKnownHostsFile /dev/null\"'" \
			" ubuntu@%s:work/mutant/cassandra/logs/* %s/" \
			% (s0_ip, dn)
	Util.RunSubp(cmd, measure_time=True)

	ZipAndUploadToS3()


_s3_region  = "us-east-1"
_s3_bucket_name = "mutant-log"

def ZipAndUploadToS3():
	fn = "/mnt/local-ssd0/mutant/log/%s.tar.7z" % Ec2Util.JobId()
	# Zip the directory if the zipped file does not already exists
	if not os.path.isfile(fn):
		cmd = "cd /mnt/local-ssd0/mutant/log; tar cvf - %s | 7z a -mx -si %s.tar.7z" \
				% (Ec2Util.JobId(), Ec2Util.JobId())
		Util.RunSubp(cmd, measure_time=True)

	# Upload to S3
	with Cons.MT("Uploading data to S3 ..."):
		s3 = boto3.resource("s3", region_name = _s3_region)
		# If you don't specify a region, the bucket will be created in US Standard.
		#  http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.create_bucket
		r = s3.create_bucket(Bucket=_s3_bucket_name)
		#Cons.P(pprint.pformat(r))
		r = s3.Object(_s3_bucket_name, "%s.tar.7z" % Ec2Util.JobId()).put(Body=open(fn, "rb"))
		#Cons.P(pprint.pformat(r))


_server_ips = None
def ServerIPs():
	global _server_ips
	if _server_ips is not None:
		return _server_ips

	fn = "%s/work/mutant/.run/cassandra-server-ips" % os.path.expanduser("~")
	with open(fn) as fo:
		_server_ips = []
		for line in fo.readlines():
			_server_ips.append(line.strip())
	return _server_ips


if __name__ == "__main__":
	sys.exit(main(sys.argv))
