#!/usr/bin/env python

import base64
import boto3
import botocore
import datetime
import errno
import imp
import os
import pprint
import sys
import threading
import time
import traceback
import yaml
import zipfile

sys.path.insert(0, "%s/../lib/util" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s/../lib" % os.path.dirname(__file__))
import GetIPs


def _Log(msg):
	Cons.P("%s: %s" % (time.strftime("%y%m%d-%H%M%S"), msg))


_az = None
_region = None

def _SetHostname():
	# Hostname consists of availability zone name and launch req datetime
	hn = "%s-%s" % (_az, _tags["job_id"])

	# http://askubuntu.com/questions/9540/how-do-i-change-the-computer-name
	Util.RunSubp("sudo sh -c 'echo \"%s\" > /etc/hostname'" % hn)
	Util.RunSubp("sudo sed -i '/^127.0.0.1 localhost.*/c\\127.0.0.1 localhost %s' /etc/hosts" % hn)
	Util.RunSubp("sudo service hostname restart")


def _SyncTime():
	# Sync time. Important for Cassandra.
	# http://askubuntu.com/questions/254826/how-to-force-a-clock-update-using-ntp
	_Log("Synching time ...")
	Util.RunSubp("sudo service ntp stop")

	# Fails with a rc 1 in the init script. Mask with true for now.
	Util.RunSubp("sudo /usr/sbin/ntpd -gq || true")

	Util.RunSubp("sudo service ntp start")


def _InstallPkgs():
	Util.RunSubp("sudo apt-get update && sudo apt-get install -y pssh dstat")


def _MountAndFormatLocalSSDs():
	# Make sure we are using the known machine types
	inst_type = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-type", print_cmd = False, print_output = False)

	ssds = []
	devs = []

	# All c3 types has 2 SSDs
	if inst_type.startswith("c3."):
		ssds = ["ssd0", "ssd1"]
		devs = ["xvdb", "xvdc"]
	elif inst_type in ["r3.large", "r3.xlarge", "r3.2xlarge", "r3.4xlarge"
			, "i2.xlarge"]:
		ssds = ["ssd0"]
		devs = ["xvdb"]
	else:
		raise RuntimeError("Unexpected instance type %s" % inst_type)

	Util.RunSubp("sudo umount /mnt || true")
	for i in range(len(ssds)):
		_Log("Setting up Local %s ..." % ssds[i])
		Util.RunSubp("sudo umount /dev/%s || true" % devs[i])
		Util.RunSubp("sudo mkdir -p /mnt/local-%s" % ssds[i])

		# Instance store volumes come TRIMmed when they are allocated. Without
		# nodiscard, it takes about 80 secs for a 800GB SSD.
		Util.RunSubp("sudo mkfs.ext4 -m 0 -E nodiscard -L local-%s /dev/%s" % (ssds[i], devs[i]))

		# I suspect /etc/fstab is updated when the instance is initiated. Give it a
		# bit of time and umount
		time.sleep(1)
		Util.RunSubp("sudo umount /dev/%s || true" % devs[i])

		# -o discard for TRIM
		Util.RunSubp("sudo mount -t ext4 -o discard /dev/%s /mnt/local-%s" % (devs[i], ssds[i]))
		Util.RunSubp("sudo chown -R ubuntu /mnt/local-%s" % ssds[i])


def _CloneSrcAndBuild():
	# Make parent
	Util.RunSubp("mkdir -p /mnt/local-ssd0/work/mutants")

	# Git clone
	Util.RunSubp("rm -rf /mnt/local-ssd0/work/mutants/cassandra")
	Util.RunSubp("git clone https://github.com/hobinyoon/mutants-cassandra /mnt/local-ssd0/work/mutants/cassandra")

	# Symlink
	Util.RunSubp("rm -rf /home/ubuntu/work/mutants/cassandra")
	Util.RunSubp("ln -s /mnt/local-ssd0/work/mutants-cassandra /home/ubuntu/work/mutants/cassandra")

	# Build
	#   Note: workaround for unmappable character for encoding ASCII.
	#   http://stackoverflow.com/questions/26067350/unmappable-character-for-encoding-ascii-but-my-files-are-in-utf-8
	Util.RunSubp("cd /home/ubuntu/work/mutants/cassandra && (JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8 ant)")


def _EditCassConf():
	_Log("Getting IP addrs of all running instances of tags %s ..." % _tags)
	ips = GetIPs.GetByTags(_tags)
	_Log(ips)

	fn_cass_yaml = "/home/ubuntu/work/mutants/cassandra/conf/cassandra.yaml"
	_Log("Editing %s ..." % fn_cass_yaml)

	# Update cassandra cluster name if specified.
	if "cass_cluster_name" in _tags:
		# http://stackoverflow.com/questions/7517632/how-do-i-escape-double-and-single-quotes-in-sed-bash
		Util.RunSubp("sed -i 's/^cluster_name: .*/cluster_name: '\"'\"'%s'\"'\"'/g' %s"
				% (_tags["cass_cluster_name"], fn_cass_yaml))

	Util.RunSubp("sed -i 's/" \
			"^          - seeds: .*" \
			"/          - seeds: \"%s\"" \
			"/g' %s" % (",".join(ips), fn_cass_yaml))

	Util.RunSubp("sed -i 's/" \
			"^listen_address: localhost" \
			"/#listen_address: localhost" \
			"/g' %s" % fn_cass_yaml)

	Util.RunSubp("sed -i 's/" \
			"^# listen_interface: eth0" \
			"/listen_interface: eth0" \
			"/g' %s" % fn_cass_yaml)

	# sed doesn't support "?"
	#   http://stackoverflow.com/questions/4348166/using-with-sed
	Util.RunSubp("sed -i 's/" \
			"^\(# \|\)broadcast_address: .*" \
			"/broadcast_address: %s" \
			"/g' %s" % (GetIPs.GetMyPubIp(), fn_cass_yaml))

	Util.RunSubp("sed -i 's/" \
			"^rpc_address: localhost" \
			"/#rpc_address: localhost" \
			"/g' %s" % fn_cass_yaml)

	Util.RunSubp("sed -i 's/" \
			"^# rpc_interface: eth1" \
			"/rpc_interface: eth0" \
			"/g' %s" % fn_cass_yaml)

	Util.RunSubp("sed -i 's/" \
			"^\(# \|\)broadcast_rpc_address: .*" \
			"/broadcast_rpc_address: %s" \
			"/g' %s" % (GetIPs.GetMyPubIp(), fn_cass_yaml))

	Util.RunSubp("sed -i 's/" \
			"^endpoint_snitch:.*" \
			"/endpoint_snitch: Ec2MultiRegionSnitch" \
			"/g' %s" % fn_cass_yaml)

	# Edit parameters requested from tags
	for k, v in _tags.iteritems():
		if k.startswith("mutants_options."):
			#              0123456789012345
			k1 = k[16:]
			Util.RunSubp("sed -i 's/" \
					"^    %s:.*" \
					"/    %s: %s" \
					"/g' %s" % (k1, k1, v, fn_cass_yaml))


# Note: This will be the YCSB configuration file
#_fn_acorn_youtube_yaml = "/home/ubuntu/work/acorn/acorn/clients/youtube/acorn-youtube.yaml"
#
#def _EditYoutubeClientConf():
#	_Log("Editing %s ..." % _fn_acorn_youtube_yaml)
#	for k, v in _tags.iteritems():
#		if k.startswith("acorn-youtube."):
#			#              01234567890123
#			k1 = k[14:]
#			Util.RunSubp("sed -i 's/" \
#					"^%s:.*" \
#					"/%s: %s" \
#					"/g' %s" % (k1, k1, v, _fn_acorn_youtube_yaml))


def _RunCass():
	_Log("Running Cassandra ...")
	Util.RunSubp("rm -rf ~/work/mutants/cassandra/data")
	Util.RunSubp("/home/ubuntu/work/mutants/cassandra/bin/cassandra")


def _WaitUntilYouSeeAllCassNodes():
	_Log("Wait until all Cassandra nodes are up ...")
	# Keep checking until you see all nodes are up -- "UN" status.
	while True:
		# Get all IPs with the tags. Hope every node sees all other nodes by this
		# time.
		num_nodes = Util.RunSubp("/home/ubuntu/work/mutants/cassandra/bin/nodetool status | grep \"^UN \" | wc -l", shell = True)
		num_nodes = int(num_nodes)

		# The number of regions (_num_regions) needs to be explicitly passed. When
		# a data center goes over capacity, it doesn't even get to the point where
		# a node is tagged, making the cluster think it has less nodes.
		if num_nodes == _num_regions:
			break
		time.sleep(2)


# Note: Some of these will be needed for batch experiments
_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None
_num_regions = None
_tags = {}
_job_id = None

def main(argv):
	try:
		# This script is run under the user 'ubuntu'.

		if len(argv) != 5:
			raise RuntimeError("Unexpected argv %s" % argv)

		global _jr_sqs_url, _jr_sqs_msg_receipt_handle, _num_regions
		_jr_sqs_url = argv[1]
		_jr_sqs_msg_receipt_handle = argv[2]
		_num_regions = int(argv[3])
		tags_str = argv[4]

		global _tags
		for t in tags_str.split(","):
			t1 = t.split(":")
			if len(t1) != 2:
				raise RuntimeError("Unexpected format %s" % t1)
			_tags[t1[0]] = t1[1]
		_Log("tags:\n%s" % "\n".join(["  %s:%s" % (k, v) for (k, v) in sorted(_tags.items())]))

		global _job_id
		_job_id = _tags["job_id"]

		global _az, _region
		_az = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_output = False)
		_region = _az[:-1]

		_SetHostname()
		_SyncTime()
		#_InstallPkgs()
		_MountAndFormatLocalSSDs()
		#_CloneSrcAndBuild()
		#_EditCassConf()

		# Note: No experiment data needed for Mutants
		#_EditYoutubeClientConf()
		#_UnzipExpDataToLocalSsd()

		# Note: Not needed for now
		#_RunCass()
		#_WaitUntilYouSeeAllCassNodes()

		# The node is not terminated by the job controller. When done with the
		# development, it needds to be terminated manually.
	except Exception as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
