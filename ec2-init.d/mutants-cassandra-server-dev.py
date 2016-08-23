#!/usr/bin/env python

import base64
import boto3
import botocore
import datetime
import errno
import imp
import json
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
	hn = "%s-%s-%s" % (_az, _job_id, _tags["name"].replace("server", "s").replace("client", "c"))

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

		# Prevent lazy Initialization
		# - "When creating an Ext4 file system, the existing regions of the inode
		#   tables must be cleaned (overwritten with nulls, or "zeroed"). The
		#   "lazyinit" feature should significantly accelerate the creation of a
		#   file system, because it does not immediately initialize all inode
		#   tables, initializing them gradually instead during the initial mounting
		#   process in background (from Kernel version 2.6.37)."
		#   - https://www.thomas-krenn.com/en/wiki/Ext4_Filesystem
		# - Default values are 1s, which do lazy init.
		#   - man mkfs.ext4
		#
		# nodiscard is in the documentation
		# - https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ssd-instance-store.html
		# - Without nodiscard, it takes about 80 secs for a 800GB SSD.

		Util.RunSubp("sudo mkfs.ext4 -m 0 -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 -L local-%s /dev/%s"
				% (ssds[i], devs[i]))

		# Some are already mounted. I suspect /etc/fstab does the magic when the
		# file system is created. Give it some time and umount
		time.sleep(1)
		Util.RunSubp("sudo umount /dev/%s || true" % devs[i])

		# -o discard for TRIM
		Util.RunSubp("sudo mount -t ext4 -o discard /dev/%s /mnt/local-%s" % (devs[i], ssds[i]))
		Util.RunSubp("sudo chown -R ubuntu /mnt/local-%s" % ssds[i])


# Local SSD structure:
# - ssd0 for database server, ycsb
# - ssd1 for system or experiment logs
#
# ~
# `-- work
#     `-- mutants
#         |-- ec2-tools
#         |-- ycsb      (symlink to /mnt/local-ssd0/mutants/ycsb)
#         |-- cassandra (symlink to /mnt/local-ssd0/mutants/cassandra)
#         `-- log       (symlink to /mnt/local-ssd1/mutants/log)
#             `-- system
#
# Cassandra data and log goes under its own directory.

def _StartSystemLogging():
	Util.RunSubp("mkdir -p /mnt/local-ssd1/mutants/log/system")
	Util.RunSubp("rm /home/ubuntu/work/mutants/log || true")
	Util.RunSubp("ln -s /mnt/local-ssd1/mutants/log /home/ubuntu/work/mutants/log")

	# dstat parameters
	#   -d, --disk
	#     enable disk stats (read, write)
	#   -r, --io
	#     enable I/O request stats (read, write requests)
	#   -t, --time
	#     enable time/date output
	#   -tdrf
	Util.RunDaemon("cd /home/ubuntu/work/mutants/log && dstat -tdrf --output dstat-`date +\"%y%m%d-%H%M%S\"`.csv >/dev/null 2>&1")


# How do you know the average IOPS of a disk from the system boot? dtat shows
# it only once in the beginning.
# - cat /sys/block/xvda/stat
#   - It has the number of read IOs and write IOs processed
#   - https://www.kernel.org/doc/Documentation/block/stat.txt
#
# File system IOs can be inflated or deflated (e.g., from read caching or write
# buffering) when translated to block device IOs.
#
# IOPS vs TPS (transactions per second)? T is a single IO command written to
# the raw disk. IOPS includes the requests absorbed by caches. At which
# level? Probably at the block device level.
#   dstat --disk-tps
#     per disk transactions per second (tps) stats
#   http://serverfault.com/questions/558523/relation-between-disk-iops-and-sar-tps


def _CloneSrcAndBuild():
	# Make parent
	Util.RunSubp("mkdir -p /mnt/local-ssd0/mutants")

	# Git clone
	Util.RunSubp("rm -rf /mnt/local-ssd0/mutants/cassandra")
	Util.RunSubp("git clone https://github.com/hobinyoon/cassandra-3.9 /mnt/local-ssd0/mutants/cassandra")

	# Symlink
	Util.RunSubp("rm -rf /home/ubuntu/work/mutants/cassandra")
	Util.RunSubp("ln -s /mnt/local-ssd0/mutants/cassandra /home/ubuntu/work/mutants/cassandra")

	# Build
	Util.RunSubp("cd /home/ubuntu/work/mutants/cassandra && ant")


def _EditCassConf():
	_Log("Getting IP addrs of all running instances of servers with job_id %s ..." % _job_id)
	ips = GetIPs.GetServerPubIpsByJobId(_job_id)
	_Log(ips)

	fn_cass_yaml = "/home/ubuntu/work/mutants/cassandra/conf/cassandra.yaml"
	_Log("Editing %s ..." % fn_cass_yaml)

	# Update cassandra cluster name if specified. No need to.
	#if "cass_cluster_name" in _tags:
	#	# http://stackoverflow.com/questions/7517632/how-do-i-escape-double-and-single-quotes-in-sed-bash
	#	Util.RunSubp("sed -i 's/^cluster_name: .*/cluster_name: '\"'\"'%s'\"'\"'/g' %s"
	#			% (_tags["cass_cluster_name"], fn_cass_yaml))

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

	# No need for a single data center deployment
	#Util.RunSubp("sed -i 's/" \
	#		"^endpoint_snitch:.*" \
	#		"/endpoint_snitch: Ec2MultiRegionSnitch" \
	#		"/g' %s" % fn_cass_yaml)

	# TODO: Edit parameters requested from tags
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
#def _EditMutantsClientConf():
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


# TODO: get the number of servers from the json parameter
#
# How does a client node know that the servers are ready? It can query
# system.peers and system.local with cqlsh.
#
#def _WaitUntilYouSeeAllCassNodes():
#	_Log("Wait until all Cassandra nodes are up ...")
#	# Keep checking until you see all nodes are up -- "UN" status.
#	while True:
#		# Get all IPs with the tags. Hope every node sees all other nodes by this
#		# time.
#		num_nodes = Util.RunSubp("/home/ubuntu/work/mutants/cassandra/bin/nodetool status | grep \"^UN \" | wc -l", shell = True)
#		num_nodes = int(num_nodes)
#
#		# The number of regions (_num_regions) needs to be explicitly passed. When
#		# a data center goes over capacity, it doesn't even get to the point where
#		# a node is tagged, making the cluster think it has less nodes.
#
#		#if num_nodes == _num_regions:
#		#	break
#		time.sleep(2)


# Note: Some of these will be needed for batch experiments
#_jr_sqs_url = None
#_jr_sqs_msg_receipt_handle = None

_params = None
_tags = {}

_job_id = None


# TODO: Don't let the logs to go out to stdout, unless it's an exception. It
# goes to cloud-init-output.log, which eats up EBS gp2 volume IO credit.

def main(argv):
	try:
		# This script is run under the user 'ubuntu'.

		if len(argv) != 3:
			raise RuntimeError("Unexpected argv %s" % argv)

		params_encoded = argv[1]
		tags_json = argv[2]

		global _params
		_params = json.loads(base64.b64decode(params_encoded))

		global _tags
		_tags = json.loads(tags_json)
		_Log("_tags: %s" % pprint.pformat(_tags))

		global _job_id
		_job_id = _params["extra"]["job_id"]

		global _az, _region
		_az = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_output = False)
		_region = _az[:-1]

		_SetHostname()
		_SyncTime()
		#_InstallPkgs()
		_MountAndFormatLocalSSDs()
		_StartSystemLogging()
		_CloneSrcAndBuild()
		_EditCassConf()

		# TODO: _EditMutantsClientConf()
		# Note: No experiment data needed for Mutants
		#_UnzipExpDataToLocalSsd()

		_RunCass()

		# Only the client node need this. Server nodes don't need this.
		#_WaitUntilYouSeeAllCassNodes()

		# The node is not terminated by the job controller. When done with the
		# development, it needds to be terminated manually.
	except Exception as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
