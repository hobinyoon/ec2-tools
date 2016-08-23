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

	# sudo service hostname restart
	#   Failed to restart hostname.service: Unit hostname.service is masked.
	#   http://forums.debian.net/viewtopic.php?f=5&t=126007
	Util.RunSubp("sudo rm /lib/systemd/system/hostname.service || true")
	Util.RunSubp("sudo systemctl unmask hostname.service")
	Util.RunSubp("sudo service hostname restart")


def _SyncTime():
	# Sync time. Important for Cassandra.
	# http://askubuntu.com/questions/254826/how-to-force-a-clock-update-using-ntp
	_Log("Synching time ...")
	Util.RunSubp("sudo service ntp stop || true")

	# Fails with a rc 1 in the init script. Mask with true for now.
	Util.RunSubp("sudo /usr/sbin/ntpd -gq || true")

	Util.RunSubp("sudo service ntp start")


def _InstallPkgs():
	Util.RunSubp("sudo apt-get update && sudo apt-get install -y pssh dstat")


def _MountAndFormatLocalSSDs():
	# Make sure we are using the known machine types
	inst_type = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-type", print_cmd = False, print_output = False)

	# TODO: use a dict.
	# TODO: attach other EBS devices too.
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


def _CloneSrc():
	# Git clone
	Util.RunSubp("rm -rf /mnt/local-ssd0/castnet")
	Util.RunSubp("git clone https://github.com/hobinyoon/castnet.git /mnt/local-ssd0/castnet")

	# Symlink
	Util.RunSubp("rm -rf /home/ubuntu/work/castnet")
	Util.RunSubp("ln -s /mnt/local-ssd0/castnet /home/ubuntu/work/castnet")


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
		#_StartSystemLogging()
		_CloneSrc()

		# Dev nodes are not terminated automatically.
	except Exception as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
