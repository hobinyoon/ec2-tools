#!/usr/bin/env python

import datetime
import os
import pprint
import sys
import time
import traceback

sys.path.insert(0, "%s/../lib/util" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s" % os.path.dirname(__file__))
import Ec2InitUtil


# This is run under the user 'ubuntu'.
def main(argv):
	try:
		if len(argv) != 3:
			raise RuntimeError("Unexpected argv %s" % argv)

		Ec2InitUtil.SetParams(argv[1])
		Ec2InitUtil.SetEc2Tags(argv[2])

		SetHostname()
		Ec2InitUtil.SyncTime()
		PrepareBlockDevs()
		Ec2InitUtil.ChangeLogOutput()
		CloneSrcAndBuild()

		# This is a dev node, which is not terminated automatically.
	except Exception as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		Cons.P(msg)


def SetHostname():
	with Cons.MT("Setting host name ..."):
		# Hostname consists of availability zone name and launch req datetime
		hn = "%s-%s-%s" % (Ec2InitUtil.GetAz(), Ec2InitUtil.GetJobId()
				, Ec2InitUtil.GetEc2Tag("name").replace("server", "s").replace("client", "c"))

		# http://askubuntu.com/questions/9540/how-do-i-change-the-computer-name
		Util.RunSubp("sudo sh -c 'echo \"%s\" > /etc/hostname'" % hn)
		# "c" command in sed is used to replace every line matches with the pattern
		# or ranges with the new given line.
		# - http://www.thegeekstuff.com/2009/11/unix-sed-tutorial-append-insert-replace-and-count-file-lines/?ref=driverlayer.com
		Util.RunSubp("sudo sed -i '/^127.0.0.1 localhost.*/c\\127.0.0.1 localhost %s' /etc/hosts" % hn)

		# "sudo service hostname restart" on Ubuntu 16.04
		#   Failed to restart hostname.service: Unit hostname.service is masked.
		#   http://forums.debian.net/viewtopic.php?f=5&t=126007
		Util.RunSubp("sudo rm /lib/systemd/system/hostname.service || true")
		Util.RunSubp("sudo systemctl unmask hostname.service")
		Util.RunSubp("sudo service hostname restart")


#def InstallPkgs():
#	with Cons.MT("Installing packages ..."):
#		Util.RunSubp("sudo apt-get update && sudo apt-get install -y pssh dstat")


def PrepareBlockDevs():
	with Cons.MT("Preparing block storage devices ..."):
		# Make sure we are using the known machine types
		inst_type = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-type", print_cmd = False, print_output = False)

		# {dev_name: directory_name}
		# ext4 label is the same as the directory_name
		blk_devs = {"xvdb": "local-ssd0"}
		# All c3 types have 2 SSDs
		if inst_type.startswith("c3."):
			# Not needed for now
			#blk_devs["xvdc"] = "local-ssd1"
			pass
		elif inst_type in ["r3.large", "r3.xlarge", "r3.2xlarge", "r3.4xlarge"
				, "i2.xlarge"]:
			pass
		else:
			raise RuntimeError("Unexpected instance type %s" % inst_type)
		if os.path.exists("/dev/xvdd"):
			blk_devs["xvdd"] = "ebs-gp2"
		if os.path.exists("/dev/xvde"):
			blk_devs["xvde"] = "ebs-st1"
		if os.path.exists("/dev/xvdf"):
			blk_devs["xvdf"] = "ebs-sc1"

		# Init local SSDs
		# - https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/disk-performance.html
		# - Skip for Castnet, in which local SSD speed doesn't matter.

		Util.RunSubp("sudo umount /mnt || true")
		for dev_name, dir_name in blk_devs.iteritems():
			Cons.P("Setting up %s ..." % dev_name)
			Util.RunSubp("sudo umount /dev/%s || true" % dev_name)
			Util.RunSubp("sudo mkdir -p /mnt/%s" % dir_name)

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
			Util.RunSubp("sudo mkfs.ext4 -m 0 -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 -L %s /dev/%s"
					% (dir_name, dev_name), measure_time=True)

			# Some are already mounted. I suspect /etc/fstab does the magic when the
			# file system is created. Give it some time and umount
			time.sleep(1)
			Util.RunSubp("sudo umount /dev/%s || true" % dev_name)

			# -o discard for TRIM
			Util.RunSubp("sudo mount -t ext4 -o discard /dev/%s /mnt/%s" % (dev_name, dir_name))
			Util.RunSubp("sudo chown -R ubuntu /mnt/%s" % dir_name)


def CloneSrcAndBuild():
	with Cons.MT("Cloning src and build ..."):
		# Git clone
		Util.RunSubp("rm -rf /mnt/local-ssd0/castnet")
		Util.RunSubp("git clone https://github.com/hobinyoon/castnet.git /mnt/local-ssd0/castnet")

		# Edit the git source repository for easy development.
		Util.RunSubp("sed -i 's/" \
				"^\\turl = https:\\/\\/github.com\\/hobinyoon\\/castnet.git" \
				"/\\turl = git@github.com:hobinyoon\/castnet.git" \
				"/g' %s" % "/mnt/local-ssd0/castnet/.git/config")

		# Symlink
		Util.RunSubp("rm -rf /home/ubuntu/work/castnet")
		Util.RunSubp("ln -s /mnt/local-ssd0/castnet /home/ubuntu/work/castnet")

		# Build to save time
		Util.RunSubp("cd /home/ubuntu/work/castnet/simulator && ./build-and-run.sh 2>&1")


if __name__ == "__main__":
	sys.exit(main(sys.argv))
