#!/usr/bin/env python

import datetime
import imp
import os
import pprint
import re
import sys
import time
import traceback
import zipfile

sys.path.insert(0, "%s/../lib/util" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s/../lib" % os.path.dirname(__file__))
import BotoClient
import TermInst

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
		WaitForServers()
		RunYcsb()

		GetLogsFromServersAndUpoadToS3()

		MayTerminateCluster()
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
		# - Client node probably won't need this -- YCSB won't get bottlenecked by
		#   the local SSD --, but it needs to wait for the server, it might as well
		#   do something.
		if inst_type.startswith("c3."):
			Util.RunSubp("sudo umount /dev/xvdb || true")
			Util.RunSubp("sudo umount /dev/xvdc || true")
			Util.RunSubp("sudo dd if=/dev/zero bs=1M of=/dev/xvdb || true", measure_time=True)

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
		# Make parent
		Util.RunSubp("mkdir -p /mnt/local-ssd0/mutant")

		_CloneAndBuildCassandra()
		_CloneMisc()
		_CloneAndBuildYcsb()


def _CloneAndBuildCassandra():
	# Git clone
	Util.RunSubp("rm -rf /mnt/local-ssd0/mutant/cassandra")
	Util.RunSubp("git clone https://github.com/hobinyoon/mutant-cassandra-3.9 /mnt/local-ssd0/mutant/cassandra")

	# Symlink
	Util.RunSubp("rm -rf /home/ubuntu/work/mutant/cassandra")
	Util.RunSubp("ln -s /mnt/local-ssd0/mutant/cassandra /home/ubuntu/work/mutant/cassandra")

	# Build. For cassandra-cli
	Util.RunSubp("cd /home/ubuntu/work/mutant/cassandra && ant")

	# Edit the git source repository for easy development.
	Util.RunSubp("sed -i 's/" \
			"^\\turl = https:\\/\\/github.com\\/hobinyoon\\/mutant-cassandra-3.9" \
			"/\\turl = git@github.com:hobinyoon\/mutant-cassandra-3.9.git" \
			"/g' %s" % "~/work/mutant/cassandra/.git/config")


def _CloneMisc():
	# Git clone
	Util.RunSubp("rm -rf /mnt/local-ssd0/mutant/misc")
	Util.RunSubp("git clone https://github.com/hobinyoon/mutant-misc /mnt/local-ssd0/mutant/misc")

	# Symlink
	Util.RunSubp("rm -rf /home/ubuntu/work/mutant/misc")
	Util.RunSubp("ln -s /mnt/local-ssd0/mutant/misc /home/ubuntu/work/mutant/misc")

	# Edit the git source repository for easy development.
	Util.RunSubp("sed -i 's/" \
			"^\\turl = https:\\/\\/github.com\\/hobinyoon\\/mutant-misc" \
			"/\\turl = git@github.com:hobinyoon\/mutant-misc.git" \
			"/g' %s" % "~/work/mutant/misc/.git/config")


def _CloneAndBuildYcsb():
	# Git clone
	Util.RunSubp("rm -rf /mnt/local-ssd0/mutant/YCSB")
	Util.RunSubp("git clone https://github.com/hobinyoon/YCSB /mnt/local-ssd0/mutant/YCSB")

	# Symlink
	Util.RunSubp("rm -rf /home/ubuntu/work/mutant/YCSB")
	Util.RunSubp("ln -s /mnt/local-ssd0/mutant/YCSB /home/ubuntu/work/mutant/YCSB")

	# Build
	Util.RunSubp("cd /home/ubuntu/work/mutant/YCSB && mvn -pl com.yahoo.ycsb:cassandra-binding -am clean package -DskipTests >/dev/null 2>&1")

	# Edit the git source repository for easy development.
	Util.RunSubp("sed -i 's/" \
			"^\\turl = https:\\/\\/github.com\\/hobinyoon\\/YCSB" \
			"/\\turl = git@github.com:hobinyoon\/YCSB.git" \
			"/g' %s" % "~/work/mutant/YCSB/.git/config")


def WaitForServers():
	_WaitForServerNodes()
	_WaitForCassServers()

_nm_ip = None
def _WaitForServerNodes():
	# Wait for all the server nodes to be up
	server_num_nodes_expected = int(Ec2InitUtil.GetParam(["server", "num_nodes"]))
	with Cons.MTnnl("Waiting for %d server node(s) with job_id %s"
			% (server_num_nodes_expected, Ec2InitUtil.GetJobId())):
		global _nm_ip
		_nm_ip = None
		while True:
			_nm_ip = {}
			r = BotoClient.Get(Ec2InitUtil.GetRegion()).describe_instances(
					Filters=[ { "Name": "tag:job_id", "Values": [ Ec2InitUtil.GetJobId() ] }, ],
					)
			for r0 in r["Reservations"]:
				for i in r0["Instances"]:
					#Cons.P(pprint.pformat(i))
					pub_ip = i["PublicIpAddress"]
					for t in i["Tags"]:
						if t["Key"] == "name":
							name = t["Value"]
							if name.startswith("s"):
								_nm_ip[name] = pub_ip
			#Cons.P(pprint.pformat(_nm_ip))
			if len(_nm_ip) == server_num_nodes_expected:
				break

			# Log progress. By now, the log file is in the local EBS.
			sys.stdout.write(".")
			sys.stdout.flush()
			time.sleep(1)
		sys.stdout.write(" all up.\n")

	Util.RunSubp("mkdir -p /mnt/local-ssd0/mutant/.run", )
	Util.RunSubp("rm /home/ubuntu/work/mutant/.run || true")
	Util.RunSubp("ln -s /mnt/local-ssd0/mutant/.run /home/ubuntu/work/mutant/.run")
	fn = "/home/ubuntu/work/mutant/.run/cassandra-server-ips"
	with open(fn, "w") as fo:
		fo.write(" ".join(v for k, v in _nm_ip.iteritems()))
	Cons.P("Created %s %d" % (fn, os.path.getsize(fn)))
	# Note: Will need to to round-robin the server nodes when there are multiple of them.


def _WaitForCassServers():
	with Cons.MTnnl("Wating for %d Cassandra server(s) " % len(_nm_ip)):
		# Query the first server node in the dict
		server_ip = _nm_ip.itervalues().next()

		# Can you just use nodetool? Yes, but needs additional authentication step
		# to prevent anyone from checking on that. cqlsh is already open to
		# everyone.  Better keep the number of open services minimal.
		# - http://stackoverflow.com/questions/15299302/cassandra-nodetool-connection-timed-out

		while True:
			cqlsh = "%s/work/mutant/cassandra/bin/cqlsh" % os.path.expanduser("~")
			lines = Util.RunSubp("%s -e \"select count(*) from system.peers\" %s || true" \
					% (cqlsh, server_ip), print_cmd=False, print_output=False)
			if lines.startswith("Connection error:"):
				time.sleep(1)
				sys.stdout.write(".")
				sys.stdout.flush()
				continue

			#   count
			#  -------
			#       0
			#  (1 rows)
			#  Warnings :
			#  Aggregation query used without partition key
			m = re.match(r"\n\s+count\n-+\n\s*(?P<count>\d+)\n.*", lines)
			if m is None:
				raise RuntimeError("Unexpected [%s]" % lines)
			if len(_nm_ip) == (1 + int(m.group("count"))):
				break

			time.sleep(1)
			sys.stdout.write(".")
			sys.stdout.flush()
			continue

		sys.stdout.write("all up\n")


def RunYcsb():
	with Cons.MT("Running YCSB ..."):
		cmd = "%s/work/mutant/YCSB/mutant/cassandra/restart-dstat-run-workload.py %s %s" \
				% (os.path.expanduser("~")
						, Ec2InitUtil.GetParam(["client", "ycsb", "workload_type"])
						, Ec2InitUtil.GetParam(["client", "ycsb", "params"]))
		Util.RunSubp(cmd)


def GetLogsFromServersAndUpoadToS3():
	fn_module = "%s/rsync-server-logs-to-client-upload-to-S3.py" % os.path.dirname(__file__)

	mod_name,file_ext = os.path.splitext(os.path.split(fn_module)[-1])
	if file_ext.lower() != '.py':
		raise RuntimeError("Unexpected file_ext: %s" % file_ext)
	try:
		py_mod = imp.load_source(mod_name, fn_module)
	except IOError as e:
		_Log("fn_module: %s" % fn_module)
		raise e
	getattr(py_mod, "main")([fn_module])


def MayTerminateCluster():
	if "terminate_cluster_when_done" not in Ec2InitUtil.GetParam("client"):
		return
	if not bool(Ec2InitUtil.GetParam(["client", "terminate_cluster_when_done"])):
		return

	# Terminate other nodes first and terminate self
	TermInst.ByJobIdTermSelfLast()


if __name__ == "__main__":
	sys.exit(main(sys.argv))
