#!/usr/bin/env python

import datetime
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
		MountAndFormatLocalSSDs()
		Ec2InitUtil.ChangeLogOutput()
		CloneSrcAndBuild()
		WaitForServerNodes()
		WaitForCassServers()
		RunYcsb()

		# TODO: UploadToS3()

		# The client node requests termination of the job with the job_id. Job
		# controller gets the requests and terminates all node with the job_id.
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
		Util.RunSubp("sudo service hostname restart")


#def InstallPkgs():
#	with Cons.MT("Installing packages ..."):
#		Util.RunSubp("sudo apt-get update && sudo apt-get install -y pssh dstat")


def MountAndFormatLocalSSDs():
	with Cons.MT("Mount and format block storage devices ..."):
		# Make sure we are using the known machine types
		inst_type = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-type", print_cmd = False, print_output = False)

		# {dev_name: directory_name}
		# ext4 label is the same as the directory_name
		blk_devs = {}

		# All c3 types has 2 SSDs
		if inst_type.startswith("c3."):
			blk_devs = {
					"xvdb": "local-ssd0"
					# Not needed for now
					#, "xvdc": "local-ssd1"
					}
		elif inst_type in ["r3.large", "r3.xlarge", "r3.2xlarge", "r3.4xlarge"
				, "i2.xlarge"]:
			blk_devs = {
					"xvdb": "local-ssd0"
					}
		else:
			raise RuntimeError("Unexpected instance type %s" % inst_type)

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


# You don't need dstat logging here. The YCSB script will restart it.
# You don't need dstat logging here. The YCSB script will restart dstat.
#def StartDstatLogging():
#	dn_log_ssd0 = "/mnt/local-ssd0/mutants/log"
#	dn_log = "/home/ubuntu/work/mutants/log"
#
#	Util.RunSubp("mkdir -p %s" % dn_log_ssd0)
#
#	# Create a symlink
#	Util.RunSubp("rm %s || true" % dn_log)
#	Util.RunSubp("ln -s %s %s" % (dn_log_ssd0, dn_log))
#
#	dn_log_dstat = "%s/%s/dstat" % (dn_log, Ec2InitUtil.GetJobId())
#	Util.RunSubp("mkdir -p %s" % dn_log_dstat)
#
#	# dstat parameters
#	#   -d, --disk
#	#     enable disk stats (read, write)
#	#   -r, --io
#	#     enable I/O request stats (read, write requests)
#	#   -t, --time
#	#     enable time/date output
#	#   -tdrf
#	Util.RunDaemon("dstat -cdn -C total -D xvda,xvdb -r --output %s/%s.csv"
#			% (dn_log_dstat, datetime.datetime.now().strftime("%y%m%d-%H%M%S")))


def CloneSrcAndBuild():
	with Cons.MT("Cloning src and build ..."):
		# Make parent
		Util.RunSubp("mkdir -p /mnt/local-ssd0/mutants")

		_CloneAndBuildCassandra()
		_CloneMisc()
		_CloneAndBuildYcsb()


def _CloneAndBuildCassandra():
	# Git clone
	Util.RunSubp("rm -rf /mnt/local-ssd0/mutants/cassandra")
	Util.RunSubp("git clone https://github.com/hobinyoon/mutants-cassandra-3.9 /mnt/local-ssd0/mutants/cassandra")

	# Symlink
	Util.RunSubp("rm -rf /home/ubuntu/work/mutants/cassandra")
	Util.RunSubp("ln -s /mnt/local-ssd0/mutants/cassandra /home/ubuntu/work/mutants/cassandra")

	# Build. For cassandra-cli
	Util.RunSubp("cd /home/ubuntu/work/mutants/cassandra && ant")

	# Edit the git source repository for easy development.
	Util.RunSubp("sed -i 's/" \
			"^\\turl = https:\\/\\/github.com\\/hobinyoon\\/mutants-cassandra-3.9" \
			"/\\turl = git@github.com:hobinyoon\/mutants-cassandra-3.9.git" \
			"/g' %s" % "~/work/mutants/cassandra/.git/config")


def _CloneMisc():
	# Git clone
	Util.RunSubp("rm -rf /mnt/local-ssd0/mutants/misc")
	Util.RunSubp("git clone https://github.com/hobinyoon/mutants-misc /mnt/local-ssd0/mutants/misc")

	# Symlink
	Util.RunSubp("rm -rf /home/ubuntu/work/mutants/misc")
	Util.RunSubp("ln -s /mnt/local-ssd0/mutants/misc /home/ubuntu/work/mutants/misc")

	# Edit the git source repository for easy development.
	Util.RunSubp("sed -i 's/" \
			"^\\turl = https:\\/\\/github.com\\/hobinyoon\\/mutants-misc" \
			"/\\turl = git@github.com:hobinyoon\/mutants-misc.git" \
			"/g' %s" % "~/work/mutants/misc/.git/config")


def _CloneAndBuildYcsb():
	# Git clone
	Util.RunSubp("rm -rf /mnt/local-ssd0/mutants/YCSB")
	Util.RunSubp("git clone https://github.com/hobinyoon/YCSB /mnt/local-ssd0/mutants/YCSB")

	# Symlink
	Util.RunSubp("rm -rf /home/ubuntu/work/mutants/YCSB")
	Util.RunSubp("ln -s /mnt/local-ssd0/mutants/YCSB /home/ubuntu/work/mutants/YCSB")

	# Build
	Util.RunSubp("cd /home/ubuntu/work/mutants/YCSB && mvn -pl com.yahoo.ycsb:cassandra-binding -am clean package -DskipTests >/dev/null 2>&1")

	# Edit the git source repository for easy development.
	Util.RunSubp("sed -i 's/" \
			"^\\turl = https:\\/\\/github.com\\/hobinyoon\\/YCSB" \
			"/\\turl = git@github.com:hobinyoon\/YCSB.git" \
			"/g' %s" % "~/work/mutants/YCSB/.git/config")


_nm_ip = None
def WaitForServerNodes():
	# Wait for all the server nodes to be up
	server_num_nodes_expected = int(Ec2InitUtil.GetParam("server")["num_nodes"])
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

	Util.RunSubp("mkdir -p /mnt/local-ssd0/mutants/.run", )
	Util.RunSubp("rm /home/ubuntu/work/mutants/.run || true")
	Util.RunSubp("ln -s /mnt/local-ssd0/mutants/.run /home/ubuntu/work/mutants/.run")
	fn = "/home/ubuntu/work/mutants/.run/cassandra-server-ips"
	with open(fn, "w") as fo:
		fo.write(" ".join(v for k, v in _nm_ip.iteritems()))
	Cons.P("Created %s %d" % (fn, os.path.getsize(fn)))
	# Note: Will need to to round-robin the server nodes when there are multiple of them.


def WaitForCassServers():
	with Cons.MTnnl("Wating for %d Cassandra server(s) " % len(_nm_ip)):
		# Query the first server node in the dict
		server_ip = _nm_ip.itervalues().next()

		# Can you just use nodetool? Yes, but needs additional authentication step
		# to prevent anyone from checking on that. cqlsh is already open to
		# everyone.  Better keep the number of open services minimal.
		# - http://stackoverflow.com/questions/15299302/cassandra-nodetool-connection-timed-out

		while True:
			lines = Util.RunSubp("cqlsh -e \"select count(*) from system.peers\" %s || true" % server_ip
					, print_cmd=False, print_output=False)
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
		cmd = "%s/work/mutants/YCSB/mutants/restart-dstat-run-workload.py \"%s\"" \
				% (os.path.expanduser("~")
						, Ec2InitUtil.GetParam("client")["ycsb"]["workload_type"]
						, Ec2InitUtil.GetParam("client")["ycsb"]["params"])
		Util.RunSubp(cmd)


def MayTerminateCluster():
	if "terminate_cluster_when_done" in Ec2InitUtil.GetParam("client"):
		if Ec2InitUtil.GetParam("client")["terminate_cluster_when_done"] == "true":
			pass
			# TODO: make a termination request

			# Note: Some of these will be needed for batch experiments
			#_jr_sqs_url = None
			#_jr_sqs_msg_receipt_handle = None


if __name__ == "__main__":
	sys.exit(main(sys.argv))
