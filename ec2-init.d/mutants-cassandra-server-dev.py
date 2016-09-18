#!/usr/bin/env python

import datetime
import os
import pprint
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
		EditCassConf()
		RunCassandra()

		# Server nodes don't terminate by themselves. They may be terminated by the
		# request from the client.
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
					, "xvdd": "ebs-gp2"
					, "xvde": "ebs-st1"
					, "xvdf": "ebs-sc1"
					}
		elif inst_type in ["r3.large", "r3.xlarge", "r3.2xlarge", "r3.4xlarge"
				, "i2.xlarge"]:
			blk_devs = {
					"xvdb": "local-ssd0"
					, "xvdd": "ebs-gp2"
					, "xvde": "ebs-st1"
					, "xvdf": "ebs-sc1"
					}
		else:
			raise RuntimeError("Unexpected instance type %s" % inst_type)

		# Init local SSDs
		# - https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/disk-performance.html
		if inst_type.startswith("c3."):
			Util.RunSubp("sudo umount /dev/xvdb || true")
			Util.RunSubp("sudo umount /dev/xvdc || true")
			# tee has a problem of not stopping. For now, you can give up on ssd1.
			# - https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=89224
			#Util.RunSubp("sudo sh -c \"dd if=/dev/zero bs=1M | tee /dev/xvdb > /dev/xvdc\"", measure_time=True)
			#
			# sudo dd if=/dev/zero bs=1M of=/dev/xvdb || true
			#   dd: error writing '/dev/xvdb': No space left on device
			#   81910+0 records in
			#   81909+0 records out
			#   85887811584 bytes (86 GB) copied, 1394.5 s, 61.6 MB/s
			#   1394510 ms = 23 mins
			Util.RunSubp("sudo dd if=/dev/zero bs=1M of=/dev/xvdb || true", measure_time=True)

			# Test with virtual block devices
			#   $ sudo dd if=/dev/zero of=/run/dev0-backstore bs=1M count=100
			#   $ sudo dd if=/dev/zero of=/run/dev1-backstore bs=1M count=100
			#   $ grep loop /proc/devices
			#   7 loop
			#   $ sudo mknod /dev/fake-dev0 b 7 200
			#   $ sudo losetup /dev/fake-dev0  /run/dev0-backstore
			#   $ sudo mknod /dev/fake-dev1 b 7 201
			#   $ sudo losetup /dev/fake-dev1  /run/dev1-backstore
			#   $ lsblk
			#   - http://askubuntu.com/questions/546921/how-to-create-virtual-block-devices
			#   - You can use /dev/full too, which is easier.
			#Util.RunSubp("sudo umount /dev/loop200 || true")
			#Util.RunSubp("sudo umount /dev/loop201 || true")
			#
			#Util.RunSubpStopOn("sudo sh -c \"dd if=/dev/zero bs=1M | tee /dev/loop200 > /dev/loop201\"", measure_time=True)
			#Util.RunSubp("sudo dd if=/dev/zero bs=1M of=/dev/loop201 || true", measure_time=True)

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
			#
			# Could be parallelized
			#   local-ssd0   9,799 ms
			#   ebs-gp2     11,728 ms
			#   ebs-st1     68,082 ms
			#   ebs-sc1    207,481 ms
			Util.RunSubp("sudo mkfs.ext4 -m 0 -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 -L %s /dev/%s"
					% (dir_name, dev_name), measure_time=True)

			# Some are already mounted. I suspect /etc/fstab does the magic when the
			# file system is created. Give it some time and umount
			time.sleep(1)
			Util.RunSubp("sudo umount /dev/%s || true" % dev_name)

			# -o discard for TRIM
			Util.RunSubp("sudo mount -t ext4 -o discard /dev/%s /mnt/%s" % (dev_name, dir_name))
			Util.RunSubp("sudo chown -R ubuntu /mnt/%s" % dir_name)


# Local SSD structure:
# - ssd0 for database server, ycsb
# - ssd1 for system or experiment logs. Well, most r3 types have only 1 SSD. ss0 for now.
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


# You don't need dstat logging here. The Cassandra restart script will restart dstat.
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
#	#
#	# xvdb  80G /mnt/local-ssd0
#	# xvdd  80G /mnt/ebs-gp2
#	# xvde 500G /mnt/ebs-st1
#	# xvdf 500G /mnt/ebs-sc1
#	Util.RunDaemon("dstat -cdn -C total -D xvda,xvdb,xvdd,xvde,xvdf -r --output %s/%s.csv"
#			% (dn_log_dstat, datetime.datetime.now().strftime("%y%m%d-%H%M%S")))
#
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

	# Build
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
def EditCassConf():
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

	fn_cass_yaml = "/home/ubuntu/work/mutants/cassandra/conf/cassandra.yaml"
	Cons.P("Editing %s ..." % fn_cass_yaml)

	# Update cassandra cluster name if specified. No need to.
	#if "cass_cluster_name" in _tags:
	#	# http://stackoverflow.com/questions/7517632/how-do-i-escape-double-and-single-quotes-in-sed-bash
	#	Util.RunSubp("sed -i 's/^cluster_name: .*/cluster_name: '\"'\"'%s'\"'\"'/g' %s"
	#			% (_tags["cass_cluster_name"], fn_cass_yaml))

	Util.RunSubp("sed -i 's/" \
			"^          - seeds: .*" \
			"/          - seeds: \"%s\"" \
			"/g' %s" % (",".join(v for k, v in _nm_ip.items()), fn_cass_yaml))

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
			"/g' %s" % (Ec2InitUtil.GetPubIp(), fn_cass_yaml))

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
			"/g' %s" % (Ec2InitUtil.GetPubIp(), fn_cass_yaml))

	# No need for a single data center deployment
	#Util.RunSubp("sed -i 's/" \
	#		"^endpoint_snitch:.*" \
	#		"/endpoint_snitch: Ec2MultiRegionSnitch" \
	#		"/g' %s" % fn_cass_yaml)

	# Note: Edit additional mutants options specified from the job submission client
	#for k, v in _tags.iteritems():
	#	if k.startswith("mutants_options."):
	#		#              0123456789012345
	#		k1 = k[16:]
	#		Util.RunSubp("sed -i 's/" \
	#				"^    %s:.*" \
	#				"/    %s: %s" \
	#				"/g' %s" % (k1, k1, v, fn_cass_yaml))


def RunCassandra():
	with Cons.MT("Running Cassandra ..."):
		cmd = "%s/work/mutants/cassandra/mutants/restart-dstat-run-cass.py" \
				% os.path.expanduser("~")
		Util.RunSubp(cmd)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
