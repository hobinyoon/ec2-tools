#!/usr/bin/env python

import base64
import boto3
import datetime
import json
import multiprocessing
import os
import pprint
import re
import sys
import time
import traceback
import zipfile
import zlib

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
    PrepareBlockDevs()
    Ec2InitUtil.ChangeLogOutput()
    CloneSrcAndBuild()

    if Ec2InitUtil.GetParam(["unzip_quizup_data"]) == "true":
      UnzipQuizupData()
    RunRocksDBQuizup()
    RunYcsb()

    # Terminate instance
    if Ec2InitUtil.GetParam(["terminate_inst_when_done"]) == "true":
      Util.RunSubp("sudo shutdown -h now")
  except Exception as e:
    msg = "Exception: %s\n%s" % (e, traceback.format_exc())
    Cons.P(msg)


def SetHostname():
  with Cons.MT("Setting host name ..."):
    # Hostname consists of availability zone name and launch req datetime
    hn = "%s-%s" % (Ec2InitUtil.GetAz(), Ec2InitUtil.GetJobId())

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
      blk_devs["xvdc"] = "local-ssd1"
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
    if inst_type.startswith("c3."):
      Util.RunSubp("sudo umount /dev/xvdb || true")
      Util.RunSubp("sudo umount /dev/xvdc || true")

      if Ec2InitUtil.GetParam(["erase_local_ssd"]) == "true":
        # tee has a problem of not stopping. For now, you can give up on ssd1.
        # - https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=89224
        #
        # "count" one less than what it says below: 81909 - 1
        #
        # 81908+0 records in
        # 81908+0 records out
        # 85886763008 bytes (86 GB) copied, 1631.16 s, 52.7 MB/s
        # 1683187 ms = 28 mins
        Util.RunSubp("sudo sh -c \"dd if=/dev/zero bs=1M count=81908 | tee /dev/xvdb > /dev/xvdc\"", measure_time=True)

      #
      # sudo dd if=/dev/zero bs=1M of=/dev/xvdb || true
      #   dd: error writing '/dev/xvdb': No space left on device
      #   81910+0 records in
      #   81909+0 records out
      #   85887811584 bytes (86 GB) copied, 1394.5 s, 61.6 MB/s
      #   1394510 ms = 23 mins
      #Util.RunSubp("sudo dd if=/dev/zero bs=1M of=/dev/xvdb || true", measure_time=True)

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


def CloneSrcAndBuild():
  with Cons.MT("Cloning src and build ..."):
    # Make parent
    Util.RunSubp("mkdir -p /mnt/local-ssd0/mutant")

    _CloneAndBuildCassandra()
    _CloneCassandra2x()

    _CloneAndBuildRocksDb()
    _CloneMisc()
    _CloneAndBuildYcsb()


def _CloneAndBuildCassandra():
  with Cons.MT("Cloning Cassandra src and build ..."):
    # Git clone
    Util.RunSubp("rm -rf /mnt/local-ssd0/mutant/cassandra")
    Util.RunSubp("git clone https://github.com/hobinyoon/mutant-cassandra-3.9 /mnt/local-ssd0/mutant/cassandra")

    # Symlink
    Util.RunSubp("rm -rf /home/ubuntu/work/mutant/cassandra")
    Util.RunSubp("ln -s /mnt/local-ssd0/mutant/cassandra /home/ubuntu/work/mutant/cassandra")

    # Build
    if Ec2InitUtil.GetParam(["run_cassandra_server"]) == "true":
      Util.RunSubp("cd /home/ubuntu/work/mutant/cassandra && ant")

    # Edit the git source repository for easy development.
    Util.RunSubp("sed -i 's/" \
        "^\\turl = https:\\/\\/github.com\\/hobinyoon\\/mutant-cassandra-3.9" \
        "/\\turl = git@github.com:hobinyoon\/mutant-cassandra-3.9.git" \
        "/g' %s" % "~/work/mutant/cassandra/.git/config")


#def _CloneAndBuildMongoDb():
#	# Git clone
#	Util.RunSubp("rm -rf /mnt/local-ssd0/mutant/mongo")
#	Util.RunSubp("git clone https://github.com/hobinyoon/mongo /mnt/local-ssd0/mutant/mongo")
#
#	# Symlink
#	Util.RunSubp("rm -rf /home/ubuntu/work/mutant/mongo")
#	Util.RunSubp("ln -s /mnt/local-ssd0/mutant/mongo /home/ubuntu/work/mutant/mongo")
#
#	# Build. May take a long time.
#	Util.RunSubp("cd /home/ubuntu/work/mutant/mongo && scons mongod -j16", measure_time=True)
#
#	# Edit the git source repository for easy development.
#	Util.RunSubp("sed -i 's/" \
#			"^\\turl = https:\\/\\/github.com\\/hobinyoon\\/mongo" \
#			"/\\turl = git@github.com:hobinyoon\/mongo.git" \
#			"/g' %s" % "~/work/mutant/mongo/.git/config")
#
#	# Create data and system log directories
#	dn = "/mnt/local-ssd1/mongo-data"
#	Util.RunSubp("sudo mkdir -p %s && sudo chown ubuntu %s" % (dn, dn))
#	dn = "/mnt/local-ssd0/mongo-log"
#	Util.RunSubp("sudo mkdir -p %s && sudo chown ubuntu %s" % (dn, dn))


def _CloneAndBuildRocksDb():
  if Ec2InitUtil.GetParam(["rocksdb"]) is None:
    return

  with Cons.MT("Cloning RocksDB src and build ..."):
    # Git clone
    Util.RunSubp("rm -rf /mnt/local-ssd0/mutant/rocksdb")
    Util.RunSubp("git clone https://github.com/hobinyoon/rocksdb /mnt/local-ssd0/mutant/rocksdb")

    # Symlink
    Util.RunSubp("rm -rf /home/ubuntu/work/mutant/rocksdb")
    Util.RunSubp("ln -s /mnt/local-ssd0/mutant/rocksdb /home/ubuntu/work/mutant/rocksdb")

    # Edit the git source repository for easy development.
    Util.RunSubp("sed -i 's/" \
        "^\\turl = https:\\/\\/github.com\\/hobinyoon\\/rocksdb" \
        "/\\turl = git@github.com:hobinyoon\/rocksdb.git" \
        "/g' %s" % "~/work/mutant/rocksdb/.git/config")

    # Switch to the mutant branch
    Util.RunSubp("cd /home/ubuntu/work/mutant/rocksdb" \
        " && git branch -f mutant origin/mutant" \
        " && git checkout mutant")

    # Build. Takes about 5 mins. You can save the pre-built one in the AMI. JNI build fails sometimes and succeeds the next time.
    #   Must be some race condition in the build. Build 10x until it builds.
    Util.RunSubp("cd /home/ubuntu/work/mutant/rocksdb && make -j16 shared_lib && (make -j16 rocksdbjavastatic" \
        " || make -j16 rocksdbjavastatic" \
        " || make -j16 rocksdbjavastatic" \
        " || make -j16 rocksdbjavastatic" \
        " || make -j16 rocksdbjavastatic" \
        " || make -j16 rocksdbjavastatic" \
        " || make -j16 rocksdbjavastatic" \
        " || make -j16 rocksdbjavastatic" \
        " || make -j16 rocksdbjavastatic" \
        " || make -j16 rocksdbjavastatic" \
        ")"
        , measure_time=True)

    # Create data directory
    dn = "/mnt/local-ssd1/rocksdb-data"
    Util.RunSubp("sudo mkdir -p %s && sudo chown ubuntu %s" % (dn, dn))
    Util.RunSubp("rm -rf ~/work/rocksdb-data")
    Util.RunSubp("ln -s %s ~/work/rocksdb-data" % dn)


def _CloneCassandra2x():
  with Cons.MT("Cloning Cassandra 2.x src and build ..."):
    # Git clone
    Util.RunSubp("rm -rf /mnt/local-ssd0/mutant/mutant-cassandra-2.2.3")
    Util.RunSubp("git clone https://github.com/hobinyoon/mutant-cassandra-2.2.3 /mnt/local-ssd0/mutant/mutant-cassandra-2.2.3")

    # Symlink
    Util.RunSubp("rm -rf /home/ubuntu/work/mutant/cassandra-2.2.3")
    Util.RunSubp("ln -s /mnt/local-ssd0/mutant/mutant-cassandra-2.2.3 /home/ubuntu/work/mutant/cassandra-2.2.3")

    # Edit the git source repository for easy development.
    Util.RunSubp("sed -i 's/" \
        "^\\turl = https:\\/\\/github.com\\/hobinyoon\\/mutant-cassandra-2.2.3" \
        "/\\turl = git@github.com:hobinyoon\/mutant-cassandra-2.2.3.git" \
        "/g' %s" % "~/work/mutant/cassandra-2.2.3/.git/config")


def _CloneMisc():
  with Cons.MT("Cloning misc ..."):
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
  with Cons.MT("Cloning YCSB and build ..."):
    # Git clone
    Util.RunSubp("rm -rf /mnt/local-ssd0/mutant/YCSB")
    Util.RunSubp("git clone https://github.com/hobinyoon/YCSB /mnt/local-ssd0/mutant/YCSB")

    # Symlink
    Util.RunSubp("rm -rf /home/ubuntu/work/mutant/YCSB")
    Util.RunSubp("ln -s /mnt/local-ssd0/mutant/YCSB /home/ubuntu/work/mutant/YCSB")

    # Edit the git source repository for easy development.
    Util.RunSubp("sed -i 's/" \
        "^\\turl = https:\\/\\/github.com\\/hobinyoon\\/YCSB" \
        "/\\turl = git@github.com:hobinyoon\/YCSB.git" \
        "/g' %s" % "~/work/mutant/YCSB/.git/config")

    # Switch to mutant branch
    Util.RunSubp("cd /home/ubuntu/work/mutant/YCSB" \
        " && git branch -f mutant origin/mutant" \
        " && git checkout mutant")

    # Build
    #Util.RunSubp("cd /home/ubuntu/work/mutant/YCSB && mvn -pl com.yahoo.ycsb:cassandra-binding -am clean package -DskipTests >/dev/null 2>&1")
    Util.RunSubp("cd /home/ubuntu/work/mutant/YCSB && mvn -pl com.yahoo.ycsb:rocksdb-binding -am clean package -DskipTests >/dev/null 2>&1")


def RunRocksDBQuizup():
  for params in Ec2InitUtil.GetParam(["rocksdb-quizup-runs"]):
    with Cons.MT("Running RocksDB Quizup ..."):
      Cons.P(pprint.pformat(params))

      params1 = []

      # Parameters for run.sh
      if "fast_dev_path" in params:
        params1.append("--fast_dev_path=%s" % params["fast_dev_path"])
      if "slow_dev_paths" in params:
        slow_dev_paths = params["slow_dev_paths"]
        for k, v in slow_dev_paths.iteritems():
          params1.append("--slow_dev%s_path=%s" % (k[-1:], v))
      if "db_path" in params:
        params1.append("--db_path=%s" % params["db_path"])
      if "init_db_to_90p_loaded" in params:
        params1.append("--init_db_to_90p_loaded=%s" % params["init_db_to_90p_loaded"])
      if "evict_cached_data" in params:
        params1.append("--evict_cached_data=%s" % params["evict_cached_data"])
      if "memory_limit_in_mb" in params:
        params1.append("--memory_limit_in_mb=%s" % params["memory_limit_in_mb"])
      params1.append("--upload_result_to_s3")

      # Parameters for the quizup binary
      if "exp_desc" in params:
        params1.append("--exp_desc=%s" % base64.b64encode(params["exp_desc"]))

      if "cache_filter_index_at_all_levels" in params:
        params1.append("--cache_filter_index_at_all_levels=%s" % params["cache_filter_index_at_all_levels"])

      if "monitor_temp" in params:
        params1.append("--monitor_temp=%s" % params["monitor_temp"])
      if "migrate_sstables" in params:
        params1.append("--migrate_sstables=%s" % params["migrate_sstables"])
      if "workload_start_from" in params:
        params1.append("--workload_start_from=%s" % params["workload_start_from"])
      if "workload_stop_at" in params:
        params1.append("--workload_stop_at=%s" % params["workload_stop_at"])
      if "simulation_time_dur_in_sec" in params:
        params1.append("--simulation_time_dur_in_sec=%s" % params["simulation_time_dur_in_sec"])
      if "sst_ott" in params:
        params1.append("--sst_ott=%s" % params["sst_ott"])

      cmd = "cd %s/work/mutant/misc/rocksdb/quizup && stdbuf -i0 -o0 -e0 ./run.py %s" \
          % (os.path.expanduser("~"), " ".join(params1))
      Util.RunSubp(cmd)


def RunYcsb():
  params_encoded = base64.b64encode(zlib.compress(json.dumps(Ec2InitUtil.GetParam(["ycsb-runs"]))))
  cmd = "cd %s/work/mutant/misc/rocksdb/ycsb && stdbuf -i0 -o0 -e0 ./restart-dstat-run-ycsb.py %s" \
      % (os.path.expanduser("~"), params_encoded)
  Util.RunSubp(cmd)


_nm_ip = None
def EditCassConf():
  fn_cass_yaml = "/home/ubuntu/work/mutant/cassandra/conf/cassandra.yaml"
  with Cons.MT("Editing %s ..." % fn_cass_yaml):
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

    Util.RunSubp("sed -i 's/" \
        "^\(#\|\)concurrent_compactors: .*" \
        "/concurrent_compactors: %d" \
        "/g' %s" % (multiprocessing.cpu_count(), fn_cass_yaml))

    Util.RunSubp("sed -i 's/" \
        "^\(#\|\)memtable_flush_writers: .*" \
        "/memtable_flush_writers: %d" \
        "/g' %s" % (multiprocessing.cpu_count(), fn_cass_yaml))

    _EditCassConfDataFileDir(fn_cass_yaml)

    #Util.RunSubp("sed -i 's/" \
        #		"^\(# \|\)data_file_directories: .*" \
        #		"/data_file_directories: \[\"%s\"\]" \
        #		"/g' %s" % (dn.replace("/", "\/"), fn_cass_yaml))

    # Let the commit logs go to the default directory, local-ssd0.
    #dn_cl = "/mnt/local-ssd1/cassandra-commitlog"
    #Util.MkDirs(dn_cl)
    #Util.RunSubp("sed -i 's/" \
        #		"^\(# \|\)commitlog_directory: .*" \
        #		"/commitlog_directory: %s" \
        #		"/g' %s" % (dn_cl.replace("/", "\/"), fn_cass_yaml))

    # No need for a single data center deployment
    #Util.RunSubp("sed -i 's/" \
        #		"^endpoint_snitch:.*" \
        #		"/endpoint_snitch: Ec2MultiRegionSnitch" \
        #		"/g' %s" % fn_cass_yaml)

    # Note: Edit additional mutant options specified from the job submission client
    #for k, v in _tags.iteritems():
    #	if k.startswith("mutant_options."):
    #		#              0123456789012345
    #		k1 = k[16:]
    #		Util.RunSubp("sed -i 's/" \
        #				"^    %s:.*" \
        #				"/    %s: %s" \
        #				"/g' %s" % (k1, k1, v, fn_cass_yaml))


def _EditCassConfDataFileDir(fn):
  with Cons.MT("Edit data_file_directories ..."):
    # data_file_directories:
    # - Can't get the bracket notation working. Go for the dash one.
    dn = "/mnt/local-ssd1/cassandra-data"
    Util.MkDirs(dn)
    lines_new = []
    with open(fn) as fo:
      lines = fo.readlines()
      i = 0
      while i < len(lines):
        line = lines[i].rstrip()
        #Cons.P("line=[%s]" % line)
        if re.match(r"(# )?data_file_directories:", line):
          # Remove all following lines with -, which is a list item
          while i < len(lines) - 1:
            i += 1
            line = lines[i].rstrip()
            # #     - /var/lib/cassandra/data
            if re.match(r"\#? +- .+", line) is None:
              break
          # Insert new one
          lines_new.append("data_file_directories:")
          lines_new.append("    - %s" % dn)
        else:
          lines_new.append(line)
          i += 1

      # Save lines_new back to the file
      with open(fn, "w") as fo:
        for l in lines_new:
          fo.write("%s\n" % l)


def RunCassandra():
  PrePopulateCassData()

  with Cons.MT("Running Cassandra ..."):
    # Run Cassandra foreground, as a non-daemon. It's easier for debugging. The
    # cloud-init script never ends but it's okay, for now. You can see the
    # Cassandra log in the log file ~/work/mutant/log/..., so that's a plus.
    cmd = "%s/work/mutant/cassandra/mutant/restart-dstat-run-cass.py" \
        % os.path.expanduser("~")
    Util.RunSubp(cmd)


def UnzipQuizupData():
  with Cons.MT("Unzipping QuizUp data ..."):
    Util.RunSubp("mkdir -p /mnt/local-ssd0/quizup-data")
    Util.RunSubp("rm -rf %s/work/quizup-data" % os.path.expanduser("~"))
    Util.RunSubp("ln -s /mnt/local-ssd0/quizup-data %s/work/quizup-data" % os.path.expanduser("~"))
    Util.RunSubp("mkdir -p %s/work/quizup-data/memcached-2w/simulator-data" % os.path.expanduser("~"))
    Util.RunSubp("cd %s/work/quizup-data-zipped && ./unzip.sh" % os.path.expanduser("~"))


def PrePopulateCassData():
  if not bool(Ec2InitUtil.GetParam(["pre_populate_db"])):
    return

  with Cons.MT("Pre-popularing Cassandra data ..."):
    dn = "/mnt/local-ssd0/mutant/cassandra/data-stored"
    Util.MkDirs(dn)
    fn_tar = "%s/cass-data-data.tar" % dn

    if os.path.isfile(fn_tar):
      return

    s3 = boto3.client("s3", region_name = "us-east-1")
    with Cons.MT("downloading ..."):
      # 20GB data
      s3.download_file("mutants-cass-data-snapshots"
          , "cass-data-data.tar"
          , fn_tar)

      # Un-tar
    Util.RunSubp("rm -rf /mnt/local-ssd1/cassandra-data")
    Util.RunSubp("tar xvf /mnt/local-ssd0/mutant/cassandra/data-stored/cass-data-data.tar -C /mnt/local-ssd1/"
        , measure_time=True)

    # It doesn't make much sense to compress the tar file since the records are
    # randomly generated.


if __name__ == "__main__":
  sys.exit(main(sys.argv))
