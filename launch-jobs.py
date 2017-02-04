#!/usr/bin/env python

import boto3
import botocore
import json
import os
import pprint
import sys
import time
import types

sys.path.insert(0, "%s/lib/util" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
import Ec2Region

import LaunchOnDemandInsts
import ReqSpotInsts


def main(argv):
	job_list = []
	for k, v in globals().iteritems():
		if type(v) != types.FunctionType:
			continue
		if k.startswith("Job_"):
			job_list.append(k[4:])
	#Cons.P(job_list)

	if len(argv) != 2:
		Cons.P("Usage: %s job_name" % argv[0])
		Cons.P("  Jobs available:")
		Cons.P(Util.Indent("\n".join(sorted(job_list)), 4))
		sys.exit(1)

	job = "Job_" + argv[1]

	# http://stackoverflow.com/questions/3061/calling-a-function-of-a-module-from-a-string-with-the-functions-name-in-python
	globals()[job]()


def Job_MutantStorageSizeByTime():
	params = { \
			# us-east-1, which is where the S3 buckets for experiment are.
			"region": "us-east-1"
			, "inst_type": "c3.2xlarge"
			, "spot_req_max_price": 1.0
			# RocksDB can use the same AMI
			, "init_script": "mutant-cassandra-server-dev"
			, "ami_name": "mutant-cassandra-server"
			# 100 GB is good enough. 300 baseline IOPS. 2,000 bust IOPS. 3T sc1 has only 36 IOPS.
			, "block_storage_devs": [{"VolumeType": "gp2", "VolumeSize": 100, "DeviceName": "d"}]
			, "unzip_quizup_data": "true"
			, "run_cassandra_server": "false"
			# For now, it doesn't do much other than checking out the code and building.
			, "rocksdb": { }
			, "rocksdb-quizup-runs": []
			, "terminate_inst_when_done": "true"
			}
	p1 = { \
			"exp_desc": "Mutant storage usage mesurement"
			, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
			, "slow_dev_paths": {"t1": "/mnt/ebs-gp2/rocksdb-data-quizup-t1"}
			, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
			, "init_db_to_90p_loaded": "false"
			, "evict_cached_data": "true"
			, "memory_limit_in_mb": 2.0 * 1024

			, "mutant_enabled": "true"
			, "workload_start_from": -1.0
			, "workload_stop_at":    -1.0
			, "simulation_time_dur_in_sec": 60000
			}
	params["rocksdb-quizup-runs"].append(dict(p1))
	LaunchJob(params)


def Job_UnmodifiedRocksDBLatencyByMemorySizes():
	class Conf:
		exp_per_ec2inst = 8
		def __init__(self, stg_dev):
			self.stg_dev = stg_dev
			self.mem_sizes = []
		def Full(self):
			return (len(self.mem_sizes) >= Conf.exp_per_ec2inst)
		def Add(self, mem_size, force=False):
			if False:
				# Experiment already done
				if not force:
					if ((self.stg_dev == "local-ssd1") and (mem_size in [3.8, 3.6, 3.4, 3.2])) \
							or ((self.stg_dev == "ebs-gp2") and (mem_size in [4.2])):
								return
			self.mem_sizes.append(mem_size)
		def Size(self):
			return len(self.mem_sizes)
		def __repr__(self):
			return "(%s, %s)" % (self.stg_dev, self.mem_sizes)

	# The lower bound without getting the system overloaded are different for
	# different storage devices.  local-ssd1 and ebs-gp2 have 14, which is 1.4GB,
	# which the range() function sets the lower bound as 12. They are capped by
	# the main memory. OOM killer.  ebs-st1 can go 1.6GB without the storage
	# device overloaded, ebs-sc1 2.0GB.
	num_exp_per_conf = 5
	confs = []
	for stg_dev in ["local-ssd1", "ebs-gp2"]:
		conf = Conf(stg_dev)
		for j in range(num_exp_per_conf):
			for i in range(30, 12, -2):
				if conf.Full():
					confs.append(conf)
					conf = Conf(stg_dev)
				conf.Add(i/10.0)
		if conf.Size() > 0:
			confs.append(conf)

	stg_dev = "ebs-st1"
	conf = Conf(stg_dev)
	for j in range(num_exp_per_conf):
		for i in range(30, 14, -2):
			if conf.Full():
				confs.append(conf)
				conf = Conf(stg_dev)
			conf.Add(i/10.0)
	if conf.Size() > 0:
		confs.append(conf)

	stg_dev = "ebs-sc1"
	conf = Conf(stg_dev)
	for j in range(num_exp_per_conf):
		for i in range(30, 18, -2):
			if conf.Full():
				confs.append(conf)
				conf = Conf(stg_dev)
			conf.Add(i/10.0)
	if conf.Size() > 0:
		confs.append(conf)

	# Patch a missed experiment
	#confs[18].mem_sizes = confs[18].mem_sizes[4:]
	#confs = confs[18:19]

	Cons.P("%d machines" % len(confs))
	Cons.P(pprint.pformat(confs, width=100))
	#sys.exit(0)

	for conf in confs:
		params = { \
				# us-east-1, which is where the S3 buckets for experiment are.
				"region": "us-east-1"
				, "inst_type": "c3.2xlarge"
				, "spot_req_max_price": 1.0
				# RocksDB can use the same AMI
				, "init_script": "mutant-cassandra-server-dev"
				, "ami_name": "mutant-cassandra-server"
				, "block_storage_devs": []
				, "unzip_quizup_data": "true"
				, "run_cassandra_server": "false"
				# For now, it doesn't do much other than checking out the code and building.
				, "rocksdb": { }
				, "rocksdb-quizup-runs": []
				, "terminate_inst_when_done": "true"
				}
		if conf.stg_dev == "local-ssd1":
			pass
		elif conf.stg_dev == "ebs-gp2":
			params["block_storage_devs"].append({"VolumeType": "gp2", "VolumeSize": 1000, "DeviceName": "d"})
		elif conf.stg_dev == "ebs-st1":
			params["block_storage_devs"].append({"VolumeType": "st1", "VolumeSize": 3000, "DeviceName": "e"})
		elif conf.stg_dev == "ebs-sc1":
			params["block_storage_devs"].append({"VolumeType": "sc1", "VolumeSize": 3000, "DeviceName": "f"})
		else:
			raise RuntimeError("Unexpected")

		p1 = { \
				"exp_desc": "Unmodified RocksDB latency by different memory sizes"
				, "fast_dev_path": "/mnt/%s/rocksdb-data" % conf.stg_dev
				, "db_path": "/mnt/%s/rocksdb-data/quizup" % conf.stg_dev
				, "init_db_to_90p_loaded": "true"
				, "evict_cached_data": "true"
				, "memory_limit_in_mb": 1024 * 3

				, "mutant_enabled": "false"
				, "workload_start_from": 0.899
				, "workload_stop_at":    -1.0
				, "simulation_time_dur_in_sec": 60000
				}
		for ms in conf.mem_sizes:
			p1["memory_limit_in_mb"] = 1024.0 * ms
			params["rocksdb-quizup-runs"].append(dict(p1))
		LaunchJob(params)


def Job_MutantLatencyBySstMigTempThresholds():
	params = { \
			# us-east-1, which is where the S3 buckets for experiment are.
			"region": "us-east-1"
			, "inst_type": "c3.2xlarge"

			# RocksDB can use the same AMI
			, "init_script": "mutant-cassandra-server-dev"
			, "ami_name": "mutant-cassandra-server"
			, "block_storage_devs": [
				# 1TB gp2 for 3000 IOPS
				{"VolumeType": "gp2", "VolumeSize": 1000, "DeviceName": "d"}

				# 3TB st1 for 120 Mib/s, 500 Mib/s (burst) throughput.
				#   http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html
				#{"VolumeType": "st1", "VolumeSize": 3000, "DeviceName": "e"}

				# 3TB sc1 for 36 Mib/s, 240 Mib/s (burst).
				#{"VolumeType": "sc1", "VolumeSize": 3000, "DeviceName": "f"}
				]
			, "unzip_quizup_data": "true"

			, "run_cassandra_server": "false"

			# For now, it doesn't do much other than checking out the code and building.
			, "rocksdb": { }

			, "rocksdb-quizup-runs": []
			}

	p1 = { \
			"mutant_enabled": "true"
			#, "sst_migration_temperature_threshold": 10
			, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
			, "slow_dev_paths": {"t1": "/mnt/ebs-gp2/rocksdb-data-quizup-t1"}
			#, "slow_dev_paths": {"t1": "/mnt/ebs-st1/rocksdb-data-quizup-t1"}
			#, "slow_dev_paths": {"t1": "/mnt/ebs-sc1/rocksdb-data-quizup-t1"}
			, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
			, "init_db_to_90p_loaded": "true"
			, "evict_cached_data": "true"
			, "workload_start_from": 0.899
			, "workload_stop_at":    -1.0
			, "simulation_time_dur_in_sec": 60000
			, "terminate_inst_when_done": "true"
			}

	for sst_mig_temp_th in [200, 150, 100, 50, 40, 30, 20, 15, 10, 5, 4, 3, 2, 1] * 2:
		p1["sst_migration_temperature_threshold"] = sst_mig_temp_th
		params["rocksdb-quizup-runs"].append(dict(p1))
	LaunchJob(params)


def LaunchJob(params):
	# Spot instance
	ReqSpotInsts.Req(params)

	# On-demand instance
	#LaunchOnDemandInsts.Launch(params)



if __name__ == "__main__":
	sys.exit(main(sys.argv))
