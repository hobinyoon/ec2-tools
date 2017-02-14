#!/usr/bin/env python

import boto3
import botocore
import inspect
import json
import math
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


def Job_LowSstMigTempThresholds_LocalSsd1EbsSt1():
	params = { \
			"region": "us-east-1"
			, "inst_type": "c3.2xlarge"
			, "spot_req_max_price": 1.0
			, "init_script": "mutant-cassandra-server-dev"
			, "ami_name": "mutant-cassandra-server"
			, "block_storage_devs": [{"VolumeType": "st1", "VolumeSize": 3000, "DeviceName": "e"}]
			, "unzip_quizup_data": "true"
			, "run_cassandra_server": "false"
			# For now, it doesn't do much other than checking out the code and building.
			, "rocksdb": { }
			, "rocksdb-quizup-runs": []
			, "terminate_inst_when_done": "true"
			}

	# Full time range experiments. I don't need this until I am convinced by the
	# performance test.
	if False:
		# From 2^-10 = 0.0009765625 down to 2^-36
		for i in range(-10, -38, -2):
			sst_mig_temp_thrds = pow(2, i)
			p1 = { \
					# Use the current function name since you always forget to set this
					"exp_desc": inspect.currentframe().f_code.co_name[4:]
					, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
					, "slow_dev_paths": {"t1": "/mnt/ebs-st1/rocksdb-data-quizup-t1"}
					, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
					, "init_db_to_90p_loaded": "false"
					, "evict_cached_data": "true"
					, "memory_limit_in_mb": 9.0 * 1024

					# This doesn't really matter for measuring the number of compactions
					# and SSTables.
					, "cache_filter_index_at_all_levels": "false"
					, "monitor_temp": "true"
					, "migrate_sstables": "true"
					, "workload_start_from": -1.0
					, "workload_stop_at":    -1.0
					, "simulation_time_dur_in_sec": 20000
					, "sst_migration_temperature_threshold": sst_mig_temp_thrds
					}
			params["rocksdb-quizup-runs"] = []
			params["rocksdb-quizup-runs"].append(dict(p1))
			#Cons.P(pprint.pformat(params))
			LaunchJob(params)

	# 95% to 100% time range experiments for measuring latency and the number of IOs.
	if True:
		class Conf:
			exp_per_ec2inst = 5
			def __init__(self):
				self.sst_mig_temp_thrds = []
			def Full(self):
				return (len(self.sst_mig_temp_thrds) >= Conf.exp_per_ec2inst)
			def Add(self, smtt):
				self.sst_mig_temp_thrds.append(smtt)
			def Size(self):
				return len(self.sst_mig_temp_thrds)
			def __repr__(self):
				return "(%s)" % (self.sst_mig_temp_thrds)

		num_exp_per_conf = 4
		confs = []
		conf = Conf()
		for j in range(num_exp_per_conf):
			# From 2^-10 = 0.0009765625 down to 2^-36
			for i in range(-10, -38, -2):
				if conf.Full():
					confs.append(conf)
					conf = Conf()
				mig_temp_thrds = math.pow(2, i)
				conf.Add(mig_temp_thrds)
		if conf.Size() > 0:
			confs.append(conf)

		Cons.P("%d machines" % len(confs))
		Cons.P(pprint.pformat(confs, width=100))
		#sys.exit(1)

		for conf in confs:
			params["rocksdb-quizup-runs"] = []
			for mt in conf.sst_mig_temp_thrds:
				p1 = { \
						# Use the current function name since you always forget to set this
						"exp_desc": inspect.currentframe().f_code.co_name[4:]
						, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
						, "slow_dev_paths": {"t1": "/mnt/ebs-st1/rocksdb-data-quizup-t1"}
						, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
						, "init_db_to_90p_loaded": "true"
						, "evict_cached_data": "true"
						, "memory_limit_in_mb": 2.0 * 1024

						# Not caching metadata might be a better idea. So the story is you
						# present each of the optimizations separately, followed by the
						# combined result.
						, "cache_filter_index_at_all_levels": "false"
						, "monitor_temp": "true"
						, "migrate_sstables": "true"
						, "workload_start_from": 0.899
						, "workload_stop_at":    -1.0
						, "simulation_time_dur_in_sec": 60000
						, "sst_migration_temperature_threshold": mt
						}
				params["rocksdb-quizup-runs"].append(dict(p1))
			#Cons.P(pprint.pformat(params))
			LaunchJob(params)


def Job_LowSstMigTempThresholds_LocalSsd1Only():
	params = { \
			"region": "us-east-1"
			, "inst_type": "c3.2xlarge"
			, "spot_req_max_price": 1.0
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

	# Full time range experiments are for the number of compactions and SSTables.
	# They don't show the number of disk IOs.
	if False:
		# From 2^-10 = 0.0009765625 down to 2^-32
		for i in range(-10, -34, -2):
			sst_mig_temp_thrds = pow(2, i)
			p1 = { \
					# Use the current function name since you always forget to set this
					"exp_desc": inspect.currentframe().f_code.co_name[4:]
					, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
					, "slow_dev_paths": {}
					, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
					, "init_db_to_90p_loaded": "false"
					, "evict_cached_data": "true"
					, "memory_limit_in_mb": 9.0 * 1024

					# This doesn't really matter for measuring the number of compactions
					# and SSTables.
					, "cache_filter_index_at_all_levels": "false"
					, "monitor_temp": "true"
					, "migrate_sstables": "true"
					, "workload_start_from": -1.0
					, "workload_stop_at":    -1.0
					, "simulation_time_dur_in_sec": 20000
					, "sst_migration_temperature_threshold": sst_mig_temp_thrds
					}
			params["rocksdb-quizup-runs"] = []
			params["rocksdb-quizup-runs"].append(dict(p1))
			#Cons.P(pprint.pformat(params))
			LaunchJob(params)

	# 95% to 100% time range experiments for measuring latency and the number of IOs.
	if True:
		class Conf:
			exp_per_ec2inst = 2
			def __init__(self):
				self.sst_mig_temp_thrds = []
			def Full(self):
				return (len(self.sst_mig_temp_thrds) >= Conf.exp_per_ec2inst)
			def Add(self, smtt):
				self.sst_mig_temp_thrds.append(smtt)
			def Size(self):
				return len(self.sst_mig_temp_thrds)
			def __repr__(self):
				return "(%s)" % (self.sst_mig_temp_thrds)

		num_exp_per_conf = 2
		confs = []
		conf = Conf()
		for j in range(num_exp_per_conf):
			# From 2^-10 = 0.0009765625 down to 2^-32
			for i in range(-10, -34, -2):
				if conf.Full():
					confs.append(conf)
					conf = Conf()
				mig_temp_thrds = math.pow(2, i)
				conf.Add(mig_temp_thrds)
		if conf.Size() > 0:
			confs.append(conf)

		Cons.P("%d machines" % len(confs))
		Cons.P(pprint.pformat(confs, width=100))
		#sys.exit(1)

		for conf in confs:
			params["rocksdb-quizup-runs"] = []
			for mt in conf.sst_mig_temp_thrds:
				p1 = { \
						# Use the current function name since you always forget to set this
						"exp_desc": inspect.currentframe().f_code.co_name[4:]
						, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
						, "slow_dev_paths": {}
						, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
						, "init_db_to_90p_loaded": "true"
						, "evict_cached_data": "true"
						, "memory_limit_in_mb": 2.0 * 1024

						# Not caching metadata might be a better idea. So the story is you
						# present each of the optimizations separately, followed by the
						# combined result.
						, "cache_filter_index_at_all_levels": "false"
						, "monitor_temp": "true"
						, "migrate_sstables": "true"
						, "workload_start_from": 0.899
						, "workload_stop_at":    -1.0
						, "simulation_time_dur_in_sec": 60000
						, "sst_migration_temperature_threshold": mt
						}
				params["rocksdb-quizup-runs"].append(dict(p1))
			#Cons.P(pprint.pformat(params))
			LaunchJob(params)


def Job_2LevelMutantBySstMigTempThresholdsToMeasureStorageUsage():
	class Conf:
		exp_per_ec2inst = 2
		def __init__(self, slow_dev):
			self.slow_dev = slow_dev
			self.sst_mig_temp_thrds = []
		def Full(self):
			return (len(self.sst_mig_temp_thrds) >= Conf.exp_per_ec2inst)
		def Add(self, mem_size):
			self.sst_mig_temp_thrds.append(mem_size)
		def Size(self):
			return len(self.sst_mig_temp_thrds)
		def __repr__(self):
			return "(%s, %s)" % (self.slow_dev, self.sst_mig_temp_thrds)

	# [0.25, 256]
	for i in range(-2, 9):
		#Cons.P("%d %f" % (i, math.pow(2, i)))
		mig_temp_thrds = math.pow(2, i)

	num_exp_per_conf = 2
	confs = []
	if False:
		for slow_dev in ["ebs-gp2"]:
			conf = Conf(slow_dev)
			for j in range(num_exp_per_conf):
				#for i in range(-14, 12, 2):
				for i in range(-20, -14, 2):
					if conf.Full():
						confs.append(conf)
						conf = Conf(slow_dev)
					mig_temp_thrds = math.pow(2, i)
					conf.Add(mig_temp_thrds)
			if conf.Size() > 0:
				confs.append(conf)

	Cons.P("%d machines" % len(confs))
	Cons.P(pprint.pformat(confs, width=100))

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
		if conf.slow_dev == "ebs-gp2":
			# 100GB gp2: 300 baseline IOPS. 2,000 burst IOPS.
			params["block_storage_devs"].append({"VolumeType": "gp2", "VolumeSize": 100, "DeviceName": "d"})
		else:
			raise RuntimeError("Unexpected")

		for mt in conf.sst_mig_temp_thrds:
			p1 = { \
					"exp_desc": inspect.currentframe().f_code.co_name[4:]
					, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
					, "slow_dev_paths": {"t1": "/mnt/%s/rocksdb-data-quizup-t1" % conf.slow_dev}
					, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
					, "init_db_to_90p_loaded": "false"
					, "evict_cached_data": "true"
					, "memory_limit_in_mb": 9.0 * 1024

					, "cache_filter_index_at_all_levels": "true"
					, "monitor_temp": "true"
					, "migrate_sstables": "true"
					, "workload_start_from": -1.0
					, "workload_stop_at":    -1.0
					, "simulation_time_dur_in_sec": 20000
					, "sst_migration_temperature_threshold": mt
					}
			params["rocksdb-quizup-runs"].append(dict(p1))
		#Cons.P(pprint.pformat(params))
		LaunchJob(params)

	# RocksDB with temperature monitoring and no sstable migration for a comparision
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
	p1 = { \
			"exp_desc": inspect.currentframe().f_code.co_name[4:]
			, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
			, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
			, "init_db_to_90p_loaded": "false"
			, "evict_cached_data": "true"
			, "memory_limit_in_mb": 9.0 * 1024

			# Doesn't matter. We are interested in the size only.
			, "cache_filter_index_at_all_levels": "false"

			, "monitor_temp": "true"
			, "migrate_sstables": "false"
			, "workload_start_from": -1.0
			, "workload_stop_at":    -1.0
			, "simulation_time_dur_in_sec": 20000
			, "sst_migration_temperature_threshold": 0
			}
	params["rocksdb-quizup-runs"].append(dict(p1))
	#Cons.P(pprint.pformat(params))
	LaunchJob(params)
	return

	# Unmodified RocksDB too for a comparison
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
	p1 = { \
			"exp_desc": inspect.currentframe().f_code.co_name[4:]
			, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
			, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
			, "init_db_to_90p_loaded": "false"
			, "evict_cached_data": "true"
			, "memory_limit_in_mb": 9.0 * 1024

			, "cache_filter_index_at_all_levels": "false"
			, "monitor_temp": "false"
			, "migrate_sstables": "false"
			, "workload_start_from": -1.0
			, "workload_stop_at":    -1.0
			, "simulation_time_dur_in_sec": 20000
			, "sst_migration_temperature_threshold": 0
			}
	params["rocksdb-quizup-runs"].append(dict(p1))
	#Cons.P(pprint.pformat(params))
	LaunchJob(params)


def Job_UnmodifiedRocksDbWithWithoutMetadataCachingByStgDevs():
	class Conf:
		exp_per_ec2inst = 5
		def __init__(self, stg_dev):
			self.stg_dev = stg_dev
			self.metadata_caching = []
		def Full(self):
			return (len(self.metadata_caching) >= Conf.exp_per_ec2inst)
		def Add(self, metadata_caching):
			self.metadata_caching.append(metadata_caching)
		def Size(self):
			return len(self.metadata_caching)
		def __repr__(self):
			return "(%s, %s)" % (self.stg_dev, self.metadata_caching)

	# The lower bound without getting the system overloaded are different for
	# different storage devices.  local-ssd1 and ebs-gp2 have 14, which is 1.4GB,
	# which the range() function sets the lower bound as 12. They are capped by
	# the main memory. OOM killer.  ebs-st1 can go 1.6GB without the storage
	# device overloaded, ebs-sc1 2.0GB.
	num_exp_per_conf = 4
	confs = []
	for stg_dev in ["local-ssd1", "ebs-gp2", "ebs-st1", "ebs-sc1"]:
		conf = Conf(stg_dev)
		for j in range(num_exp_per_conf):
			for i in ["true", "false"]:
				if conf.Full():
					confs.append(conf)
					conf = Conf(stg_dev)
				conf.Add(i)
		if conf.Size() > 0:
			confs.append(conf)

	Cons.P("%d machines" % len(confs))
	Cons.P(pprint.pformat(confs, width=100))

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
				"exp_desc": inspect.currentframe().f_code.co_name[4:]
				, "fast_dev_path": "/mnt/%s/rocksdb-data" % conf.stg_dev
				, "db_path": "/mnt/%s/rocksdb-data/quizup" % conf.stg_dev
				, "init_db_to_90p_loaded": "true"
				, "evict_cached_data": "true"
				, "memory_limit_in_mb": 1024 * 2

				, "cache_filter_index_at_all_levels": None

				, "monitor_temp": "false"
				, "migrate_sstables": "false"
				, "workload_start_from": 0.899
				, "workload_stop_at":    -1.0
				, "simulation_time_dur_in_sec": 60000
				}
		for mc in conf.metadata_caching:
			p1["cache_filter_index_at_all_levels"] = mc
			params["rocksdb-quizup-runs"].append(dict(p1))

		#Cons.P(pprint.pformat(params))
		LaunchJob(params)


def Job_2LevelMutantLatencyByColdStgBySstMigTempThresholds():
	class Conf:
		exp_per_ec2inst = 7
		def __init__(self, slow_dev):
			self.slow_dev = slow_dev
			self.sst_mig_temp_thrds = []
		def Full(self):
			return (len(self.sst_mig_temp_thrds) >= Conf.exp_per_ec2inst)
		def Add(self, smtt):
			self.sst_mig_temp_thrds.append(smtt)
		def Size(self):
			return len(self.sst_mig_temp_thrds)
		def __repr__(self):
			return "(%s, %s)" % (self.slow_dev, self.sst_mig_temp_thrds)

	# [0.25, 256]
	for i in range(-2, 9):
		#Cons.P("%d %f" % (i, math.pow(2, i)))
		mig_temp_thrds = math.pow(2, i)

	num_exp_per_conf = 4
	confs = []
	# Redo some exps
	if True:
		for slow_dev in ["ebs-gp2", "ebs-st1", "ebs-sc1"]:
			conf = Conf(slow_dev)
			for j in range(num_exp_per_conf):
				#for i in range(-2, 9):
				for i in range(-2, -10, -1):
					if conf.Full():
						confs.append(conf)
						conf = Conf(slow_dev)
					mig_temp_thrds = math.pow(2, i)
					conf.Add(mig_temp_thrds)
			if conf.Size() > 0:
				confs.append(conf)

	if False:
		num_exp_per_conf = 2
		for slow_dev in ["ebs-st1"]:
			conf = Conf(slow_dev)
			for j in range(num_exp_per_conf):
				for i in range(1, 9):
					if conf.Full():
						confs.append(conf)
						conf = Conf(slow_dev)
					mig_temp_thrds = math.pow(2, i)
					conf.Add(mig_temp_thrds)
			if conf.Size() > 0:
				confs.append(conf)

	if False:
		num_exp_per_conf = 4
		for slow_dev in ["ebs-sc1"]:
			conf = Conf(slow_dev)
			for j in range(num_exp_per_conf):
				for i in range(2, 9):
					if conf.Full():
						confs.append(conf)
						conf = Conf(slow_dev)
					mig_temp_thrds = math.pow(2, i)
					conf.Add(mig_temp_thrds)
			if conf.Size() > 0:
				confs.append(conf)

	if False:
		num_exp_per_conf = 4
		for slow_dev in ["ebs-sc1"]:
			conf = Conf(slow_dev)
			for j in range(num_exp_per_conf):
				for i in range(0, 2):
					if conf.Full():
						confs.append(conf)
						conf = Conf(slow_dev)
					mig_temp_thrds = math.pow(2, i)
					conf.Add(mig_temp_thrds)
			if conf.Size() > 0:
				confs.append(conf)

	Cons.P("%d machines" % len(confs))
	Cons.P(pprint.pformat(confs, width=100))
	sys.exit(0)

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
		if conf.slow_dev == "ebs-gp2":
			# 100GB gp2: 300 baseline IOPS. 2,000 burst IOPS.
			params["block_storage_devs"].append({"VolumeType": "gp2", "VolumeSize": 100, "DeviceName": "d"})
		elif conf.slow_dev == "ebs-st1":
			# 1T st1: 40 MiB/s. 250 burst MiB/s. Since most of the requests will be
			# absorbed by local SSD, I think you can go a lot lower than this.
			# Redo some of the experiment with a 3T disk. From SSTable migration temperature threshold 2.
			params["block_storage_devs"].append({"VolumeType": "st1", "VolumeSize": 3000, "DeviceName": "e"})
		elif conf.slow_dev == "ebs-sc1":
			# 1T sc1: 12 MiB/s. 80 burst MiB/s
			# Redo some of the experiment with a 3T disk. From SSTable migration temperature threshold 4.
			params["block_storage_devs"].append({"VolumeType": "sc1", "VolumeSize": 3000, "DeviceName": "f"})
		else:
			raise RuntimeError("Unexpected")

		for mt in conf.sst_mig_temp_thrds:
			p1 = { \
					"exp_desc": inspect.currentframe().f_code.co_name[4:]
					, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
					, "slow_dev_paths": {"t1": "/mnt/%s/rocksdb-data-quizup-t1" % conf.slow_dev}
					, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
					, "init_db_to_90p_loaded": "true"
					, "evict_cached_data": "true"
					, "memory_limit_in_mb": 2.0 * 1024

					, "monitor_temp": "true"
					, "workload_start_from": 0.899
					, "workload_stop_at":    -1.0
					, "simulation_time_dur_in_sec": 60000
					, "sst_migration_temperature_threshold": mt
					}
			params["rocksdb-quizup-runs"].append(dict(p1))
		#Cons.P(pprint.pformat(params))
		LaunchJob(params)


def Job_TestServer():
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
	LaunchJob(params)


def Job_MutantStorageSizeByTime():
	params = { \
			# us-east-1, which is where the S3 buckets for experiment are.
			"region": "us-east-1"
			, "inst_type": "c3.2xlarge"
			, "spot_req_max_price": 1.0
			# RocksDB can use the same AMI
			, "init_script": "mutant-cassandra-server-dev"
			, "ami_name": "mutant-cassandra-server"
			# 100 GB is good enough. 300 baseline IOPS. 2,000 burst IOPS. 3T sc1 has only 36 IOPS.
			, "block_storage_devs": [{"VolumeType": "gp2", "VolumeSize": 100, "DeviceName": "d"}]
			, "unzip_quizup_data": "true"
			, "run_cassandra_server": "false"
			# For now, it doesn't do much other than checking out the code and building.
			, "rocksdb": { }
			, "rocksdb-quizup-runs": []
			, "terminate_inst_when_done": "true"
			}
	p1 = { \
			"exp_desc": inspect.currentframe().f_code.co_name[4:]
			, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
			, "slow_dev_paths": {"t1": "/mnt/ebs-gp2/rocksdb-data-quizup-t1"}
			, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"
			, "init_db_to_90p_loaded": "false"
			, "evict_cached_data": "true"
			, "memory_limit_in_mb": 2.0 * 1024

			, "monitor_temp": "true"
			, "workload_start_from": -1.0
			, "workload_stop_at":    -1.0
			, "simulation_time_dur_in_sec": 60000
			, "sst_migration_temperature_threshold": 10
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
				"exp_desc": inspect.currentframe().f_code.co_name[4:]
				, "fast_dev_path": "/mnt/%s/rocksdb-data" % conf.stg_dev
				, "db_path": "/mnt/%s/rocksdb-data/quizup" % conf.stg_dev
				, "init_db_to_90p_loaded": "true"
				, "evict_cached_data": "true"
				, "memory_limit_in_mb": 1024 * 3

				, "monitor_temp": "false"
				, "workload_start_from": 0.899
				, "workload_stop_at":    -1.0
				, "simulation_time_dur_in_sec": 60000
				}
		for ms in conf.mem_sizes:
			p1["memory_limit_in_mb"] = 1024.0 * ms
			params["rocksdb-quizup-runs"].append(dict(p1))
		LaunchJob(params)


def LaunchJob(params):
	# Spot instance
	ReqSpotInsts.Req(params)

	# On-demand instance
	#LaunchOnDemandInsts.Launch(params)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
