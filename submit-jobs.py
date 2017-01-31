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

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
import Ec2Region

import LaunchOnDemandInsts


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
		Cons.P("  Jobs available: %s" % " ".join(job_list))
		sys.exit(1)

	job = "Job_" + argv[1]

	# http://stackoverflow.com/questions/3061/calling-a-function-of-a-module-from-a-string-with-the-functions-name-in-python
	globals()[job]()


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
				#{"VolumeType": "gp2", "VolumeSize": 1000, "DeviceName": "d"}

				# 3TB st1 for 120 Mib/s, 500 Mib/s (burst) throughput.
				#   http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html
				#{"VolumeType": "st1", "VolumeSize": 3000, "DeviceName": "e"}

				# 3TB sc1 for 36 Mib/s, 240 Mib/s (burst).
				{"VolumeType": "sc1", "VolumeSize": 3000, "DeviceName": "f"}
				]
			, "unzip_quizup_data": "true"

			, "run_cassandra_server": "false"

			# For now, it doesn't do much other than checking out the code and building.
			, "rocksdb": { }

			, "rocksdb-quizup-runs": []
			}

	p1 = { \
			"mutant_enabled": "true"
			, "sst_migration_temperature_threshold": [200, 150, 100, 50, 40, 30, 20, 15, 10, 5, 4, 3, 2, 1] * 2
			, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"
			#, "slow_dev_paths": {"t1": "/mnt/ebs-gp2/rocksdb-data-quizup-t1"}
			#, "slow_dev_paths": {"t1": "/mnt/ebs-st1/rocksdb-data-quizup-t1"}
			, "slow_dev_paths": {"t1": "/mnt/ebs-sc1/rocksdb-data-quizup-t1"}
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
	LaunchOnDemandInsts.Launch(params)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
