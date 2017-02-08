# Storage usage measurement for unmodified RocksDB and Mutant with
# sstable migration threshold 10.
storage_usage_measurement_unmodified_rocksdb = {
	"monitor_temp": "false"

	# Fast device paths.
	, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"

	# Slow device paths.
	, "slow_dev_paths": {"t1": "/mnt/ebs-gp2/rocksdb-data-quizup-t1"}

	# Main db_path. ~/work/rocksdb-data will be symlinked to this.
	, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"

	# Init with the 90%-loaded db
	, "init_db_to_90p_loaded": "false"

	, "evict_cached_data": "true"

	# Workload start and stop in percent. -1.0 for undefined.
	, "workload_start_from": -1.0
	, "workload_stop_at":    -1.0

	, "simulation_time_dur_in_sec": 60000

	, "terminate_inst_when_done": "true"
}

storage_usage_measurement_mutant = {
	"monitor_temp": "true"

	# Fast device paths.
	, "fast_dev_path": "/mnt/local-ssd1/rocksdb-data"

	# Slow device paths.
	, "slow_dev_paths": {"t1": "/mnt/ebs-gp2/rocksdb-data-quizup-t1"}

	# Main db_path. ~/work/rocksdb-data will be symlinked to this.
	, "db_path": "/mnt/local-ssd1/rocksdb-data/quizup"

	# Init with the 90%-loaded db
	, "init_db_to_90p_loaded": "false"

	, "evict_cached_data": "true"

	# Workload start and stop in percent. -1.0 for undefined.
	, "workload_start_from": -1.0
	, "workload_stop_at":    -1.0

	#, "simulation_time_dur_in_sec": 60000
	# Run faster for test
	, "simulation_time_dur_in_sec": 2000

	, "terminate_inst_when_done": "true"
}
