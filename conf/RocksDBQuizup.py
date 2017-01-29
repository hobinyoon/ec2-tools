# Storage usage measurement for unmodified RocksDB and Mutant with
# sstable migration threshold 10.
storage_usage_measurement_unmodified_rocksdb = {
	"enable_mutant": "false"

	# TODO: modify quizup client to setup the db_db_paths and make symbolic links
	, "db_db_paths": {"t0": "~/work/rocksdb-data/quizup/t0"
		}

	# TODO: Do not load existing DB
	, "load_existing_db": "false"
}

storage_usage_measurement_mutant = {
	"enable_mutant": "true"

	# TODO: modify quizup client to setup the db_db_paths and make symbolic links
	, "db_db_paths": {"t0": "~/work/rocksdb-data/quizup/t0"
		, "t1": "/mnt/ebs-gp2/rocksdb-data/quizup/t1"
		}

	, "load_existing_db": "false"
}
