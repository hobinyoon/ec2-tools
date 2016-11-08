# Workload A: Update heavy workload
def A():
	ycsb_params = ""
	ycsb_params += " -threads 100"

	# The server can handle >6000 IOPS when unthrottled
	# With 4000, server CPU load < 20%
	#
	# CPU up to 47%, when running multiple compactions.
	ycsb_params += " -target 4000"

	ycsb_params += " -p recordcount=20000000"

	# 100000 : 27736.0 ms = ? : 1 h
	#ycsb_params += " -p operationcount=100000"
	# 12979521 operations to run for 1 hour.
	ycsb_params += " -p operationcount=12979521"

	ycsb_params += " -p status.interval=1"
	ycsb_params += " -p fieldcount=10"
	ycsb_params += " -p fieldlength=100"
	return {
			"workload_type": "a"
			, "params" : ycsb_params
			}


# Workload B: Read mostly workload
def B():
	ycsb_params = ""
	ycsb_params += " -threads 100"
	ycsb_params += " -target 2000"
	ycsb_params += " -p recordcount=20000000"

	# Both target IOPS and operation count to the halves of workload a, since
	# it's read-heavy, it will be more server resource intensive.
	ycsb_params += " -p operationcount=6489760"

	ycsb_params += " -p status.interval=1"
	ycsb_params += " -p fieldcount=10"
	ycsb_params += " -p fieldlength=100"
	return {
			"workload_type": "b"
			, "params" : ycsb_params
			}


# Workload C: Read only
def C():
	ycsb_params = ""
	ycsb_params += " -threads 100"
	ycsb_params += " -target 2000"
	ycsb_params += " -p recordcount=20000000"

	# Load set to the same level as B
	ycsb_params += " -p operationcount=6489760"

	ycsb_params += " -p status.interval=1"
	ycsb_params += " -p fieldcount=10"
	ycsb_params += " -p fieldlength=100"
	return {
			"workload_type": "c"
			, "params" : ycsb_params
			}


# Workload C: Read only
def C_uniform():
	ycsb_params = ""
	ycsb_params += " -threads 100"
	ycsb_params += " -target 2000"
	ycsb_params += " -p recordcount=20000000"

	# Load set to the same level as B
	ycsb_params += " -p operationcount=6489760"

	ycsb_params += " -p status.interval=1"
	ycsb_params += " -p fieldcount=10"
	ycsb_params += " -p fieldlength=100"
	ycsb_params += " -p requestdistribution=uniform"
	return {
			"workload_type": "c"
			, "params" : ycsb_params
			}


# Workload D: Read latest workload
def D():
	return {
			"workload_type": "d"
			, "params" : "-p recordcount=1000" \
					" -p operationcount=400000000" \
					" -p status.interval=1" \
					" -p fieldcount=10" \
					" -p fieldlength=100" \
					" -threads 100" \
					" -target 17000"
					}


# Workload E: Short ranges
def E():
	return {
			"workload_type": "e"
			, "params" : "-p recordcount=1000" \
					" -p operationcount=400000000" \
					" -p status.interval=1" \
					" -p fieldcount=10" \
					" -p fieldlength=100" \
					" -threads 100" \
					# Just taking a guess. It scans "zipian(100) with 0.99" records at
					# once. Not sure tell how many it is on average. Let's say 3.  Then
					# roughly 17000 / 3 = 5400, ignoring the write overhead.
					" -target 5000"
					}
