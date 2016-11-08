import os
import pprint
import socket
import sys

sys.path.insert(0, "%s/../lib/util" % os.path.dirname(__file__))
import Cons
import Util


_job_id = None
def JobId():
	global _job_id
	if _job_id is not None:
		return _job_id

	hn = socket.gethostname()
	t = hn.split("-")

	# It can happen when you are not on a EC2 machine created for mutant. For
	# instance, mjolnir.
	if len(t) < 4:
		return None

	_job_id = t[3] + "-" + t[4]
	return _job_id


# For example, c, s0, s1, ...
_node_name = None
def NodeName():
	global _node_name
	if _node_name is not None:
		return _node_name

	hn = socket.gethostname()
	t = hn.split("-")

	# It can happen when you are not on a EC2 machine created for mutant. For
	# instance, mjolnir.
	if len(t) < 5:
		return None

	_node_name = t[5]
	return _node_name


_inst_id = None
def InstId():
	global _inst_id
	if _inst_id is not None:
		return _inst_id

	_inst_id  = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-id", print_cmd = False, print_output = False)
	return _inst_id
