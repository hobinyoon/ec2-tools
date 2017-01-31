import base64
import json
import os
import pprint
import sys
import zlib

sys.path.insert(0, "%s/../lib/util" % os.path.dirname(__file__))
import Cons
import Util


_params = None

def SetParams(v):
	global _params
	_params = json.loads(zlib.decompress(base64.b64decode(v)))
	#Cons.P("_params: %s" % pprint.pformat(_params))


# This function takes either a key or a list of keys.
# When a key is given as a parameter, it returns _params[k1].
# When a list, [k1, k2, ...], is given as a parameter, it returns _params[k1][k2][...].
def GetParam(k):
	if isinstance(k, list):
		n = _params
		for k1 in k:
			if k1 not in n:
				return None
			n = n[k1]
		return n
	else:
		if k not in _params:
			return None
		else:
			return _params[k]


def GetJobId():
	return GetParam(["extra", "job_id"])


_ec2_tags = None

def SetEc2Tags(v):
	global _ec2_tags
	_ec2_tags = json.loads(v)
	Cons.P("_ec2_tags: %s" % pprint.pformat(_ec2_tags))

def GetEc2Tag(k):
	return _ec2_tags[k]


_az = None
_region = None
def GetAz():
	global _az, _region
	if _az is not None:
		return _az

	_az = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_output = False)
	_region = _az[:-1]
	return _az

def GetRegion():
	global _az, _region
	if _region is not None:
		return _region

	_az = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_output = False)
	_region = _az[:-1]
	return _region


_pub_ip = None
def GetPubIp():
	global _pub_ip
	if _pub_ip is not None:
		return _pub_ip

	_pub_ip = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/public-ipv4", print_cmd = False, print_output = False)
	return _pub_ip


def SyncTime():
	# Sync time. Not only important for Cassandra, it helps consistent analysis
	# of logs across the server nodes and the client node.
	# - http://askubuntu.com/questions/254826/how-to-force-a-clock-update-using-ntp
	with Cons.MT("Synching time ..."):
		Util.RunSubp("sudo service ntp stop || true")

		# Fails with a rc 1 in the init script. Mask with true for now.
		Util.RunSubp("sudo /usr/sbin/ntpd -gq || true")

		Util.RunSubp("sudo service ntp start")


def ChangeLogOutput():
	with Cons.MT("Changing log output the local SSD ..."):
		dn_log_ssd0 = "/mnt/local-ssd0/mutant/log"
		dn_log = "/home/ubuntu/work/mutant/log"

		Util.RunSubp("mkdir -p %s" % dn_log_ssd0)

		# Create a symlink
		Util.RunSubp("rm %s || true" % dn_log)
		Util.RunSubp("ln -s %s %s" % (dn_log_ssd0, dn_log))

		dn = "%s/%s/%s" % (dn_log, GetJobId()
				, GetEc2Tag("name").replace("server", "s").replace("client", "c"))
		Util.MkDirs(dn)

		# Redict stdout to the log file in local SSD
		fn = "%s/cloud-init" % dn
		Cons.P("Redirecting stdout to %s" % fn)

		# buffering to 1, "line buffered"
		fo = open(fn, "a", 1)
		sys.stdout = fo
		Cons.SetStdout(fo)
