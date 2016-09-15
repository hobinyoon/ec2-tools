import base64
import json
import pprint


_params = None

def SetParams(v):
	global _params
	_params = json.loads(base64.b64decode(v))
	#Cons.P("_params: %s" % pprint.pformat(_params))

def GetParam(k):
	return _params[k]


def GetJobId():
	return GetParam("extra")["job_id"]


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


_inst_id = None
def InstId():
	global _inst_id
	if _inst_id is not None:
		return _inst_id

	_inst_id  = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-id", print_cmd = False, print_output = False)
	return _inst_id


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
	dn_log_ssd0 = "/mnt/local-ssd0/mutants/log"
	dn_log = "/home/ubuntu/work/mutants/log"

	Util.RunSubp("mkdir -p %s" % dn_log_ssd0)

	# Create a symlink
	Util.RunSubp("rm %s || true" % dn_log)
	Util.RunSubp("ln -s %s %s" % (dn_log_ssd0, dn_log))

	dn = "%s/%s" % (dn_log, Ec2InitUtil.GetJobId())
	Util.RunSubp("mkdir -p %s" % dn)

	# Redict stdout to the log file in local SSD
	fn = "%s/%s/cloud-init" % (dn, Ec2InitUtil.GetEc2Tag("name").replace("server", "s").replace("client", "c"))
	Cons.P("Redirecting stdout to %s" % fn)

	fo = open(fn, "a")
	sys.stdout = fo
	Cons.SetStdout(fo)


