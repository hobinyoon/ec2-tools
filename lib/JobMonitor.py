import botocore
import datetime
import os
import pprint
import Queue
import sys
import threading
import time
import traceback

sys.path.insert(0, "%s/util" % os.path.dirname(__file__))
import Cons
import Util

import BotoClient
import Ec2Region
import SpotPrice


_mode = None
_stop_requested = False
_cv = None
_dio = None
_t = None

_lock = threading.Lock()

_in_restart = False

def Restart():
	if _in_restart:
		return

	# This needs to be immediate in the same thread.
	if _dio is not None:
		_dio.MayPrintNewlines()

	t = threading.Thread(target=_Restart)
	t.daemon = True
	t.start()


def Stop():
	with _lock:
		_Stop()


# Called by desc-insts.py
def RunOnce():
	global _mode, _stop_requested, _cv
	_mode = "run_once"
	_stop_requested = False
	dio = Dio(buffered=False)
	_cv = threading.Condition()
	_Run(dio)


def _Restart():
	with _lock:
		global _in_restart
		_in_restart = True
		_Stop()
		_Start()
		_in_restart = False


def _Stop():
	global _stop_requested, _t
	if _t is None:
		return
	_stop_requested = True
	with _cv:
		_cv.notifyAll()
	_t.join()
	_t = None


def _Start():
	global _mode, _stop_requested, _cv, _dio, _t
	_mode = "run_until_stopped"
	_stop_requested = False
	_dio = Dio()
	_cv = threading.Condition()

	_t = threading.Thread(target=_Run, args=[_dio])
	_t.daemon = True
	_t.start()


_desc_inst_start_time = None
_monitor_interval_in_sec = 10

def _Run(dio):
	try:
		global _desc_inst_start_time
		_desc_inst_start_time = datetime.datetime.now()
		if _mode == "run_until_stopped":
			with _cv:
				_cv.wait(_monitor_interval_in_sec)
		while _stop_requested == False:
			bt = time.time()
			_DescInst(dio)
			wait_time = _monitor_interval_in_sec - (time.time() - bt)
			if wait_time > 0:
				with _cv:
					_cv.wait(wait_time)
		dio.MayPrintNewlines()
	except Exception as e:
		Cons.P("\n%s Got an exception: %s\n%s" % (time.strftime("%y%m%d-%H%M%S"), e, traceback.format_exc()))
		os._exit(1)


def _DescInst(dio):
	if _mode == "run_until_stopped":
		dio.P("\n")
	dio.P("# Describing instances:")

	DescInstPerRegion.Reset()

	region_desc_inst = {}
	for r in Ec2Region.All():
		region_desc_inst[r] = DescInstPerRegion(r, dio)

	if _stop_requested:
		return

	threads = []
	for r, di in region_desc_inst.iteritems():
		t = threading.Thread(target=di.Run)
		threads.append(t)
		t.daemon = True
		t.start()

	# Exit immediately when requested
	for t in threads:
		while t.isAlive():
			if _stop_requested:
				return
			t.join(0.1)

	dio.P("\n#\n")

	num_insts = 0
	for r, di in region_desc_inst.iteritems():
		num_insts += len(di.Instances())

	if num_insts == 0:
		dio.P("No instances found.\n")
	else:
		# Header
		fmt = "%-15s %13s %-10s %6.4f %2s %19s %15s %13s"
		dio.P(Util.BuildHeader(fmt,
				"az"
				" job_id"
				" inst_type"
				" cur_spot_price"
				" name"
				" InstanceId"
				" PublicIpAddress"
				" State:Name") + "\n")

		for r, di in sorted(region_desc_inst.iteritems()):
			for i in di.Instances():
				# Note: could be grouped by job_id later
				dio.P((fmt + "\n") % (
					i.az
					, i.job_id
					, i.inst_type
					, SpotPrice.GetCur(i.az, i.inst_type)
					, i.name.replace("server", "s").replace("client", "c")
					, i.inst_id
					, i.public_ip
					, i.state
					))
	if _mode == "run_once":
		sys.exit(0)

		# Note: JobCleaner could use this node info

	if _stop_requested:
		return

	if _mode == "run_until_stopped":
		# Since the last JobContConsole output
		dio.P("# Time since the last msg: %s" % (datetime.datetime.now() - _desc_inst_start_time))
		dio.Flush()


# Describe instance output
class Dio:
	max_column_width = 120

	def __init__(self, buffered = True):
		# buffered could be useful for debugging
		self.buffered = buffered
		self.msg = ""
		self.msg_lock = threading.Lock()
		self.lines_printed = 0

	def P(self, msg):
		with self.msg_lock:
			self.msg += msg
			if self.buffered == False:
				Cons.Pnnl(msg)
				sys.stdout.flush()

	def LastLineWidth(self):
		with self.msg_lock:
			return len(self.msg.split("\n")[-1])

	def Flush(self):
		with self.msg_lock:
			# Clear previous printed lines
			if self.lines_printed > 0:
				for i in range(self.lines_printed - 1):
					# Clear current line
					sys.stdout.write(chr(27) + "[2K")
					# Move the cursor up
					sys.stdout.write(chr(27) + "[1A")
					# Move the cursor to column 1
					sys.stdout.write(chr(27) + "[1G")
				# Clear current line
				sys.stdout.write(chr(27) + "[2K")

			Cons.Pnnl(self.msg)
			self.lines_printed = len(self.msg.split("\n"))
			self.msg = ""

	def MayPrintNewlines(self):
		with self.msg_lock:
			if self.lines_printed > 0:
				Cons.P("\n")
				self.lines_printed = 0
				self.msg = ""


class DescInstPerRegion:
	boto_responses_received = 0
	boto_responses_received_lock = threading.Lock()

	@staticmethod
	def Reset():
		with DescInstPerRegion.boto_responses_received_lock:
			DescInstPerRegion.boto_responses_received = 0

	def __init__(self, region, dio):
		self.region = region
		self.dio = dio
		self.instances = []

	def Run(self):
		while True:
			try:
				response = BotoClient.Get(self.region).describe_instances()
				self.instances = []
				for r in response["Reservations"]:
					for r1 in r["Instances"]:
						if _Value(_Value(r1, "State"), "Name") == "terminated":
							continue
						self.instances.append(DescInstPerRegion.Inst(r1))
				self.instances.sort()

				# Show progress by region
				with DescInstPerRegion.boto_responses_received_lock:
					DescInstPerRegion.boto_responses_received += 1
					if DescInstPerRegion.boto_responses_received == 7:
						#             # Describing instances:
						self.dio.P("\n#                       %s" % self.region)
					else:
						self.dio.P(" %s" % self.region)
				return
			except (botocore.exceptions.ClientError, botocore.exceptions.EndpointConnectionError) as e:
				Cons.P("%s. Region=%s. Resetting boto client after 1 sec ..." % (e, self.region))
				time.sleep(1)
				BotoClient.Reset(self.region)

	def Instances(self):
		return self.instances

	class Inst():
		def __init__(self, desc_inst_resp):
			self.tags = {}
			if "Tags" in desc_inst_resp:
				for t in desc_inst_resp["Tags"]:
					self.tags[t["Key"]] = t["Value"]

			self.job_id = self.tags.get("job_id", "")
			self.name = self.tags.get("name", "")
			self.az = _Value(_Value(desc_inst_resp, "Placement"), "AvailabilityZone")
			self.region = self.az[:-1]
			self.inst_id = _Value(desc_inst_resp, "InstanceId")
			self.inst_type = _Value(desc_inst_resp, "InstanceType")
			self.public_ip = _Value(desc_inst_resp, "PublicIpAddress")
			self.state = _Value(_Value(desc_inst_resp, "State"), "Name")

		def __lt__(self, other):
			if self.az < other.az:
				return True
			elif self.az > other.az:
				return False

			if self.job_id < other.job_id:
				return True
			elif self.job_id > other.job_id:
				return False

			if self.name < other.name:
				return True
			elif self.name > other.name:
				return False

			return (self.inst_id < other.inst_id)



def _Value(dict_, key):
	if dict_ is None:
		return None
	if key is None:
		return None
	return dict_.get(key)
