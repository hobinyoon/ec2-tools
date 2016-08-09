import botocore
import os
import sys
import threading
import time

sys.path.insert(0, "%s/util" % os.path.dirname(__file__))
import Cons
import Util

import SpotPrice


class SpotInstLaunchProgMon:
	def __enter__(self):
		# The key is region. Spot req ID can't be used. A status (or an error) can be
		# returned before a spot request id is returned.
		# {region: [status]}
		self.status_by_regions = {}
		self.status_by_regions_lock = threading.Lock()
		self.stop_requested = False

		self.t = threading.Thread(target=self.Run)
		self.t.daemon = True
		self.t.start()
		return self


	def __exit__(self, type, value, traceback):
		self.stop_requested = True
		self.t.join()


	def Run(self):
		output_lines_written = 0
		while True:
			status_by_regions = None
			with self.status_by_regions_lock:
				status_by_regions = self.status_by_regions.copy()

			output = ""
			for region, status in sorted(status_by_regions.iteritems()):
				if len(output) > 0:
					output += "\n"
				output += ("%-15s" % region)

				prev_s = None
				same_s_cnt = 1

				for s in status:
					s1 = None
					if type(s) is str:
						s1 = s
					elif isinstance(s, DescSpotInstResp):
						s1 = s.r["SpotInstanceRequests"][0]["Status"]["Code"]
					elif isinstance(s, DescInstResp):
						s1 = s.r["Reservations"][0]["Instances"][0]["State"]["Name"]
					elif isinstance(s, Error):
						if isinstance(s.e, botocore.exceptions.ClientError):
							s1 = s.e.response["Error"]["Code"]
						else:
							s1 = str(s.e)
					else:
						raise RuntimeError("Unexpected s: %s" % s)

					# Print prev one when it's different from the current one
					if prev_s == s1:
						same_s_cnt += 1
					else:
						if prev_s is not None:
							if same_s_cnt == 1:
								if len(output.split("\n")[-1]) > 100:
									output += "\n               "
								output += (" %s" % prev_s)
							else:
								if len(output.split("\n")[-1]) > 100:
									output += "\n               "
								output += (" %s x%2d" % (prev_s, same_s_cnt))
							same_s_cnt = 1
					prev_s = s1

				# Print the last one
				if same_s_cnt == 1:
					if len(output.split("\n")[-1]) > 100:
						output += "\n               "
					output += (" %s" % prev_s)
				else:
					if len(output.split("\n")[-1]) > 100:
						output += "\n               "
					output += (" %s x%2d" % (prev_s, same_s_cnt))

			# Clear prev output
			if output_lines_written > 0:
				for l in range(output_lines_written - 1):
					# Clear current line
					sys.stdout.write(chr(27) + "[2K")
					# Move up
					sys.stdout.write(chr(27) + "[1F")
				# Clear current line
				sys.stdout.write(chr(27) + "[2K")
				# Move the cursor to column 1
				sys.stdout.write(chr(27) + "[1G")

			sys.stdout.write(output)
			sys.stdout.flush()
			output_lines_written = len(output.split("\n"))

			# Are we done?
			if self.stop_requested:
				break

			# Update status every so often
			time.sleep(0.1)
		print ""

		self._DescInsts()


	def _DescInsts(self):
		fmt = "%-15s %19s %10s %13s %15s %10s %6.4f"
		Cons.P(Util.BuildHeader(fmt,
			"Placement:AvailabilityZone"
			" InstanceId"
			" InstanceType"
			" LaunchTime"
			#" PrivateIpAddress"
			" PublicIpAddress"
			" State:Name"
			" CurSpotPrice"
			))

		with self.status_by_regions_lock:
			for region, status in sorted(self.status_by_regions.iteritems()):
				for s in reversed(status):
					if isinstance(s, DescInstResp):
						# Print only the last desc instance response per region
						r = s.r["Reservations"][0]["Instances"][0]
						az = _Value(_Value(r, "Placement"), "AvailabilityZone")
						Cons.P(fmt % (
							az
							, _Value(r, "InstanceId")
							, _Value(r, "InstanceType")
							, _Value(r, "LaunchTime").strftime("%y%m%d-%H%M%S")
							#, _Value(r, "PrivateIpAddress")
							, _Value(r, "PublicIpAddress")
							, _Value(_Value(r, "State"), "Name")
							, SpotPrice.GetCurPrice(az)
							))
						break


	def SetSpotReqId(self, region, spot_req_id):
		with self.status_by_regions_lock:
			if region not in self.status_by_regions:
				self.status_by_regions[region] = []
			self.status_by_regions[region].append(spot_req_id)


	def UpdateDescSpotInstResp(self, region, r):
		with self.status_by_regions_lock:
			self.status_by_regions[region].append(DescSpotInstResp(r))


	def SetInstID(self, region, inst_id):
		with self.status_by_regions_lock:
			self.status_by_regions[region].append(inst_id)


	def UpdateDescInstResp(self, region, r):
		with self.status_by_regions_lock:
			self.status_by_regions[region].append(DescInstResp(r))


	def UpdateError(self, region, e):
		with self.status_by_regions_lock:
			if region not in self.status_by_regions:
				self.status_by_regions[region] = []
			self.status_by_regions[region].append(Error(e))


class DescSpotInstResp():
	def __init__(self, r):
		self.r = r


class DescInstResp():
	def __init__(self, r):
		self.r = r


class Error():
	def __init__(self, e):
		self.e = e


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""
