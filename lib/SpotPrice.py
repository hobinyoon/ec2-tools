import datetime
import os
import pickle
import sys
import threading
import traceback

sys.path.insert(0, "%s/util" % os.path.dirname(__file__))
import Util

import BotoClient
import JobContOutput


_lock = threading.Lock()


class SpKey:
	def __init__(self, region, inst_type):
		self.region = region
		self.inst_type = inst_type

	def __hash__(self):
		# () is for tuple
		return hash((self.region, self.inst_type))

	def __eq__(self, other):
		return (self.region, self.inst_type) == (other.region, other.inst_type)

	def __str__(self):
		return "%s %s" % (self.region, self.inst_type)


class SpValue:
	def __init__(self, az_price, time_checked):
		# {az: [cur, 2d_avg, 2d_max]}
		self.az_price = az_price
		self.time_checked = time_checked

	def Valid(self):
		diff = datetime.datetime.now() - self.time_checked
		return (diff.seconds < 300)

	def MostStableAz(self):
		az_ms = None
		p_ms = None
		for az, p in self.az_price.iteritems():
			if az_ms is None:
				az_ms = az
				p_ms = p[2]
			else:
				if p[2] < p_ms:
					az_ms = az
					p_ms = p[2]
		return az_ms

	def CurPrice(self, az):
		return self.az_price[az][0]

	def __str__(self):
		fmt = "%-15s %6.4f %6.4f %6.4f"
		o = ""
		#o += ("# time checked: %s" % (self.time_checked.strftime("%y%m%d-%H%M%S")))
		o += Util.BuildHeader(fmt, "Az cur 2d_avg 2d_max")

		for k, v in sorted(self.az_price.iteritems()):
			o += ("\n" + fmt) % (k, v[0], v[1], v[2])
		return o


class Cache:
	def __init__(self):
		dn = "%s/../.run" % os.path.dirname(__file__)
		Util.MkDirs(dn)
		self.fn = "%s/spot-price-cache" % dn
		self.d_lock = threading.Lock()
		self.d = {}
		self._LoadFromFile()

	def _LoadFromFile(self):
		try:
			with open(self.fn) as fo:
				with self.d_lock:
					self.d = pickle.load(fo)
		except IOError:
			pass

	def Get(self, k):
		with self.d_lock:
			return self.d.get(k)

	def Put(self, k, v):
		with self.d_lock:
			self.d[k] = v
			# Write to file
			with open(self.fn, "w") as fo:
				# json doesn't support dumping object as a key
				pickle.dump(self.d, fo)


# Query at most once for a 5 minute intervals. The second and later requests
# get the cached result.
#
# {SpKey: SpValue}
_cache = Cache()


def MostStableAz(region, inst_type):
	k = SpKey(region, inst_type)

	# Multiple job requests can be in flight. Protect cache and boto query.
	with _lock:
		global _cache
		v = _cache.Get(k)
		if (v is not None) and v.Valid():
			return v.MostStableAz()

		v = _GetSpotPrice(k)
		_cache.Put(k, v)
		return v.MostStableAz()


def GetCur(az, inst_type):
	region = az[:-1]
	k = SpKey(region, inst_type)

	v = None
	with _lock:
		global _cache
		v = _cache.Get(k)
		if (v is not None) and v.Valid():
			return v.CurPrice(az)

		v = _GetSpotPrice(k)
		_cache.Put(k, v)
		return v.CurPrice(az)


def _GetSpotPrice(k):
	try:
		now = datetime.datetime.now()
		start_time = now - datetime.timedelta(days=2)
		JobContOutput.P("Getting spot prices for (%s) ..." % k)

		r = BotoClient.Get(k.region).describe_spot_price_history(
				StartTime = start_time,
				EndTime = now,
				ProductDescriptions = ["Linux/UNIX"],
				InstanceTypes = [k.inst_type],
				)

		# {az: {timestamp: price}}
		az_ts_price = {}
		for sp in r["SpotPriceHistory"]:
			az = sp["AvailabilityZone"]
			ts = sp["Timestamp"]
			sp = float(sp["SpotPrice"])
			if az not in az_ts_price:
				az_ts_price[az] = {}
			az_ts_price[az][ts] = sp

		if len(az_ts_price) == 0:
			raise RuntimeError("No price history for (%s)" % k)

		# {az, [price_cur, price_avg, price_max]}
		az_price = {}
		for az, v in sorted(az_ts_price.iteritems()):
			ts_prev = None
			price_prev = None
			dur_sum = 0
			dur_price_sum = 0.0
			price_max = 0.0
			price_avg = 0.0

			for ts, price in sorted(v.iteritems()):
				if ts_prev is not None:
					dur = (ts - ts_prev).total_seconds()
					dur_sum += dur
					dur_price_sum += (dur * price)

				price_max = max(price, price_max)
				ts_prev = ts
				price_prev = price

			if dur_sum != 0.0:
				price_avg = dur_price_sum / dur_sum

			az_price[az] = [price_prev, price_avg, price_max]
		v = SpValue(az_price, now)
		JobContOutput.P("Spot prices for (%s):\n%s" % (k, Util.Indent(str(v), 2)))
		return v
	except Exception as e:
		JobContOutput.P("%s\nSpKey=[%s]\n%s" % (e, k, traceback.format_exc()))
		os._exit(1)
