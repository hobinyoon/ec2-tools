# Console output utilities

import re
import sys
import threading
import time

_ind_len = 0
_ind = ""

_print_lock = threading.Lock()

def P(o, ind = 0, fo = sys.stdout, prefix = None):
	with _print_lock:
		global _ind_len, _ind
		if ind > 0:
			_ind_len += ind
			for i in range(ind):
				_ind += " "

		if _ind_len > 0:
			#print str(o).split("\n")
			lines = str(o).split("\n")
			for i in range(len(lines)):
				if (i == len(lines) - 1) and (len(lines[i]) == 0):
					continue
				if prefix is None:
					fo.write(_ind + lines[i] + "\n")
				else:
					fo.write(prefix + _ind + lines[i] + "\n")
		else:
			if prefix is not None:
				fo.write(prefix)
			fo.write(str(o))
			fo.write("\n")

		if ind > 0:
			_ind_len -= ind
			_ind = _ind[: len(_ind) - ind]


# No new-line
def Pnnl(o, ind = 0):
	with _print_lock:
		global _ind_len, _ind
		if ind > 0:
			_ind_len += ind
			for i in range(ind):
				_ind += " "

		if _ind_len > 0:
			#print str(o).split("\n")
			lines = str(o).split("\n")
			for i in range(len(lines)):
				if (i == len(lines) - 1) and (len(lines[i]) == 0):
					continue
				sys.stdout.write(_ind + lines[i])
				sys.stdout.flush()
		else:
			sys.stdout.write(o)
			sys.stdout.flush()

		if ind > 0:
			_ind_len -= ind
			_ind = _ind[: len(_ind) - ind]


# Measure time
class MT:
	def __init__(self, msg, print_time=True):
		self.msg = msg
		self.print_time = print_time

	def __enter__(self):
		P(self.msg)
		global _ind_len, _ind
		_ind_len += 2
		_ind += "  "
		if self.print_time:
			self.start_time = time.time()
		return self

	def __exit__(self, type, value, traceback):
		global _ind_len, _ind
		if self.print_time:
			dur = time.time() - self.start_time
			P("%.0f ms" % (dur * 1000.0))
		_ind_len -= 2
		_ind = _ind[: len(_ind) - 2]


# No new-line
class MTnnl:
	def __init__(self, msg):
		self.msg = msg

	def __enter__(self):
		global _ind_len, _ind
		Pnnl(self.msg)
		_ind_len += 2
		_ind += "  "
		self.start_time = time.time()
		return self

	def __exit__(self, type, value, traceback):
		global _ind_len, _ind
		dur = time.time() - self.start_time
		P("%.0f ms" % (dur * 1000.0))
		_ind_len -= 2
		_ind = _ind[: len(_ind) - 2]


def sys_stdout_write(msg):
	with _print_lock:
		sys.stdout.write(msg)
		sys.stdout.flush()


class Indent:
	def __init__(self, msg):
		self.msg = msg

	def __enter__(self):
		global _ind_len, _ind
		P(self.msg)
		_ind_len += 2
		_ind += "  "
		return self

	def __exit__(self, type, value, traceback):
		global _ind_len, _ind
		_ind_len -= 2
		_ind = _ind[: len(_ind) - 2]


def Test():
	P("aa")

	with MT("dkdkdk"):
		P(1.5)
		P(True)

	P("aa\nbb\n\n cc\n\n  dd")
	P(1)
