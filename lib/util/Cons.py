# Console output utilities

import re
import sys
import threading
import time

_ind_len = 0
_ind = ""

_print_lock = threading.Lock()

_stdout = sys.stdout

def SetStdout(fo):
	global _stdout
	_stdout = fo


def P(o, ind = 0, prefix = None):
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
					_stdout.write(_ind + lines[i] + "\n")
				else:
					_stdout.write(prefix + _ind + lines[i] + "\n")
		else:
			if prefix is not None:
				_stdout.write(prefix)
			_stdout.write(str(o))
			_stdout.write("\n")

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
				_stdout.write(_ind + lines[i])
				_stdout.flush()
		else:
			_stdout.write(o)
			_stdout.flush()

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
	def __init__(self, msg, print_time=True):
		self.msg = msg
		self.print_time = print_time

	def __enter__(self):
		global _ind_len, _ind
		Pnnl(self.msg)
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


def sys_stdout_write(msg):
	with _print_lock:
		_stdout.write(msg)
		_stdout.flush()


def ClearLine():
	sys.stdout.write("\033[1K") # Clear to the beginning of line
	sys.stdout.write("\033[1G") # Move the cursor to the beginning of the column


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
