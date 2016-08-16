import os
import yaml

_conf = None


def _Load():
	global _conf
	if _conf is None:
		fn = "%s/../conf/ec2-tools.yaml" % os.path.dirname(__file__)
		with open(fn) as fo:
			_conf = yaml.safe_load(fo)


def Get():
	_Load()
	return _conf
