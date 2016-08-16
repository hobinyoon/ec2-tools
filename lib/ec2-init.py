#!/usr/bin/env python

import base64
import datetime
import getpass
import imp
import json
import os
import pprint
import re
import sys
import time
import traceback

sys.path.insert(0, "%s/util" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util

import BotoClient


_inst_id = None
_region = None


def _Log(msg):
	Cons.P("%s: %s" % (time.strftime("%y%m%d-%H%M%S"), msg))


def _LogInstInfo():
	ami_id    = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/ami-id", print_cmd = False, print_output = False)
	global _inst_id
	_inst_id  = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-id", print_cmd = False, print_output = False)
	inst_type = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-type", print_cmd = False, print_output = False)
	az        = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_output = False)
	pub_ip    = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/public-ipv4", print_cmd = False, print_output = False)
	local_ip  = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/local-ipv4", print_cmd = False, print_output = False)

	# http://stackoverflow.com/questions/4249488/find-region-from-within-ec2-instance
	doc       = Util.RunSubp("curl -s http://169.254.169.254/latest/dynamic/instance-identity/document", print_cmd = False, print_output = False)
	for line in doc.split("\n"):
		# "region" : "us-west-1"
		tokens = filter(None, re.split(":| |,|\"", line))
		#_Log(tokens)
		if len(tokens) == 2 and tokens[0] == "region":
			global _region
			_region = tokens[1]
			break

	_Log("ami_id:    %s" % ami_id)
	_Log("inst_id:   %s" % _inst_id)
	_Log("inst_type: %s" % inst_type)
	_Log("az:        %s" % az)
	_Log("pub_ip:    %s" % pub_ip)
	_Log("local_ip:  %s" % local_ip)
	_Log("region:    %s" % _region)


def _RunInitScript(params_encoded):
	_Log("params_encoded: %s" % params_encoded)
	params = json.loads(base64.b64decode(params_encoded))
	_Log("params: %s" % pprint.pformat(params))

	type_ = params["extra"]["type"]
	fn_init_script = params[type_]["init_script"]
	_Log("fn_init_script: %s" % fn_init_script)

	r = BotoClient.Get(_region).describe_tags()
	#_Log(pprint.pformat(r, indent=2, width=100))
	tags = {}
	for r0 in r["Tags"]:
		res_id = r0["ResourceId"]
		if _inst_id != res_id:
			continue
		if _inst_id == res_id:
			tags[r0["Key"]] = r0["Value"]
	tags_json = json.dumps(tags)
	_Log("tags_json: %s" % tags_json)

	fn_module = "%s/../ec2-init.d/%s.py" % (os.path.dirname(__file__), fn_init_script)
	mod_name,file_ext = os.path.splitext(os.path.split(fn_module)[-1])
	if file_ext.lower() != '.py':
		raise RuntimeError("Unexpected file_ext: %s" % file_ext)
	try:
		py_mod = imp.load_source(mod_name, fn_module)
	except IOError as e:
		_Log("fn_module: %s" % fn_module)
		raise e
	getattr(py_mod, "main")([fn_module, params_encoded, tags_json])


def main(argv):
	# This script is run under the user 'ubuntu'.
	#Util.RunSubp("touch /tmp/%s" % getpass.getuser())

	if len(argv) != 2:
		raise RuntimeError("Usage: %s base64_json_encoded_params" % argv[0])

	_LogInstInfo()
	_RunInitScript(argv[1])


if __name__ == "__main__":
	sys.exit(main(sys.argv))
