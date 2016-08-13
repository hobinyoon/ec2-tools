#!/usr/bin/env python

import datetime
import getpass
import imp
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


def _RunInitByTags():
	_Log("_fn_init_script           : %s" % _fn_init_script)
	_Log("_jr_sqs_url               : %s" % _jr_sqs_url)
	_Log("_jr_sqs_msg_receipt_handle: %s" % _jr_sqs_msg_receipt_handle)
	_Log("_num_regions              : %s" % _num_regions)

	r = BotoClient.Get(_region).describe_tags()
	#_Log(pprint.pformat(r, indent=2, width=100))
	tags = {}
	for r0 in r["Tags"]:
		res_id = r0["ResourceId"]
		if _inst_id != res_id:
			continue
		if _inst_id == res_id:
			tags[r0["Key"]] = r0["Value"]
	tags_str = ",".join(["%s:%s" % (k, v) for (k, v) in sorted(tags.items())])
	_Log("tags_str: %s" % tags_str)

	fn_module = "%s/ec2-init.d/%s.py" % (os.path.dirname(__file__), _fn_init_script)
	mod_name,file_ext = os.path.splitext(os.path.split(fn_module)[-1])
	if file_ext.lower() != '.py':
		raise RuntimeError("Unexpected file_ext: %s" % file_ext)
	py_mod = imp.load_source(mod_name, fn_module)
	getattr(py_mod, "main")([fn_module, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _num_regions, tags_str])


_fn_init_script = None
_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None
_num_regions = None

def main(argv):
	# This script is run under the user 'ubuntu'.
	#Util.RunSubp("touch /tmp/%s" % getpass.getuser())

	if len(argv) != 5:
		raise RuntimeError("Usage: %s init_script jr_sqs_url jr_sqs_msg_receipt_handle num_regions\n"
				"  E.g.: %s mutants-server None None 1"
				% (argv[0], argv[0]))

	global _fn_init_script
	global _jr_sqs_url
	global _jr_sqs_msg_receipt_handle
	global _num_regions
	_fn_init_script = argv[1]
	_jr_sqs_url = argv[2]
	_jr_sqs_msg_receipt_handle = argv[3]
	_num_regions = argv[4]

	_LogInstInfo()
	_RunInitByTags()


if __name__ == "__main__":
	sys.exit(main(sys.argv))
