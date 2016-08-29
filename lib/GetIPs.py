#!/usr/bin/env python

import boto3
import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/util" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util

import BotoClient
import Ec2Region


def GetServerPubIpsByJobId(job_id):
	threads = []

	dis = []
	for r in Ec2Region.All():
		dis.append(_DescInst(r, {"job_id": job_id}))

	for di in dis:
		t = threading.Thread(target=di.Run)
		threads.append(t)
		t.start()
	for t in threads:
		t.join()

	ips = []
	for di in dis:
		ips.extend(di.GetIPs())
	return ips


#def GetByTags(tags):
#	threads = []
#
#	dis = []
#	for r in Ec2Region.All():
#		dis.append(_DescInst(r, tags))
#
#	for di in dis:
#		t = threading.Thread(target=di.Run)
#		threads.append(t)
#		t.start()
#
#	for t in threads:
#		t.join()
#
#	ips = []
#	for di in dis:
#		ip = di.GetIPs()
#		if ip == None:
#			continue
#		ips.append(ip)
#	return ips


class _DescInst:
	def __init__(self, region, tags):
		self.region = region
		self.tags = tags

	def Run(self):
		filters = []
		for k, v in self.tags.iteritems():
		 d = {}
		 d["Name"] = ("tag:%s" % k)
		 d["Values"] = [v]
		 filters.append(d)
		self.response = BotoClient.Get(self.region).describe_instances(Filters = filters)

	def GetIPs(self):
		#Cons.P(pprint.pformat(self.response, indent=2, width=100))
		#Cons.P(pprint.pformat(self.response["Reservations"], indent=2, width=100))
		pub_ips = []
		for r in self.response["Reservations"]:
			if len(r["Instances"]) == 0:
				continue
			for i in r["Instances"]:
				if i["State"]["Name"] != "running":
					continue
				for t in i["Tags"]:
					if t["Key"] != "name":
						continue
					inst_name = t["Value"]
					if inst_name.startswith("server"):
						pub_ips.append(i["PublicIpAddress"])
		return pub_ips


_my_pub_ip = None

def GetMyPubIp():
	global _my_pub_ip
	if _my_pub_ip == None:
		_my_pub_ip = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/public-ipv4"
				, print_cmd = False, print_output = False)
	return _my_pub_ip
