#!/usr/bin/env python

import boto3
import datetime
import os
import pprint
import re
import sys
import threading
import time
import traceback

sys.path.insert(0, "%s/lib/util" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
import Ec2Region


# Leave the 2 newest AMIs per region and delete all the others and their
# associated snapshots.

_AMI_prefix_to_keep_at_most_2 = [
		"acorn-server"
		, "castnet"
		, "mutants-cassandra-server"
		, "mutants-client"]

def main(argv):
	iscs = []
	for r in Ec2Region.All():
		iscs.append(ImageSnapshotCleaner(region=r))
	for i in iscs:
		i.GetImages()
	for i in iscs:
		i.Join()

	for i in iscs:
		i.GetSnapshots()
	for i in iscs:
		i.Join()

	for i in iscs:
		i.PrintWhatToKeepAndDelete()

	Cons.P("")
	Cons.P("Deregistering Amis and deleting snapshots ...")
	for i in iscs:
		i.DeleteOldAmisSnapshots()
	for i in iscs:
		i.Join()


class ImageSnapshotCleaner:
	def __init__(self, region):
		self.region = region
		self.bc = None

		self.imgs_others = []
		self.imgs_myproj_to_keep = []
		self.imgs_myproj_to_delete = []

		self.ss_to_keep = []
		self.ss_to_delete = []

	def GetImages(self):
		self.t = threading.Thread(target=self._GetImages)
		self.t.daemon = True
		self.t.start()

	def _Bc(self):
		if self.bc is None:
			self.bc = boto3.session.Session().client("ec2", region_name=self.region)
		return self.bc

	class AMI:
		def __init__(self, img_response):
			self.image_id      = img_response["ImageId"]
			self.name          = img_response["Name"]
			self.creation_date = img_response["CreationDate"]

		def __lt__(self, other):
			if self.name < other.name:
				return True
			elif self.name > other.name:
				return False
			return (self.creation_date < other.creation_date)

	def _GetImages(self):
		try:
			response = self._Bc().describe_images(
					Owners=["self"],
					#Filters=[]
					)

			#Cons.P(pprint.pformat(response["Images"]))
			# {u'Architecture': 'x86_64',
			#  u'BlockDeviceMappings': [{u'DeviceName': '/dev/sda1',
			#                            u'Ebs': {u'DeleteOnTermination': True,
			#                                     u'Encrypted': False,
			#                                     u'SnapshotId': 'snap-55411eb5',
			#                                     u'VolumeSize': 8,
			#                                     u'VolumeType': 'gp2'}},
			#                           {u'DeviceName': '/dev/sdb',
			#                            u'VirtualName': 'ephemeral0'},
			#                           {u'DeviceName': '/dev/sdc',
			#                            u'VirtualName': 'ephemeral1'}],
			#  u'CreationDate': '2016-06-13T22:26:41.000Z',
			#  u'Description': '',
			#  u'Hypervisor': 'xen',
			#  u'ImageId': 'ami-f051949d',
			#  u'ImageLocation': '998754746880/acorn-server-160613-1826',
			#  u'ImageType': 'machine',
			#  u'Name': 'acorn-server-160613-1826',
			#  u'OwnerId': '998754746880',
			#  u'Public': False,
			#  u'RootDeviceName': '/dev/sda1',
			#  u'RootDeviceType': 'ebs',
			#  u'SriovNetSupport': 'simple',
			#  u'State': 'available',
			#  u'VirtualizationType': 'hvm'}

			imgs_all = []
			for img in response["Images"]:
				imgs_all.append(ImageSnapshotCleaner.AMI(img))

			# {prefix: img}
			imgs_myproj_to_keep_at_most_2 = {}
			for img in imgs_all:
				classified = False
				for prefix in _AMI_prefix_to_keep_at_most_2:
					if img.name.startswith(prefix):
						if prefix not in imgs_myproj_to_keep_at_most_2:
							imgs_myproj_to_keep_at_most_2[prefix] = []
						imgs_myproj_to_keep_at_most_2[prefix].append(img)
						classified = True
						break
				if not classified:
					self.imgs_others.append(img)
			#Cons.P(pprint.pformat(imgs_myproj_to_keep_at_most_2))

			# Sort by image creation_date
			for prefix, v in sorted(imgs_myproj_to_keep_at_most_2.iteritems()):
				sorted_v = sorted(v)
				for i in range(len(v)):
					if i < len(v) - 2:
						self.imgs_myproj_to_delete.append(sorted_v[i])
					else:
						self.imgs_myproj_to_keep.append(sorted_v[i])

		except Exception as e:
			Cons.P("%s\n%s\nregion=%s" %
					(e, traceback.format_exc(), self.region))
			os._exit(1)

	def Join(self):
		self.t.join()

	def PrintWhatToKeepAndDelete(self):
		Cons.P("%s" % self.region)

		m = "  ami_myproj_to_keep  :"
		i = 0
		for img in self.imgs_myproj_to_keep:
			if (i >= 2) and (i % 2 == 0):
				m += "\n                       "
			m += (" (%s, %s, %s)" % (img.name, SimpleDatetime(img.creation_date), img.image_id))
			i += 1
		Cons.P(m)

		m = "  ami_myproj_to_delete:"
		i = 0
		for img in self.imgs_myproj_to_delete:
			if (i >= 2) and (i % 2 == 0):
				m += "\n                       "
			m += (" (%s, %s, %s)" % (img.name, SimpleDatetime(img.creation_date), img.image_id))
			i += 1
		Cons.P(m)

		m = "  ami_others          :"
		i = 0
		for img in self.imgs_others:
			if (i >= 2) and (i % 2 == 0):
				m += "\n                       "
			m += (" (%s, %s, %s)" % (img.name, SimpleDatetime(img.creation_date), img.image_id))
			i += 1
		Cons.P(m)

		m = "  snapshots_to_keep   :"
		i = 0
		for sn in self.ss_to_keep:
			if (i >= 3) and (i % 3 == 0):
				m += "\n                       "
			m += (" %s(%s)" % (sn.snapshot_id, sn.ami_id))
			i += 1
		Cons.P(m)

		m = "  snapshots_to_delete:"
		i = 0
		for sn in self.ss_to_delete:
			if (i >= 3) and (i % 3 == 0):
				m += "\n                      "
			m += (" %s(%s)" % (sn.snapshot_id, sn.ami_id))
			i += 1
		Cons.P(m)

	def GetSnapshots(self):
		self.t = threading.Thread(target=self._GetSnapshots)
		self.t.daemon = True
		self.t.start()

	class Snapshot:
		def __init__(self, response):
			self.snapshot_id = response["SnapshotId"]
			self.desc        = response["Description"]
			self.ami_id = None
			self._ParseAmiId()

		def _ParseAmiId(self):
			# Created by CreateImage(i-5133aecd) for ami-abf83cc6 from vol-ee05363e
			#                                        012345678901
			m = re.match(r"Created by CreateImage\(i-........\) for ami-", self.desc)
			if m is not None:
				self.ami_id = self.desc[len(m.group(0)) - 4:len(m.group(0)) - 4 + 11 + 1]
				return

			# Copied for DestinationAmi ami-f7612497 from SourceAmi ami-abf83cc6 for SourceSnapshot snap-cf65a7d1. Task created on 1,465,926,614,769.
			# 012345678901234567890123456
			m = re.match(r"Copied for DestinationAmi ami-........ from SourceAmi ami-", self.desc)
			if m is not None:
				self.ami_id = self.desc[26 : 26 + 11 + 1]
				return

			raise RuntimeError("Cannot parse ami id from Snapshot: id (%s) description (%s)" %
					(self.snapshot_id, self.desc))

		def __str__(self):
			return "%s %s" % (self.snapshot_id, self.ami_id)

	def _GetSnapshots(self):
		try:
			response = self._Bc().describe_snapshots(
					OwnerIds=['self'],
					#Filters=[]
					)
			ss_all = []
			for sn in response["Snapshots"]:
				ss_all.append(ImageSnapshotCleaner.Snapshot(sn))

			#for sn in ss_all:
			#	Cons.P(sn)

			for ss in ss_all:
				to_keep = False
				for img in self.imgs_myproj_to_keep + self.imgs_others:
					if ss.ami_id == img.image_id:
						to_keep = True
						break
				if to_keep:
					self.ss_to_keep.append(ss)
				else:
					self.ss_to_delete.append(ss)
		except Exception as e:
			Cons.P("%s\n%s\nregion=%s" %
					(e, traceback.format_exc(), self.region))
			os._exit(1)

	def DeleteOldAmisSnapshots(self):
		self.t = threading.Thread(target=self._DeleteOldAmisSnapshots)
		self.t.daemon = True
		self.t.start()

	def _DeleteOldAmisSnapshots(self):
		try:
			for img in self.imgs_myproj_to_delete:
				try:
					r = self._Bc().deregister_image(ImageId=img.image_id)
					Cons.P("%-20s deregistered AMI %s" % (self.region, img.image_id))
				except botocore.exceptions.ClientError as e:
					if e.response["Error"]["Code"] == "InvalidAMIID.Unavailable":
						pass
					else:
						raise e

			if len(self.ss_to_delete) > 0:
				time.sleep(1)

			for ss in self.ss_to_delete:
				r= self._Bc().delete_snapshot(SnapshotId=ss.snapshot_id)
				Cons.P("%-20s deleted snapshot %s" % (self.region, ss.snapshot_id))
		except Exception as e:
			Cons.P("%s\n%s\nregion=%s" %
					(e, traceback.format_exc(), self.region))
			os._exit(1)


def SimpleDatetime(dt):
	# 2016-06-14T17:31:23.000Z
	date_fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
	return datetime.datetime.strptime(dt, date_fmt).strftime("%y%m%d-%H%M%S")


if __name__ == "__main__":
	sys.exit(main(sys.argv))
