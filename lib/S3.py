import os
import sys

sys.path.insert(0, "%s/util" % os.path.dirname(__file__))
import Util


def Sync():
	dn = "%s/work/acorn-log" % os.path.expanduser("~")
	Util.MkDirs(dn)

	# http://docs.aws.amazon.com/cli/latest/reference/s3/sync.html
	Util.RunSubp("aws s3 sync s3://acorn-youtube %s" % dn)


def Test():
	Sync()


def main(argv):
	Test()


if __name__ == "__main__":
	sys.exit(main(sys.argv))
