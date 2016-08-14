#!/usr/bin/env python

import os
import sys

sys.path.insert(0, "%s/lib" % os.path.dirname(__file__))
#import InstMonitor
import ClusterMonitor


def main(argv):
	cm = ClusterMonitor.CM()
	cm.RunOnce()


if __name__ == "__main__":
	sys.exit(main(sys.argv))
