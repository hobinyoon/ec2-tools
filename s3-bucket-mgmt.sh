#!/bin/bash

set -e
set -u

# List all buckets
#  2016-06-06 16:54:02 acorn-youtube
bucket_names=`aws s3 ls | awk '{print $3}'`
#echo $bucket_names

# Check size of each bucket
for bucket_name in $bucket_names
do
	echo $bucket_name
	aws s3 ls --summarize --human-readable --recursive s3://$bucket_name | grep "   Total Size:"
done

# I have less than 2.6GB stored now, which is about $0.06 / Month with $0.023 /
# GB / Month. That's okay.

# AMI backed by EBS gp3
#   $0.10/GB/Month
#   I have 11 AMIs. Let's say the average is about 10GB.
#   11 * 10GB * $0.1/GB/Month = $11 Month. This is something to think about.
#
# Move old AMIs to the machines at gatech. Like to glacier.

# Data transfer out to Internet: $0.09 per GB
#   Compared with the storage price, it's better to move it out when you don't use it for more than 1 month.

# Cleaning up a lot of not-so-important AMIs
#   Now total 41GB. $4.4/Month. Will want to download the data later on.
#   Or move to s3, which is $0.023/GB/Month. Or even to
#     Infrequently accessed storage: $0.0125/GB/Month
#     Glacier storage:               $0.004 /GB/Month. This is the way to go. Or free cloud storage services like Mega
