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
