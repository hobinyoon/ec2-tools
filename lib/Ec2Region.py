# TODO: Make a yaml file and move to conf/ directory.

def GetLatestAmiId(region, name = "acorn-server"):
	region_ami = None
	if name == "acorn-server":
		region_ami = {
				"ap-northeast-1": "ami-7352af12"
				, "ap-northeast-2": "ami-d5bc76bb"
				, "ap-south-1": "ami-74f3991b"
				, "ap-southeast-1": "ami-1b4c9178"
				, "ap-southeast-2": "ami-8c80abef"
				, "eu-central-1": "ami-14678d7b"
				, "eu-west-1": "ami-46ea8d35"
				, "sa-east-1": "ami-2f9f0b43"
				, "us-east-1": "ami-ac32b4bb"
				, "us-west-1": "ami-9c4f09fc"
				, "us-west-2": "ami-b5c201d5"
				}
	elif name == "tweets-db":
		region_ami = {
				"us-east-1": "ami-645cda73"
				}
	elif name == "mutants-server":
		region_ami = {
				"us-east-1": "ami-1fc7d575"
				}
	else:
		raise RuntimeError("Unexpected name %s" % name)

	return region_ami[region]


def All():
	return [
			"ap-northeast-1"
			, "ap-northeast-2"
			, "ap-south-1"
			, "ap-southeast-1"
			, "ap-southeast-2"
			, "eu-central-1"
			, "eu-west-1"
			, "sa-east-1"
			, "us-east-1"
			, "us-west-1"
			, "us-west-2"
			]
