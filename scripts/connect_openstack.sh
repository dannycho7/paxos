#!/bin/bash

if [ $# -ne 1 ]; then
	echo "Correct Usage: ./connect_openstack.sh <node_index>"
	exit
fi

INSTANCE_IPS=(128.111.34.164 128.111.34.172 128.111.34.174 128.111.34.171 128.111.34.166)

ssh -i ~/.ssh/openstack.pem ubuntu@${INSTANCE_IPS[$1]}
