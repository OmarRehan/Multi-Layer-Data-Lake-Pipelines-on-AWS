#!/bin/bash

#sudo easy_install-3.4 pip
sudo pip-3.6 install pandas boto3 s3fs

sudo aws s3 cp s3://flights-dl-edge-node/BOOTSTRAP_ACTIONS/delta-core_2.11-0.6.1.jar /usr/lib/spark/jars/
