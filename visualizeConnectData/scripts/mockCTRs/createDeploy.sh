#!/bin/bash

source ../common/parameters.ini

python3 create.py

aws s3 sync ./ctr s3://$CTRS3Bucket/ctr