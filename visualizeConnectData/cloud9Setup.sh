#!/bin/bash

sudo yum -y update
sudo yum -y install jq

find . -type f -iname "*.sh" -exec chmod +x {} \;

cd scripts
./installDependencies.sh
cd ..