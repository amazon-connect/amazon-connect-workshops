#!/bin/bash

echo "*** lambdaDeployS3 ***"
cd ~/environment/wsMaterials/scripts/lambdaDeployS3
./deploy.sh

echo "*** ctrPipeline ***"
cd ~/environment/wsMaterials/scripts/ctrPipeline
./deploy.sh

echo "*** lex ***"
cd ~/environment/wsMaterials/scripts/lex
./deploy.sh

echo "*** connect ***"
cd ~/environment/wsMaterials/scripts/connect
./deploy.sh

echo "*** mockCTRs ***"
cd ~/environment/wsMaterials/scripts/mockCTRs
./createDeploy.sh

echo "*** athena ***"
cd ~/environment/wsMaterials/scripts/athena
./deploy.sh

echo "*** finished ***"