#!/bin/bash

source ../common/parameters.ini
StackName=$Prefix-LambdaDeployS3

aws cloudformation deploy --template-file template.cft --stack-name $StackName \
    --parameter-overrides $(cat ../common/parameters.ini)

CfOutput=$(aws cloudformation describe-stacks --stack-name $StackName \
    --query Stacks[0].Outputs)
    
LambdaBucket=$( echo $CfOutput | jq -r '.[] | select(.OutputKey | contains("LambdaBucket"))? | .OutputValue' )

sed -i "s/^LambdaDeployS3Bucket.*/LambdaDeployS3Bucket=$LambdaBucket/" ../common/parameters.ini