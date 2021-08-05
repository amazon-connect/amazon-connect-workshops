#!/bin/bash

source ../common/functions.sh
source ../common/parameters.ini

returnValue=''
PackageLambdaS3 'firehoseAddNewLine' returnValue
FirehoseAddNewLineZip=$returnValue

PackageLambdaS3 'modifyCTR' returnValue
ModifyCTRZip=$returnValue

StackName=$Prefix-CtrPipeline
CTRBucketName=$Prefix-connect-ctr-$(date +%s)-$((1 + $RANDOM % 10000))

aws cloudformation deploy --template-file template.cft --stack-name $StackName \
    --parameter-overrides $(cat ../common/parameters.ini) \
    CTRBucketName=$CTRBucketName \
    FirehoseAddNewLineLambdaKey=$FirehoseAddNewLineZip \
    ModifyCtrLambdaKey=$ModifyCTRZip \
    --capabilities CAPABILITY_NAMED_IAM

CfOutput=$(aws cloudformation describe-stacks --stack-name $StackName \
    --query Stacks[0].Outputs)
    
CTRBucket=$( echo $CfOutput | jq -r '.[] | select(.OutputKey | contains("CTRBucket"))? | .OutputValue' )

sed -i "s/^CTRS3Bucket.*/CTRS3Bucket=$CTRBucket/" ../common/parameters.ini