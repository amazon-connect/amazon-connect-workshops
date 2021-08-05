#!/bin/bash

function PackageLambdaS3 () {
    LambdaName=$1
    
    ThisDir=$(pwd)
    cd ../../lambdas/$LambdaName
    
    rm -f $LambdaName*.zip
    zip -r $LambdaName.zip *
    CheckSum=($(md5sum $LambdaName.zip))
    LambdaZipFileWithCheckSum=$LambdaName-$CheckSum.zip
    mv $LambdaName.zip $LambdaZipFileWithCheckSum
    aws s3 cp $LambdaZipFileWithCheckSum s3://$LambdaDeployS3Bucket/$LambdaZipFileWithCheckSum
    
    cd $ThisDir
    
    eval "$2=$LambdaZipFileWithCheckSum"
}