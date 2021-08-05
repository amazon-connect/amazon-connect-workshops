#!/bin/bash

# Do not run this script.  I use it to create the template from my account.  I committed it so others can see the process

source ../common/parameters.ini

CallerIdentity=$(aws sts get-caller-identity)
AccountId=$( echo $CallerIdentity | jq -r ".Account")
RegionId=$(aws configure get region)

# aws quicksight list-templates --aws-account-id <>
# aws quicksight delete-template --aws-account-id <> --template-id <>
# aws quicksight list-analyses --aws-account-id <>
# aws quicksight list-data-sets --aws-account-id <>

TemplateId=workshopTemplate20210413
SourceAnalysisArn=arn:aws:quicksight:$RegionId:$AccountId:analysis/d4709ef6-715d-4707-8c69-fe291f1d90a4
SourceDataSetArn=arn:aws:quicksight:$RegionId:$AccountId:dataset/5c565c4b-0229-444e-9c14-4e8aa6edb491

TemplateCreate=$(cat <<-END
    {
        "AwsAccountId": "$AccountId",
        "TemplateId": "$TemplateId",
        "Name": "$TemplateId",
        "Permissions": [
            {
                "Principal": "*",
                "Actions": ["quicksight:DescribeTemplate"]
            }
        ],
        "SourceEntity": {
            "SourceAnalysis": {
                "Arn": "$SourceAnalysisArn",
                "DataSetReferences": [
                    {
                        "DataSetPlaceholder": "$QuickSightTemplateDatasetPlaceholder",
                        "DataSetArn": "$SourceDataSetArn"
                    }
                ]
            }
        },
        "VersionDescription": "1"
    }
END
)
echo $TemplateCreate | jq '.' > createTemplate.auto

aws quicksight delete-template --aws-account-id $AccountId --template-id $TemplateId

TemplateArn=$(aws quicksight create-template --cli-input-json file://createTemplate.auto | jq -r ".Arn")

aws quicksight describe-template --aws-account-id $AccountId --template-id $TemplateId
aws quicksight describe-template-permissions --aws-account-id $AccountId --template-id $TemplateId

# TemplateArn contains a forward slash which needs to be escaped in sed
TemplateArn=${TemplateArn/'/'/'\/'}
sed -i "s/^QuickSightTemplateArn.*/QuickSightTemplateArn=$TemplateArn/" ../common/parameters.ini