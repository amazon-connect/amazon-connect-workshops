#!/bin/bash

source ../common/parameters.ini
AwsRegion=$(aws configure get region)

function ConnectInstanceWait () {
    InstanceId=$1
    
    echo This will take several minutes to complete
    
    while true; do
        InstanceStatus=$(aws connect describe-instance --instance-id $InstanceId | jq -r ".Instance.InstanceStatus");
        
        echo Connect Status $InstanceStatus
        
        if [[ "$InstanceStatus" == "ACTIVE" ]]
        then
            break
        elif [[ "$InstanceStatus" == "CREATION_FAILED" ]]
        then
            exit 1
        else
            sleep 30
        fi
    done
}

#Connect instance
InstanceAlias=$Prefix-$(date +%s)-$((1 + $RANDOM % 10000))

CreateInstanceOutput=$(aws connect create-instance --identity-management-type CONNECT_MANAGED \
    --instance-alias $InstanceAlias --inbound-calls-enabled --outbound-calls-enabled)
    
ConnectId=$( echo $CreateInstanceOutput | jq -r '.Id' )

ConnectInstanceWait $ConnectId

#Lex - See lex section
LexBotName=QA
aws connect associate-lex-bot --instance-id $ConnectId --lex-bot Name=$LexBotName,LexRegion=$AwsRegion

#Publish contact flow
TemplateFile=LexQAFlow.template
ConcreteFile=LexQAFlow.json

AwsRegion=$(aws configure get region)

sed "s/<<AwsRegion>>/$AwsRegion/g" $TemplateFile > $ConcreteFile

Content=$(cat $ConcreteFile)

aws connect create-contact-flow --instance-id $ConnectId --name 1LexQA --type CONTACT_FLOW --content "$Content"

#S3 Bucket
StackName=$Prefix-ConnectS3

aws cloudformation deploy --template-file template.cft --stack-name $StackName \
    --parameter-overrides $(cat ../common/parameters.ini)

CfOutput=$(aws cloudformation describe-stacks --stack-name $StackName \
    --query Stacks[0].Outputs)
    
ConnectBucket=$( echo $CfOutput | jq -r '.[] | select(.OutputKey | contains("ConnectBucket"))? | .OutputValue' )

#KMS Arn
ConnectKmsArn=$(aws kms describe-key --key-id alias/aws/connect | jq -r '.KeyMetadata.Arn')

#Connect storage
StorageCallRecordings=$(cat <<-END
    {
        "InstanceId": "$ConnectId", 
        "ResourceType": "CALL_RECORDINGS", 
        "StorageConfig": {
            "AssociationId": "$ConnectId-CallRecordings", 
            "StorageType": "S3", 
            "S3Config": {
                "BucketName": "$ConnectBucket", 
                "BucketPrefix": "callRecordings/", 
                "EncryptionConfig": {
                    "EncryptionType": "KMS", 
                    "KeyId": "$ConnectKmsArn"
                }
            }
        }
    }
END
)
aws connect associate-instance-storage-config --cli-input-json "$StorageCallRecordings"

StorageChatTranscripts=$(cat <<-END
    {
        "InstanceId": "$ConnectId", 
        "ResourceType": "CHAT_TRANSCRIPTS", 
        "StorageConfig": {
            "AssociationId": "$ConnectId-ChatTranscripts", 
            "StorageType": "S3", 
            "S3Config": {
                "BucketName": "$ConnectBucket", 
                "BucketPrefix": "chatTranscripts/", 
                "EncryptionConfig": {
                    "EncryptionType": "KMS", 
                    "KeyId": "$ConnectKmsArn"
                }
            }
        }
    }
END
)
aws connect associate-instance-storage-config --cli-input-json "$StorageChatTranscripts"

StorageScheduledReports=$(cat <<-END
    {
        "InstanceId": "$ConnectId", 
        "ResourceType": "SCHEDULED_REPORTS", 
        "StorageConfig": {
            "AssociationId": "$ConnectId-ScheduledReports", 
            "StorageType": "S3", 
            "S3Config": {
                "BucketName": "$ConnectBucket", 
                "BucketPrefix": "scheduledReports/", 
                "EncryptionConfig": {
                    "EncryptionType": "KMS", 
                    "KeyId": "$ConnectKmsArn"
                }
            }
        }
    }
END
)
aws connect associate-instance-storage-config --cli-input-json "$StorageScheduledReports"

#Kinesis Stream Arn - See ctrPipeline section
KinesisStreamArn=$(aws kinesis describe-stream --stream-name "$Prefix"Ctr | jq -r '.StreamDescription.StreamARN')

StorageCtrStream=$(cat <<-END
    {
        "InstanceId": "$ConnectId", 
        "ResourceType": "CONTACT_TRACE_RECORDS", 
        "StorageConfig": {
            "AssociationId": "$ConnectId-CtrStream", 
            "StorageType": "KINESIS_STREAM", 
            "KinesisStreamConfig": {
                "StreamArn": "$KinesisStreamArn"
            }
        }
    }
END
)
aws connect associate-instance-storage-config --cli-input-json "$StorageCtrStream"
