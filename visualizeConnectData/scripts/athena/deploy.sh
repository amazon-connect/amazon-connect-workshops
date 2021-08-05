#!/bin/bash

source ../common/functions.sh
source ../common/parameters.ini

function AthenaQueryResult () {
    QueryExecutionId=$1
    Description=$2
    
    while true; do
        QueryState=$(
            aws athena get-query-execution --query-execution-id $QueryExecutionId \
                | jq -r ".QueryExecution.Status.State"
        );
        
        echo $Description $QueryState
        
        if [[ "$QueryState" == "SUCCEEDED" ]]
        then
            break
        elif [[ "$QueryState" == "FAILED" ]]
        then
            exit 1
        else
            sleep 2
        fi
    done
    
    QueryResults=$(aws athena get-query-results --query-execution-id $QueryExecutionId)
    echo $QueryResults
}

StackName=$Prefix-AthenaS3
AthenaTableS3Location=s3://$CTRS3Bucket/$CTRModifiedS3Folder/

AthenaQueryString=$(cat <<-END
    CREATE external TABLE $AthenaTableName (
    	AWSAccountId STRING,
    	AWSContactTraceRecordFormatVersion STRING,
    	Agent_ARN STRING,
    	Agent_AfterContactWorkDuration INT,
    	Agent_AfterContactWorkEndTimestamp TIMESTAMP,
    	Agent_AfterContactWorkStartTimestamp TIMESTAMP,
    	Agent_AgentInteractionDuration INT,
    	Agent_ConnectedToAgentTimestamp TIMESTAMP,
    	Agent_CustomerHoldDuration INT,
    	Agent_TalkDuration INT,
    	Agent_HierarchyGroups_Level1_ARN STRING,
    	Agent_HierarchyGroups_Level1_GroupName STRING,
    	Agent_HierarchyGroups_Level2_ARN STRING,
    	Agent_HierarchyGroups_Level2_GroupName STRING,
    	Agent_HierarchyGroups_Level3_ARN STRING,
    	Agent_HierarchyGroups_Level3_GroupName STRING,
    	Agent_HierarchyGroups_Level4_ARN STRING,
    	Agent_HierarchyGroups_Level4_GroupName STRING,
    	Agent_HierarchyGroups_Level5_ARN STRING,
    	Agent_HierarchyGroups_Level5_GroupName STRING,
    	Agent_LongestHoldDuration INT,
    	Agent_NumberOfHolds INT,
    	Agent_RoutingProfile_ARN STRING,
    	Agent_RoutingProfile_Name STRING,
    	Agent_Username STRING,
    	AgentConnectionAttempts INT,
    	Attributes_udCounter STRING,
    	Attributes_udPlay STRING,
    	Attributes_udCity STRING,
    	Attributes_udCity_Latitude DOUBLE,
    	Attributes_udCity_Longitude DOUBLE,
    	Attributes_udCity_State STRING,
    	Attributes_udColor STRING,
    	Attributes_udDOB DATE,
    	Attributes_udFood STRING,
    	Attributes_udProjectTime INT,
        Attributes_udThisWillNeverExist STRING,
    	Channel STRING,
    	ConnectedToSystemTimestamp TIMESTAMP,
    	ContactId STRING,
    	CustomerEndpoint_Address STRING,
    	CustomerEndpoint_Type STRING,
    	DisconnectReason STRING,
    	DisconnectTimestamp TIMESTAMP,
    	InitialContactId STRING,
    	InitiationMethod STRING,
    	InitiationTimestamp TIMESTAMP,
    	InstanceARN STRING,
    	LastUpdateTimestamp TIMESTAMP,
    	NextContactId STRING,
    	PreviousContactId STRING,
    	Queue_ARN STRING,
    	Queue_DequeueTimestamp TIMESTAMP,
    	Queue_Duration INT,
    	Queue_EnqueueTimestamp TIMESTAMP,
    	Queue_Name STRING,
    	SystemEndpoint_Address STRING,
    	SystemEndpoint_Type STRING,
    	TransferCompletedTimestamp TIMESTAMP,
    	TransferredToEndpoint STRING,
    	ContactDuration INT,
    	IvrDuration INT,
    	Source_Bucket STRING,
    	Source_Key STRING
    )
    PARTITIONED BY (year SMALLINT, month TINYINT, day TINYINT)
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    LOCATION '$AthenaTableS3Location'
END
)

returnValue=''
PackageLambdaS3 'updatePartitions' returnValue
UpdatePartitionsZip=$returnValue

aws cloudformation deploy --template-file template.cft --stack-name $StackName \
    --parameter-overrides $(cat ../common/parameters.ini) \
    UpdateCtrModifiedPartitionsLambdaKey=$UpdatePartitionsZip \
    AthenaDatabaseName=$AthenaDatabaseName \
    AthenaTableName=$AthenaTableName \
    --capabilities CAPABILITY_NAMED_IAM
    
CfOutput=$(aws cloudformation describe-stacks --stack-name $StackName \
    --query Stacks[0].Outputs)
    
AthenaBucket=$( echo $CfOutput | jq -r '.[] | select(.OutputKey | contains("AthenaBucket"))? | .OutputValue' )
ConnectDatabase=$( echo $CfOutput | jq -r '.[] | select(.OutputKey | contains("ConnectDatabase"))? | .OutputValue' )

DropTableExecutionId=$(
    aws athena start-query-execution \
    --query-string "DROP TABLE IF EXISTS $AthenaTableName;" \
    --query-execution-context "Database=$ConnectDatabase" \
    --result-configuration "OutputLocation"="s3://$AthenaBucket" \
    | jq -r ".QueryExecutionId"
)

AthenaQueryResult $DropTableExecutionId DropTable

CreateTableExecutionId=$(
    aws athena start-query-execution \
    --query-string "$AthenaQueryString" \
    --query-execution-context "Database=$ConnectDatabase" \
    --result-configuration "OutputLocation"="s3://$AthenaBucket" \
    | jq -r ".QueryExecutionId"
)

AthenaQueryResult $CreateTableExecutionId CreateTable

CreatePartitionsExecutionId=$(
    aws athena start-query-execution \
    --query-string "MSCK REPAIR TABLE $AthenaTableName" \
    --query-execution-context "Database=$ConnectDatabase" \
    --result-configuration "OutputLocation"="s3://$AthenaBucket" \
    | jq -r ".QueryExecutionId"
)

AthenaQueryResult $CreatePartitionsExecutionId CreatePartitions

sed -i "s/^AthenaS3Output.*/AthenaS3Output=$AthenaBucket/" ../common/parameters.ini