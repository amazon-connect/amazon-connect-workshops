import os
import json
import boto3
import logging
from datetime import datetime
 
logger = logging.getLogger()
logger.setLevel(os.environ['LOG_LEVEL'])

athenaClient = boto3.client('athena')
glueClient = boto3.client('glue')
s3Client = boto3.client('s3')

s3Bucket = os.environ['S3Bucket']
s3KeyPrefix = os.environ['S3KeyPrefix']
athenaTable = s3KeyPrefix
athenaDB = os.environ['AthenaDB']
athenaOutput = os.environ['AthenaOutput']

def lambda_handler(event, context):
    try:
        logger.info('Start {}, Version {}'.format(context.function_name, context.function_version))
        logger.info('Event: ' + json.dumps(event))
        
        currentTime = datetime.utcnow()
        year = currentTime.year
        month = currentTime.strftime('%m')
        day = currentTime.strftime('%d')
        
        if DoS3FilesExist(s3Bucket, s3KeyPrefix, year, month, day):
            if DoesAthenaPartitionExist(athenaDB, athenaTable, year, month, day):
                logger.info('PartitionExist: Everything is current')
            else:
                RebuildTable(athenaDB, athenaTable, athenaOutput)
        else:
            logger.info('No S3 Files: everything is current')
        
    except Exception as e:
        logger.exception(e)
        raise Exception(e)

    finally:
        logger.info('Finished')
        
def DoS3FilesExist(bucket, prefix, year, month, day):
    s3PrefixExpression = 'year={0}/month={1}/day={2}/'.format(year, month, day)
    
    if len (prefix) > 0:
        s3PrefixExpression = prefix + '/' + s3PrefixExpression
    
    s3Response = s3Client.list_objects_v2(
        Bucket=bucket,
        Delimiter='/',
        Prefix=s3PrefixExpression,
    )
    
    numOfFiles = s3Response['KeyCount']
    if numOfFiles > 0:
        return True
    else:
        return False
        
def DoesAthenaPartitionExist(database, table, year, month, day):
    glueExpression = 'year={0} AND month={1} AND day={2}'.format(year, month, day)

    glueResponse = glueClient.get_partitions(
        DatabaseName=database,
        TableName=table,
        Expression=glueExpression
    )
    
    if 'Partitions' in glueResponse:
        partitions = glueResponse['Partitions']
        if len(partitions) > 0:
            return True
    
    return False
    
def RebuildTable(database, table, output):
    athenaResponse = athenaClient.start_query_execution(
        QueryString='MSCK REPAIR TABLE ' + table,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={
            'OutputLocation': 's3://{0}/'.format(output)
        }
    )
    
    logger.info('athenaResponse: ' + json.dumps(athenaResponse))