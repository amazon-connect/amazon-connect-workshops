#!/usr/bin/python

import sys
sys.path.insert(1, '../packages')

import logging
import os
import uuid
import time
import re
import boto3
from botocore.exceptions import ClientError

LOG_FILE = 'deploy.log'
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)
    
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

qsClient = boto3.client('quicksight')
sleepTime = 2

def getCurrentUserInfo():
    stsResp = boto3.client('sts').get_caller_identity()
    logger.info(stsResp)
    accountId = stsResp['Account']
    currentUserArn = stsResp['Arn']
    
    userNameIndexInd = 'assumed-role/'
    if (userNameIndexInd not in currentUserArn):
        userNameIndexInd = 'user/'
        if (userNameIndexInd not in currentUserArn):
            raise RuntimeError('userNameIndexInd is not in the currentUserArn: {0}'.format(currentUserArn))
    
    name = currentUserArn[currentUserArn.index(userNameIndexInd) + len(userNameIndexInd):]
    
    qsListUsersResp = None
    qsNamespace = 'default'
    try:
        qsAdminRegion = 'us-east-1' #This is the default region
        qsClientLocal = boto3.client('quicksight', region_name=qsAdminRegion)
        qsListUsersResp = qsClientLocal.list_users(
            AwsAccountId=accountId,
            Namespace=qsNamespace
        )
        logger.info(qsListUsersResp)
        if ('NextToken' in qsListUsersResp):
            raise RuntimeError('Code does not support paginating')

    except ClientError as e:
        if e.response['Error']['Code'] == 'AccessDeniedException':
            message = e.response['Message']
            logger.info(message)
            
            regexPattern = '^.*(Please use the)(.*)(endpoint).*$'
            matchObj = re.match(regexPattern, message, re.I)
            qsAdminRegion = matchObj.group(2).strip()
            
            qsClientLocal = boto3.client('quicksight', region_name=qsAdminRegion)
            qsListUsersResp = qsClientLocal.list_users(
                AwsAccountId=accountId,
                Namespace=qsNamespace
            )
            if ('NextToken' in qsListUsersResp):
                raise RuntimeError('Code does not support paginating')
        else:
            raise RuntimeError(e)

    qsUserArn = None
    for user in qsListUsersResp['UserList']:
        if (user['UserName'] == name):
            logger.info('Found user {0}.  The role is {1}'.format(user['UserName'], user['Role']))
            if (user['Role'] == 'ADMIN'):
                qsUserArn = user['Arn']
                break

    if (qsUserArn is None):
        raise RuntimeError('QuickSight user {0} is not found or is not an ADMIN'.format(name))
    else:
        return accountId, qsUserArn
        
def getTableProperties(catalog, database, tableName, s3Output):
    athenaClient = boto3.client('athena')
    
    sqeResp = athenaClient.start_query_execution(
        QueryString='DESCRIBE FORMATTED {0}'.format(tableName),
        QueryExecutionContext={
            'Database': database,
            'Catalog': catalog
        },
        ResultConfiguration={
            'OutputLocation': 's3://{0}'.format(s3Output),
        }
    )
    logger.info(sqeResp)
    queryExecutionId = sqeResp['QueryExecutionId']
    
    s3ResultLocation = None
    while True:
        time.sleep(sleepTime)
        
        gqeResp = athenaClient.get_query_execution(
            QueryExecutionId=queryExecutionId
        )
        logger.info(gqeResp)
        
        status = gqeResp['QueryExecution']['Status']['State']
        
        if (status == 'SUCCEEDED'):
            s3ResultLocation = gqeResp['QueryExecution']['ResultConfiguration']['OutputLocation']
            break
        
        if (status == 'FAILED'):
            raise RuntimeError('Athena get_query_execution failed')
    
    gqrResp = athenaClient.get_query_results(
        QueryExecutionId=queryExecutionId,
    )
    logger.info(gqrResp)
    resultSet = gqrResp['ResultSet']['Rows']
    
    tableInfo = []
    for row in resultSet:
        data = row['Data']
        if len(data) == 1:
            valueWhole = data[0]['VarCharValue']
            valueParts = re.split(r'\t+', valueWhole.rstrip('\t'))
            fieldName = valueParts[0].strip()
            dataType = valueParts[1].strip()
            dataType = dataType.lower()
            
            if (fieldName):
                if (fieldName == '# Detailed Table Information'):
                    break
                
                if (fieldName.startswith('#')):
                    continue
                
                quickSightDataType = None 
                if (dataType == 'string') or (dataType == 'char') or (dataType == 'varchar'):
                    quickSightDataType = 'STRING'
                elif (dataType == 'boolean'):
                    quickSightDataType = 'BOOLEAN'
                elif (dataType == 'tinyint') or (dataType == 'smallint') or (dataType == 'int') or (dataType == 'integer') or (dataType == 'bigint'):
                    quickSightDataType = 'INTEGER'
                elif (dataType == 'double') or (dataType == 'float') or (dataType == 'decimal'):
                    quickSightDataType = 'DECIMAL'
                elif (dataType == 'date') or (dataType == 'timestamp'):
                    quickSightDataType = 'DATETIME'
                else:
                    raise RuntimeError('Athena has an unsupported data type: {0}'.format(dataType))
                
                info = {
                    'name': fieldName.lower(),
                    'athenaDataType': dataType,
                    'quickSightDataType': quickSightDataType
                }
                
                tableInfo.append(info)
        else:
            raise RuntimeError('There should only be one data element')
    
    logger.info(tableInfo)
    return tableInfo
    
def createQsStagingBucket(accountId, region):
    bucket = 'aws-athena-query-results-{0}-{1}'.format(region, accountId)
    
    try:
        s3Client = boto3.client('s3')
        
        resp = None
        if (region == 'us-east-1'):
            resp = s3Client.create_bucket(Bucket=bucket)
        else:
            location = {'LocationConstraint': region}
            resp = s3Client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration=location
            )
        logger.info(resp)
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyExists':
            logger.info('No problem - BucketAlreadyExists')
        else:
            raise RuntimeError(e)
    
def deleteDataSource(accountId, name):
    ldsResp = qsClient.list_data_sources(
        AwsAccountId=accountId,
    )
    logger.info(ldsResp)
    if ('NextToken' in ldsResp):
        raise RuntimeError('Code does not support paginating')
    
    for ds in ldsResp['DataSources']:
        if (name == ds['Name']):
            dsId = ds['DataSourceId']
            ddsResp = qsClient.delete_data_source(
                AwsAccountId=accountId,
                DataSourceId=dsId
            )
            logger.info(ddsResp)
            
            while True:
                time.sleep(sleepTime)
                
                try:
                    ddsResp = qsClient.describe_data_source(
                        AwsAccountId = accountId,
                        DataSourceId = dsId
                    )
                    logger.info(ddsResp)
                    
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundException':
                        logger.info('Datasource deleted - ResourceNotFoundException')
                        break
                    else:
                        raise RuntimeError(e)

def createDataSource(accountId, qsUserArn, name):
    deleteDataSource(accountId, name)

    cdsResp = qsClient.create_data_source(
        AwsAccountId = accountId,
        DataSourceId = name,
        Name = name,
        Type = 'ATHENA',
        DataSourceParameters = {
            'AthenaParameters': {
                'WorkGroup': 'primary'
            }
        }
    )
    
    logger.info(cdsResp)
    
    dsArn = cdsResp['Arn']
    dsId = cdsResp['DataSourceId']
    
    while True:
        time.sleep(sleepTime)
        
        ddsResp = qsClient.describe_data_source(
            AwsAccountId = accountId,
            DataSourceId = dsId
        )
        logger.info(ddsResp)
        
        dsStatus = ddsResp['DataSource']['Status']
        logger.info(dsStatus)
        
        if dsStatus != 'CREATION_IN_PROGRESS':
            break
        
    if dsStatus == 'CREATION_SUCCESSFUL':
        udspResp = qsClient.update_data_source_permissions(
            AwsAccountId=accountId,
            DataSourceId=dsId,
            GrantPermissions=[
                {
                    'Principal': qsUserArn,
                    'Actions': [
                        'quicksight:UpdateDataSourcePermissions',
                        'quicksight:DescribeDataSource',
                        'quicksight:DescribeDataSourcePermissions',
                        'quicksight:PassDataSource',
                        'quicksight:UpdateDataSource',
                        'quicksight:DeleteDataSource',
                    ]
                }
            ]
        )
        logger.info(udspResp)
    
        return dsArn
    else:
        raise RuntimeError('{0} failed'.format(name))

def deleteDataset(accountId, name):
    ldsResp = qsClient.list_data_sets(
        AwsAccountId=accountId,
    )
    logger.info(ldsResp)
    if ('NextToken' in ldsResp):
        raise RuntimeError('Code does not support paginating')
    
    for ds in ldsResp['DataSetSummaries']:
        if (name == ds['Name']):
            dsId = ds['DataSetId']
            ddsResp = qsClient.delete_data_set(
                AwsAccountId=accountId,
                DataSetId=dsId
            )
            logger.info(ddsResp)
            
            while True:
                time.sleep(sleepTime)
                
                try:
                    ddsResp = qsClient.describe_data_set(
                        AwsAccountId = accountId,
                        DataSetId = dsId
                    )
                    logger.info(ddsResp)
                    
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundException':
                        logger.info('Dataset deleted - ResourceNotFoundException')
                        break
                    else:
                        raise RuntimeError(e)
        
def createDataset(
    accountId, 
    qsUserArn, 
    dataSourceArn, 
    tableInfo, 
    athenaCatalog,
    athenaDatabaseName,
    athenaTableName,
    name):
        
    deleteDataset(accountId, name)
        
    inputColumns = []
    dataTransforms = []
    for item in tableInfo:
        column = {
            'Name': item['name'],
            'Type': item['quickSightDataType'] 
        } 
        inputColumns.append(column)
        
        if 'latitude' in item['name']:
            dataTransforms.append(
                {
                    'TagColumnOperation': {
                        'ColumnName': item['name'],
                        'Tags': [{'ColumnGeographicRole': 'LATITUDE'}]
                    }
                }
            )
        elif 'longitude' in item['name']:
            dataTransforms.append(
                {
                    'TagColumnOperation': {
                        'ColumnName': item['name'],
                        'Tags': [{'ColumnGeographicRole': 'LONGITUDE'}]
                    }
                }
            )
        elif 'state' in item['name']:
            dataTransforms.append(
                {
                    'TagColumnOperation': {
                        'ColumnName': item['name'],
                        'Tags': [{'ColumnGeographicRole': 'STATE'}]
                    }
                }
            )
        elif 'city' in item['name'] :   #need to be last because the other two contain city
            dataTransforms.append(
                {
                    'TagColumnOperation': {
                        'ColumnName': item['name'],
                        'Tags': [{'ColumnGeographicRole': 'CITY'}]
                    }
                }
            )
    
    #Calcuated Field
    dataTransforms.append(
        {
            'CreateColumnsOperation': { 
                'Columns': [
                    {
                        'ColumnName': 'QueueSLA',
                        'ColumnId': 'QueueSLA-ID',
                        'Expression': "ifelse(queue_duration < 60, 'Goal', queue_duration >= 60 and queue_duration <= 90, 'Met', 'Exceeded')"
                    }
                ]
            }
        }
    )

    cdsResp = qsClient.create_data_set(
        AwsAccountId = accountId,
        DataSetId = name,
        Name = name,
        ImportMode = 'DIRECT_QUERY',
        PhysicalTableMap = {
            'ctrModifiedPhysicalTable': {
                'RelationalTable': {
                    'DataSourceArn': dataSourceArn, 
                    'Catalog': athenaCatalog,
                    'Name': athenaTableName, 
                    'Schema': athenaDatabaseName,
                    'InputColumns': inputColumns
                }
            }
        },
        LogicalTableMap={
            'ctrModifiedLogicalTable' : {
                'Alias': 'ctrModifiedLogicalTable',
                'Source': {
                    'PhysicalTableId': 'ctrModifiedPhysicalTable'
                },
                'DataTransforms': dataTransforms
            }
        }
    )
    
    logger.info(cdsResp)
    dsId = cdsResp['DataSetId']
    dsArn = cdsResp['Arn']
    
    udspResp = qsClient.update_data_set_permissions(
        AwsAccountId=accountId,
        DataSetId=dsId,
        GrantPermissions=[
            {
                'Principal': qsUserArn,
                'Actions': [
                    'quicksight:UpdateDataSetPermissions',
                    'quicksight:DescribeDataSet',
                    'quicksight:DescribeDataSetPermissions',
                    'quicksight:PassDataSet',
                    'quicksight:DescribeIngestion',
                    'quicksight:ListIngestions',
                    'quicksight:UpdateDataSet',
                    'quicksight:DeleteDataSet',
                    'quicksight:CreateIngestion',
                    'quicksight:CancelIngestion'
                ]
            }
        ]
    )
    logger.info(udspResp)
    
    return dsArn
    
def deleteDashboard(accountId, name):
    ldResp = qsClient.list_dashboards(
        AwsAccountId=accountId,
    )
    logger.info(ldResp)
    if ('NextToken' in ldResp):
        raise RuntimeError('Code does not support paginating')
    
    for dashboard in ldResp['DashboardSummaryList']:
        if (name == dashboard['Name']):
            dashboardId = dashboard['DashboardId']
            ddResp = qsClient.delete_dashboard(
                AwsAccountId=accountId,
                DashboardId=dashboardId
            )
            logger.info(ddResp)
            
            while True:
                time.sleep(sleepTime)
                
                try:
                    ddResp = qsClient.describe_dashboard(
                        AwsAccountId = accountId,
                        DashboardId = dashboardId
                    )
                    logger.info(ddResp)
                    
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundException':
                        logger.info('Dashboard deleted - ResourceNotFoundException')
                        break
                    else:
                        raise RuntimeError(e)
    
def createDashboard(
    accountId, 
    qsUserArn, 
    templateArn, 
    templateDatasetPlaceholder, 
    datasetArn, 
    name):
        
    deleteDashboard(accountId, name)
        
    cdResp = qsClient.create_dashboard(
        AwsAccountId = accountId,
        DashboardId = name,
        Name = name,
        SourceEntity = {
            'SourceTemplate': {
                'DataSetReferences': [
                    {
                        'DataSetPlaceholder': templateDatasetPlaceholder,
                        'DataSetArn': datasetArn
                    }
                ],
                'Arn': templateArn
            }
        },
        DashboardPublishOptions={
            'AdHocFilteringOption': {
                'AvailabilityStatus': 'ENABLED'
            },
            'ExportToCSVOption': {
                'AvailabilityStatus': 'ENABLED'
            },
            'SheetControlsOption': {
                'VisibilityState': 'EXPANDED'
            }
        }
    )
    logger.info(cdResp)
    
    dbId = cdResp['DashboardId']
    
    while True:
        time.sleep(sleepTime)
        
        ddResp = qsClient.describe_dashboard(
            AwsAccountId = accountId,
            DashboardId = dbId
        )
        logger.info(ddResp)
        
        dbStatus = ddResp['Dashboard']['Version']['Status']
        logger.info(dbStatus)
        
        if dbStatus != 'CREATION_IN_PROGRESS':
            break
    
    if dbStatus == 'CREATION_SUCCESSFUL':
        udpResp = qsClient.update_dashboard_permissions(
            AwsAccountId=accountId,
            DashboardId=dbId,
            GrantPermissions=[
                {
                    'Principal': qsUserArn,
                    'Actions': [
                        'quicksight:DescribeDashboard',
                        'quicksight:ListDashboardVersions',
                        'quicksight:UpdateDashboardPermissions',
                        'quicksight:QueryDashboard',
                        'quicksight:UpdateDashboard',
                        'quicksight:DeleteDashboard',
                        'quicksight:DescribeDashboardPermissions',
                        'quicksight:UpdateDashboardPublishedVersion'
                    ]
                }
            ]
        )
        logger.info(udpResp)
    else:
        raise RuntimeError('{0} failed'.format(name))

def main(
    prefix,
    athenaCatalog,
    athenaDatabaseName,
    athenaTableName,
    athenaS3Output,
    quickSightTemplateDatasetPlaceholder,
    quickSightTemplateArn):
    
    region = boto3.session.Session().region_name
    accountId, qsUserArn = getCurrentUserInfo()
    tableInfo = getTableProperties(athenaCatalog, athenaDatabaseName, athenaTableName, athenaS3Output)
    createQsStagingBucket(accountId, region)

    dataSourceArn = createDataSource(accountId, qsUserArn, '{0}DataSource'.format(prefix))
    
    datasetArn = createDataset(
        accountId, 
        qsUserArn, 
        dataSourceArn, 
        tableInfo, 
        athenaCatalog,
        athenaDatabaseName,
        athenaTableName,
        '{0}Dataset'.format(prefix)
    )
    
    createDashboard(
        accountId, 
        qsUserArn, 
        quickSightTemplateArn, 
        quickSightTemplateDatasetPlaceholder, 
        datasetArn, 
        '{0}Dashboard'.format(prefix)
    )

    logger.info('Finished')
    print('Finished')
        
if __name__ == '__main__':
    main(
        sys.argv[1], 
        sys.argv[2], 
        sys.argv[3], 
        sys.argv[4], 
        sys.argv[5], 
        sys.argv[6], 
        sys.argv[7]
    )  