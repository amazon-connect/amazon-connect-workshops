import os
import json
import boto3
import botocore
import logging
import datetime
import isodate #pip3 install isodate --target .
 
from urllib.parse import unquote
from flatten_json import flatten #pip3 install flatten_json==0.1.7 --target .
 
logger = logging.getLogger()
logger.setLevel(os.environ['LOG_LEVEL'])

s3Resource = boto3.resource('s3')

#This requires Kineses to add an end of line character after each record
#This is triggered by an S3 create event
def lambda_handler(event, context):
    try:
        logger.info('Start {}, Version {}'.format(context.function_name, context.function_version))
        logger.info('Event: ' + json.dumps(event))
        
        ctrModifiedFolder = os.environ['CTRModifiedS3Folder']
        
        bucketName, objectKey = parseEvent(event)
        
        records = parseObject (bucketName, objectKey)
        for record in records:
            if (record['AWSContactTraceRecordFormatVersion'] != '2017-03-10'):
                raise Exception('Invalid CTR version')
                
            mok = modifiedObjectKey(record, ctrModifiedFolder, objectKey)
            
            if processRecord(bucketName, mok, record):
                logger.info('Processing record:' + mok)
                
                record['Source'] = {'Bucket':bucketName, 'Key':objectKey}
                    
                flattenData = flatten(record, '_') 
                
                modifiedData = modifyFlattenData(flattenData)
                
                obj = s3Resource.Object(bucketName, mok)
                obj.put(Body=(bytes(json.dumps(modifiedData).encode('UTF-8'))))
            else:
                logger.info('Not processing record:' + mok)

    except Exception as e:
        logger.exception(e)
        raise Exception(e)

    finally:
        logger.info('Finished')
        
def modifiedObjectKey(record, ctrModifiedFolder, objectKey):
    uniqueIdValue = record['ContactId']
    key = objectKey.split('/')
    key[0] = ctrModifiedFolder
    key =  key[:-1] + [uniqueIdValue]
    key = '/'.join(key)
    return key
    
def processRecord(bucketName, modifiedObjectKey, ctr):
    obj = s3Resource.Object(bucketName, modifiedObjectKey)
    try:
        body = obj.get()['Body'].read()
        bodyString = body.decode('utf-8') 
        bodyJson = json.loads(bodyString)
        modifiedLastUpdateTimestamp = datetime.datetime.strptime(bodyJson['LastUpdateTimestamp'], '%Y-%m-%d %H:%M:%S') 
        
        ctrLastUpdateTimestamp = datetime.datetime.strptime(ctr['LastUpdateTimestamp'], '%Y-%m-%dT%H:%M:%SZ') 
        
        if (ctrLastUpdateTimestamp > modifiedLastUpdateTimestamp):
            return True
        else:
            return False
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return True
        else:
            raise Exception(e)
        
def modifyFlattenData(jsonData):
    # Athena Timestamp data type does not support Connect timestamps (yyyy-mm-ddThh:mm:ssZ)
    # https://docs.aws.amazon.com/connect/latest/adminguide/ctr-data-model.html
    # https://docs.aws.amazon.com/athena/latest/ug/data-types.html
    # Without this you need to use date_parse(EventTimestamp , '%Y-%m-%dT%H:%i:%s.%fZ') and represent it as a STRING
    for key in jsonData: 
        if 'timestamp' in key.lower():
            value = jsonData[key]
            
            if value is not None:
                modifiedTimestamp = value.replace('T', ' ')
                modifiedTimestamp = modifiedTimestamp.replace('Z', '')
                jsonData[key] = modifiedTimestamp
                
    # Simulate an external api call
    if 'Attributes_udCity' in jsonData:
        geo = getGeo(jsonData['Attributes_udCity'])
        if (geo != None):
            jsonData['Attributes_udCity_Latitude'] = geo['Latitude']
            jsonData['Attributes_udCity_Longitude'] = geo['Longitude']
            jsonData['Attributes_udCity_State'] = geo['State']
            
    # Convert a field
    if 'Attributes_udProjectTime' in jsonData:
        ptDuration = isodate.parse_duration(jsonData['Attributes_udProjectTime'])
        jsonData['Attributes_udProjectTime'] = int(ptDuration.total_seconds())
                
    # Add calculated fields
    dateFormat = '%Y-%m-%d %H:%M:%S'
    initiationTimestamp = datetime.datetime.strptime(jsonData['InitiationTimestamp'], dateFormat) 
    disconnectTimestamp = datetime.datetime.strptime(jsonData['DisconnectTimestamp'], dateFormat) 
    jsonData['ContactDuration'] = int((disconnectTimestamp - initiationTimestamp).total_seconds())
    
    if 'Queue_EnqueueTimestamp' in jsonData:
        enqueueTimestamp = datetime.datetime.strptime(jsonData['Queue_EnqueueTimestamp'], dateFormat) 
        jsonData['IvrDuration'] = int((enqueueTimestamp - initiationTimestamp).total_seconds())
    else:
        jsonData['IvrDuration'] = jsonData['ContactDuration']
        
    if 'Agent_AfterContactWorkDuration' in jsonData:
        if (jsonData['Agent_CustomerHoldDuration'] > 0): 
            jsonData['Agent_TalkDuration'] = jsonData['Agent_AfterContactWorkDuration'] - jsonData['Agent_CustomerHoldDuration']
        else:
            jsonData['Agent_TalkDuration'] = jsonData['Agent_AfterContactWorkDuration']
    
    jsonDataSorted = dict(sorted(jsonData.items()))
    return jsonDataSorted
        
def parseEvent(event):
    records = event['Records']
    if len(records) != 1:
        raise Exception('Invalid number of records')
    
    s3 = records[0]['s3']
    
    bucketName = s3['bucket']['name']
    
    objectKey = s3['object']['key']
    objectKey = unquote(objectKey)
    
    objectSize = s3['object']['size']
    if objectSize == 0:
        raise Exception('Empty object')
        
    return bucketName, objectKey
    
def parseObject (bucketName, objectKey):
    obj = s3Resource.Object(bucketName, objectKey)
    body = obj.get()['Body'].read()
    
    bodyString = body.decode('utf-8') 
    bodyStringParts = bodyString.splitlines()
    
    bodyJson = []
    for part in bodyStringParts:
        bodyJson.append(json.loads(part))
    
    return bodyJson

def getGeo(city):
    # https://www.gps-coordinates.net/
    # These cities match the cities in MockCtr
    cities = {
        'newyorkcity':      {'Latitude': 40.712728, 'Longitude': -74.006015,    'State': 'NY'}, 
        'losangeles':       {'Latitude': 34.053691, 'Longitude': -118.242766,   'State': 'CA'}, 
        'chicago':          {'Latitude': 41.875562, 'Longitude': -87.624421,    'State': 'IL'},  
        'houston':          {'Latitude': 29.758938, 'Longitude': -95.367697,    'State': 'TX'}, 
        'phoenix':          {'Latitude': 33.448437, 'Longitude': -112.074142,   'State': 'AZ'},  
        'philadelphia':     {'Latitude': 39.952724, 'Longitude': -75.163526,    'State': 'PA'},  
        'sanantonio':       {'Latitude': 29.4246,   'Longitude': -98.49514,     'State': 'TX'},  
        'sandiego':         {'Latitude': 32.71742,  'Longitude': -117.162773,   'State': 'CA'},  
        'dallas':           {'Latitude': 32.776272, 'Longitude': -96.796856,    'State': 'TX'},  
        'sanjose':          {'Latitude': 37.336191, 'Longitude': -121.890583,   'State': 'CA'},  
        'austin':           {'Latitude': 30.271129, 'Longitude': -97.743699,    'State': 'TX'},  
        'jacksonville':     {'Latitude': 30.332184, 'Longitude': -81.655651,    'State': 'FL'},  
        'fortworth':        {'Latitude': 32.753177, 'Longitude': -97.332746,    'State': 'TX'},  
        'columbus':         {'Latitude': 39.96226,  'Longitude': -83.000706,    'State': 'OH'},  
        'charlotte':        {'Latitude': 35.227209, 'Longitude': -80.843083,    'State': 'NC'},  
        'sanfrancisco':     {'Latitude': 37.779026, 'Longitude': -122.419906,   'State': 'CA'},  
        'indianapolis':     {'Latitude': 39.768333, 'Longitude': -86.15835,     'State': 'IN'},  
        'seattle':          {'Latitude': 47.603832, 'Longitude': -122.330062,   'State': 'WA'},  
        'denver':           {'Latitude': 39.739236, 'Longitude': -104.984862,   'State': 'CO'},  
        'washington':       {'Latitude': 38.895037, 'Longitude': -77.036543,    'State': 'WA'},  
        'boston':           {'Latitude': 42.360253, 'Longitude': -71.058291,    'State': 'CA'},  
        'elpaso':           {'Latitude': 31.775415, 'Longitude': -106.464634,   'State': 'TX'},  
        'nashville':        {'Latitude': 36.16223,  'Longitude': -86.774353,    'State': 'TN'}, 
        'detroit':          {'Latitude': 42.331551, 'Longitude': -83.04664,     'State': 'MI'}, 
        'oklahomacity':     {'Latitude': 35.472989, 'Longitude': -97.517054,    'State': 'OK'}, 
        'portland':         {'Latitude': 45.520247, 'Longitude': -122.674195,   'State': 'OR'}, 
        'lasvegas':         {'Latitude': 36.167256, 'Longitude': -115.148516,   'State': 'NV'}, 
        'memphis':          {'Latitude': 35.149022, 'Longitude': -90.051628,    'State': 'TN'}, 
        'louisville':       {'Latitude': 38.254238, 'Longitude': -85.759407,    'State': 'KY'}, 
        'baltimore':        {'Latitude': 39.290882, 'Longitude': -76.610759,    'State': 'MD'}, 
        'milwaukee':        {'Latitude': 43.034993, 'Longitude': -87.922497,    'State': 'WI'}, 
        'albuquerque':      {'Latitude': 35.084103, 'Longitude': -106.650985,   'State': 'NM'}, 
        'tucson':           {'Latitude': 32.222877, 'Longitude': -110.974848,   'State': 'AZ'}, 
        'fresno':           {'Latitude': 36.739442, 'Longitude': -119.784831,   'State': 'CA'}, 
        'mesa':             {'Latitude': 33.415112, 'Longitude': -111.831479,   'State': 'AZ'}, 
        'sacramento':       {'Latitude': 38.581061, 'Longitude': -121.493895,   'State': 'CA'}, 
        'atlanta':          {'Latitude': 33.748992, 'Longitude': -84.390264,    'State': 'GA'}, 
        'kansascity':       {'Latitude': 39.100105, 'Longitude': -94.578142,    'State': 'KS'}, 
        'coloradosprings':  {'Latitude': 38.833958, 'Longitude': -104.825348,   'State': 'CO'}, 
        'omaha':            {'Latitude': 41.258746, 'Longitude': -95.938376,    'State': 'NE'}, 
        'raleigh':          {'Latitude': 35.780398, 'Longitude': -78.639099,    'State': 'NC'}, 
        'miami':            {'Latitude': 25.774173, 'Longitude': -80.19362,     'State': 'FL'}, 
        'longbeach':        {'Latitude': 33.769016, 'Longitude': -118.191604,   'State': 'CA'}, 
        'virginiabeach':    {'Latitude': 36.852984, 'Longitude': -75.977418,    'State': 'VA'}, 
        'oakland':          {'Latitude': 37.804456, 'Longitude': -122.271356,   'State': 'CA'}, 
        'minneapolis':      {'Latitude': 44.9773,   'Longitude': -93.265469,    'State': 'MN'}, 
        'tulsa':            {'Latitude': 36.155681, 'Longitude': -95.992911,    'State': 'OK'}, 
        'tampa':            {'Latitude': 27.94776,  'Longitude': -82.458444,    'State': 'FL'}, 
        'arlington':        {'Latitude': 32.701939, 'Longitude': -97.105624,    'State': 'TX'}, 
        'neworleans':       {'Latitude': 29.949932, 'Longitude': -90.070116,    'State': 'LA'}, 
    }
    
    city = city.lower()
    city = city.replace(' ', '')
    city = city.replace('_', '')
    
    geo = None
    if city in cities:
        geo = cities[city]
    
    return geo