import os
import json
import logging
import base64
 
logger = logging.getLogger()
logger.setLevel(os.environ['LOG_LEVEL'])

def lambda_handler(event, context):
    try:
        logger.info('Start {}, Version {}'.format(context.function_name, context.function_version))
        logger.info('Event: ' + json.dumps(event))
        
        output = []
        for record in event['records']:
            payload = base64.b64decode(record['data'])
            payload = payload + '\n'.encode('ascii')

            outputRecord = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(payload)
            }
            output.append(outputRecord)
            
        returnValue = {'records': output}
        logger.info(returnValue)
        return returnValue
        
    except Exception as e:
        logger.exception(e)
        raise Exception(e)

    finally:
        logger.info('Finished')
