#!/usr/bin/python

import sys
sys.path.insert(1, '../packages')

import logging
import os
import boto3
from botocore.exceptions import ClientError
import zipfile
import time

LOG_FILE = 'deploy.log'
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)
    
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

class LexWrapper:
    # static
    LEX_MODEL_CLIENT = boto3.client('lex-models')
    
    RETRY_TIME = 2
    RETRY_NEEDED = 'retryNeeded'
    RETRY_RESPONSE = 'response'
    
    BOT_LATEST_VERSION = '$LATEST'
    
    def __init__(self):
        pass
    
    def initReturn(self):
        return {LexWrapper.RETRY_NEEDED: False, LexWrapper.RETRY_RESPONSE: None}
        
class StartImport(LexWrapper):
    def __init__(self):
        super().__init__()
        
    def run(self, botFile):
        logger.info('StartImport run')
        
        ZIP_FILE_NAME = 'bot.zip'
        zipObj = zipfile.ZipFile(ZIP_FILE_NAME, 'w')
        zipObj.write(botFile)
        zipObj.close()
        
        with open(ZIP_FILE_NAME, mode='rb') as f:
            zipFileData = f.read()
        
        if os.path.exists(ZIP_FILE_NAME):
            os.remove(ZIP_FILE_NAME)
        
        while True:
            resp = self.__wrapper(zipFileData, 'BOT', 'OVERWRITE_LATEST')
            if resp[LexWrapper.RETRY_NEEDED]:
                time.sleep(LexWrapper.RETRY_TIME)
            else:
                break
        
        return resp[LexWrapper.RETRY_RESPONSE]
    
    def __wrapper(self, payload, resourceType, mergeStrategy):
        retVal = self.initReturn()
        
        try:
            response = LexWrapper.LEX_MODEL_CLIENT.start_import(
                payload=payload,
                resourceType=resourceType,
                mergeStrategy=mergeStrategy,
            )
            logger.info(response)
            retVal[LexWrapper.RETRY_RESPONSE] = response
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConflictException':
                logger.info('ConflictException - Try again')
                retVal[LexWrapper.RETRY_NEEDED] = True
            else:
                raise RuntimeError(e)
        
        return retVal
        
class WaitForImportToComplete(LexWrapper):
    def __init__(self):
        super().__init__()
        
    def run(self, startImportResp):
        logger.info('WaitForImportToComplete run')
        
        while True:
            time.sleep(LexWrapper.RETRY_TIME)
            
            response = LexWrapper.LEX_MODEL_CLIENT.get_import(
                importId=startImportResp['importId']
            )
            logger.info(response)
            
            importStatus = response['importStatus']
            logger.info(importStatus)
            
            if importStatus != 'IN_PROGRESS':
                break
            
        if importStatus != 'COMPLETE':
            raise RuntimeError('WaitForImportToComplete failed')
            
class GetBot(LexWrapper):
    def __init__(self):
        super().__init__()
        
    def run(self, startImportResp):
        logger.info('GetBot run')
 
        response = LexWrapper.LEX_MODEL_CLIENT.get_bot(
            name=startImportResp['name'],
            versionOrAlias=LexWrapper.BOT_LATEST_VERSION
        )
        logger.info(response)
        
        return response
        
class PutBot(LexWrapper):
    def __init__(self):
        super().__init__()
        
    def run(self, getBotResp):
        logger.info('PutBot run')
        
        while True:
            resp = self.__wrapper(getBotResp)
            if resp[LexWrapper.RETRY_NEEDED]:
                time.sleep(LexWrapper.RETRY_TIME)
            else:
                break
        
        return resp[LexWrapper.RETRY_RESPONSE]
    
    def __wrapper(self, getBotResp):
        retVal = self.initReturn()
        
        try:
            response = LexWrapper.LEX_MODEL_CLIENT.put_bot(
                name=getBotResp['name'],
                intents=getBotResp['intents'],
                enableModelImprovements=getBotResp['enableModelImprovements'],
                clarificationPrompt=getBotResp['clarificationPrompt'],
                abortStatement=getBotResp['abortStatement'],
                idleSessionTTLInSeconds=getBotResp['idleSessionTTLInSeconds'],
                voiceId=getBotResp['voiceId'],
                checksum=getBotResp['checksum'],
                processBehavior='BUILD',
                locale=getBotResp['locale'],
                childDirected=getBotResp['childDirected'],
                detectSentiment=getBotResp['detectSentiment'],
                createVersion=False
            )
            logger.info(response)
            retVal[LexWrapper.RETRY_RESPONSE] = response
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConflictException':
                logger.info('ConflictException - Try again')
                retVal[LexWrapper.RETRY_NEEDED] = True
            else:
                raise RuntimeError(e)
        
        return retVal
        
class WaitForPutBotToComplete(LexWrapper):
    def __init__(self):
        super().__init__()
        
    def run(self, startImportResp):
        logger.info('WaitForPutBotToComplete run')
 
        while True:
            time.sleep(LexWrapper.RETRY_TIME)
            
            response = GetBot().run(startImportResp)
            status = response['status']
            logger.info(status)
            
            if status not in ['BUILDING', 'READY_BASIC_TESTING']:
                break
            
        if status == 'READY':
            return response
        else:
            raise RuntimeError('WaitForPutBotToComplete failed')
            
class CreateBotVersion(LexWrapper):
    def __init__(self):
        super().__init__()
        
    def run(self, waitPutBotResp):
        logger.info('CreateBotVersion run')
        
        while True:
            resp = self.__wrapper(waitPutBotResp)
            if resp[LexWrapper.RETRY_NEEDED]:
                time.sleep(LexWrapper.RETRY_TIME)
            else:
                break
        
        return resp[LexWrapper.RETRY_RESPONSE]
    
    def __wrapper(self, waitPutBotResp):
        retVal = self.initReturn()
        
        try:
            response = LexWrapper.LEX_MODEL_CLIENT.create_bot_version(
                name=waitPutBotResp['name'],
            )
            logger.info(response)
            retVal[LexWrapper.RETRY_RESPONSE] = response
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConflictException':
                logger.info('ConflictException - Try again')
                retVal[LexWrapper.RETRY_NEEDED] = True
            else:
                raise RuntimeError(e)
        
        return retVal
        
class WaitForCreateVersionToComplete(LexWrapper):
    def __init__(self):
        super().__init__()
        
    def run(self, createBotVersionResp):
        logger.info('WaitForCreateVersionToComplete run')
        
        while True:
            resp = self.__wrapper(createBotVersionResp)
            if resp[LexWrapper.RETRY_NEEDED]:
                time.sleep(LexWrapper.RETRY_TIME)
            else:
                break
        
        return resp[LexWrapper.RETRY_RESPONSE]
    
    def __wrapper(self, createBotVersionResp):
        retVal = self.initReturn()
        
        while True:
            time.sleep(LexWrapper.RETRY_TIME)
            
            try:
                response = LexWrapper.LEX_MODEL_CLIENT.create_bot_version(
                    name=createBotVersionResp['name'],
                    checksum = createBotVersionResp['checksum']
                )
                logger.info(response)
                status = response['status']
                logger.info(status)
                
                retVal[LexWrapper.RETRY_RESPONSE] = response

                if status not in ['BUILDING', 'READY_BASIC_TESTING']:
                    break
            
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConflictException':
                    logger.info('ConflictException - Try again')
                    retVal[LexWrapper.RETRY_NEEDED] = True
                    return retVal
                else:
                    raise RuntimeError(e)
        
        if status == 'READY':
            return retVal
        else:
            raise RuntimeError('WaitForCreateVersionToComplete failed')
        
class PutBotAlias(LexWrapper):
    def __init__(self):
        super().__init__()
        
    def run(self, waitForCreateVersionResp, botAlias):
        logger.info('PutBotAlias run')
        
        while True:
            resp = self.__wrapper(waitForCreateVersionResp, botAlias)
            if resp[LexWrapper.RETRY_NEEDED]:
                time.sleep(LexWrapper.RETRY_TIME)
            else:
                break
        
        return resp[LexWrapper.RETRY_RESPONSE]
    
    def __wrapper(self, waitForCreateVersionResp, botAlias):
        retVal = self.initReturn()
        
        try:
            response = LexWrapper.LEX_MODEL_CLIENT.put_bot_alias(
                name=botAlias,
                botName=waitForCreateVersionResp['name'],
                botVersion=waitForCreateVersionResp['version'],
            )
            logger.info(response)
            retVal[LexWrapper.RETRY_RESPONSE] = response
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConflictException':
                logger.info('ConflictException - Try again')
                retVal[LexWrapper.RETRY_NEEDED] = True
            else:
                raise RuntimeError(e)
        
        return retVal
        

def main():
    BOT_FILE = 'lexQA.json'
    BOT_ALIAS = 'prod'
    
    print('This script will take serveral minutes to complete.  See {} for progress'.format(LOG_FILE))
    
    startImportResp = StartImport().run(BOT_FILE)
    WaitForImportToComplete().run(startImportResp)
    
    getBotResp = GetBot().run(startImportResp)
    
    PutBot().run(getBotResp)
    waitPutBotResp = WaitForPutBotToComplete().run(startImportResp)
    
    createBotVersionResp = CreateBotVersion().run(waitPutBotResp)
    waitForCreateVersionResp = WaitForCreateVersionToComplete().run(createBotVersionResp)
    
    PutBotAlias().run(waitForCreateVersionResp, BOT_ALIAS)
    
    logger.info('Finished')
    print('Finished')
        
if __name__ == '__main__':
    main()  