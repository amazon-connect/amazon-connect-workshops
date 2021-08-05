#!/usr/bin/python

# python3 create.py 

import sys
sys.path.insert(1, '../packages')

import boto3
import logging
import os
import shutil
import json
import datetime
import random
import string
from calendar import monthrange

LOG_FILE = 'create.log'
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)
    
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

ARN_ID = '00000000-0000-0000-0000-00000000000'
    
def getCity():
    # https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population
    values = [
        'New York City','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas',
        'San Jose','Austin','Jacksonville','Fort Worth','Columbus','Charlotte','San Francisco','Indianapolis','Seattle',
        'Denver','Washington','Boston','El Paso','Nashville','Detroit','Oklahoma City','Portland','Las Vegas','Memphis',
        'Louisville','Baltimore','Milwaukee','Albuquerque','Tucson','Fresno','Mesa','Sacramento','Atlanta','Kansas City',
        'Colorado Springs','Omaha','Raleigh','Miami','Long Beach','Virginia Beach','Oakland','Minneapolis','Tulsa',
        'Tampa','Arlington','New Orleans'
    ]
    
    return values[random.randint(0, len(values)-1)]
    
def getColor():
    # https://en.wikipedia.org/wiki/List_of_Crayola_crayon_colors
    values = [
        'Apricot','Black','Blue','Blue Green','Blue Violet','Brown','Carnation Pink','Cerulean','Dandelion','Gray',
        'Green','Green Yellow','Indigo','Orange','Red','Red Orange','Red Violet','Scarlet','Purple','Violet Red',
        'White','Yellow','Yellow Green','Yellow Orange'
    ]
    return values[random.randint(0, len(values)-1)]
    
def getFood():
    # https://www.listchallenges.com/print-list/38635
    values = [
        'Pasta','French Fries','Ice Cream','Bread','Fried Rice','Pancakes','Burger','Pizza','Pumpkin Pie',
        'Chicken Pot Pie','Banana','Apple Pie','Bagel','Muffins','Alfredo Sauce','Reece Peanut Cups',
        'Ice Cream Cake','Cheesecake','Cheese','Banana Bread','Potato Chips','Cheetos','Doritos','Tacos',
        'Burritos','Chimichanga','Enchilada','Salsa','Marinara Sauce','Broccoli',
        'Chocolate Covered Strawberries','Kiwi','Tomato','Salad','Steak','Chicken Tenders',
        'Grilled Chicken','Ribs','Biscuits and Gravy','Hot Dogs','Fried Chicken',
        'Roasted Chicken and Garlic','Eggs','Bacon','Sausage','Mashed Potatoes','Stuffing',
        'Brownies','Cookies','Submarine Sandwiches'
    ]
    return values[random.randint(0, len(values)-1)]
    
def getDOB():
    startDate = datetime.date(1920, 1, 1)
    endDate = datetime.date(2000, 1, 1)
    dayRange = (endDate - startDate).days
    randomNum = random.randint(0, dayRange)
    
    randomDate = startDate + datetime.timedelta(days=randomNum)
    strRandomDate = randomDate.strftime('%Y-%m-%d')
    
    return strRandomDate
    
def getProjectTime():
    # https://en.wikipedia.org/wiki/ISO_8601#Durations
    duration = ''
    
    minutes = random.randint(0, 59)
    if (minutes > 0):
        duration = str(minutes) + 'M'
        
    hours = random.randint(0, 23)
    if (hours > 0):
        duration = str(hours) + 'H' + duration
        
    if len(duration) > 0:
        duration = 'T' + duration
    
    days = random.randint(0, 2)
    if (days > 0):
        duration = str(days) + 'D' + duration
    
    if len(duration) > 0:
        duration = 'P' + duration
    else:
        duration = 'P1DT10M'
    
    return duration
    
def getCustomerPhoneNumber():
    # NPA-NXX-XXXX
    phoneNumber = '+1{}{}{}{}'.format(
        random.randint(2, 9),
        random.randint(1, 9),
        random.randint(1, 9),
        random.randint(2, 9)
        )
        
    for x in range (6):
        phoneNumber = phoneNumber + str(random.randint(1, 9))
    
    return phoneNumber
    
def addIvrAttributes():
    city = getCity()
    color = getColor()
    dob = getDOB()
    projectTime = getProjectTime()
    food = getFood()
    
    attr = {}
    
    #simulate abandoned calls in the IVR
    if (random.randint(1, 100) > 3):
        attr['udCity'] = city
        attr['udCounter'] = '1'
        attr['udPlay'] = 'What is your favorite US city?'
        
        if (random.randint(1, 100) > 5):
            attr['udColor'] = color
            attr['udCounter'] = '11'
            attr['udPlay'] = 'What is your favorite US color?'
            
            if (random.randint(1, 100) > 1):
                attr['udDOB'] = dob
                attr['udCounter'] = '111'
                attr['udPlay'] = 'What is your date of birth?'

                if (random.randint(1, 100) > 4):
                    attr['udProjectTime'] = projectTime
                    attr['udCounter'] = '1111'
                    attr['udPlay'] = 'How much time have you spent on this project?'
                    
                    if (random.randint(1, 100) > 2):
                        attr['udFood'] = food
                        attr['udCounter'] = '11111'
                        attr['udPlay'] = 'What is your favorite food?'

    return attr
    
def getQueue():
    queues = [
        {
            'arnID':'00000000-0000-0000-0000-00000000001',
            'name': 'Sales'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000002',
            'name': 'TechSupport'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000003',
            'name': 'Accounting'
        }
    ]
    
    return queues[random.randint(0, len(queues)-1)]
    
def addQueue(accountId, region):
    queueScheme = {
        'ARN': None,
        "DequeueTimestamp": None,
        "Duration": None,
        "EnqueueTimestamp": None,
        'Name': None
    }
    
    queue = getQueue()
    
    queueScheme['ARN'] = 'arn:aws:connect:{0}:{1}:instance/{2}/queue/{3}'.format(
        region,
        accountId,
        ARN_ID,
        queue['arnID']
    )
    
    queueScheme['Name'] = queue['name']
    
    return queueScheme
    
def getAgent():
    agents = [
        {
            'arnID':'00000000-0000-0000-0000-00000000001',
            'username': 'Liam'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000002',
            'username': 'Olivia'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000003',
            'username': 'Noah'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000004',
            'username': 'Emma'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000005',
            'username': 'Oliver'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000006',
            'username': 'Ava'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000007',
            'username': 'William'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000008',
            'username': 'Sophia'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000009',
            'username': 'James'
        },
        {
            'arnID':'00000000-0000-0000-0000-00000000010',
            'username': 'Charlotte'
        }
    ]
    
    return agents[random.randint(0, len(agents)-1)]
    
def addAgent(accountId, region):
    agentScheme = {
        'ARN': None,
        'AfterContactWorkDuration': None,
        'AfterContactWorkEndTimestamp': None,
        'AfterContactWorkStartTimestamp': None,
        'AgentInteractionDuration': None,
        'ConnectedToAgentTimestamp': None,
        'CustomerHoldDuration': 0,
        'HierarchyGroups': None,
        'LongestHoldDuration': 0,
        'NumberOfHolds': 0,
        'RoutingProfile': {
          'ARN': None,
          'Name': 'QABot Routing Profile'
        },
        'Username': None
    }
    
    agent = getAgent()
        
    agentScheme['ARN'] = 'arn:aws:connect:{0}:{1}:instance/{2}/agent/{3}'.format(
        region,
        accountId,
        ARN_ID,
        agent['arnID']
    )
    
    agentScheme['Username'] = agent['username']
    
    agentScheme['RoutingProfile']['ARN'] = 'arn:aws:connect:{0}:{1}:instance/{2}/routing-profile/{3}'.format(
        region,
        accountId,
        ARN_ID,
        ARN_ID
    )
    
    if (random.randint(1, 100) > 75):
        numOfHolds = random.randint(1, 5)
        agentScheme['NumberOfHolds'] = numOfHolds
        
        holdDurationMin = random.randint(1, 5) * 60
        agentScheme['LongestHoldDuration'] = holdDurationMin
        agentScheme['CustomerHoldDuration'] = holdDurationMin
        
        if (numOfHolds > 1):
            agentScheme['CustomerHoldDuration'] = holdDurationMin + ((numOfHolds-1) * (random.randint(1, holdDurationMin)))
    
    return agentScheme
    
def setDisconnectReason(isQueue, isAgent):
    if (isQueue):
        if (isAgent):
            val = random.randint(1, 100)
            if (val > 97):
                return 'TELECOM_PROBLEM'
            elif (val > 90):
                return 'AGENT_DISCONNECT'
            else:
                return 'CUSTOMER_DISCONNECT' 
        else:
            if (random.randint(1, 100) > 3):
                return 'CUSTOMER_DISCONNECT'
            else:
                return 'TELECOM_PROBLEM'
    else:
        return 'CONTACT_FLOW_DISCONNECT'
    
def setCustomerJourneyTS(templateCTR, year, month, day):
    isoTimestampFormat = '%Y-%m-%dT%H:%M:%SZ'
    
    startTime = datetime.datetime(year, month, day) + datetime.timedelta(hours=random.randint(8, 18))
    strStartTime = startTime.strftime(isoTimestampFormat)
    templateCTR['InitiationTimestamp'] = strStartTime
    templateCTR['ConnectedToSystemTimestamp'] = strStartTime
    
    timeInIVRSec = random.randint(10, 45)
    
    timeInQueueSec = 0
    if (templateCTR['Queue'] is not None):
        enqueueTime = startTime + datetime.timedelta(seconds=timeInIVRSec)
        templateCTR['Queue']['EnqueueTimestamp'] = enqueueTime.strftime(isoTimestampFormat)
        
        timeInQueueSec = random.randint(10, 120)
        templateCTR['Queue']['Duration'] = timeInQueueSec
        
        dequeueTime = enqueueTime + datetime.timedelta(seconds=timeInQueueSec)
        templateCTR['Queue']['DequeueTimestamp'] = dequeueTime.strftime(isoTimestampFormat)
    
    timeWithAgentSec = 0
    acwSec = 0
    if (templateCTR['Agent'] is not None):
        if (templateCTR['Queue'] is None):
            raise RuntimeError('Queue cannot be None')  
        
        templateCTR['Agent']['ConnectedToAgentTimestamp'] = templateCTR['Queue']['DequeueTimestamp']
        
        holdDuration = templateCTR['Agent']['CustomerHoldDuration']
        timeWithAgentSec = (random.randint(1, 10) * 60) + holdDuration
        templateCTR['Agent']['AgentInteractionDuration'] = timeWithAgentSec
        
        acwStartTime = dequeueTime + datetime.timedelta(seconds=timeWithAgentSec)
        templateCTR['Agent']['AfterContactWorkStartTimestamp'] = acwStartTime.strftime(isoTimestampFormat)
        
        acwSec = random.randint(1, 10) * 60
        templateCTR['Agent']['AfterContactWorkDuration'] = acwSec
        
        acwEndTime = acwStartTime + datetime.timedelta(seconds=acwSec)
        templateCTR['Agent']['AfterContactWorkEndTimestamp'] = acwEndTime.strftime(isoTimestampFormat)
        
    endTime = startTime + \
        datetime.timedelta(seconds=timeInIVRSec) + \
        datetime.timedelta(seconds=timeInQueueSec) + \
        datetime.timedelta(seconds=timeWithAgentSec) + \
        datetime.timedelta(seconds=acwSec)
    strEndTime = endTime.strftime(isoTimestampFormat)
    templateCTR['DisconnectTimestamp'] = strEndTime
    templateCTR['LastUpdateTimestamp'] = strEndTime

    return templateCTR
    
def createCTR(accountId, region, year, month, day):
    templateCTR = {
        'AWSAccountId': None,
        'AWSContactTraceRecordFormatVersion': '2017-03-10',
        'Agent': None,
        'AgentConnectionAttempts': 0,
        'Attributes': {},
        'Channel': 'VOICE',
        'ConnectedToSystemTimestamp': None,
        'ContactDetails': {},
        'ContactId': None,
        'CustomerEndpoint': {
            'Address': None,
            'Type': 'TELEPHONE_NUMBER'
        },
        'DisconnectReason': 'CONTACT_FLOW_DISCONNECT',
        'DisconnectTimestamp': None,
        'InitialContactId': None,
        'InitiationMethod': 'INBOUND',
        'InitiationTimestamp': None,
        'InstanceARN': None,
        'LastUpdateTimestamp': None,
        'MediaStreams': [{
                'Type': 'AUDIO'
            }
        ],
        'NextContactId': None,
        'PreviousContactId': None,
        'Queue': None,
        'Recording': None,
        'Recordings': None,
        'References': [],
        'SystemEndpoint': {
            'Address': '+19999999999',
            'Type': 'TELEPHONE_NUMBER'
        },
        'TransferCompletedTimestamp': None,
        'TransferredToEndpoint': None
    }
    
    templateCTR['AWSAccountId'] = accountId
    templateCTR['InstanceARN'] = 'arn:aws:connect:{0}:{1}:instance/{2}'.format(
        region,
        accountId,
        ARN_ID
    )
    
    templateCTR['CustomerEndpoint']['Address'] = getCustomerPhoneNumber()
        
    # aabafe69-0d00-4783-9d70-2842c10700b2
    charChoice = string.ascii_letters + string.digits
    contactId = 'FakeCtr1-'
    contactId += ''.join(random.choice(charChoice) for i in range(4)) + '-'
    contactId += ''.join(random.choice(charChoice) for i in range(4)) + '-'
    contactId += ''.join(random.choice(charChoice) for i in range(4)) + '-'
    contactId += ''.join(random.choice(charChoice) for i in range(12))
    templateCTR['ContactId'] = contactId
    templateCTR['InitialContactId'] = contactId
    
    templateCTR['Attributes'] = addIvrAttributes()
    
    if (random.randint(1, 100) > 75): 
        templateCTR['Queue'] = addQueue(accountId, region)
        
        if (random.randint(1, 100) > 25):  
            templateCTR['Agent'] = addAgent(accountId, region)
            
    templateCTR['DisconnectReason'] = setDisconnectReason(
        templateCTR['Queue'] is not None, 
        templateCTR['Agent'] is not None
    )
            
    templateCTR = setCustomerJourneyTS(templateCTR, year, month, day)
    
    strCTR = json.dumps(templateCTR)
    json.loads(strCTR) #verify that there are no parsing errors
    return strCTR

def main():
    CTR_DIR = './ctr'
    if os.path.exists(CTR_DIR):
        shutil.rmtree(CTR_DIR)
    os.mkdir(CTR_DIR)
    
    accountId=boto3.client("sts").get_caller_identity()["Account"]
    region = boto3.session.Session().region_name
    
    counter = 0
    today = datetime.datetime.utcnow()

    for year in range(today.year -1, today.year + 1):
        yearDir = '{0}/year={1}'.format(CTR_DIR, year)
        os.mkdir(yearDir)

        for month in range(1, 13):
            if (year == today.year) and (month > today.month): 
                continue
            
            monthDir = '{0}/month={1:02}'.format(yearDir, month)
            os.mkdir(monthDir)
            
            notUsed, days = monthrange(year, month)
            days+=1
            
            for day in range(1, days): 
                if (year == today.year) and (month == today.month) and (day > today.day): 
                    continue
                
                dayDir = '{0}/day={1:02}'.format(monthDir, day)
                os.mkdir(dayDir)
                
                numOfCtrBatches = random.randint(10, 50)
                
                for batch in range(numOfCtrBatches):
                    batchFileName = '{0}/fakeCtr-{1}-{2:02}-{3:02}-B{4:02}'.format(dayDir, year, month, day, batch)
                    
                    with open(batchFileName, 'w') as f:
                        numOfCtrs = random.randint(1, 10)
                        for ctr in range(numOfCtrs):
                            f.write(createCTR(accountId, region, year, month, day))
                            f.write('\n') #firehoseAddNewLine lambda
                            counter+=1
    
    logger.info('Finished {0} CTRs created'.format(counter))
        
if __name__ == '__main__':
    main() 