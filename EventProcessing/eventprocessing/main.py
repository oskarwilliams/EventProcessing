import boto3
import ast
import json
import logging
import cProfile
import pstats
from pstats import SortKey
import concurrent.futures
import threading
import random
import time
from functools import reduce

thread_local = threading.local()

topicArn = 'arn:aws:sns:eu-west-1:552908040772:EventProcessing-Stack1-snsTopicSensorDataPart2-7992TP4VVW59'
# topicArn = 'arn:aws:sns:eu-west-1:552908040772:EventProcessing-Stack1-snsTopicSensorDataPart1-MEC8N0UVX4AR'
queueName = f'locationsQueueOW{random.randint(1,100)}'
bucketName = 'eventprocessing-stack1-locationss3bucket-ivyo76ekdc6y'
locationsKey = 'locations-part2.json'
outputAverageName = 'outputAverageValue.json'
averageCounter = {}

def allow_sns_to_write_to_sqs(queueArn):
    policy_document = """{{
  "Version":"2012-10-17",
  "Statement":[
    {{
      "Sid":"MyPolicy",
      "Effect":"Allow",
      "Principal" : {{"AWS" : "*"}},
      "Action":"SQS:SendMessage",
      "Resource": "{}",
      "Condition":{{
        "ArnEquals":{{
          "aws:SourceArn": "{}"
        }}
      }}
    }}
  ]
}}""".format(queueArn, topicArn)

    return policy_document


def connectToBucket():
    s3 = boto3.resource('s3')
    logging.info(f'Connecting to S3 Bucket: {bucketName}')
    bucket = s3.Bucket(bucketName)
    logging.info(f'Downloading: {locationsKey}')
    bucket.download_file(locationsKey, 'locations.json')
    return bucket

def setupQueue():
    sqs = boto3.resource('sqs')
    logging.info(f'Creating queue: {queueName}')
    queue = sqs.create_queue(QueueName=queueName)
    queueArn = queue.attributes.get('QueueArn')
    policy_json = allow_sns_to_write_to_sqs(queueArn)
    response = boto3.client('sqs').set_queue_attributes(
        QueueUrl=queue.url,
        Attributes={
            'Policy': policy_json
        }
    )
    return queue, queueArn

def subscribeQueue(queueArn):
    sns = boto3.resource('sns')
    logging.info(f'Subscribing queue to topic: {topicArn}')
    topic = sns.Topic(topicArn)
    notifier = topic.subscribe(
        Protocol='sqs',
        Endpoint=queueArn,
        ReturnSubscriptionArn=True
    )

def startup():
    connectToBucket()
    queue, queueArn = setupQueue()
    subscribeQueue(queueArn)
    return queue

def writeAverageValue(endTime, messageArray, locationsDict):
    totalValue = {}
    numOfMessages = {}
    for location in locationsDict:
        totalValue[location['id']] = 0
        numOfMessages[location['id']] = 0

    for messageDict in messageArray:
        for location in locationsDict:
            for message in messageDict.values():
                message = message[0]
                try:
                    if (message['timestamp'] > (endTime - 360)*1000 and message['timestamp'] < (endTime - 300)*1000) and message['locationId']==location['id']:
                        totalValue[location['id']] += message['value']
                        numOfMessages[location['id']] += 1
                except KeyError:
                    logging.warning(f'Key Error in: {message}')

    for location in locationsDict:
        if numOfMessages[location['id']] != 0:
            averageValue = totalValue[location['id']]/numOfMessages[location['id']]

            with open(f"outputs/{outputAverageName}AtID{location['id']}.json", "a") as outfile:
                if averageCounter[location['id']] != 0:
                    outfile.write(', ')
                json.dump({
                    'averageValue': averageValue,
                    'startTime': endTime - 360,
                    'endTime': endTime - 300
                }, outfile)
            averageCounter[location['id']] += 1

def doesMessageExistInDict(messageRead, messageDict):
    doesntAlreadyExists = True
    if any([messageRead['eventId'] == messageKey for messageKey in messageDict]):
        logging.info('Duplicate message removed')
        doesntAlreadyExists = False
    return doesntAlreadyExists

def doesMessageExistAtAll(messageRead, messageArray):
    for messageDict in messageArray:
        alreadyExists = [doesMessageExistInDict(messageRead, messageDict)]
    alreadyExistsReduced = all(alreadyExists)
    return alreadyExistsReduced

def parseMessage(message):
    try:
        messageBodyDict = ast.literal_eval(message.body)
        messageDict = ast.literal_eval(messageBodyDict['Message'])
        message.delete()
        return messageDict
    except SyntaxError:
        logging.warning(f'Syntax Error in message {message.body}')
        message.delete()        
    except ValueError:
        logging.warning(f'Value Error in message {message.body}')
        message.delete()

def processMessage(messageRead, messageArray, locationsDict):
    try:
        if any([location['id'] == messageRead['locationId'] for location in locationsDict]):
            doesntAlreadyExists = doesMessageExistAtAll(messageRead, messageArray)
            if doesntAlreadyExists:
                return messageRead
    except KeyError:
        logging.warning(f'Syntax Error in message {messageRead}')
        return None

def processMessagesThreading(messages, messageArray, locationsDict):
    max_workers=10

    with concurrent.futures.ThreadPoolExecutor(max_workers = max_workers) as executor:
        parsedMessagesFutures = [executor.submit(parseMessage, messages[i]) for i in range(len(messages))]

    parsedMessages = []
    for parsedMessageFuture in parsedMessagesFutures:
        parsedMessages += [parsedMessageFuture.result()]
    messagesCleaned = [dict(t) for t in {tuple(message.items()) for message in parsedMessages if message is not None }]

    with concurrent.futures.ThreadPoolExecutor(max_workers = max_workers) as executor:
        processedMessages = [executor.submit(processMessage, messagesCleaned[i], messageArray, locationsDict) for i in range(len(messagesCleaned))]

    for processedMessage in processedMessages:
        message = processedMessage.result()
        if message != None:
            messageArray[0][message['eventId']] = [message]
    return messageArray

def getMessages(queue):
    return queue.receive_messages(AttributeNames=['All'], MaxNumberOfMessages=10)

def getMessagesThreading(queue):
    max_workers=1

    with concurrent.futures.ThreadPoolExecutor(max_workers = max_workers) as executor:
        messagesFutures = [executor.submit(getMessages, queue) for _ in range(max_workers)]

    messages = reduce(lambda m1, m2: m1 + m2.result(), messagesFutures, [])
    return messages

def logMessages(queue):
    with open('locations.json', 'r') as infile:
        locationsDict = json.load(infile)
    for location in locationsDict:
        averageCounter[location['id']] = 0
        with open(f"outputs/{outputAverageName}AtID{location['id']}.json", "w") as outfile:
            outfile.write('[')

    startTime = time.time()
    startCounter = 0
    messageArray = [{} for i in range(14)]
    messageArray[0] = {}
    excepter = True

    try:
        logging.info('Taking messages from queue')
        while True:
            messages = getMessagesThreading(queue)
            messageArray = processMessagesThreading(messages, messageArray, locationsDict)
            endTime = time.time()

            if endTime - startTime > 30:
                startCounter += 1
                logging.info(f'Queue is of approximate size: {queue.attributes["ApproximateNumberOfMessages"]}')
                if startCounter >= 5 and startCounter % 2 == 0:
                    writeAverageValue(endTime, messageArray, locationsDict)
                startTime = time.time()

                for i in range(14, 0):
                    if i == 0:
                        messageArray[i] = []
                    else:
                        messageArray[i] = messageArray[i-1]

    except KeyboardInterrupt:
        print('Shutting Down Gracefully')
        for location in locationsDict:
            with open(f"outputs/{outputAverageName}AtID{location['id']}.json", "a") as outfile:
                outfile.write(']')
        queue.delete()
        excepter = False

    finally:
        if excepter:
            print('Shutting Down Horribly')
            for location in locationsDict:
                with open(f"outputs/{outputAverageName}AtID{location['id']}.json", "a") as outfile:
                    outfile.write(']')
            queue.delete()

def runProg():
    queue = startup()
    logMessages(queue)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # runProg()
    cProfile.run('runProg()', 'restats')
    pstats.Stats('restats').sort_stats(SortKey.TIME).print_stats('main')
