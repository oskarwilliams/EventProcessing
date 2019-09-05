import boto3
import ast
import json
import time
import logging

topicArn = 'arn:aws:sns:eu-west-1:552908040772:EventProcessing-Stack1-snsTopicSensorDataPart1-MEC8N0UVX4AR'
queueName = 'locationsQueueOW'
bucketName = 'eventprocessing-stack1-locationss3bucket-ivyo76ekdc6y'
outputRawName = 'outputRaw.json'
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
    logging.info('Downloading locations.json')
    bucket.download_file('locations.json', 'locations.json')
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


def writeAverageValue(end, messageArrays, locationsDict):
    totalValue = {}
    numOfMessages = {}
    for location in locationsDict:
        totalValue[location['id']] = 0
        numOfMessages[location['id']] = 0

    for messageArray in messageArrays:
        for location in locationsDict:
            messages = list(filter(lambda message: message['locaitonId']==location['id'], messageArrays))
            for message in messageArray:
                if message['timestamp'] > (end - 360)*1000 and message['timestamp'] < (end - 300)*1000:
                    totalValue[location['id']] += message['value']
                    numOfMessages[location['id']] += 1
    for location in locationsDict:
        if numOfMessages[location['id']] != 0:
            averageValue = totalValue[location['id']]/numOfMessages[location['id']]

            with open(f"outputs/{outputAverageName}AtID{location['id']}.json", "a") as outfile:
                if averageCounter[location['id']] != 0:
                    outfile.write(', ')
                json.dump({
                    'averageValue': averageValue,
                    'startTime': end - 360,
                    'endTime': end - 300
                }, outfile)
            averageCounter[location['id']] += 1


def processMessages(messageDict, messageArrays, locationsDict):

    if len(list(filter(lambda location: location['id'] == messageDict['locationId'], locationsDict))) != 0:
        doesntAlreadyExists = True
        for messageArray in messageArrays:
            if messageDict in messageArray:
                doesntAlreadyExists = False

        if doesntAlreadyExists:
            messageArrays[0] += [messageDict]

    return messageArrays


def logMessages(queue):
    with open('locations.json', 'r') as infile:
        locationsDict = json.load(infile)
    for location in locationsDict:
        averageCounter[location['id']] = 0
        with open(f"outputs/{outputAverageName}AtID{location['id']}.json", "w") as outfile:
            outfile.write('[')

    start = time.time()
    startCounter = 0
    messageArrays = [[]]
    messageArrays[0] = []

    try:
        logging.info('Taking messages from queue')
        while True:

            messages = queue.receive_messages(AttributeNames=[
                'All'
            ],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10,
            )

            for message in messages:
                messageBodyDict = ast.literal_eval(message.body)
                messageDict = ast.literal_eval(messageBodyDict['Message'])
                messageArrays = processMessages(messageDict, messageArrays, locationsDict)
                message.delete()

            end = time.time()

            if end - start > 60:
                startCounter += 1
                logging.info(f'Queue is of approximate size: {queue.attributes["ApproximateNumberOfMessages"]}')
                if startCounter >= 3:
                    writeAverageValue(end, messageArrays, locationsDict)
                start = time.time()

                for i in range(9, 0):
                    if i == 0:
                        messageArrays[i] = []
                    else:
                        messageArrays[i] = messageArrays[i-1]

    except KeyboardInterrupt:
        print('Shutting Down Gracefully')
        # with open(f"outputs/{outputRawName}", "a") as outfile:
        #     outfile.write(']')
        for location in locationsDict:
            with open(f"outputs/{outputAverageName}AtID{location['id']}.json", "a") as outfile:
                outfile.write(']')

        queue.delete()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    queue = startup()
    logMessages(queue)