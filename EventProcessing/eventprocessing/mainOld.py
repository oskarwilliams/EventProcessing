import boto3
import ast
import json
import time
import logging

def allow_sns_to_write_to_sqs(topicarn, queuearn):
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
}}""".format(queuearn, topicarn)

    return policy_document


def connectToBucket(bucketName):
    s3 = boto3.resource('s3')
    logging.info(f'Connecting to S3 Bucket: {bucketName}')
    bucket = s3.Bucket(bucketName)
    logging.info('Downloading locations.json')
    bucket.download_file('locations.json', 'locations.json')
    return bucket

def setupQueue(queueName, topicArn):
    sqs = boto3.resource('sqs')
    logging.info(f'Creating queue: {queueName}')
    queue = sqs.create_queue(QueueName=queueName)
    queueArn = queue.attributes.get('QueueArn')
    policy_json = allow_sns_to_write_to_sqs(topicArn, queueArn)
    response = boto3.client('sqs').set_queue_attributes(
        QueueUrl=queue.url,
        Attributes={
            'Policy': policy_json
        }
    )
    return queue, queueArn


def subscribeQueue(topicArn, queueArn):
    sns = boto3.resource('sns')
    logging.info('Subscribing queue to topic: ' + topicArn)
    topic = sns.Topic(topicArn)
    notifier = topic.subscribe(
        Protocol='sqs',
        Endpoint=queueArn,
        ReturnSubscriptionArn=True
    )


def startup(topicArn, queueName, bucketName):

    connectToBucket(bucketName)
    queue, queueArn = setupQueue(queueName, topicArn)
    subscribeQueue(topicArn, queueArn)
    return queue


def writeAverageValue(averageCounter, end, messageArrays, outputName):
    totalValue = 0
    numOfMessages = 0

    for messageArray in messageArrays:
        for message in messageArray:
            if message['timestamp'] > (end - 360)*1000 and message['timestamp'] < (end - 300)*1000:
                totalValue += message['value']
                numOfMessages += 1

    if numOfMessages != 0:
        averageValue = totalValue/numOfMessages

        with open(f"outputs/{outputName}", "a") as outfile:
            if averageCounter != 0:
                outfile.write(', ')
            json.dump({
                'averageValue': averageValue,
                'startTime': end - 360,
                'endTime': end - 300
            }, outfile)
        averageCounter += 1
    return averageCounter


def writeRawOutput(messageDict, messageArrays, locationsDict, rawCounter, outputName):

    if len(list(filter(lambda location: location['id'] == messageDict['locationId'], locationsDict))) != 0:
        doesntAlreadyExists = True
        for messageArray in messageArrays:
            if messageDict in messageArray:
                doesntAlreadyExists = False

        if doesntAlreadyExists:
            # with open(f"outputs/{outputName}", "a") as outfile:
            #     if rawCounter != 0:
            #         outfile.write(', ')
            #     json.dump(messageDict, outfile)
            rawCounter += 1
            messageArrays[0] += [messageDict]
    return messageArrays, rawCounter


def logMessages(queue, outputRawName, outputAverageName):
    with open('locations.json', 'r') as infile:
        locationsDict = json.load(infile)
    # with open(f"outputs/{outputRawName}", "w") as outfile:
        # outfile.write('[')
    with open(f"outputs/{outputAverageName}", "w") as outfile:
        outfile.write('[')

    startTime = time.time()
    rawCounter = 0
    averageCounter = 0
    messageArrays = [[]]
    messageArrays[0] = []

    try:
        logging.info('Taking messages from queue')
        logging.info(f'Start time is {startTime}')
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
                messageArrays, rawCounter = writeRawOutput(
                    messageDict, messageArrays, locationsDict, rawCounter, outputRawName)
                message.delete()

            endTime = time.time()

            if endTime - startTime > 60:

                averageCounter = writeAverageValue(
                    averageCounter, endTime, messageArrays, outputAverageName)
                startTime = time.time()

                for i in range(9, 0):
                    if i == 0:
                        messageArrays[i] = []
                    else:
                        messageArrays[i] = messageArrays[i-1]

    except KeyboardInterrupt:
        print('Shutting Down Gracefully')
        # with open(f"outputs/{outputRawName}", "a") as outfile:
        #     outfile.write(']')
        with open(f"outputs/{outputAverageName}", "a") as outfile:
            outfile.write(']')

        queue.delete()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    topicArn = 'arn:aws:sns:eu-west-1:552908040772:EventProcessing-Stack1-snsTopicSensorDataPart1-MEC8N0UVX4AR'
    queueName = 'locationsQueueOW'
    bucketName = 'eventprocessing-stack1-locationss3bucket-ivyo76ekdc6y'
    outputRawName = 'outputRaw.json'
    outputAverageName = 'outputAverageValue.json'
    queue = startup(topicArn, queueName, bucketName)
    logMessages(queue, outputRawName, outputAverageName)
