import boto3
import ast
import json
import time


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


def startup(topicArn, queueName):

    s3 = boto3.resource('s3')
    sqs = boto3.resource('sqs')
    sns = boto3.resource('sns')
    topic = sns.Topic(topicArn)
    bucket = s3.Bucket('eventprocessing-stack1-locationss3bucket-ivyo76ekdc6y')

    queue = sqs.create_queue(QueueName=queueName)
    queueArn = queue.attributes.get('QueueArn')

    policy_json = allow_sns_to_write_to_sqs(topicArn, queueArn)

    response = boto3.client('sqs').set_queue_attributes(
        QueueUrl=queue.url,
        Attributes={
            'Policy': policy_json
        }
    )

    notifier = topic.subscribe(
        Protocol='sqs',
        Endpoint=queueArn,
        ReturnSubscriptionArn=True
    )

    bucket.download_file('locations.json', 'locations.json')
    return queue


def writeAverageValue(averageCounter, end, messageArrays):
    totalValue = 0
    numOfMessages = 0

    for messageArray in messageArrays:
        for message in messageArray:
            if message['timestamp'] > (end - 360)*1000 and message['timestamp'] < (end - 300)*1000:
                totalValue += message['value']
                numOfMessages += 1

    if numOfMessages != 0:
        averageValue = totalValue/numOfMessages

        with open("outputAverageValue.json", "a") as outfile:
            if averageCounter != 0:
                outfile.write(', ')
            json.dump({
                'averageValue': averageValue,
                'startTime': end - 360,
                'endTime': end - 300
            }, outfile)
        averageCounter += 1
    return averageCounter


def writeRawOutput(messageDict, messageArrays, locationsDict, rawCounter):

    if len(list(filter(lambda location: location['id'] == messageDict['locationId'], locationsDict))) != 0:
        doesntAlreadyExists = True
        for messageArray in messageArrays:
            if messageDict in messageArray:
                doesntAlreadyExists = False

        if doesntAlreadyExists:
            with open("outputRaw.json", "a") as outfile:
                if rawCounter != 0:
                    outfile.write(', ')
                json.dump(messageDict, outfile)
            rawCounter += 1
            messageArrays[0] += [messageDict]
    return messageArrays, rawCounter


def logMessages(queue):
    with open('locations.json', 'r') as f:
        locationsDict = json.load(f)
    with open("outputRaw.json", "w") as outfile:
        outfile.write('[')
    with open("outputAverageValue.json", "w") as outfile:
        outfile.write('[')

    start = time.time()
    rawCounter = 0
    averageCounter = 0
    messageArrays = [[]]
    messageArrays[0] = []

    try:
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
                    messageDict, messageArrays, locationsDict, rawCounter)

            end = time.time()

            if end - start > 60:

                averageCounter = writeAverageValue(
                        averageCounter, end, messageArrays)   
                start = time.time()

                for i in range(9, 0):
                    if i == 0:
                        messageArrays[i] = []
                    else:
                        messageArrays[i] = messageArrays[i-1]

    except KeyboardInterrupt:
        print('Shutting Down Gracefully')
        with open("outputRaw.json", "a") as outfile:
            outfile.write(']')
        with open("outputAverageValue.json", "a") as outfile:
            outfile.write(']')

        boto3.client('sqs').delete_queue(
            QueueUrl=queue.url
        )

topicArn = 'arn:aws:sns:eu-west-1:552908040772:EventProcessing-Stack1-snsTopicSensorDataPart1-MEC8N0UVX4AR'
queueName = 'locationsQueueOW'
queue = startup(topicArn, queueName)
logMessages(queue)
