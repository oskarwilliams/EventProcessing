import boto3
import ast
import json


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


def printMessage(queue):
    with open('locations.json', 'r') as f:
        locations_dict = json.load(f)
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
                location = messageDict['locationId']
                if len(list(filter(lambda validLocation: validLocation['id'] == location, locations_dict))) != 0:
                    print(messageDict)

    except KeyboardInterrupt:
        print('Shutting Down Gracefully')
        boto3.client('sqs').delete_queue(
            QueueUrl=queue.url
        )




topicArn = 'arn:aws:sns:eu-west-1:552908040772:EventProcessing-Stack1-snsTopicSensorDataPart1-MEC8N0UVX4AR'
queueName = 'locationsQueueOW'
queue = startup(topicArn, queueName)
printMessage(queue)
