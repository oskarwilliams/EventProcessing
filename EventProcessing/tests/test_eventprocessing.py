from eventprocessing import __version__
from eventprocessing.main import connectToBucket, setupQueue
import pytest

topicArn = 'arn:aws:sns:eu-west-1:552908040772:EventProcessing-Stack1-snsTopicSensorDataPart1-MEC8N0UVX4AR'
queueName = 'locationsQueueOW'
bucketName = 'eventprocessing-stack1-locationss3bucket-ivyo76ekdc6y'

def test_version():
    assert __version__ == '0.1.0'

def test_bucketConnecting():
    assert connectToBucket(bucketName).name == bucketName

def test_setupQueue():
    queue, queueArn = setupQueue(queueName, topicArn)
    assert queueArn == 'arn:aws:sqs:eu-west-1:552908040772:locationsQueueOW'
    queue.delete()