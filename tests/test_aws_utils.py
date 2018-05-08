import io
import logging
import uuid
import boto3
import json
from botocore.stub import Stubber
from botocore.response import StreamingBody
from unittest import TestCase
from unittest.mock import patch
from cod_services.aws_utils import AWSHelper
logger = logging.getLogger()


class SNSTestCase(TestCase):
    def setUp(self):
        with patch.dict('os.environ', {"AWS_DEFAULT_REGION": "us-west-2"}):
            client = boto3.client('sns')
            self.stubber = Stubber(client)
            self.helper = AWSHelper(logger=logger, sns_client=client)

    def test_publish_to_sns_topic(self):
        message = {"message": "test"}
        activity_id = uuid.uuid4().hex
        subject = "Subject"
        event_type = 'test'
        topic_arn = "arn:aws:sns:us-west-2:111122223333:MyTopic"
        attrs = {
            'EventType': {'DataType': 'String', 'StringValue': event_type},
            'ActivityID': {'DataType': 'String', 'StringValue': activity_id}
        }
        params = dict(
            TopicArn=topic_arn,
            Message=json.dumps(message),
            Subject=subject,
            MessageAttributes=attrs
        )
        self.stubber.add_response('publish', {}, params)
        self.stubber.activate()
        self.helper.publish_to_sns_topic(
            message=message,
            activity_id=activity_id,
            topic_arn=topic_arn,
            event_type=event_type,
            subject=subject
        )


class SQSTestCase(TestCase):
    def setUp(self):
        with patch.dict('os.environ', {"AWS_DEFAULT_REGION": "us-west-2"}):
            client = boto3.client('sqs')
            self.stubber = Stubber(client)
            self.helper = AWSHelper(logger=logger, sqs_client=client)

    def test_get_queue_messages(self):
        queue_url = "https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue"
        timeout = 20
        params = dict(
            QueueUrl=queue_url,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            WaitTimeSeconds=timeout
        )
        response = {
            'Messages': [
                {
                    'MessageId': uuid.uuid4().hex,
                },
                {
                    'MessageId': uuid.uuid4().hex,
                }
            ],
            'ResponseMetadata': {}
        }
        self.stubber.add_response('receive_message', response, params)
        self.stubber.activate()
        out = self.helper.get_queue_messages(
            queue_url=queue_url,
            timeout=timeout
        )
        self.assertEqual(len(out), len(response['Messages']))
        self.assertIn('ResponseMetadata', out[0])


class S3TestCase(TestCase):
    def setUp(self):
        with patch.dict('os.environ', {"AWS_DEFAULT_REGION": "us-west-2"}):
            client = boto3.client('s3')
            self.stubber = Stubber(client)
            self.helper = AWSHelper(logger=logger, s3_client=client)

    def test_load_data_dump_to_dict_object(self):
        bucket = "codTest"
        key = "key"
        expected = {"test": 1}
        expected_json = json.dumps(expected)
        params = dict(Bucket=bucket, Key=key)
        file_obj = io.BytesIO(expected_json.encode('utf-8'))
        stream = StreamingBody(file_obj, content_length=len(expected_json))
        response = {
            'Body': stream
        }
        self.stubber.add_response('get_object', response, params)
        self.stubber.activate()
        out = self.helper.load_data_dump_to_dict_object(s3_bucket=bucket, file_key=key)
        self.assertEqual(expected, out)


    def test_upload_json_to_s3_bucket(self):
        data = {"test": 1}
        s3_bucket = "codTest"
        file_key = "key"
        params = dict(
            Body=bytes(json.dumps(data), encoding="utf-8"),
            Bucket=s3_bucket,
            Key=file_key
        )
        self.stubber.add_response('put_object', {}, params)
        self.stubber.activate()
        out = self.helper.upload_json_to_s3_bucket(s3_bucket=s3_bucket, file_key=file_key, data=data)
        self.assertEqual(out, file_key)
