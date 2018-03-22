import json
import boto3

from logging import Logger
from typing import Dict, List, Any


class AWSHelper:

    def __init__(self, *, logger: Logger) -> None:
        self.sns = boto3.client("sns")
        self.sqs = boto3.client("sqs")
        self.s3 = boto3.client("s3")
        self.logger = logger

    def publish_to_sns_topic(self,
                             *,
                             message: Dict[str, str],
                             activity_id: str,
                             topic_arn: str,
                             event_type: str,
                             subject: str) -> None:
        """
        :param message: A JSON object, body of the message
        :param activity_id: ID of the activity
        :param topic_arn: The topic's Amazon Resource Name
        :param event_type: Type of event for SNS message
        :param subject: Subject of SNS message
        :return: N/A
        """
        self.logger.info(f"Publishing {activity_id} to {event_type} SNS Topic.")

        attrs = {'EventType': {'DataType': 'String', 'StringValue': event_type},
                 'ActivityID': {'DataType': 'String', 'StringValue': activity_id}}

        response = self.sns.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message),
            Subject=subject,
            MessageAttributes=attrs
        )

        self.logger.debug(f"Publish to SNS Response: {response}")

    def get_queue_messages(self, *,
                           queue_url: str,
                           timeout: int = 20) -> List[Dict[str, Any]]:
        """
        Pull messages from an AWS SQS queue
        :param queue_url: URL of the AWS SQS queue
        :param timeout: How long to wait for a message to come across the queue. From 0 - 20.
        :return: A dict, with a single key 'Messages', that maps to a list of dicts where
                 each element defines an message. 
        """
        # TODO: Decide how many messages to receive
        messages = self.sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            WaitTimeSeconds=timeout
        )
        if "Messages" in messages:
            cleaned_messages = [
                {**msg, 'ResponseMetadata': messages['ResponseMetadata']} for msg in messages['Messages']
            ]
        else:
            cleaned_messages = []
        return cleaned_messages

    def load_data_dump_to_dict_object(self, *,
                                      s3_bucket: str,
                                      file_key: str) -> Dict[str, Any]:
        """
        Retrieve data from an S3 Bucket and converts to a JSON dict
        :param s3_bucket: Name of S3 Bucket
        :param file_key: Key to access a file within the S3 Bucket
        :return: A dict, JSON object of the S3 Bucket contents
        """
        file_obj = self.s3.get_object(Bucket=s3_bucket, Key=file_key)
        json_data = json.loads(file_obj['Body'].read())
        return json_data

    def upload_json_to_s3_bucket(self, *,
                                 s3_bucket: str,
                                 file_key: str,
                                 data: Dict) -> str:
        """
        Insert a JSON object into an S3 bucket
        :param s3_bucket: Name of S3 Bucket
        :param file_key: Key to access a file within the S3 Bucket
        :param data: Data to insert into S3
        :return:
        """
        self.s3.put_object(
            Body=bytes(json.dumps(data), encoding="utf-8"),
            Bucket=s3_bucket,
            Key=file_key
        )

        self.logger.debug(f"Uploaded data to S3 with name: {file_key}")

        return file_key
