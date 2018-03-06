import json
import boto3
from typing import Dict, List, Any
from tempfile import TemporaryFile


class AWSHelper:
    def __init__(self, logger=None) -> None:
        self.sns = boto3.client("sns")
        self.sqs = boto3.client("sqs")
        self.s3 = boto3.client("s3")
        self.logger = logger

    def publish_to_sns_topic(self, *, message: Dict[str, str],
                             activity_id: str,
                             topic_arn: str,
                             event_type: str,
                             subject: str):
        self.logger.info(f"Publishing {activity_id} to {event_type} SNS Topic.")

        attrs = {'EventType': {'DataType': "String", 'StringValue': event_type},
                 'ActivityID': {'DataType': "String", 'StringValue': activity_id}}

        response = self.sns.publish(TopicArn=topic_arn,
                                    Message=json.dumps(message),
                                    Subject=subject,
                                    MessageAttributes=attrs)
        self.logger.debug(f"Publish to SNS Response: {response}")

    def get_queue_messages(self, *, queue_url: str, timeout: int = 20) -> List[Dict[str, Any]]:
        """

        :param queue_url: URL of the SQS Queue to retrieve messages from
        :param timeout: How long to wait for a message to come across the queue. From 0 - 20.
        :return: A dict, with a single key 'Messages', that maps to a list of dicts where
                 each element defines an message. 
        """

        # TODO: Decide how many messages to receive
        messages = self.sqs.receive_message(QueueUrl=queue_url,
                                            AttributeNames=["All"],
                                            MessageAttributeNames=["All"],
                                            WaitTimeSeconds=timeout)
        cleaned_messages = [{**msg, 'ResponseMetadata': messages['ResponseMetadata']}
                            for msg in messages['Messages']]
        return cleaned_messages

    def load_data_dump_to_dict_object(self, *, s3_bucket: str, file_key: str) -> Dict[str, Any]:
        with TemporaryFile("wb") as data_dump:
            self.s3.download_fileobj(s3_bucket,
                                     file_key,
                                     data_dump)
            binary_content = data_dump.read()
            json_data = json.loads(binary_content, encoding="utf-8")
        return json_data

    def upload_json_to_s3_bucket(self, *, bucket_name: str, upload_name: str, data: Dict):
        temp_file = TemporaryFile("wb")
        temp_file.write(bytes(json.dumps(data)))
        self.s3.upload_fileobj(temp_file, bucket_name, upload_name)

