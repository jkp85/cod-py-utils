import boto3
import boto3.session
import json
import time
import logging
import os
import sys

from typing import Dict
from abc import ABCMeta, abstractmethod


class SqsListener(object):
    __metaclass__ = ABCMeta

    def __init__(self, queue: str, **kwargs: Dict) -> None:
        """
        :param queue: (str) name of queue to listen to
        :param kwargs: error_topic=None, interval=0, wait_time=20, force_delete=False
        """
        if (not os.environ.get('AWS_ACCOUNT_ID', None) and
                not ('iam-role' == boto3.Session().get_credentials().method)):
            raise EnvironmentError(
                'Environment variable `AWS_ACCOUNT_ID` not set and no role found.')
        self._queue_name = queue
        self._poll_interval = kwargs.get("interval", 0)
        self._error_topic_arn = kwargs.get('error_topic_arn', None)
        self._message_attribute_names = kwargs.get('message_attribute_names', [])
        self._attribute_names = kwargs.get('attribute_names', ['All'])
        self._wait_time = kwargs.get('wait_time', 20)
        self._max_retries = kwargs.get('max_retries', 3)
        self._logger = kwargs.get('logger', logging.getLogger('sqs_listener'))

        self._failed_messages = {}
        self._client = self._initialize_client()

    def _initialize_client(self) -> None:
        # new session for each instantiation
        self._session = boto3.session.Session()
        sqs = self._session.client('sqs')

        queues = sqs.list_queues(QueueNamePrefix=self._queue_name)
        mainQueueExists = False
        if 'QueueUrls' in queues:
            for q in queues['QueueUrls']:
                qname = q.split('/')[-1]
                if qname == self._queue_name:
                    mainQueueExists = True

        if not mainQueueExists:
            raise EnvironmentError(f"Could not find queue: f{self._queue_name}")

        if os.environ.get('AWS_ACCOUNT_ID', None):
            qs = sqs.get_queue_url(QueueName=self._queue_name,
                                   QueueOwnerAWSAccountId=os.environ.get('AWS_ACCOUNT_ID', None))
        else:
            qs = sqs.get_queue_url(QueueName=self._queue_name)
        self._queue_url = qs['QueueUrl']

        return sqs

    def _start_listening(self) -> None:
        while True:
            messages = self._client.receive_message(
                QueueUrl=self._queue_url,
                MessageAttributeNames=self._message_attribute_names,
                AttributeNames=self._attribute_names,
                WaitTimeSeconds=self._wait_time,
            )
            if 'Messages' in messages:
                self._logger.info(str(len(messages['Messages'])) + " messages received")
                for m in messages['Messages']:
                    receipt_handle = m['ReceiptHandle']
                    m_body = m['Body']
                    m_id = m['MessageId']
                    message_attribs = m.get('MessageAttributes', {})
                    attribs = m.get('Attributes', {})

                    # catch problems with malformed JSON, usually a result of someone writing poor JSON directly in the AWS console
                    try:
                        params_dict = json.loads(m_body)
                    except:
                        self._logger.warning(
                            "Unable to parse message - JSON is not formatted properly")
                        continue
                    try:
                        self.handle_message(params_dict, attribs, message_attribs)
                        self._client.delete_message(
                            QueueUrl=self._queue_url,
                            ReceiptHandle=receipt_handle
                        )
                        self._failed_messages.pop(m_id)
                    except Exception as ex:
                        self.process_error(m_id, receipt_handle, params_dict,
                                           attribs, message_attribs, ex)

            else:
                time.sleep(self._poll_interval)

    def process_error(self, m_id: str, receipt_handle: Dict, body: Dict,
                      attributes: Dict, message_attributes: Dict,
                      error: Exception) -> None:
        self.handle_error(body, attributes, message_attributes, error)

        if not self._failed_messages.get(m_id, None):
            self._failed_messages[m_id] = 0

        retry_count = self._failed_messages.get(m_id)

        if retry_count >= self._max_retries:
            self._logger.debug(f"Message {m_id} has failed and exceeded retry count. "
                               f"Deleting from {self._queue_name} and attempting to error topic.")
            self.handle_failed_message(body, attributes, message_attributes, error)
            self._client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=receipt_handle
            )
            self._failed_messages.pop(m_id)
            if self._error_topic_arn:
                sns = boto3.client("sns")
                sns.publish(
                    TopicArn=self._error_topic_arn,
                    Message=json.dumps(body),
                    Subject="Failed SQS Message",
                    MessageAttributes=message_attributes
                )
                self._logger.debug(f"Message {m_id} sent to error topic.")
        else:
            self._logger.debug(f"Message {m_id} has failed. "
                               f"Current retry count: {retry_count}. "
                               f"Max retries: {self._max_retries}")
            self._failed_messages[m_id] += 1

    def listen(self) -> None:
        self._logger.info("Listening to queue " + self._queue_name)
        if self._error_topic_arn:
            self._logger.info("Using error topic " + self._error_topic_arn)

        self._start_listening()

    @abstractmethod
    def handle_message(self, body: Dict, attributes: Dict, messages_attributes: Dict):
        """
        Implement this method to do something with the SQS message contents
        :param body: dict
        :param attributes: dict
        :param messages_attributes: dict
        :return:
        """
        return

    @abstractmethod
    def handle_error(self, body: Dict, attributes: Dict, messages_attributes: Dict, error: Exception):
        """
        Implement this method to handle when a message has an error
        :param body: dict
        :param attributes: dict
        :param messages_attributes: dict
        :param ex: dict
        :return:
        """
        return

    @abstractmethod
    def handle_failed_message(self, body: Dict, attributes: Dict, messages_attributes: Dict, error: Exception):
        """
        Implement this method to handle when a message max retries are exceeded
        :param body: dict
        :param attributes: dict
        :param messages_attributes: dict
        :return:
        """
        return
