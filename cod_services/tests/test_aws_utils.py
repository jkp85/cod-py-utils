import logging
from unittest.mock import patch, Mock
from services.aws_utils import AWSHelper
logger = logging.getLogger()


@patch("services.aws_utils.boto3")
def test_get_queue_messages_empty_queue_returns_empty_list(mock_boto):
    aws = AWSHelper(logger=logger)
    aws.sqs = Mock()
    aws.sqs.receive_message.return_value = {}
    messages = aws.get_queue_messages(queue_url="foo.sqs/dummyQueue")
    assert (messages == [])
