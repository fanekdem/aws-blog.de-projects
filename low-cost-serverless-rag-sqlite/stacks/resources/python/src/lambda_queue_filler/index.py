import os
import boto3
import sys
import time
import re
import pathlib
import json

src_dir_path = str(pathlib.Path(__file__).parent.parent)
sys.path.append(src_dir_path)

SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
MESSAGE_GROUP_ID = "SINGLETON"

sqs_client = boto3.client("sqs")


TEST_EVENTS = {"s3:TestEvent"}


def lambda_handler(event: dict[str, object], context: object):
    message_body = json.dumps(event)

    if event.get("Event") in TEST_EVENTS:
        return

    response = sqs_client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=message_body,
        MessageDeduplicationId=context.aws_request_id,
        MessageGroupId=MESSAGE_GROUP_ID,
    )
    return response


if __name__ == "__main__":
    lambda_handler({}, None)
