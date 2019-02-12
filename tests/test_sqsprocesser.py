# import pytest
import asyncio

# import aiobotocore
from cowfish.sqsprocesser import SQSProcesser, plain_handler

loop = asyncio.get_event_loop()
client_params = {
    "endpoint_url": "http://localhost:4576/",
    "aws_access_key_id": "xxx",
    "aws_secret_access_key": "xxx",
}


def t_processer(sqs_server):
    # session = aiobotocore.get_session()
    # client = session.create_client("sqs", region_name="us-east-1", **client_params)
    processer = SQSProcesser("fake", plain_handler)
    loop.call_later(2, processer.quit_event.set)
    processer.start()
