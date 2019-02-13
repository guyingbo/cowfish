# import pytest
import asyncio
from cowfish.sqsprocesser import SQSProcesser, plain_handler


def test_processer(sqs_server):
    url, stop = sqs_server
    client_params = {
        "endpoint_url": url,
        "aws_access_key_id": "xxx",
        "aws_secret_access_key": "xxx",
    }
    loop = asyncio.get_event_loop()
    queue_name = "test-sqsprocesser"
    processer = SQSProcesser(
        queue_name, plain_handler, region_name="us-east-1", client_params=client_params
    )
    loop.run_until_complete(processer.client.create_queue(QueueName=queue_name))
    loop.call_later(2, processer.quit_event.set)
    processer.start()
