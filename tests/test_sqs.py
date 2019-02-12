import pytest
import asyncio
from cowfish.sqs import SQSWriter


@pytest.mark.asyncio
async def test_sqs(sqs_server):
    url, stop = sqs_server
    client_params = {
        "endpoint_url": url,
        "aws_access_key_id": "xxx",
        "aws_secret_access_key": "xxx",
    }
    writer = SQSWriter("fake", "us-east-1", client_params=client_params)
    writer.sleep_base = 0.001
    try:
        await writer.client.create_queue(QueueName="fake")
    except Exception as e:
        print(e)
    await writer.write_one({"test": 1})
    for i in range(30):
        await writer.write_one({"test": 2}, queued=True)
    await asyncio.sleep(0.2)
    stop()
    for i in range(10):
        await writer.write_one({"test": 2}, queued=True)
    await writer.stop()


@pytest.mark.asyncio
async def test_sqs2(sqs_server):
    url, stop = sqs_server
    client_params = {
        "endpoint_url": url,
        "aws_access_key_id": "xxx",
        "aws_secret_access_key": "xxx",
    }
    writer = SQSWriter("fake", "us-east-1", client_params=client_params)
    writer.sleep_base = 0.001
    try:
        await writer.client.create_queue(QueueName="fake")
    except Exception as e:
        print(e)

    async def coro(*args, **kw):
        return {
            "Failed": [
                {"Id": "1", "SenderFault": False, "Code": 500, "Message": ""},
                {"Id": "2", "SenderFault": True, "Code": 500, "Message": ""},
            ]
        }

    writer.client.send_message_batch = coro
    for i in range(10):
        await writer.write_one({"test": 3}, queued=True)
    await asyncio.sleep(1)
    await writer.stop()
