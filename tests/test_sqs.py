import pytest
from cowfish.sqs import SQSWriter

client_params = {
    "endpoint_url": "http://localhost:4576/",
    "aws_access_key_id": "xxx",
    "aws_secret_access_key": "xxx",
}


@pytest.mark.asyncio
async def test_sqs(sqs_server):
    writer = SQSWriter("fake", "us-east-1", client_params=client_params)
    try:
        await writer.client.create_queue(QueueName="fake")
    except Exception:
        pass
    await writer.write_one({"test": 1})
    await writer.write_one({"test": 2}, queued=True)
    await writer.stop()
