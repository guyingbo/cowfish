import pytest
import asyncio
from cowfish.kinesis import Kinesis, CompactKinesis

loop = asyncio.get_event_loop()
client_params = {
    "endpoint_url": "http://localhost:4568/",
    "aws_access_key_id": "xxx",
    "aws_secret_access_key": "xxx",
}


@pytest.mark.asyncio
async def test_kinesis(kinesis_server):
    kinesis = Kinesis("fake", "us-east-1", client_params=client_params)
    await kinesis.client.create_stream(StreamName="fake", ShardCount=2)
    await kinesis.write_one({"test": 1})
    await kinesis.write_one({"test": 2}, queued=True)
    await kinesis.stop()


@pytest.mark.asyncio
async def test_compact_kinesis(kinesis_server):
    kinesis = CompactKinesis("fake", "us-east-1", client_params=client_params)
    await kinesis.write({"test": 1})
    await kinesis.flush()
    await kinesis.stop()
