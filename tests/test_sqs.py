import pytest
import asyncio
import secrets
from cowfish.sqs import SQSWriter, StringAttribute, BinaryAttribute, NumberAttribute


@pytest.mark.asyncio
async def test_sqs(sqs_server):
    url, stop = sqs_server
    client_params = {
        "endpoint_url": url,
        "aws_access_key_id": "xxx",
        "aws_secret_access_key": "xxx",
    }
    writer = SQSWriter(
        "fake.fifo", region_name="us-east-1", client_params=client_params
    )
    writer.sleep_base = 0.001
    try:
        async with writer.create_client() as client:
            await client.create_queue(QueueName="fake.fifo")
    except Exception as e:
        print(e)
    await writer.write_one(
        {"test": 1},
        a=StringAttribute("test"),
        b=BinaryAttribute(b"test"),
        c=NumberAttribute(25),
        deduplication_id=secrets.token_urlsafe(5),
        group_id="1",
    )
    for i in range(30):
        await writer.write_one({"test": 2}, queued=True)
    await asyncio.sleep(0.1)

    @writer.async_rpc
    async def foo(a, b):
        return a + b

    await foo(3, 4)

    @writer.async_rpc()
    async def bar():
        return 10

    await bar()
    await writer.async_rpc("foo")(3, 4)
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
    writer = SQSWriter("fake", region_name="us-east-1", client_params=client_params)
    writer.sleep_base = 0.001
    try:
        async with writer.create_client() as client:
            await client.create_queue(QueueName="fake")
    except Exception as e:
        print(e)

    for i in range(10):
        await writer.write_one({"test": 3}, queued=True)
    await asyncio.sleep(1)
    await writer.stop()
