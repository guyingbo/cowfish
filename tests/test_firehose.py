import pytest
from cowfish.firehose import Firehose

client_params = {
    "endpoint_url": "http://localhost:4573/",
    "aws_access_key_id": "xxx",
    "aws_secret_access_key": "xxx",
}


@pytest.mark.asyncio
async def test_firehose():
    firehose = Firehose("fake", "us-east-1", client_params=client_params)
    await firehose.put({"test": 1})
    await firehose.stop()
