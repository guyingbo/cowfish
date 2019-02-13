import pytest
from cowfish.firehose import Firehose


@pytest.mark.asyncio
async def test_firehose():
    client_params = {
        "endpoint_url": "http://localhost:4573",
        "aws_access_key_id": "xxx",
        "aws_secret_access_key": "xxx",
    }
    firehose = Firehose(
        "fake",
        region_name="us-east-1",
        client_params=client_params,
        worker_params={"maxsize": 1000},
    )
    firehose.sleep_base = 0.001
    for i in range(10):
        await firehose.put({"test": 1})
    await firehose.stop()
