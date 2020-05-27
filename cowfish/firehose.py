import time
import json
import asyncio
import aiobotocore
from typing import Callable, Optional
from .worker import BatchWorker
from . import utils


class Firehose:
    service_name = "firehose"
    MAX_RETRY = 10
    sleep_base = 0.3

    def __init__(
        self,
        stream_name: str,
        region_name: Optional[str] = None,
        encode_func: Optional[Callable] = None,
        delimiter: bytes = b"\n",
        *,
        worker_params: Optional[dict] = None,
        client_params: Optional[dict] = None,
        original_api: bool = False,
    ):
        self.session = aiobotocore.get_session()
        self.stream_name = stream_name
        self.encode_func = encode_func or (lambda o: json.dumps(o).encode())
        self.delimiter = delimiter
        self.client_params = client_params or {}
        self.client_params["region_name"] = region_name
        worker_params = worker_params or {}
        worker_params.setdefault("name", "FirehoseWorker")
        batch_func = self.original_batch if original_api else self.write_batch
        self.worker = BatchWorker(batch_func, **worker_params)

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}: "
            f"stream={self.stream_name}, worker={self.worker!r}>"
        )

    def create_client(self):
        return self.session.create_client(self.service_name, **self.client_params)

    async def put(self, obj) -> None:
        return await self.worker.put(obj)

    async def stop(self) -> None:
        timestamp = time.time()
        await self.worker.stop()
        cost = time.time() - timestamp
        await utils.info(f"{self!r} stopped in {cost:.1f} seconds")

    def _encode(self, obj_list: list) -> bytes:
        encoded = [self.encode_func(obj) for obj in obj_list]
        encoded.append(b"")
        return self.delimiter.join(encoded)

    async def write_batch(self, obj_list: list) -> None:
        record = {"Data": self._encode(obj_list)}
        n = 0
        async with self.create_client() as client:
            while n < self.MAX_RETRY:
                if n > 0:
                    await asyncio.sleep(self.sleep_base * (2 ** n))
                try:
                    await client.put_record(
                        DeliveryStreamName=self.stream_name, Record=record
                    )
                except Exception as e:
                    await utils.handle_exc(e)
                    n += 1
                    if n >= self.MAX_RETRY:
                        raise
                    continue
                return

    async def original_batch(self, obj_list: list) -> None:
        records = [{"Data": self.encode_func(obj) + self.delimiter} for obj in obj_list]
        n = 0
        async with self.create_client() as client:
            while n < self.MAX_RETRY:
                if n > 0:
                    await asyncio.sleep(self.sleep_base * (2 ** n))
                try:
                    resp = await client.put_record_batch(
                        DeliveryStreamName=self.stream_name, Records=records
                    )
                except Exception as e:
                    await utils.handle_exc(e)
                    n += 1
                    if n >= self.MAX_RETRY:
                        raise
                    continue
                if resp["FailedPutCount"] > 0:
                    records = [
                        records[i]
                        for i, record in enumerate(resp["RequestResponses"])
                        if "ErrorCode" in record
                    ]
                    n += 1
                    continue
                return
            else:
                raise Exception("write_batch error: firehose put_record_batch failed")
