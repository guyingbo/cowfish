import os
import time
import json
import base64
import asyncio
import aiobotocore
from typing import Callable, Optional
from .worker import BatchWorker
from . import utils


class Kinesis:
    service_name = "kinesis"
    MAX_RETRY = 10
    sleep_base = 0.3

    def __init__(
        self,
        stream_name: str,
        region_name: Optional[str] = None,
        encode_func: Optional[Callable] = None,
        key_func: Optional[Callable] = None,
        *,
        worker_params: Optional[dict] = None,
        client_params: Optional[dict] = None,
    ):
        self.session = aiobotocore.get_session()
        self.stream_name = stream_name
        self.encode_func = encode_func or (lambda o: json.dumps(o).encode())
        self.key_func = key_func
        self.client_params = client_params or {}
        self.client_params["region_name"] = region_name
        worker_params = worker_params or {}
        worker_params.setdefault("name", "KinesisWorker")
        self.worker = BatchWorker(self.write_batch, **worker_params)

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}: "
            f"stream={self.stream_name}, worker={self.worker!r}>"
        )

    def create_client(self):
        return self.session.create_client(self.service_name, **self.client_params)

    async def stop(self) -> None:
        timestamp = time.time()
        await self.worker.stop()
        cost = time.time() - timestamp
        await utils.info(f"{self!r} stopped in {cost:.1f} seconds")

    def _get_key(self, obj) -> bytes:
        if self.key_func is None:
            return base64.b64encode(os.urandom(24)).decode("ascii")
        return self.key_func(obj)

    def _encode(self, obj) -> bytes:
        return self.encode_func(obj)

    async def write_batch(self, obj_list: list) -> None:
        records = [
            {"PartitionKey": self._get_key(obj), "Data": self._encode(obj)}
            for obj in obj_list
        ]
        n = 0
        async with self.create_client() as client:
            while n < self.MAX_RETRY:
                if n > 0:
                    await asyncio.sleep(self.sleep_base * (2 ** n))
                try:
                    resp = await client.put_records(
                        StreamName=self.stream_name, Records=records
                    )
                except Exception as e:
                    await utils.handle_exc(e)
                    n += 1
                    if n >= self.MAX_RETRY:
                        raise
                    continue
                if resp["FailedRecordCount"] == 0:
                    return
                records = [
                    records[i]
                    for i, record in enumerate(resp["Records"])
                    if "ErrorCode" in record
                ]
                n += 1
        raise Exception("write_batch error: kinesis put_records failed")

    async def write_one(self, obj, queued: bool = False):
        if queued:
            await self.worker.put(obj)
            return
        async with self.create_client() as client:
            return await client.put_record(
                StreamName=self.stream_name,
                Data=self._encode(obj),
                PartitionKey=self._get_key(obj),
            )


class CompactKinesis(Kinesis):
    def __init__(self, *args, **kw):
        import msgpack

        super().__init__(*args, **kw)
        self.buffer = bytearray()
        self.packer = msgpack.Packer()
        self.bufmax = 1024 * 25

    def _encode(self, obj) -> bytes:
        return obj

    async def write(self, obj, flush: bool = False) -> None:
        packed = self.packer.pack(obj)
        if len(self.buffer) + len(packed) >= self.bufmax:
            payload = bytes(self.buffer)
            self.buffer.clear()
            self.buffer.extend(packed)
            await self.write_one(payload, queued=True)
        else:
            self.buffer.extend(packed)
            if flush and self.buffer:
                payload = bytes(self.buffer)
                self.buffer.clear()
                await self.write_one(payload, queued=True)

    async def flush(self) -> None:
        if self.buffer:
            payload = bytes(self.buffer)
            self.buffer.clear()
            await self.write_one(payload, queued=True)

    async def write_fluent(self, label: str, data: str) -> None:
        timestamp = int(time.time())
        packet = (label, timestamp, data)
        await self.write(packet)

    async def stop(self) -> None:
        if self.buffer:
            payload = bytes(self.buffer)
            self.buffer.clear()
            await self.write_one(payload, queued=True)
        await super().stop()
