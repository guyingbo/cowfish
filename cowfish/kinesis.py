import os
import time
import json
import base64
import asyncio
import logging
import aiobotocore
from types import FunctionType
from .worker import BatchWorker

try:
    import msgpack
except ImportError:
    pass
logger = logging.getLogger(__package__)


class Kinesis:
    service_name = "kinesis"
    MAX_RETRY = 10
    sleep_base = 0.3

    def __init__(
        self,
        stream_name: str,
        region_name: str,
        encode_func: FunctionType = None,
        key_func: FunctionType = None,
        *,
        worker_params: dict = None,
        client_params: dict = None
    ):
        self.session = aiobotocore.get_session()
        self.stream_name = stream_name
        self.encode_func = encode_func or (lambda o: json.dumps(o).encode())
        self.key_func = key_func
        client_params = client_params or {}
        client_params["region_name"] = region_name
        self.client = self.session.create_client(self.service_name, **client_params)
        worker_params = worker_params or {}
        self.worker = BatchWorker(self.write_batch, **worker_params)

    def __repr__(self):
        return "<{}: stream={}, worker={!r}>".format(
            self.__class__.__name__, self.stream_name, self.worker
        )

    async def stop(self) -> None:
        timestamp = time.time()
        await self.worker.stop()
        await self.client.close()
        cost = time.time() - timestamp
        logger.info("{0!r} stopped in {1:.1f} seconds".format(self, cost))

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
        while n < self.MAX_RETRY:
            if n > 0:
                await asyncio.sleep(self.sleep_base * (2 ** n))
            try:
                resp = await self.client.put_records(
                    StreamName=self.stream_name, Records=records
                )
            except Exception as e:
                logger.exception(e)
                n += 1
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
        return await self.client.put_record(
            StreamName=self.stream_name,
            Data=self._encode(obj),
            PartitionKey=self._get_key(obj),
        )


class CompactKinesis(Kinesis):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.buffer = bytearray()
        try:
            self.packer = msgpack.Packer()
        except NameError:
            raise ImportError("need msgpack")
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
