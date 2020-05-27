import time
import base64
import pickle
import asyncio
import functools
import aiobotocore
from types import FunctionType
from typing import Callable, Optional, Any, Union
from .worker import BatchWorker
from . import utils


class SQSWriter:
    service_name = "sqs"
    MAX_RETRY = 10
    sleep_base = 0.3

    def __init__(
        self,
        queue_name: str,
        region_name: Optional[str] = None,
        encode_func: Optional[Callable] = None,
        *,
        worker_params: Optional[dict] = None,
        client_params: Optional[dict] = None,
    ):
        self.session = aiobotocore.get_session()
        self.queue_name = queue_name
        self.is_fifo = queue_name.endswith(".fifo")
        self.encode_func = encode_func or (
            lambda o: base64.b64encode(pickle.dumps(o, 2)).decode("ascii")
        )
        self.client_params = client_params or {}
        self.client_params["region_name"] = region_name
        self.QueueUrl = None
        self.lock = asyncio.Lock()
        worker_params = worker_params or {}
        worker_params.setdefault("name", "SQSWorker")
        self.worker = BatchWorker(self.write_batch, **worker_params)

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}: "
            f"queue={self.queue_name}, worker={self.worker!r}>"
        )

    def create_client(self):
        return self.session.create_client(self.service_name, **self.client_params)

    async def _get_queue_url(self) -> str:
        if self.QueueUrl is None:
            async with self.lock:
                if self.QueueUrl is None:
                    async with self.create_client() as client:
                        resp = await client.get_queue_url(QueueName=self.queue_name)
                    self.QueueUrl = resp["QueueUrl"]
        return self.QueueUrl

    async def stop(self) -> None:
        timestamp = time.time()
        await self.worker.stop()
        cost = time.time() - timestamp
        await utils.info(f"{self!r} stopped in {cost:.1f} seconds")

    def _encode(self, obj: Any) -> str:
        return self.encode_func(obj)

    async def write_one(
        self,
        record: Any,
        delay_seconds: int = 0,
        deduplication_id: Optional[str] = None,
        group_id: Optional[str] = None,
        queued: bool = False,
        **attributes,
    ):
        message = {
            "record": record,
            "delay_seconds": delay_seconds,
            "attributes": attributes,
            "params": {},
        }
        if self.is_fifo:
            if deduplication_id:
                message["params"]["MessageDeduplicationId"] = deduplication_id
            if group_id:
                message["params"]["MessageGroupId"] = group_id
        if queued:
            await self.worker.put(message)
            return
        queue_url = await self._get_queue_url()
        async with self.create_client() as client:
            return await client.send_message(
                QueueUrl=queue_url,
                MessageBody=self._encode(record),
                DelaySeconds=delay_seconds,
                MessageAttributes=attributes,
                **message["params"],
            )

    async def write_batch(self, obj_list: list) -> None:
        Entries = [
            {
                "Id": str(i),
                "MessageBody": self._encode(message["record"]),
                "DelaySeconds": message["delay_seconds"],
                "MessageAttributes": message["attributes"],
                **message["params"],
            }
            for i, message in enumerate(obj_list)
        ]
        queue_url = await self._get_queue_url()
        n = 0
        async with self.create_client() as client:
            while n < self.MAX_RETRY:
                if n > 0:
                    await asyncio.sleep(self.sleep_base * (2 ** n))
                try:
                    resp = await client.send_message_batch(
                        QueueUrl=queue_url, Entries=Entries
                    )
                except Exception as e:
                    await utils.handle_exc(e)
                    n += 1
                    if n >= self.MAX_RETRY:
                        raise
                    continue
                if "Failed" not in resp:
                    return
                failed_ids = set(
                    d["Id"] for d in resp["Failed"] if not d["SenderFault"]
                )
                await utils.info(f"Send failed {n}: {failed_ids}, {resp['Failed']}")
                Entries = [entry for entry in Entries if entry["Id"] in failed_ids]
                n += 1
            else:
                raise Exception("write_batch error: SQS send_message_batch failed")

    def async_rpc(
        self,
        func: Union[FunctionType, str, None] = None,
        *,
        delay_seconds: int = 0,
        deduplication_id: Optional[str] = None,
        group_id: Optional[str] = None,
        queued: bool = True,
        **attributes,
    ) -> FunctionType:
        if func is None:
            return functools.partial(
                self.async_rpc,
                delay_seconds=delay_seconds,
                deduplication_id=deduplication_id,
                group_id=group_id,
                queued=queued,
                **attributes,
            )
        if type(func) == str:

            async def async_func(*args, **kw):
                record = {"fpath": func, "args": args, "kw": kw}
                await self.write_one(
                    record,
                    delay_seconds=delay_seconds,
                    deduplication_id=deduplication_id,
                    group_id=group_id,
                    queued=queued,
                    **attributes,
                )

        else:

            @functools.wraps(func)
            async def async_func(*args, **kw):
                fpath = func.__module__ + "." + func.__name__
                record = {"fpath": fpath, "args": args, "kw": kw}
                await self.write_one(
                    record,
                    delay_seconds=delay_seconds,
                    deduplication_id=deduplication_id,
                    group_id=group_id,
                    queued=queued,
                    **attributes,
                )

        return async_func


class StringAttribute(dict):
    def __init__(self, value: str):
        self["DataType"] = "String"
        self["StringValue"] = str(value)


class BinaryAttribute(dict):
    def __init__(self, value: bytes):
        self["DataType"] = "Binary"
        self["BinaryValue"] = bytes(value)


class NumberAttribute(dict):
    def __init__(self, value: str):
        self["DataType"] = "Number"
        self["StringValue"] = str(value)
