import sys
import base64
import pickle
import signal
import inspect
import asyncio
import argparse
import importlib
import aiobotocore
from types import FunctionType
from typing import Callable, Union, Coroutine, Optional
from . import utils
from .worker import BatchWorker
from . import __version__

__description__ = "An AWS SQS processer using asyncio/aiobotocore"


class SQSRetry(Exception):
    def __init__(self, *, max_times: int, after: int = None):
        self.max_times = max_times
        self.seconds_later = after

    def delay_seconds(self, retry_times):
        if self.seconds_later is None:
            return 1 + 2 ** retry_times
        return self.seconds_later


class Message(dict):
    @property
    def message_id(self):
        return self["MessageId"]

    @property
    def body(self):
        if not hasattr(self, "_body"):
            Body = self["Body"]
            self._body = pickle.loads(base64.b64decode(Body))
        return self._body

    @property
    def attributes(self):
        return self.get("Attributes", {})


class SQSProcesser:
    service_name = "sqs"
    MAX_RETRY = 10
    sleep_base = 0.3

    def __init__(
        self,
        queue_name: str,
        message_handler: Union[Callable, Coroutine],
        *,
        region_name: Optional[str] = None,
        concurrency: int = 10,
        visibility_timeout: int = 60,
        idle_sleep: int = 0,
        batch_ops: bool = True,
        client_params: Optional[dict] = None,
        delete_worker_params: Optional[dict] = None,
        change_worker_params: Optional[dict] = None,
    ):
        self.queue_name = queue_name
        self.concurrency = concurrency
        self.message_handler = message_handler
        self.visibility_timeout = visibility_timeout
        self.idle_sleep = idle_sleep
        self.QueueUrl = None
        self.hooks = {"after_server_stop": set()}
        self.client_params = client_params or {}
        self.client_params["region_name"] = region_name
        self.lock = asyncio.Lock()
        self.session = aiobotocore.get_session()
        self.quit_event = asyncio.Event()
        self.semaphore = asyncio.Semaphore(concurrency)
        self.futures = set()
        if batch_ops:
            delete_worker_params = delete_worker_params or {}
            delete_worker_params.setdefault("name", "SQSDeleteWorker")
            self.delete_worker = BatchWorker(self.delete_batch, **delete_worker_params)
            change_worker_params = change_worker_params or {}
            change_worker_params.setdefault("name", "SQSChangeWorker")
            self.change_worker = BatchWorker(self.change_batch, **change_worker_params)
        else:
            self.delete_worker = None
            self.change_worker = None

    def __repr__(self):
        return "<{}: queue={}, concurrency={}, working={}>".format(
            self.__class__.__name__,
            self.queue_name,
            self.concurrency,
            len(self.futures),
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

    async def run_forever(self) -> None:
        while not self.quit_event.is_set():
            try:
                await self._fetch_messages()
            except Exception as e:
                try:
                    await utils.handle_exc(e)
                except Exception:
                    pass
                continue
        await self.close()

    async def close(self) -> None:
        if self.futures:
            await asyncio.wait(self.futures)
        if self.change_worker:
            await self.change_worker.stop()
        if self.delete_worker:
            await self.delete_worker.stop()

    async def _fetch_messages(self) -> None:
        async with self.create_client() as client:
            job = client.receive_message(
                QueueUrl=(await self._get_queue_url()),
                AttributeNames=["ApproximateReceiveCount"],
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=10,
                VisibilityTimeout=self.visibility_timeout,
                WaitTimeSeconds=20,
            )
            response = await utils.cancel_on_event(job, self.quit_event)
            if self.quit_event.is_set():
                return
            if "Messages" not in response and self.idle_sleep > 0:
                await asyncio.sleep(self.idle_sleep)
            if "Messages" in response:
                for message_dict in response["Messages"]:
                    await self.semaphore.acquire()
                    fut = asyncio.ensure_future(self.handle(Message(message_dict)))
                    self.futures.add(fut)
                    fut.add_done_callback(self.futures.remove)

    async def handle(self, message) -> None:
        try:
            delete = True
            try:
                result = self.message_handler(message)
                # if asyncio.iscoroutine(result):
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                if e.__class__.__name__ == SQSRetry.__name__:
                    receive_count = int(
                        message.attributes.get("ApproximateReceiveCount", 1)
                    )
                    if e.max_times > receive_count:
                        seconds = e.delay_seconds(receive_count)
                        await self.change_one(message, seconds)
                        delete = False
                else:
                    await utils.handle_exc(e)
            finally:
                if delete:
                    await self.delete_one(message)
        finally:
            self.semaphore.release()

    async def change_batch(self, messages: list) -> None:
        entries = [
            {
                "Id": str(index),
                "ReceiptHandle": message["ReceiptHandle"],
                "VisibilityTimeout": timeout,
            }
            for index, (message, timeout) in enumerate(messages)
        ]
        queue_url = await self._get_queue_url()
        n = 0
        async with self.create_client() as client:
            while n < self.MAX_RETRY:
                if n > 0:
                    await asyncio.sleep(self.sleep_base * (2 ** n))
                try:
                    resp = await client.change_message_visibility_batch(
                        QueueUrl=queue_url, Entries=entries
                    )
                except Exception as e:
                    await utils.handle_exc(e)
                    n += 1
                    if n >= self.MAX_RETRY:
                        raise
                    continue
                if "Failed" not in resp:
                    return
                failed_ids = set(d["Id"] for d in resp["Failed"])
                await utils.info(f"Change failed {n}: {failed_ids} {resp['Failed']}")
                entries = [entry for entry in entries if entry["Id"] in failed_ids]
                n += 1
            else:
                raise Exception("change_message_visibility_batch failed")

    async def change_one(self, message, visibility_timeout: int):
        if self.change_worker:
            await self.change_worker.put((message, visibility_timeout))
            return
        async with self.create_client() as client:
            return await client.change_message_visibility(
                QueueUrl=(await self._get_queue_url()),
                ReceiptHandle=message["ReceiptHandle"],
                VisibilityTimeout=visibility_timeout,
            )

    async def delete_batch(self, messages: list) -> None:
        entries = [
            {"Id": str(index), "ReceiptHandle": message["ReceiptHandle"]}
            for index, message in enumerate(messages)
        ]
        queue_url = await self._get_queue_url()
        n = 0
        async with self.create_client() as client:
            while n < self.MAX_RETRY:
                if n > 0:
                    await asyncio.sleep(self.sleep_base * (2 ** n))
                try:
                    resp = await client.delete_message_batch(
                        QueueUrl=queue_url, Entries=entries
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
                await utils.info(f"Delete failed {n}: {failed_ids}, {resp['Failed']}")
                entries = [entry for entry in entries if entry["Id"] in failed_ids]
                n += 1
            else:
                raise Exception("delete_message_batch failed", messages)

    async def delete_one(self, message):
        if self.delete_worker:
            await self.delete_worker.put(message)
            return
        async with self.create_client() as client:
            return await client.delete_message(
                QueueUrl=(await self._get_queue_url()),
                ReceiptHandle=message["ReceiptHandle"],
            )

    def after_server_stop(self, func: FunctionType) -> None:
        self.hooks["after_server_stop"].add(func)

    def start(self, loop=None):
        loop = loop or asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, self.quit_event.set)
        loop.add_signal_handler(signal.SIGTERM, self.quit_event.set)
        try:
            loop.run_until_complete(self._get_queue_url())
        except Exception:
            loop.run_until_complete(self.close())
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            print(
                f"The queue {self.queue_name} doesn't exists, exiting...",
                file=sys.stderr,
            )
            sys.exit(1)
        try:
            loop.run_until_complete(self.run_forever())
        finally:
            try:
                for func in self.hooks["after_server_stop"]:
                    if asyncio.iscoroutinefunction(func):
                        loop.run_until_complete(func(loop))
                    else:
                        func(loop)
            except Exception as e:
                loop.run_until_complete(utils.handle_exc(e))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()


def import_function(path: str) -> FunctionType:
    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


async def plain_handler(message) -> None:
    print(message)


async def rpc_handler(message) -> None:
    record = message.body
    func = import_function(record["fpath"])
    func = getattr(func, "_real", func)
    result = func(*record["args"], **record["kw"])
    if inspect.isawaitable(result):
        result = await result
    log = "{0}:{1}({2},{3})={4}".format(
        message.message_id,
        func.__name__,
        ", ".join([f"{arg!r}" for arg in record["args"]]),
        ", ".join([f"{k}={v}" for k, v in record["kw"].items()]),
        result,
    )
    await utils.info(log)


def main():
    epilog = (
        f"version info: cowfish/{__version__} aiobotocore/{aiobotocore.__version__}"
    )
    parser = argparse.ArgumentParser(description=__description__, epilog=epilog)
    parser.add_argument("queue_name")
    parser.add_argument("region")
    parser.add_argument(
        "-c", "--concurrency", type=int, default=20, help="default to 20"
    )
    parser.add_argument(
        "-v", "--visibility-timeout", type=int, default=60, help="default to 60"
    )
    parser.add_argument("--handler", type=str, help="default to rpc_handler")
    parser.add_argument(
        "--no-batch",
        dest="batch",
        action="store_false",
        default=True,
        help="do not use batch ops",
    )
    args = parser.parse_args()
    if args.handler is None:
        handler = rpc_handler
    else:
        handler = import_function(args.handler)
    processer = SQSProcesser(
        args.queue_name,
        args.region,
        handler,
        concurrency=args.concurrency,
        visibility_timeout=args.visibility_timeout,
        batch_ops=args.batch,
    )
    processer.start()


if __name__ == "__main__":
    main()
