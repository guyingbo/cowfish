import time
import asyncio
import async_timeout
from . import utils


class BatchWorker:
    """
    BatchWorker holds some amount of objects for a few seconds before sending them out.
    """

    def __init__(
        self,
        handler,
        *,
        maxsize: int = 0,
        aggr_num: int = 10,
        timeout: int = 5,
        concurrency: int = 10,
        name: str = "BatchWorker"
    ):
        self.queue = asyncio.Queue(maxsize)
        self.handler = handler
        self.aggr_num = aggr_num
        self.timeout = timeout
        self.concurrency = concurrency
        self.name = name
        self.semaphore = asyncio.Semaphore(concurrency)
        self.quit = object()
        self.shutdown = False
        self.fut = None
        self.futures = set()
        self.start()

    def __repr__(self):
        return "<{}: qsize={}, concurrency={}, working={}>".format(
            self.name,
            self.queue.qsize(),
            self.concurrency,
            len(self.futures),
        )

    @property
    def qsize(self) -> int:
        return self.queue.qsize()

    async def put(self, obj) -> None:
        await self.queue.put(obj)

    def start(self) -> None:
        self.fut = asyncio.ensure_future(self.run())

    async def stop(self) -> None:
        await utils.info(f"Stopping {self!r}")
        await self.queue.put(self.quit)
        if self.fut:
            await self.fut

    async def _get_obj_list(self) -> list:
        obj_list = []
        timeout = self.timeout
        while timeout > 0 and len(obj_list) < self.aggr_num:
            timestamp = time.time()
            try:
                async with async_timeout.timeout(timeout):
                    obj = await self.queue.get()
            except asyncio.TimeoutError:
                break
            if obj is self.quit:
                self.shutdown = True
                break
            obj_list.append(obj)
            timeout -= time.time() - timestamp
        return obj_list

    async def run(self) -> None:
        await utils.info(f"Starting {self!r}")
        while not self.shutdown:
            obj_list = await self._get_obj_list()
            if not obj_list:
                continue
            await self.semaphore.acquire()
            fut = asyncio.ensure_future(self.handle(obj_list))
            self.futures.add(fut)
            fut.add_done_callback(self.futures.remove)
        if self.futures:
            await asyncio.wait(self.futures)

    async def handle(self, obj_list: list) -> None:
        try:
            try:
                result = self.handler(obj_list)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                await utils.handle_exc(e)
        finally:
            self.semaphore.release()
