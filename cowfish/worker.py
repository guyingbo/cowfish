import time
import logging
import asyncio
logger = logging.getLogger(__name__)


class BatchWorker:
    def __init__(self, handler, maxsize=0, aggr_num=10, timeout=5):
        self.queue = asyncio.Queue(maxsize)
        self.handler = handler
        self.aggr_num = aggr_num
        self.timeout = timeout
        self.quit = object()
        self.shutdown = False
        self.fut = None
        self.start()

    def __repr__(self):
        return '<{}: qsize={}>'.format(
            self.__class__.__name__, self.queue.qsize()
        )

    @property
    def qsize(self):
        return self.queue.qsize()

    async def put(self, obj):
        await self.queue.put(obj)

    def start(self):
        self.fut = asyncio.ensure_future(self.run())

    async def stop(self):
        logger.info('Stopping {0!r}'.format(self))
        await self.queue.put(self.quit)
        if self.fut:
            await asyncio.wait_for(self.fut, None)

    async def _get_obj_list(self):
        obj_list = []
        timeout = self.timeout
        while timeout > 0 and len(obj_list) < self.aggr_num:
            timestamp = time.time()
            try:
                obj = await asyncio.wait_for(self.queue.get(), timeout)
            except asyncio.TimeoutError:
                break
            if obj is self.quit:
                self.shutdown = True
                break
            obj_list.append(obj)
            timeout -= time.time() - timestamp
        return obj_list

    async def run(self):
        logger.info('Starting {0!r}'.format(self))
        while not self.shutdown:
            obj_list = await self._get_obj_list()
            if not obj_list:
                continue
            try:
                result = self.handler(obj_list)
                if asyncio.iscoroutine(result):
                    result = await result
                if result:
                    for obj in result:
                        await self.queue.put(obj)
            except Exception as e:
                logger.exception(e)
