import logging
import asyncio
logger = logging.getLogger(__package__)


async def cancel_on_event(coro, event: asyncio.Event):
    event_task = asyncio.ensure_future(event.wait())
    done, pending = await asyncio.wait(
        [event_task, coro], return_when=asyncio.FIRST_COMPLETED
    )
    if pending:
        pending.pop().cancel()
    while done:
        task = done.pop()
        if task is not event_task:
            return task.result()


def format_params(params: dict) -> str:
    return ", ".join(f"{k}={v}" for k, v in params.items())


class ClientMethodProxy:
    def __init__(self, pool, name: str):
        self._pool = pool
        self.name = name

    async def __call__(self, *args, **kw):
        client = await self._pool.acquire()
        try:
            return await getattr(client, self.name)(*args, **kw)
        finally:
            try:
                await self._pool.release(client)
            finally:
                self._pool = None


async def handle_exc(e):
    logger.exception(str(e))


async def info(s):
    logger.info(s)
