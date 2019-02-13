import asyncio
import collections
from typing import Callable


class Pool:
    def __init__(self, factory: Callable, minsize: int = 1, maxsize: int = 10):
        self.factory = factory
        self.minsize = minsize
        self.maxsize = maxsize
        self._pool = collections.deque(maxlen=maxsize)
        self._cond = asyncio.Condition(asyncio.Lock())
        self._using = set()
        self._acquiring = 0
        self._close_state = asyncio.Event()
        self._fill_free(override_min=False)

    def __repr__(self):
        return f"<{self.__class__.__name__}: size={self.size}>"

    @property
    def size(self) -> int:
        return self.freesize + len(self._using) + self._acquiring

    @property
    def freesize(self) -> int:
        return len(self._pool)

    async def clear(self) -> None:
        async with self._cond:
            while self._pool:
                client = self._pool.popleft()
                await client.close()

    async def _do_close(self) -> None:
        async with self._cond:
            assert self._acquiring == 0, self._acquiring
            while self._pool:
                client = self._pool.popleft()
                await client.close()
            for client in self._using:
                await client.close()
            self._using.clear()

    async def close(self) -> None:
        if not self._close_state.is_set():
            self._close_state.set()
            await self._do_close()

    @property
    def closed(self) -> bool:
        return self._close_state.is_set()

    async def acquire(self):
        async with self._cond:
            while True:
                self._fill_free(override_min=True)
                if self.freesize:
                    client = self._pool.popleft()
                    assert client not in self._using, (client, self._using)
                    self._using.add(client)
                    return client
                else:
                    await self._cond.wait()

    async def release(self, client) -> None:
        assert client in self._using
        self._using.remove(client)
        self._pool.append(client)
        await self._wakeup()

    async def _wakeup(self) -> None:
        async with self._cond:
            self._cond.notify()

    def _fill_free(self, *, override_min: bool) -> None:
        while self.size < self.minsize:
            self._acquiring += 1
            try:
                client = self.factory()
                self._pool.append(client)
            finally:
                self._acquiring -= 1
        if self.freesize:
            return
        if override_min:
            while not self._pool and self.size < self.maxsize:
                self._acquiring += 1
                try:
                    client = self.factory()
                    self._pool.append(client)
                finally:
                    self._acquiring -= 1

    async def auto_release(self, client, coro):
        try:
            return await coro
        finally:
            await self.release(client)

    def get(self):
        return AsyncClientContextManager(self)


class AsyncClientContextManager:
    __slots__ = ("_pool", "_client")

    def __init__(self, pool):
        self._pool = pool
        self._client = None

    async def __aenter__(self):
        self._client = await self._pool.acquire()
        return self._client

    async def __aexit__(self, exc_type, exc_value, tb):
        try:
            await self._pool.release(self._client)
        finally:
            self._pool = None
            self._client = None
