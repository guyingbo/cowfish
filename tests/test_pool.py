import asyncio
from cowfish.pool import Pool
loop = asyncio.get_event_loop()


class Client:
    async def close(self):
        pass


def test_pool():
    pool = Pool(lambda: Client(), minsize=2)
    assert "Pool" in repr(pool)
    assert pool.size == 2
    client = loop.run_until_complete(pool.acquire())
    assert pool.size == 2
    assert pool.freesize == 1
    client = loop.run_until_complete(pool.acquire())
    assert pool.size == 2
    assert pool.freesize == 0
    client = loop.run_until_complete(pool.acquire())
    assert pool.size == 3
    assert pool.freesize == 0
    loop.run_until_complete(pool.release(client))
    assert pool.size == 3
    assert pool.freesize == 1
    loop.run_until_complete(pool.clear())
    loop.run_until_complete(pool.close())
    assert pool.size == 0
