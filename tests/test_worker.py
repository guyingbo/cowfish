import asyncio
from cowfish.worker import BatchWorker


async def go(worker):
    assert "BatchWorker" in repr(worker)
    assert worker.qsize == 0
    await worker.put("abc")
    await worker.put("def")
    await worker.put("xyz")
    await asyncio.sleep(0.2)
    await worker.put("bad")
    await worker.stop()


def test_worker():
    async def foo(objs):
        if "bad" in objs:
            raise Exception("bad")
        await asyncio.sleep(0.3)
        return objs[-1:] if len(objs) > 1 else []

    event_loop = asyncio.get_event_loop()
    worker = BatchWorker(foo, timeout=0.1)
    event_loop.run_until_complete(go(worker))
    event_loop.close()
