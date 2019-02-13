import pytest
import asyncio
from cowfish.worker import BatchWorker


@pytest.mark.asyncio
async def test_worker():
    async def foo(objs):
        if "bad" in objs:
            raise Exception("bad")
        await asyncio.sleep(0.3)
        return objs[-1:] if len(objs) > 1 else []

    worker = BatchWorker(foo, timeout=0.1)
    assert "BatchWorker" in repr(worker)
    assert worker.qsize == 0
    await worker.put("abc")
    await worker.put("def")
    await worker.put("xyz")
    await asyncio.sleep(0.2)
    await worker.put("bad")
    await worker.stop()
