import asyncio
from cowfish.worker import BatchWorker
loop = asyncio.get_event_loop()


def test_worker():
    loop.run_until_complete(asyncio.sleep(0.1))
    worker = BatchWorker(print)
    #loop.run_until_complete(worker.put('abc'))
    #loop.run_until_complete(worker.put('def'))
    loop.run_until_complete(worker.stop())
