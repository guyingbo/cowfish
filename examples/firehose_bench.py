import json
import time
import asyncio
from cowfish.firehose import Firehose


with open('sample.json') as f:
    data = f.read()
    obj = json.loads(data)
    size = len(data)


async def go(firehose):
    fut = asyncio.ensure_future(show(firehose))
    start_time = time.time()
    n = 0
    while time.time() - start_time < 10:
        await firehose.put(obj)
        n += 1
    await firehose.stop()
    fut.cancel()
    print(f'send {n} records {n*size} bytes')


async def show(firehose):
    while True:
        await asyncio.sleep(1)
        print(firehose.worker.qsize, repr(firehose))


if __name__ == '__main__':
    firehose = Firehose('gyb', region_name='us-east-1',
                        worker_params={'maxsize': 1000})
    loop = asyncio.get_event_loop()
    loop.run_until_complete(go(firehose))
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
