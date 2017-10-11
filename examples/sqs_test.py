import sys
import time
import signal
import asyncio
import logging
from cowfish.sqs import SQSWriter
loop = asyncio.get_event_loop()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


writer = SQSWriter('gyb', region_name='us-west-1')


@writer.async_rpc
async def foo(a):
    pass


obj = {
    'name': 'gyb',
    'value': 'haha',
}


async def show():
    while not done.is_set():
        await asyncio.sleep(1)
        print(repr(writer))


done = asyncio.Event()
done2 = asyncio.Event()
fut = asyncio.ensure_future(show())


async def go():
    timeout = 300
    asyncio.gather(asyncio.sleep(1))
    while not done2.is_set() and timeout > 0:
        timestamp = time.time()
        await asyncio.sleep(0.005)
        await writer.write_one(obj, queued=True)
        timeout -= time.time() - timestamp


loop.add_signal_handler(signal.SIGINT, done2.set)
loop.run_until_complete(go())
loop.run_until_complete(writer.stop())
done.set()
loop.run_until_complete(fut)
loop.run_until_complete(loop.shutdown_asyncgens())
loop.close()
