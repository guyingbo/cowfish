import asyncio
from cowfish.kinesis import Kinesis
loop = asyncio.get_event_loop()


def test_kinesis():
    kinesis = Kinesis('fake', 'fake')
    loop.run_until_complete(kinesis.stop())
