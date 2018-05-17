import asyncio
from cowfish.kinesis import Kinesis, CompactKinesis
loop = asyncio.get_event_loop()


def test_kinesis():
    kinesis = Kinesis('fake', 'fake')
    loop.run_until_complete(kinesis.stop())


def test_compact_kinesis():
    try:
        kinesis = CompactKinesis('fake', 'fake')
        loop.run_until_complete(kinesis.stop())
    except ImportError:
        pass
