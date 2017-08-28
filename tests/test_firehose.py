import asyncio
from cowfish.firehose import Firehose
loop = asyncio.get_event_loop()


def test_firehose():
    firehose = Firehose('fake', 'fake')
    loop.run_until_complete(firehose.stop())
