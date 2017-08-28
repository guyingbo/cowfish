import asyncio
from cowfish.sqs import SQSWriter
loop = asyncio.get_event_loop()


def test_sqs():
    writer = SQSWriter('fake', 'fake')
    loop.run_until_complete(writer.stop())
