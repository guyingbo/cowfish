import asyncio
from cowfish.sqsprocesser import SQSProcesser, plain_handler
loop = asyncio.get_event_loop()


def t_processer():
    processer = SQSProcesser('fake', 'us-west-1', plain_handler)
    loop.call_later(2, processer.quit_event.set)
    processer.start()
