import asyncio
from cowfish.dynamodb import Dynamodb
loop = asyncio.get_event_loop()


def test_dynamodb():
    dynamodb = Dynamodb('fake')
    loop.run_until_complete(dynamodb.close())
