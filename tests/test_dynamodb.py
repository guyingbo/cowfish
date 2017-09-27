import asyncio
from cowfish.dynamodb import DynamodbExecutor
loop = asyncio.get_event_loop()


def test_dynamodb():
    executor = DynamodbExecutor('fake')
    loop.run_until_complete(executor.close())
