import base64
import pickle
import signal
import logging
import asyncio
import argparse
import importlib
import aiobotocore
from functools import partial
from .worker import BatchWorker
logger = logging.getLogger(__name__)
__description__ = 'An AWS SQS processer using asyncio/aiobotocore'


class SQSRetry(Exception):
    def __init__(self, *, max_times, after):
        self.max_times = max_times
        self.seconds_later = after


class Message(dict):
    @property
    def message_id(self):
        return self['MessageId']

    @property
    def body(self):
        if not hasattr(self, '_body'):
            Body = self['Body']
            self._body = pickle.loads(base64.b64decode(Body))
        return self._body

    @property
    def attributes(self):
        return self.get('Attributes', {})


class SQSProcesser:
    service_name = 'sqs'
    MAX_RETRY = 10

    def __init__(self, queue_name, region_name, message_handler, *,
                 concurrency=10, visibility_timeout=60, idle_sleep=0,
                 batch_ops=True,
                 delete_worker_params=None, change_worker_params=None):
        self.queue_name = queue_name
        self.region_name = region_name
        self.concurrency = concurrency
        self.message_handler = message_handler
        self.visibility_timeout = visibility_timeout
        self.idle_sleep = idle_sleep
        self.QueueUrl = None
        self.lock = asyncio.Lock()
        self.session = aiobotocore.get_session()
        self.loop = asyncio.get_event_loop()
        self.quit_event = asyncio.Event()
        self.loop.add_signal_handler(signal.SIGINT, self.quit_event.set)
        self.loop.add_signal_handler(signal.SIGTERM, self.quit_event.set)
        self.semaphore = asyncio.Semaphore(concurrency)
        self.futures = set()
        self.delete_client = self.create_client()
        self.change_client = self.create_client()
        if batch_ops:
            delete_worker_params = delete_worker_params or {}
            self.delete_worker = BatchWorker(
                partial(self.delete_batch, self.delete_client),
                **delete_worker_params
            )
            change_worker_params = change_worker_params or {}
            self.change_worker = BatchWorker(
                partial(self.change_batch, self.change_client),
                **change_worker_params
            )
        else:
            self.delete_worker = None
            self.change_worker = None

    def __repr__(self):
        return '<{}: queue={}, region={}, concurrency={}, working={}>'.format(
                self.__class__.__name__, self.queue_name, self.region_name,
                self.concurrency, len(self.futures))

    async def _get_queue_url(self):
        if self.QueueUrl is None:
            async with self.lock:
                async with self.create_client() as client:
                    resp = await client.get_queue_url(
                            QueueName=self.queue_name
                        )
                    self.QueueUrl = resp['QueueUrl']
        return self.QueueUrl

    def create_client(self):
        return self.session.create_client(
            self.service_name, region_name=self.region_name)

    async def run_forever(self):
        async with self.create_client() as client:
            while not self.quit_event.is_set():
                try:
                    await self._fetch_messages(client)
                except Exception as e:
                    logger.exception(e)
                    continue
        if self.change_worker:
            await self.change_worker.stop()
        self.change_client.close()
        if self.delete_worker:
            await self.delete_worker.stop()
        self.delete_client.close()
        if self.futures:
            await asyncio.wait(self.futures)

    async def _fetch_messages(self, client):
        response = await client.receive_message(
            QueueUrl=(await self._get_queue_url()),
            AttributeNames=['ApproximateReceiveCount'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=10,
            VisibilityTimeout=self.visibility_timeout,
            WaitTimeSeconds=20
        )
        if 'Messages' not in response and self.idle_sleep > 0:
            await asyncio.sleep(self.idle_sleep)
        if 'Messages' in response:
            for message_dict in response['Messages']:
                await self.semaphore.acquire()
                fut = asyncio.ensure_future(self.handle(Message(message_dict)))
                self.futures.add(fut)
                fut.add_done_callback(self.futures.remove)

    async def handle(self, message):
        try:
            delete = True
            try:
                result = self.message_handler(message)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                if e.__class__.__name__ == SQSRetry.__name__:
                    if int(message.attributes.get(
                            'ApproximateReceiveCount', 1)) <= e.max_times:
                        await self.change_one(message, e.seconds_later)
                        delete = False
                else:
                    logger.exception(e)
            finally:
                if delete:
                    await self.delete_one(message)
        finally:
            self.semaphore.release()

    async def change_batch(self, client, messages, _seq=0):
        if _seq > 0:
            await asyncio.sleep(0.1 * (2 ** _seq))
        if _seq > self.MAX_RETRY:
            raise Exception('change_message_visibility_batch failed', messages)
        try:
            resp = await client.change_message_visibility_batch(
                QueueUrl=(await self._get_queue_url()),
                Entries=[
                    {
                        'Id': str(index),
                        'ReceiptHandle': message['ReceiptHandle'],
                        'VisibilityTimeout': timeout
                    } for index, (message, timeout) in enumerate(messages)
                ]
            )
        except Exception as e:
            logger.exception(e)
            return messages
        if 'Failed' in resp:
            logger.error(
                'Change failed: {} {}'.format(messages, resp['Failed'])
            )
            server_failed_messages = [
                messages[int(d['Id'])] for d in resp['Failed']
            ]
            return await self.change_batch(
                client, server_failed_messages, _seq+1
            )

    async def change_one(self, message, visibility_timeout):
        if self.change_worker:
            await self.change_worker.put((message, visibility_timeout))
            return
        async with self.create_client() as client:
            return await client.change_message_visibility(
                QueueUrl=(await self._get_queue_url()),
                ReceiptHandle=message['ReceiptHandle'],
                VisibilityTimeout=visibility_timeout
            )

    async def delete_batch(self, client, messages, _seq=0):
        if _seq > 0:
            await asyncio.sleep(0.1 * (2 ** _seq))
        if _seq > self.MAX_RETRY:
            raise Exception('delete_message_batch failed', messages)
        try:
            resp = await client.delete_message_batch(
                QueueUrl=(await self._get_queue_url()),
                Entries=[
                    {
                        'Id': str(index),
                        'ReceiptHandle': message['ReceiptHandle']
                    } for index, message in enumerate(messages)
                ]
            )
        except Exception as e:
            logger.exception(e)
            return messages
        if 'Failed' in resp:
            logger.error('Delete failed: {}, {}'.format(
                messages, resp['Failed']))
            server_failed_messages = [
                messages[int(d['Id'])] for d in resp['Failed']
                if not d['SenderFault']
            ]
            return await self.delete_batch(
                client, server_failed_messages, _seq+1
            )

    async def delete_one(self, message):
        if self.delete_worker:
            await self.delete_worker.put(message)
            return
        async with self.create_client() as client:
            return await client.delete_message(
                QueueUrl=(await self._get_queue_url()),
                ReceiptHandle=message['ReceiptHandle']
            )

    def start(self):
        try:
            self.loop.run_until_complete(self.run_forever())
        finally:
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()


def import_function(string):
    module_path, func_name = string.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


async def plain_handler(message):
    print(message)


async def rpc_handler(message):
    record = message.body
    func = import_function(record['fpath'])
    func = getattr(func, '_real', func)
    result = func(*record['args'], **record['kw'])
    log = '{0}:{1}({2},{3})={4}'.format(
        message.message_id, func.__name__,
        ', '.join(['{0!r}'.format(arg) for arg in record['args']]),
        ', '.join([
            '{0}={1}'.format(k, v) for k, v in record['kw'].items()]),
        result
    )
    logger.info(log)


def main():
    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('queue_name')
    parser.add_argument('region')
    parser.add_argument('-c', '--concurrency', type=int, default=20)
    parser.add_argument('-v', '--visibility-timeout', type=int, default=60)
    parser.add_argument('--handler', type=str, help='default to rpc_handler')
    parser.add_argument('--no-batch', dest='batch', action='store_false',
                        default=True, help='do not use batch ops')
    args = parser.parse_args()
    if args.handler is None:
        handler = rpc_handler
    else:
        handler = import_function(args.handler)
    processer = SQSProcesser(args.queue_name, args.region, handler,
                             concurrency=args.concurrency,
                             visibility_timeout=args.visibility_timeout,
                             batch_ops=args.batch)
    processer.start()


if __name__ == '__main__':
    main()
