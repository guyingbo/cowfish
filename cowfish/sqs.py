import time
import base64
import pickle
import logging
import asyncio
import functools
import aiobotocore
from .pool import Pool
from .worker import BatchWorker
loop = asyncio.get_event_loop()
logger = logging.getLogger(__name__)


class SQSWriter:
    service_name = 'sqs'
    MAX_RETRY = 10

    def __init__(self, queue_name, region_name, encode_func=None,
                 *, worker_params=None, pool_params=None):
        self.session = aiobotocore.get_session()
        self.queue_name = queue_name
        self.is_fifo = queue_name.endswith('.fifo')
        self.region_name = region_name
        self.encode_func = encode_func or (
            lambda o: base64.b64encode(pickle.dumps(o, 2)).decode('ascii')
        )
        self.jobs = set()
        pool_params = pool_params or {}
        self.pool = Pool(self.create_client, **pool_params)
        self.QueueUrl = None
        self.lock = asyncio.Lock()
        self.loop = asyncio.get_event_loop()
        worker_params = worker_params or {}
        self.worker = BatchWorker(self.handle, **worker_params)

    def __repr__(self):
        return '<{}: queue={}, region={}, worker={!r}, pool={!r}>'.format(
                self.__class__.__name__, self.queue_name, self.region_name,
                self.worker, self.pool)

    async def _get_queue_url(self):
        if self.QueueUrl is None:
            async with self.lock:
                async with self.pool.get() as client:
                    resp = await client.get_queue_url(
                            QueueName=self.queue_name
                        )
                    self.QueueUrl = resp['QueueUrl']
        return self.QueueUrl

    def create_client(self):
        return self.session.create_client(
            self.service_name, region_name=self.region_name
        )

    async def stop(self):
        timestamp = time.time()
        await self.worker.stop()
        if self.jobs:
            await asyncio.wait(self.jobs)
        await self.pool.close()
        cost = time.time() - timestamp
        logger.info('{0!r} stopped in {1:.1f} seconds'.format(self, cost))

    def _encode(self, obj):
        return self.encode_func(obj)

    async def handle(self, obj_list):
        client = await self.pool.acquire()
        fut = asyncio.ensure_future(
            self.pool.auto_release(
                client, self.write_batch(client, obj_list)
            )
        )
        self.jobs.add(fut)
        fut.add_done_callback(self.jobs.remove)

    async def write_one(self, record, delay_seconds=0,
                        deduplication_id=None, group_id=None,
                        queued=False, _seq=0, **attributes):
        if _seq > 0:
            await asyncio.sleep(0.1 * (2 ** _seq))
        if _seq > self.MAX_RETRY - 1:
            raise Exception('write_one error: SQS send_message failed')
        message = {
            'record': record,
            'delay_seconds': delay_seconds,
            'attributes': attributes,
            'params': {},
        }
        if self.is_fifo:
            if deduplication_id:
                message['params']['MessageDeduplicationId'] = deduplication_id
            if group_id:
                message['params']['MessageGroupId'] = group_id
        if queued:
            await self.worker.put(message)
            return
        try:
            async with self.pool.get() as client:
                return await client.send_message(
                    QueueUrl=(await self._get_queue_url()),
                    MessageBody=self._encode(record),
                    DelaySeconds=delay_seconds,
                    MessageAttributes=attributes,
                    **message['params'],
                )
        except Exception as e:
            logger.exception(e)
            return await self.write_one(
                    record, delay_seconds=delay_seconds,
                    deduplication_id=deduplication_id, group_id=group_id,
                    queued=False, _seq=_seq+1, **attributes)

    async def write_batch(self, client, obj_list, _seq=0):
        if _seq > 0:
            await asyncio.sleep(0.1 * (2 ** _seq))
        if _seq > self.MAX_RETRY - 1:
            raise Exception('write_batch error: SQS send_message_batch failed',
                            obj_list)
        Entries = [
            {
                'Id': str(i),
                'MessageBody': self._encode(message['record']),
                'DelaySeconds': message['delay_seconds'],
                'MessageAttributes': message['attributes'],
                **message['params'],
            } for i, message in enumerate(obj_list)
        ]
        try:
            resp = await client.send_message_batch(
                QueueUrl=(await self._get_queue_url()),
                Entries=Entries
            )
        except Exception as e:
            logger.exception(e)
            return obj_list
        if 'Failed' in resp:
            logger.error('Send failed: {}, {}'.format(
                obj_list, resp['Failed']))
            failed_obj_list = [
                obj_list[int(d['Id'])] for d in resp['Failed']
                if not d['SenderFault']
            ]
            return await self.write_batch(
                client, failed_obj_list, _seq=_seq + 1
            )

    def async_rpc(self, func=None, *, delay_seconds=0,
                  deduplication_id=None, group_id=None,
                  queued=True, **attributes):
        if func is None:
            return functools.partial(self.async_rpc,
                                     delay_seconds=delay_seconds,
                                     deduplication_id=deduplication_id,
                                     group_id=group_id,
                                     queued=queued, **attributes)
        if type(func) == str:
            async def async_func(*args, **kw):
                record = {
                    'fpath': func,
                    'args': args,
                    'kw': kw,
                }
                await self.write_one(record, delay_seconds=delay_seconds,
                                     deduplication_id=deduplication_id,
                                     group_id=group_id,
                                     queued=queued, **attributes)
        else:
            @functools.wraps(func)
            async def async_func(*args, **kw):
                fpath = func.__module__ + '.' + func.__name__
                record = {
                    'fpath': fpath,
                    'args': args,
                    'kw': kw,
                }
                await self.write_one(record, delay_seconds=delay_seconds,
                                     deduplication_id=deduplication_id,
                                     group_id=group_id,
                                     queued=queued, **attributes)
        return async_func


class StringAttribute(dict):
    def __init__(self, value):
        self['DataType'] = 'String'
        self['StringValue'] = str(value)


class BinaryAttribute(dict):
    def __init__(self, value):
        self['DataType'] = 'Binary'
        self['BinaryValue'] = bytes(value)


class NumberAttribute(dict):
    def __init__(self, value):
        self['DataType'] = 'Number'
        self['StringValue'] = str(value)
