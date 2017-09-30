import time
import json
import asyncio
import logging
import aiobotocore
from . import utils
from .pool import Pool
from .worker import BatchWorker
logger = logging.getLogger(__name__)


class Firehose:
    service_name = 'firehose'

    def __init__(self, stream_name, region_name,
                 encode_func=None, delimiter=b'\n',
                 *, worker_params=None, pool_params=None,
                 client_params=None):
        self.session = aiobotocore.get_session()
        self.stream_name = stream_name
        self.encode_func = encode_func or (lambda o: json.dumps(o).encode())
        self.delimiter = delimiter
        self.jobs = set()
        self.client_params = client_params or {}
        self.client_params['region_name'] = region_name
        pool_params = pool_params or {}
        self.pool = Pool(self.create_client, **pool_params)
        worker_params = worker_params or {}
        self.worker = BatchWorker(self.handle, **worker_params)

    def __repr__(self):
        return '<{}: stream={}, {}, worker={!r}, pool={!r}>'.format(
                self.__class__.__name__, self.stream_name,
                utils.format_params(self.client_params),
                self.worker, self.pool)

    async def put(self, obj):
        return await self.worker.put(obj)

    def create_client(self):
        return self.session.create_client(
            self.service_name, **self.client_params
        )

    async def stop(self):
        timestamp = time.time()
        await self.worker.stop()
        if self.jobs:
            await asyncio.wait(self.jobs)
        await self.pool.close()
        cost = time.time() - timestamp
        logger.info('{0!r} stopped in {1:.1f} seconds'.format(self, cost))

    def _encode(self, obj_list):
        encoded = [self.encode_func(obj) for obj in obj_list]
        encoded.append(b'')
        return self.delimiter.join(encoded)

    async def handle(self, obj_list):
        client = await self.pool.acquire()
        fut = asyncio.ensure_future(
            self.pool.auto_release(
                client, self.write_batch(client, obj_list)
            )
        )
        self.jobs.add(fut)
        fut.add_done_callback(self.jobs.remove)

    async def write_batch(self, client, obj_list):
        try:
            data = self._encode(obj_list)
            await client.put_record(
                DeliveryStreamName=self.stream_name,
                Record={'Data': data}
            )
        except Exception as e:
            logger.exception(e)
            return obj_list
