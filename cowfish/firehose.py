import time
import json
import asyncio
import logging
import aiobotocore
from .pool import Pool
from .worker import BatchWorker
logger = logging.getLogger(__name__)


class Firehose:
    service_name = 'firehose'

    def __init__(self, stream_name, region_name,
                 encode_func=None, delimiter=b'\n',
                 aggr_num=100, flush_interval=60,
                 **pool_kwargs):
        self.session = aiobotocore.get_session()
        self.stream_name = stream_name
        self.region_name = region_name
        self.encode_func = encode_func or (lambda o: json.dumps(o).encode())
        self.delimiter = delimiter
        self.jobs = set()
        self.pool = Pool(self.create_client, **pool_kwargs)
        self.worker = BatchWorker(
            self.handle, aggr_num=aggr_num, timeout=flush_interval
        )

    def __repr__(self):
        return '<{}: stream={}, region={}, worker={!r}, pool={!r}>'.format(
                self.__class__.__name__, self.stream_name,
                self.region_name, self.worker, self.pool)

    async def put(self, obj):
        return await self.worker.put(obj)

    def create_client(self):
        return self.session.create_client(
            'firehose', region_name=self.region_name
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
        data = self._encode(obj_list)
        client = await self.pool.acquire()
        fut = asyncio.ensure_future(
            self.pool.auto_release(client, client.put_record(
                DeliveryStreamName=self.stream_name,
                Record={'Data': data}
            ))
        )
        self.jobs.add(fut)
        fut.add_done_callback(self.jobs.remove)
