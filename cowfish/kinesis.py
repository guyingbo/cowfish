import os
import time
import json
import base64
import asyncio
import logging
import aiobotocore
from .pool import Pool
from .worker import BatchWorker
logger = logging.getLogger(__name__)


class Kinesis:
    service_name = 'kinesis'
    MAX_RETRY = 10

    def __init__(self, stream_name, region_name,
                 encode_func=None, key_func=None,
                 aggr_num=100, flush_interval=60,
                 *, worker_params=None, pool_params=None):
        self.session = aiobotocore.get_session()
        self.stream_name = stream_name
        self.region_name = region_name
        self.encode_func = encode_func or (lambda o: json.dumps(o).encode())
        self.key_func = key_func
        self.jobs = set()
        pool_params = pool_params or {}
        self.pool = Pool(self.create_client, **pool_params)
        worker_params = worker_params or {}
        self.worker = BatchWorker(self.handle, **worker_params)

    def __repr__(self):
        return '<{}: stream={}, region={}, worker={!r}, pool={!r}>'.format(
                self.__class__.__name__, self.stream_name,
                self.region_name, self.worker, self.pool)

    async def put(self, obj):
        return await self.worker.put(obj)

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

    def _get_key(self, obj):
        if self.key_func is None:
            return base64.b64encode(os.urandom(24)).decode('ascii')
        return self.key_func(obj)

    def _encode(self, obj):
        return self.encode_func(obj)

    async def write_batch(self, client, obj_list, _seq=0):
        if _seq > 0:
            await asyncio.sleep(0.1 * (2 ** _seq))
        if _seq > self.MAX_RETRY:
            raise Exception('write_batch error: kinesis put_records failed',
                            obj_list)
        resp = await client.put_records(
            StreamName=self.stream_name,
            Records=[
                {
                    'PartitionKey': self._get_key(obj),
                    'Data': self._encode(obj)
                } for obj in obj_list
            ]
        )
        if resp['FailedRecordCount'] > 0:
            failed_obj_list = [
                obj_list[i] for i, record in enumerate(resp['Records'])
                if 'ErrorCode' in record]
            return await self.write_batch(
                client, failed_obj_list, _seq=_seq + 1
            )

    async def write_one(self, obj, queued=False, _seq=0):
        if _seq > 0:
            await asyncio.sleep(0.1 * (2 ** _seq))
        if _seq > self.MAX_RETRY - 1:
            raise Exception('write_one error: kinesis put_record failed')
        if queued:
            await self.worker.put(obj)
            return
        try:
            async with self.pool.get() as client:
                return await client.put_record(
                    StreamName=self.stream_name,
                    Data=self._encode(obj),
                    PartitionKey=self._get_key(obj)
                )
        except Exception as e:
            logger.exception(e)
            return await self.write_one(obj, queued=False, _seq=_seq + 1)

    async def handle(self, obj_list):
        client = await self.pool.acquire()
        fut = asyncio.ensure_future(
            self.pool.auto_release(
                client, self.write_batch(client, obj_list)
            )
        )
        self.jobs.add(fut)
        fut.add_done_callback(self.jobs.remove)
