import time
import json
import logging
import asyncio
import aiobotocore
from .worker import BatchWorker
logger = logging.getLogger(__name__)


class Firehose:
    service_name = 'firehose'
    MAX_RETRY = 10

    def __init__(self, stream_name, region_name,
                 encode_func=None, delimiter=b'\n',
                 *, worker_params=None, client_params=None,
                 original_api=False):
        self.session = aiobotocore.get_session()
        self.stream_name = stream_name
        self.encode_func = encode_func or (lambda o: json.dumps(o).encode())
        self.delimiter = delimiter
        client_params = client_params or {}
        client_params['region_name'] = region_name
        self.client = self.session.create_client(
            self.service_name, **client_params)
        worker_params = worker_params or {}
        batch_func = self.original_batch if original_api else self.write_batch
        self.worker = BatchWorker(batch_func, **worker_params)

    def __repr__(self):
        return '<{}: stream={}, worker={!r}>'.format(
                self.__class__.__name__, self.stream_name, self.worker)

    async def put(self, obj):
        return await self.worker.put(obj)

    async def stop(self):
        timestamp = time.time()
        await self.worker.stop()
        await self.client.close()
        cost = time.time() - timestamp
        logger.info('{0!r} stopped in {1:.1f} seconds'.format(self, cost))

    def _encode(self, obj_list):
        encoded = [self.encode_func(obj) for obj in obj_list]
        encoded.append(b'')
        return self.delimiter.join(encoded)

    async def write_batch(self, obj_list):
        try:
            data = self._encode(obj_list)
            await self.client.put_record(
                DeliveryStreamName=self.stream_name,
                Record={'Data': data}
            )
        except Exception as e:
            logger.exception(e)
            return obj_list

    async def original_batch(self, obj_list, _seq=0):
        if _seq > 0:
            await asyncio.sleep(0.1 * (2 ** _seq))
        if _seq > self.MAX_RETRY:
            raise Exception(
                'write_batch error: firehose put_record_batch failed')
        try:
            resp = await self.client.put_record_batch(
                DeliveryStreamName=self.stream_name,
                Records=[{
                    'Data': self.encode_func(obj) + self.delimiter
                } for obj in obj_list]
            )
        except Exception as e:
            logger.exception(e)
            return obj_list
        if resp['FailedPutCount'] > 0:
            failed_obj_list = [
                obj_list[i] for i, record in
                enumerate(resp['RequestResponses'])
                if 'ErrorCode' in record]
            return await self.write_batch(
                failed_obj_list, _seq=_seq + 1
            )
