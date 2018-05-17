import os
import time
import json
import base64
import asyncio
import logging
import aiobotocore
from .worker import BatchWorker
try:
    import msgpack
except ImportError:
    pass
logger = logging.getLogger(__name__)


class Kinesis:
    service_name = 'kinesis'
    MAX_RETRY = 10

    def __init__(self, stream_name, region_name,
                 encode_func=None, key_func=None,
                 *, worker_params=None, client_params=None):
        self.session = aiobotocore.get_session()
        self.stream_name = stream_name
        self.encode_func = encode_func or (lambda o: json.dumps(o).encode())
        self.key_func = key_func
        client_params = client_params or {}
        client_params['region_name'] = region_name
        self.client = self.session.create_client(
            self.service_name, **client_params)
        worker_params = worker_params or {}
        self.worker = BatchWorker(self.write_batch, **worker_params)

    def __repr__(self):
        return '<{}: stream={}, worker={!r}>'.format(
                self.__class__.__name__, self.stream_name, self.worker)

    async def stop(self):
        timestamp = time.time()
        await self.worker.stop()
        await self.client.close()
        cost = time.time() - timestamp
        logger.info('{0!r} stopped in {1:.1f} seconds'.format(self, cost))

    def _get_key(self, obj):
        if self.key_func is None:
            return base64.b64encode(os.urandom(24)).decode('ascii')
        return self.key_func(obj)

    def _encode(self, obj):
        return self.encode_func(obj)

    async def write_batch(self, obj_list, _seq=0):
        if _seq > 0:
            await asyncio.sleep(0.1 * (2 ** _seq))
        if _seq > self.MAX_RETRY:
            raise Exception('write_batch error: kinesis put_records failed',
                            obj_list)
        try:
            resp = await self.client.put_records(
                StreamName=self.stream_name,
                Records=[
                    {
                        'PartitionKey': self._get_key(obj),
                        'Data': self._encode(obj)
                    } for obj in obj_list
                ]
            )
        except Exception as e:
            logger.exception(e)
            return obj_list
        if resp['FailedRecordCount'] > 0:
            failed_obj_list = [
                obj_list[i] for i, record in enumerate(resp['Records'])
                if 'ErrorCode' in record]
            return await self.write_batch(
                failed_obj_list, _seq=_seq + 1
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
            return await self.client.put_record(
                StreamName=self.stream_name,
                Data=self._encode(obj),
                PartitionKey=self._get_key(obj)
            )
        except Exception as e:
            logger.exception(e)
            return await self.write_one(obj, queued=False, _seq=_seq + 1)


class CompactKinesis(Kinesis):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.buffer = bytearray()
        try:
            self.packer = msgpack.Packer()
        except NameError:
            raise ImportError('need msgpack')
        self.bufmax = 1024*25

    def _encode(self, obj):
        return obj

    async def write(self, obj):
        packed = self.packer.pack(obj)
        if len(self.buffer) + len(packed) >= self.bufmax:
            payload = bytes(self.buffer)
            self.buffer.clear()
            self.buffer.extend(packed)
            await self.write_one(payload, queued=True)
        else:
            self.buffer.extend(packed)

    async def stop(self):
        if self.buffer:
            payload = bytes(self.buffer)
            self.buffer.clear()
            await self.write_one(payload, queued=True)
        await super().stop()
