import aiobotocore
from . import utils
from cowfish.pool import Pool
from boto3.dynamodb import types
serialize = types.TypeSerializer().serialize
deserialize = types.TypeDeserializer().deserialize


class Dynamodb:
    service_name = 'dynamodb'

    def __init__(self, region_name, *, table_name_suffix='',
                 pool_params=None, client_params=None):
        self.table_name_suffix = table_name_suffix
        self.session = aiobotocore.get_session()
        self.client_params = client_params or {}
        self.client_params['region_name'] = region_name
        pool_params = pool_params or {}
        self.pool = Pool(self.create_client, **pool_params)

    def __repr__(self):
        return '<{}: {}, pool={!r}>'.format(
                self.__class__.__name__,
                utils.format_params(self.client_params),
                self.pool)

    def __getattr__(self, name):
        return ClientMethodProxy(self.pool, name)

    def create_client(self):
        return self.session.create_client(
            self.service_name, **self.client_params
        )

    async def close(self):
        await self.pool.close()

    def table(self, *args, **kw):
        return Table(self, *args, **kw)


class ClientMethodProxy:
    def __init__(self, pool, name):
        self._pool = pool
        self.name = name

    async def __call__(self, *args, **kw):
        client = await self._pool.acquire()
        try:
            return await getattr(client, self.name)(*args, **kw)
        finally:
            try:
                await self._pool.release(client)
            finally:
                self._pool = None


class Table:
    def __init__(self, executor, name, hash_key_name, range_key_name=None):
        self.executor = executor
        self.name = name
        self.hash_key_name = hash_key_name
        self.range_key_name = range_key_name

    def _get_key(self, hash_key, range_key=None):
        Key = {}
        Key[self.hash_key_name] = serialize(hash_key)
        if range_key is not None:
            Key[self.range_key_name] = serialize(range_key)
        return Key

    async def get_item(self, hash_key, range_key=None):
        Key = self._get_key(hash_key, range_key)
        resp = await self.executor.get_item(TableName=self.name, Key=Key)
        if 'Item' in resp:
            return {k: deserialize(v) for k, v in resp['Item'].items()}

    async def update_item(self, hash_key, range_key=None,
                          update_expression=None,
                          expression_attribute_values=None):
        Key = self._get_key(hash_key, range_key)
        kw = {}
        kw['TableName'] = self.name
        kw['Key'] = Key
        if update_expression:
            kw['UpdateExpression'] = update_expression
        if expression_attribute_values:
            kw['ExpressionAttributeValues'] = {
                k: serialize(v) for k, v in expression_attribute_values.items()
            }
        resp = await self.executor.update_item(**kw)
        if 'Item' in resp:
            return {k: deserialize(v) for k, v in resp['Item'].items()}

    async def put_item(self, hash_key,
                       range_key=None,
                       attributes=None,
                       condition=None,
                       expected=None,
                       return_values=None,
                       return_consumed_capacity=None,
                       return_item_collection_metrics=None):
        kw = {}
        Item = self._get_key(hash_key, range_key)
        if attributes:
            Item.update({
                k: serialize(v) for k, v in attributes.items()
            })
        kw['Item'] = Item
        kw['ConditionalOperator'] = str(conditional_operator).upper()
        kw['ReturnValues'] = str(return_values).upper()
        kw['ReturnConsumedCapacity'] = str(return_consumed_capacity).upper()
        kw['ReturnItemCollectionMetrics'] = \
            str(return_item_collection_metrics).upper()
        resp = await self.executor.put_item(**kw)
        if 'Item' in resp:
            return {k: deserialize(v) for k, v in resp['Item'].items()}
