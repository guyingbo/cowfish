import aiobotocore
from cowfish.pool import Pool
from boto3.dynamodb import types
serializer = types.TypeSerializer()
deserializer = types.TypeDeserializer()


class DynamodbExecutor:
    service_name = 'dynamodb'

    def __init__(self, region_name, *, table_name_suffix='',
                 pool_params=None, client_params=None):
        self.region_name = region_name
        self.table_name_suffix = table_name_suffix
        self.session = aiobotocore.get_session()
        self.client_params = client_params or {}
        pool_params = pool_params or {}
        self.pool = Pool(self.create_client, **pool_params)

    def __repr__(self):
        return '<{}: region={}, pool={!r}>'.format(
                self.__class__.__name__, self.region_name, self.pool)

    def __getattr__(self, name):
        return ClientMethodProxy(self.pool, name)

    def create_client(self):
        return self.session.create_client(
            self.service_name, region_name=self.region_name,
            **self.client_params
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
        Key[self.hash_key_name] = serializer.serialize(hash_key)
        if range_key is not None:
            Key[self.range_key_name] = serializer.serializer(range_key)
        return Key

    async def get_item(self, hash_key, range_key=None):
        Key = self._get_key(hash_key, range_key)
        resp = await self.executor.get_item(TableName=self.name, Key=Key)
        if 'Item' in resp:
            return {k: deserializer.deserialize(v)
                    for k, v in resp['Item'].items()}

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
                k: serializer.serialize(v)
                for k, v in expression_attribute_values.items()
            }
        await self.executor.update_item(**kw)
