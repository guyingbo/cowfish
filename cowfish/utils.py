

def format_params(params):
    return ', '.join('{}={}'.format(k, v) for k, v in params.items())


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
