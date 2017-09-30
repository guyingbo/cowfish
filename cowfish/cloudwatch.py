import aiobotocore
from . import utils
from .pool import Pool
# logger = logging.getLogger(__name__)


class Cloudwatch:
    service_name = 'cloudwatch'

    def __init__(self, region_name, *,
                 pool_params=None, client_params=None):
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

    def create_client(self):
        return self.session.create_client(
            self.service_name, **self.client_params
        )

    async def close(self):
        await self.pool.close()

    def __getattr__(self, name):
        return utils.ClientMethodProxy(self.pool, name)
