
import redis
from library import config


class RedisClient(object):

    def __init__(self, **kwargs):
        """
        Args:
            host: str, Redis ip/hostname
            port, int, Redis port
            db: int, Redis default database
            password: int(optional)
        """
        _config = {**config.redis_config, **kwargs}
        self.pool = redis.ConnectionPool(**_config)
        self.client = redis.Redis(connection_pool=self.pool)

    def get_client(self):
        return self.client
