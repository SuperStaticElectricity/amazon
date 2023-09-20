from library import config, log
from library.decorators import retry
# from library.initializer.hive_client import HiveClient
from library.initializer.mysql_client import MysqlClient
from library.initializer.redis_client import RedisClient


class Initializer(object):
    """初始化一些客户端，例如：mysql、redis、logHub"""

    def __init__(self, logger=None):
        if logger is None:
            self.logger = log.Log(config.log_config).get_logger()
        else:
            self.logger = logger

    @retry()
    def init_mysql(self, mysql_config: dict=None):
        """
        :param mysql_config:
        :return:
        """

        if mysql_config is None:
            mysql_config = config.mysql_config

        try:
            mysql_client = MysqlClient(**mysql_config)
            return mysql_client
        except Exception as e:
            self.logger.exception(e)
        return False

    @retry(tries=10, delay=2)
    def init_redis(self, redis_config: dict=None):
        """

        :param redis_config:
        :return:
        """
        if redis_config is None:
            redis_config = config.redis_config

        try:
            redis_client = RedisClient(**redis_config).get_client()
            return redis_client
        except Exception as e:
            self.logger.exception(e)
        return False


    @retry()
    def init_all(self, mysql_config=None, redis_config=None, log_hub_config=None, hive_config=None):
        """ 初始化mysql、redis、log_hub客户端"""
        mysql_client = self.init_mysql(mysql_config)
        redis_client = self.init_redis(redis_config)
        return mysql_client, redis_client, log_hub_client, hive_client
