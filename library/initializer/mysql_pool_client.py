
import traceback
import pymysql
import pandas as pd
from DBUtils.PersistentDB import PersistentDB
from DBUtils.PooledDB import PooledDB
from library.decorators import retry
from library.log import Log
logger = Log().get_logger()


class MysqlPoolClient(object):

    def __init__(self, **kwargs):

        self.mysql_config = kwargs
        self._init_pool()

    def _init_pool(self):

        if not self.mysql_config:
            raise ValueError('database config must be set.')

        pool_type = self.mysql_config.get('pool_type', 'Pooled')
        if pool_type not in ['Pooled', 'Persistent']:
            raise ValueError('"pool_type" must be Pooled or Persistent.')

        if pool_type is 'Pooled':
            self.pool = PooledDB(pymysql, **self.mysql_config)
        else:
            self.pool = PersistentDB(pymysql, **self.mysql_config)

        logger.debug(f'{pool_type} pool inited.')

    def _get_connection(self):
        return self.pool.connection()

    @retry()
    def read(self, sql):
        try:
            with self._get_connection().cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
            return data or None
        except:
            logger.warning(traceback.format_exc())
            return False

    @retry()
    def read_as_dict(self, sql):
        try:
            with self._get_connection().cursor(cursor=pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
            return data or None
        except:
            logger.warning(traceback.format_exc())
            return False

    @retry()
    def read_df(self, sql):
        try:
            with self._get_connection().cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
                columns = [x[0] for x in cursor.description]

            if not isinstance(data, (list, tuple)):
                return None
            return pd.DataFrame(list(data), columns=columns)
        except:
            logger.warning(traceback.format_exc())
            return False

    @retry()
    def write(self, sql, *args):
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, *args)
                connection.commit()
            return True
        except:
            # 如果发生错误则回滚
            connection.rollback()
            # 抛出异常原因
            raise
        finally:
            connection.close()

    @retry()
    def write_many(self, sql, *args):
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.executemany(sql, *args)
                connection.commit()
            return True
        except:
            # 如果发生错误则回滚
            connection.rollback()
            # 抛出异常原因
            raise
        finally:
            connection.close()
