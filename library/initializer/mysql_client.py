# -*- coding: utf-8 -*-
# ===============================================================
#   @author: fengyinws@gmail.com
#   @date: 2020/03/07 16:49:30
#   @brief: 通过SSH连接MySQL的客户端
#   @notice: 工具库不处理Exception，统一由业务层来处理
# ================================================================

import traceback
import pymysql
import pandas as pd
from pymysql.err import MySQLError

from library.util import timer
from library.decorators import retry
from library.log import Log
logger = Log().get_logger()


class MysqlClient(object):

    def __init__(self, **kwargs):

        self.mysql_config = kwargs
        self.connection = self._connect_mysql()

    def _connect_mysql(self):
        self.connection = None
        self.connection = pymysql.connect(**self.mysql_config)
        return self.connection

    def reconnect(self):
        self._connect_mysql()

    @retry()
    def read(self, sql):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
            return data or None
        except:
            self.reconnect()
            logger.warning(traceback.format_exc())
            return False

    @timer("read_as_dict")
    @retry()
    def read_as_dict(self, sql):
        try:
            with self.connection.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
            return data or None
        except:
            self.reconnect()
            logger.warning(traceback.format_exc())
            return False

    @retry()
    def read_df(self, sql):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
                columns = [x[0] for x in cursor.description]

            if not isinstance(data, (list, tuple)):
                return None
            return pd.DataFrame(list(data), columns=columns)
        except:
            self.reconnect()
            logger.warning(traceback.format_exc())
            return False

    @timer("mysql_write")
    @retry()
    def write(self, sql, *args):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, *args)
                self.connection.commit()
            return True
        except:
            self.reconnect()
            logger.warning(traceback.format_exc())
            return False

    @retry()
    def write_many(self, sql, *args):
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(sql, *args)
                self.connection.commit()
            return True
        except:
            self.reconnect()
            logger.warning(traceback.format_exc())
            return False

    def close_mysql(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def close(self):
        self.close_mysql()

    def __del__(self):
        self.close()
