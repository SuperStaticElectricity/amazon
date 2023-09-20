# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: spider.py
@time: 2021-04-07 15:14
@desc:
"""
import json
import os
import datetime
import time

import pandas
from pandas import DataFrame

from library import log, mail
from library.config import \
    redis_config as default_redis_config, \
    mysql_config as default_mysql_config, \
    mail_config as default_mail_config
from library.initializer import initializer
from library.util import format_redis
from library.constants import SleepTime, Position
from library.decorators import log as print_log, retry



class BaseSpider(object):
    """ 爬虫基类 """
    def __init__(self, mysql_config: dict = None, redis_config: dict = None,
                 mysql_client=None, redis_client=None, log_hub_client=None, logger=None):
        """初始化logger、spider_name、redis_client、mysql_client、log_hub_client"""
        self.project_name = None  # 项目名字
        self.spider_name = None  # 爬虫名字
        self.status = None  # 爬取的状态
        self.ip_data = None  # ip数据
        self.cookie_data = None
        self.start_time = None
        self.end_time = None
        self.monitor_spider = None
        self.ip_service = None
        self.ip_status = None
        self.except_info = None
        self.api_info = {
            'page': 0,
            'total_page': 1,
            'has_next': True
        }

        self.redis_config = redis_config or default_redis_config
        self.mysql_config = mysql_config or default_mysql_config

        self.logger = logger or log.Log().get_logger()  # 日志初始化
        self.mysql_client = mysql_client or initializer.Initializer(self.logger).init_mysql(
            self.mysql_config)  # mysql客户端
        self.redis_client = redis_client or initializer.Initializer(self.logger).init_redis(
            self.redis_config)  # redis客户端
        self.proxies_type = 0  # 代理类型

        self.redis_key_todo = None  # 读存redis队列名字
        self.seed = None  # 种子数据
        self.seed_max_failed_count = 5

        self.exit_time = SleepTime.EXIT_SLEEP_SECONDS  # 优雅退出时间
        # 2020-03-16 添加爬取统计
        self.seed_log_name = None  # 记录名
        self.seed_log = self.init_spider_log()  # 是否记录爬取成功率
        self.key_name = None
        self.log_time = datetime.datetime.now().strftime("%Y-%m-%d")  # 统计日志日期
        # for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:                  # 捕获进程信号
        #     signal.signal(signalnum=sig, handler=self.shutdown)

    def init_spider_log(self, seed_log_name='', key_name='(tb)statistics_taobao_spider', format_key: int = 0):
        """初始化爬取统计
        :param seed_log_name: 爬取记录命名(不传则不记录)
        :param key_name: redis_key
        :param format_key: 创建第x天的统计key
        :return:
        """
        if not seed_log_name:
            seed_log_name = self.seed_log_name
            if not seed_log_name:
                return False
        else:
            self.seed_log_name = seed_log_name
        if not key_name:
            return False
        self.seed_log_name = seed_log_name
        if not format_key:
            # 首次启动, 创建当日和明日统计hash
            key_name_1day = key_name + f"({datetime.datetime.now().strftime('%Y-%m-%d')})"
            key_name_2day = key_name + f"({(datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')})"
            self.check_and_create_hash(key_name_1day)
            self.check_and_create_hash(key_name_2day)
        else:
            key_name_format = key_name + f"({(datetime.datetime.now() + datetime.timedelta(days=format_key)).strftime('%Y-%m-%d')})"
            self.check_and_create_hash(key_name_format)
        return True

    def check_and_create_hash(self, key_name):
        is_key = self.redis_client.exists(name=key_name)
        if not is_key:
            self.logger.warning('找不到相应hash, 重新创建')
            self.redis_client.hmset(name=key_name,
                                    mapping={self.seed_log_name + '(SpiderNum)': 0})
            self.redis_client.hmset(name=key_name,
                                    mapping={self.seed_log_name + '(FailNum)': 0})
        return True

    def get_ip(self):
        # 获取代理ip
        pass

    def set_ip(self):
        # 处理使用过的ip
        pass

    @print_log()
    def get_seed(self, redis_key_todo: str = None, position: str = Position.RIGHT, seed: dict = None,
                 doing_field: str = None):
        """
        从redis队列中原子性提取数据
        :param redis_key_todo: 队列名字
        :param position: 提取方向
        :param seed: 自定义种子
        :param doing_field: 防止种子丢失的字段， 作为hash的key
        :return:
        """
        value = None
        self.status = None
        self.api_info = {
            'page': 0,
            'total_page': 1,
            'has_next': True
        }
        self.start_time = time.perf_counter()
        if seed:
            return seed
        try:
            if redis_key_todo is None:
                redis_key_todo = self.redis_key_todo
            if position == Position.RIGHT:
                value = self.redis_client.rpop(redis_key_todo)
            else:
                value = self.redis_client.lpop(redis_key_todo)
            if isinstance(value, (str, bytes)) and len(value) > 0:
                if doing_field:
                    # 添加到DOING队列
                    value_temp = json.loads(value)
                    value_temp['fetch_time'] = int(time.time())
                    self.redis_client.hmset(name=format_redis(redis_key_todo),
                                            mapping={value_temp[doing_field]: json.dumps(value_temp)})
                return json.loads(value)
        except json.JSONDecodeError as je:
            self.logger.error(f"{je}: parse json error -- {value}")
        except Exception as e:
            self.logger.exception(e)
        return None

    @print_log()
    def set_seed(self, status: bool = None, seed: dict = None, redis_key_todo: str = None,
                 position: str = Position.LEFT, doing_field: str = None):
        """
        当爬虫失败的时候，把seed放入队列
        :param seed: 种子信息
        :param status: 爬虫状态
        :param redis_key_todo: 队列名字
        :param position: 队列方向
        :param doing_field: 防止种子丢失的字段，作为hash的key
        :return:
        """

        try:
            if seed is None:
                seed = self.seed
                if seed is None:
                    return None

            if redis_key_todo is None:
                redis_key_todo = self.redis_key_todo
            if status is None:
                status = self.status

            if doing_field:
                # 删除DOING的种子
                self.redis_client.hdel(format_redis(redis_key_todo), seed[doing_field])

            if status is False:
                seed['fail_num'] = int(seed.get('fail_num', 0)) + 1
                if seed['fail_num'] >= self.seed_max_failed_count:
                    # 处理异常种子
                    self.redis_client.lpush(redis_key_todo.replace('TODO', 'FAILED'), json.dumps(seed))
                    # mail.Mail(receivers=['liheyou@zhiyitech.cn']).send_mail(subject='种子失败报警', content=f"""
                    #     队列名字: {self.redis_key_todo} 种子: {json.dumps(seed)}
                    # """)
                    return True
                if position == Position.LEFT:
                    self.redis_client.lpush(redis_key_todo, json.dumps(seed))
                else:
                    self.redis_client.rpush(redis_key_todo, json.dumps(seed))
                return True
            return None
        except Exception as e:
            self.logger.exception(e)
            return False

    @retry()
    def push_seeds(self, seeds, redis_key: str=None, position: str = Position.LEFT):

        if not isinstance(seeds, (list, dict)):
            raise Exception('seeds must be list or dict.')

        if isinstance(seeds, dict):
            seeds = [seeds]

        if not redis_key:
            redis_key = self.redis_key_todo

        json_list = [json.dumps(seed, ensure_ascii=False) for seed in seeds]

        try:
            if position == Position.RIGHT:
                self.redis_client.rpush(redis_key, *json_list)
            else:
                self.redis_client.lpush(redis_key, *json_list)
            return True
        except Exception as e:
            self.logger.error(e)
            return False

    def request_config(self):
        # 构造请求参数
        pass

    def do_request(self):
        # 请求操作
        pass

    def parse(self, response):
        # 解析数据
        pass

    def save_to_mysql(self):
        pass

    def insert_or_update(self, table_name: str, data_list=None, not_update_field: list = list()):
        """
        有该条记录就更新，无该条记录就插入
        :param table_name: 表名字
        :param data_list: 插入的数据
        :param not_update_field: 不更新的字段
        :return: 写入数据是否成功
        """
        if not data_list:
            return data_list

        if not isinstance(data_list, (list, dict)):
            self.logger.warning('insert data must be list or dict.')
            return False

        if isinstance(data_list, list) and not data_list[0]:
            return data_list[0]

        datas = data_list
        if isinstance(datas, dict):
            # 字典转列表
            datas = [datas]

        try:
            # data_df = DataFrame(data_list)
            data_df = DataFrame(datas).where((pandas.notnull(DataFrame(datas))), None)  # 处理字段是NULL的情况
            column_str = ','.join(f'`{field}`' for field in data_df.columns)
            value_str = ','.join(['%s'] * len(data_df.columns))

            update_list = list()
            for column in data_df.columns:
                if column in not_update_field:
                    continue
                update_list.append('`{field}`=VALUES(`{field}`)'.format(field=column))
            update_str = ','.join(update_list)
            if '.' in table_name:
                table_name = table_name.split('.')
                table_name = f"`{table_name[0].replace('`', '')}`.`{table_name[1].replace('`', '')}`"
            else:
                table_name = f"`{table_name}`"

            sql = f"INSERT INTO {table_name} ({column_str}) VALUES({value_str}) " \
                  f"ON DUPLICATE KEY UPDATE {update_str}"
            result = self.mysql_client.write_many(sql, data_df.values.tolist())
            return result

        except Exception as e:
            self.logger.exception(e)
            return False



