# ===============================================================
# -*- coding: utf-8 -*-
# @Author  : liheyou@zhiyitech.cn
# @Date    : 2018/7/26 下午2:57
# @Brief   : 
# ===============================================================
import time
import json
import datetime

from library import log, util, mail, constants
from library.util import get_version, bytes_to_string
from library.constants import Position, MailReceiver
from library.initializer import initializer
from library.config import \
    redis_config as default_redis_config, \
    mysql_config as default_mysql_config, \
    mail_config as default_mail_config

from spider_task_monitor.monitor import monitor


MAX_QUEUE_LEN = 200000  # 一次向redis队列中插入的最大元素数量


class BaseScheduler(object):
    """ 种子调度器基类 """

    def __init__(self, mysql_config=None, redis_config=None):
        self.project_name = None                                                    # 项目名字
        self.scheduler_name = None                                                  # 任务名字
        self.redis_key_todo = None                                                  # 读存redis队列名字
        self.position = None                                                        # 调度插入的方向
        self.seed_mail = False                                                      # 调度成功、失败发邮件
        self.start_num = 0                                                          # Mysql调度的起始位置
        self.limit_num = 200000                                                     # 每次从mysql提取的数量
        self.upper_limit = float('Inf')                                             # 提取上限，默认正无穷
        self.monitor_spider = None                                                  # 爬虫监控，成功次数、失败次数
        self.monitor_redis = False                                                  # 监控队列是否消费完成
        self.delay_time = 0                                                         # 调度延时时间
        self.start_time = int(time.time())                                          # 调度开始时间
        self.scheduler_num = 0                                                      # 调度次数
        # 队列长度限制（默认开启）
        self.queue_length_limit = True

        self.redis_config = redis_config
        self.logger = log.Log().get_logger()
        self.mysql_client = initializer.Initializer(self.logger).init_mysql(mysql_config)
        self.redis_client = initializer.Initializer(self.logger).init_redis(redis_config)
        # self.hive_client = initializer.Initializer(self.logger).init_hive()
        # 2020-03-16 添加爬取统计
        self.seed_log_name = None  # 记录名
        self.seed_log = self.init_spider_log()  # 是否记录爬取成功率
        self.key_name = None
        self.log_time = datetime.datetime.now().strftime("%Y-%m-%d")                # 统计日志日期

    def init_spider_log(self, seed_log_name='', key_name='(tb)statistics_taobao_spider', format_key:int=0):
        """初始化爬取统计
        :param seed_log_name: 爬取记录命名(不传则不记录)
        :param key_name: redis_key
        :return:
        """
        # 请求参数校验
        if not key_name:
            return False
        if not seed_log_name and not self.seed_log_name:
            # seed_log_name和self.seed_log_name均为空则返回
            return False

        # seed_log_name变量值覆盖
        if seed_log_name:
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
                                    mapping={self.seed_log_name + '(Seeds)': 0})
        return True

    def incr_spider_log(self, key, key_name='(tb)statistics_taobao_spider'):
        """ 爬取记录自增
        :param key: '_spider_num'/'_fail_num' 用于拼接hash_map的key
        :param key_name: redis_key
        :return:
        """
        if not key:
            raise Exception('统计名称的key不存在')
        try:
            if self.key_name:
                key_name = self.key_name
            if datetime.datetime.now().strftime("%Y-%m-%d") != self.log_time:
                self.log_time = datetime.datetime.now().strftime("%Y-%m-%d")
                # 提前创建第二天的统计报表
                self.init_spider_log(format_key=1)
            key_name = key_name + f'({datetime.datetime.now().strftime("%Y-%m-%d")})'
            self.redis_client.hincrby(name=key_name,
                                      key=self.seed_log_name + key)
        except Exception as e:
            self.logger.exception(e)
            return self.incr_spider_log(key)
        return True

    def config_queue_limit(self):
        limit = self.redis_client.hmget(constants.QUEUE_LENGTH_LIMIT_MAP, self.redis_key_todo)[0]
        if limit:
            return
        if constants.QUEUE_LENGTH_LIMIT and isinstance(constants.QUEUE_LENGTH_LIMIT, int):
            limit = constants.QUEUE_LENGTH_LIMIT
        else:
            limit = (self.limit_num or MAX_QUEUE_LEN)*10
        value = {self.redis_key_todo: limit}
        self.redis_client.hmset(constants.QUEUE_LENGTH_LIMIT_MAP, value)

    def schedule(self, df=None, redis_key_todo: str=None, position=Position.LEFT, version=None, post_data=None):
        """
        :param df: 数据
        :param redis_key_todo: 队列名字
        :param position: 插入方式
        :param version: 版本号
        :param post_data: 自定义上传种子(list)
        :return:
        """

        if not redis_key_todo:
            redis_key_todo = self.redis_key_todo

        if post_data:
            # 推自定义种子
            seeds = [json.dumps(seed, ensure_ascii=False) for seed in post_data]
            if position == Position.RIGHT:
                self.redis_client.rpush(redis_key_todo, *seeds)
            else:
                self.redis_client.lpush(redis_key_todo, *seeds)
            self.logger.info(
                f"({self.scheduler_num})success:: {self.scheduler_name}\tQueue-name={redis_key_todo}\t"
                f"version={version}\tSeed-Count={len(seeds)}")
            return

        if df is None:
            return None

        df['version'] = version or util.get_version()
        df_json = df.to_json(orient='records')                      # 存入redis队列
        json_list = [json.dumps(x) for x in json.loads(df_json)]
        seed_count = len(json_list)
        if seed_count <= MAX_QUEUE_LEN:
            if position == Position.RIGHT:
                self.redis_client.rpush(redis_key_todo, *json_list)
            else:
                self.redis_client.lpush(redis_key_todo, *json_list)
        else:
            # 切片push
            for start in range(0, len(json_list), MAX_QUEUE_LEN):
                end = start + MAX_QUEUE_LEN
                if position == Position.RIGHT:
                    self.redis_client.rpush(redis_key_todo, *json_list[start:end])
                else:
                    self.redis_client.lpush(redis_key_todo, *json_list[start:end])
                self.logger.info(f'种子分批存入队列，seed_count={seed_count}, start={start}, end={end}')
        self.scheduler_num += 1                                                         # 调度次数+1
        self.logger.info(f"({self.scheduler_num})success:: {self.scheduler_name}\tQueue-name={self.redis_key_todo}\t"
                         f"version={version}\tSeed-Count={seed_count}")

    def init_monitor(self, bis_name: str = None, task_name: str = None, redis_config: str = None,
                     mail_config: str = None):

        bis_name = bis_name or self.project_name
        task_name = task_name or self.scheduler_name
        redis_config = redis_config or default_redis_config
        mail_config = mail_config or default_mail_config
        monitor_spider = monitor.Monitor(bis_name=bis_name, task_name=task_name,
                                         redis_config=redis_config, mail_config=mail_config)  # 监控
        monitor_spider.init_config(failure_rate_up=.2, avg_time_up=30.0, warn_addresses=constants.MailRECEIVERS)
        return monitor_spider

    def execute(self, sql: str = None, version: str = None, receivers: list = constants.MailRECEIVERS,
                scheduler_type: str = None):
        """
        :param sql: sql语句
        :param version: 版本号
        :param receivers: 调度出错，报警邮件接收者
        :return:
        """
        if not sql:
            self.logger.warning('sql is None')
            return sql

        self.config_queue_limit()

        # 检测队列是否消费完成
        redis_len = self.redis_client.llen(self.redis_key_todo)
        if redis_len > 0 and self.monitor_redis:
            self.redis_client.delete(self.redis_key_todo)                       # 调度前删除原有种子，防止种子重复
            mail.Mail(receivers=receivers).send_mail(subject='redis队列长度检测',
                                                     content=f'项目名字: {self.project_name} '
                                                             f'任务名字: {self.scheduler_name} '
                                                             f'队列名字: {self.redis_key_todo} '
                                                             f'剩余: {redis_len}')

        start_num = self.start_num
        queue_length = 0
        version = version or get_version()
        if 'LIMIT' in sql or 'limit' in sql:
            pass
        else:
            sql = sql + ' LIMIT {start_num}, {limit_num}'
        self.logger.info(f'SQL语句：{sql}')

        if self.limit_num > self.upper_limit:
            self.limit_num = self.upper_limit         # 如果limit 大于设置的上限， 则limit 等于上限

        try:
            while True:
                self.logger.info('start {}'.format(start_num))
                self.start_time = int(time.time())                                                      # 调度开始的时间戳

                try:
                    self.redis_client = initializer.Initializer(self.logger).init_redis(self.redis_config)  #

                    max_queue_len = self.redis_client.hmget(constants.QUEUE_LENGTH_LIMIT_MAP, self.redis_key_todo)[0]
                    max_queue_len = (max_queue_len and int(max_queue_len)) or 0
                    allowed_redis_used_memory = int(6.5 * 1024 * 1024 * 1024)

                    while True:
                        # 只有redis使用的内存小于6.5G，才调度数据(全量内存是8G)
                        queue_len = self.redis_client.llen(self.redis_key_todo)
                        redis_used_memory = int(self.redis_client.info()['used_memory'])
                        if self.queue_length_limit and ((max_queue_len and queue_len > max_queue_len)
                                                        or (redis_used_memory > allowed_redis_used_memory)):
                            self.logger.warning(f'种子调度受限，queue_len={queue_len}, max_queue_len={max_queue_len}, '
                                                f'redis_used_memory={redis_used_memory}, '
                                                f'allowed_redis_used_memory={allowed_redis_used_memory}')
                            time.sleep(5)
                        else:
                            break

                    # 获取数据
                    df = self.mysql_client.read_df(sql=sql.format(start_num=start_num,
                                                                  limit_num=self.limit_num))
                    seed_count = int(df.shape[0])  # 当前数据量
                    self.logger.info(f'种子数据提取成功，length={seed_count}')

                    max_id = None
                    if seed_count > 0:
                        if scheduler_type == 'USE_ID':
                            max_id = df['id'].max()
                            df.drop(columns=['id'], inplace=True)

                        # 插入redis队列
                        self.schedule(df=df, position=self.position, version=version)

                    df = None

                    time.sleep(5)
                    # 平滑调度实现
                    while True:
                        # 时间间隔大于设定值，跳出循环
                        if int(time.time()) - self.start_time >= self.delay_time:
                            break
                        else:
                            time.sleep(5)

                except Exception as e:
                    self.logger.error(e)
                    time.sleep(5)
                    continue

                # 判断是否通过id获取范围内数据
                if scheduler_type == 'USE_ID':
                    if max_id:
                        start_num = max_id
                else:
                    start_num += self.limit_num                                                             # 更新偏移值

                queue_length += seed_count                                                              # 总数据量
                if (seed_count < self.limit_num) or (queue_length >= self.upper_limit) or (hasattr(self, 'section') and self.section):
                    # 如果提取的数据量小于设定值或者，提取量大于等于提取上限，则退出调度
                    break

            if self.monitor_spider is not None:
                self.monitor_spider.create(version=version, queue_length=int(queue_length))

            content = f'{self.redis_key_todo}:: 总长度：{queue_length}'
            self.logger.info(content)
            if self.seed_mail:
                mail.Mail(receivers=receivers).send_mail(subject='Success Scheduler', content=content)

            # 调度记录存入mysql
            # self.mysql_client.write(f'INSERT INTO `monitor_spider_scheduler` '
            #                         f'(`project_name`, `spider_name`, `redis_name`, `redis_len`, `version`) '
            #                         f'values ("{self.project_name}", "{self.scheduler_name}", '
            #                         f'"{self.redis_key_todo}", {queue_length}, "{version}")')
            return queue_length
        except:
            import traceback
            self.logger.info(traceback.format_exc())
            mail.Mail(receivers=receivers).send_mail(
                subject='Failed Scheduler', content=f'{self.redis_key_todo}\t {traceback.format_exc()}')
            return False

    def execute_select_sql(self, sql):
        """
        :param sql: 需要执行的sql
        :return: 返回结果集
        """
        df = self.mysql_client.read_df(sql)  # 获取数据
        if df is None or df is False:
            return json.loads(df)
        df_json = df.to_json(orient='records')
        return df_json

    def get_post_seeds_num(self, scheduler_info):
        """获取待上传种子数

        :param scheduler_info: 调度信息
        :return:
        """
        try:
            if scheduler_info.get('post_seed_num_sql'):
                post_seed_num = scheduler_info['post_seed_num_sql']
                self.logger.info(f'应调度种子SQL: {post_seed_num}')
                seeds_num = self.mysql_client.read_as_dict(post_seed_num)
                if seeds_num:
                    seeds_num = list(seeds_num[0].values())
                    return seeds_num if seeds_num else None
                else:
                    return None
            elif scheduler_info['sql'].find('from') != -1:
                index_sql = scheduler_info['sql'][scheduler_info['sql'].find('from'):]
            elif scheduler_info['sql'].find('FROM') != -1:
                index_sql = scheduler_info['sql'][scheduler_info['sql'].find('FROM'):]
            else:
                index_sql = ''
            if index_sql:
                post_seed_num = scheduler_info['sql_count'] + ' ' + index_sql
                self.logger.info(f'应调度种子SQL: {post_seed_num}')
                seeds_num = self.mysql_client.read_as_dict(post_seed_num)
                if seeds_num:
                    seeds_num = list(seeds_num[0].values())
                    return seeds_num if seeds_num else None
                else:
                    return None
            else:
                return None
        except Exception as e:
            self.logger.error('统计种子调度出错:' + str(e))
            return None

    def store_seeds_num(self, key, value, key_name='(tb)statistics_taobao_spider'):
        """保存待上传种子数

        :param key: key后缀
        :param value: 待上传种子数
        :param key_name: hash_name
        :return:
        """
        if not key:
            raise Exception('统计名称的key不存在')
        try:
            if datetime.datetime.now().strftime("%Y-%m-%d") != self.log_time:
                self.log_time = datetime.datetime.now().strftime("%Y-%m-%d")
                self.init_spider_log(format_key=1)
            key_name = key_name + f'({datetime.datetime.now().strftime("%Y-%m-%d")})'
            now_value = self.redis_client.hgetall(key_name)
            now_value = bytes_to_string(now_value).get(self.seed_log_name + key)
            if not now_value:
                now_value = 0
            self.redis_client.hset(name=key_name,
                                      key=self.seed_log_name + key, value=int(value) + int(now_value))
        except Exception as e:
            self.logger.exception(e)
            return self.store_seeds_num(key, value, key_name)
        self.logger.info('统计报表上传种子数成功')
        return True

    def check_use_id(self, sql_t: str = None):
        if not sql_t:
            return None
        temp_sql = sql_t.replace(' ', '')
        if 'id>{start_num}' in temp_sql or '`id`>{start_num}' in temp_sql:
            return 'USE_ID'
        else:
            return None
