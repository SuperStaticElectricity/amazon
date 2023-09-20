# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: config.py
@time: 2021-04-07 18:03
@desc: 通用配置
"""
import logging

# 一键切换环境。0: 测试环境、1: 正式环境
ENVIRONMENT = 0

log_config = {
    'format': '%(asctime)s %(levelname)s [%(filename)s %(funcName)s line:%(lineno)d]: %(message)s',
    'level': logging.DEBUG,
    'datefmt': None,
    # 'filename': 'tb_spider.log',
}

redis_config = {
    'host': '' if ENVIRONMENT else '127.0.0.1',
    'port': 6379 if ENVIRONMENT else 6379,
    'db': 1 if ENVIRONMENT else 0,
    'password': '' if ENVIRONMENT else '',
    'socket_timeout': 600,
}

redis_config_proxy = {
    'host': '' if ENVIRONMENT else '127.0.0.1',
    'port': 6379 if ENVIRONMENT else 6379,
    'db': 1 if ENVIRONMENT else 0,
    'password': '' if ENVIRONMENT else '',
    'socket_timeout': 600,
}

mysql_config = {
    'host': 'cdb-50gx67c8.bj.tencentcdb.com' if ENVIRONMENT else '127.0.0.1',
    'user': 'root' if ENVIRONMENT else 'root',
    'passwd': '!gaolizeng6179GLZ@=' if ENVIRONMENT else '123456',
    'db': 'bxt' if ENVIRONMENT else 'bxt',
    'port': 10144 if ENVIRONMENT else 3306,
    'charset': 'utf8mb4' if ENVIRONMENT else 'utf8mb4',
    'autocommit': True
}


mail_config = {
    'host': 'smtp.exmail.qq.com',
    'port': 465,
    'user': 'gaolizeng@bxtdata.com',
    'password': 'glz6179GLZ'
}

scheduler_info = {
    'start_num': 0,  # 提取开始位置
    'limit_num': 200000,  # 一次最大的提取量
    'upper_limit': float('Inf'),  # 总提取上限
    'num': 1,  # 调度次数
    'delay_time': 0,  # 调度延时时间
    'seed_mail': False,  # 是否发送邮件
    'redis_key_todo': None,  # 队列名称
    'delete_redis': False,  # 是否删除队列
    'position': 'LEFT',  # 插入方向
    'sql': None  # sql语句
}
