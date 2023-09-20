# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: scheduler.py
@time: 2021-04-21 16:22
@desc:
"""
import json

from library.base_class.scheduler import BaseScheduler
from library.config import scheduler_info
from library.constants import Position
from library.util import get_version, get_time

version = get_version()

class ItemDetailScheduler(BaseScheduler):
    def __init__(self):
        super(ItemDetailScheduler, self).__init__()
        self.project_name = "JD"
        self.scheduler_name = "item_comment"
        self.redis_key_todo = "(jd)list_item_comment:TODO"
        self.monitor_redis = False
        self.position = scheduler_info['position']


def main():
    scheduler = ItemDetailScheduler()
    # 先调度淘宝的
    sql = 'select sku_id from jd_sku_list'
    scheduler.execute(sql=sql, version=version)

if __name__ == '__main__':
    main()