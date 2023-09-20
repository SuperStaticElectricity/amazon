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
from library.util import get_version

version = get_version()

class ItemDetailScheduler(BaseScheduler):

    def __init__(self):
        super(ItemDetailScheduler, self).__init__()
        self.project_name = "JD"
        self.scheduler_name = "item_sku"
        self.redis_key_todo = "(jd)list_item_sku:TODO"
        self.monitor_redis = False
        self.position = scheduler_info['position']


def main():
    scheduler = ItemDetailScheduler()
    sql = 'select item_id from jd_items'
    scheduler.execute(sql=sql, version=version)

if __name__ == '__main__':
    main()