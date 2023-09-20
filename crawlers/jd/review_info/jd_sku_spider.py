# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: spider.py
@time: 2021-04-27 18:24
@desc:
"""
import datetime
import os
import json
import random
import re
import threading
import multiprocessing as mp
import time
import requests
from library.constants import Position
from library.base_class.spider import BaseSpider
from library.decorators import log, check_execute
from library.initializer.mysql_pool_client import MysqlPoolClient
from library.initializer import initializer
from library.config import redis_config, mysql_config
mysql_client = MysqlPoolClient(**mysql_config)
redis_client = initializer.Initializer().init_redis(redis_config)
proxy_redis_client = initializer.Initializer().init_redis({
    'host': '114.55.84.165',
    'port': 6379,
    'db': 0,
    'password': 'FNldn.dgdj,gDN,34md',
    'socket_timeout': 600,
})

REDIS_KEY_TODO = "(jd)list_item_sku:TODO"

INTERVAL_SECS = 1


class SkuCommentSpider(BaseSpider, threading.Thread):
    def __init__(self):
        super(SkuCommentSpider, self).__init__(redis_client=redis_client, mysql_client=mysql_client)
        threading.Thread.__init__(self)
        self.project_name = "JD"  # 项目名字
        self.spider_name = "item_sku"  # 爬虫名字
        self.redis_key_todo = REDIS_KEY_TODO
        self.proxyMeta = None

    def spider_config(self):
        sku_id = self.seed['item_id']
        print(sku_id)
        url = "https://item.m.jd.com/item/mview2"
        headers = {
            'Host': 'item.m.jd.com',
            'Referer': f'https://item.m.jd.com/product/{str(sku_id)}.html',
            'sec-ch-ua': '"Google Chrome";v="89", "Chromium";v="89", ";Not"A\\Brand";v="99"', 'sec-ch-ua-mobile': '?1',
            'Sec-Fetch-Dest': 'script', 'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Mobile Safari/537.36'
        }

        params = {
            'datatype': '1', 'callback': 'skuInfoCBA',
            'cgi_source': 'mitem', 'sku': str(sku_id),
            't': str(random.random()),
            '_fd': 'jdm'
        }
        if not self.proxyMeta:
            self.proxyMeta = proxy_redis_client.srandmember('proxy:new_ip_list').decode("utf-8")
        username = "leiyong"
        password = "w9ut6z3t"
        proxies = {
            "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password,
                                                            "proxy": self.proxyMeta},
            "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password,
                                                             "proxy": self.proxyMeta}
        }
        for i in range(3):
            try:
                res = requests.get(url, params=params, headers=headers, verify=False, proxies=proxies, timeout=10)
                txt = res.text
                data_str = re.findall("skuInfoCBA\((.+)\)", txt, re.S)[0]
                data = json.loads(data_str.replace(r'\x', ''))
                return data
            except Exception as e:
                self.logger.info("请求错误：" + str(e))
                time.sleep(1)
                try:
                    self.proxyMeta = proxy_redis_client.srandmember('proxy:new_ip_list').decode("utf-8")
                except Exception as e:
                    self.logger.info("提取代理失败：" + str(e))
        return False

    @log()
    def parse(self, data):
        data_list = []
        sku_list = data['item']['newColorSize']
        sku_ids = []
        for i in sku_list:
            sku_ids.append(i["skuId"])
        sku_ids.sort()
        for sku in sku_list:
            sku_id = sku["skuId"]
            from_sku_id = self.seed["item_id"]
            created_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            updated_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            try:
                data_list.append(
                    {
                        'sku_id': sku_id,
                        'from_sku_id': from_sku_id,
                        'data': json.dumps(sku, ensure_ascii=False),
                        'sku_list': json.dumps(sku_ids),
                        'created_at': created_at,
                        'updated_at': updated_at,
                    }
                )
            except Exception as e:
                self.logger.error("解析出错：" + str(e))
            return data_list

    @check_execute("JD", "item_sku", ['gaolizeng@bxtdata.com'])
    def execute(self, seed: dict = None):
        self.seed = self.get_seed(seed=seed)  # 提取种子
        if not self.seed:
            return
        self.logger.info(self.seed)  # 打印种子
        data = self.spider_config()  # 配置爬虫信息
        data_list = self.parse(data)  # 解析
        status = self.insert_or_update(data_list=data_list, table_name="jd_sku_list")
        self.set_seed(status=status, position=Position.LEFT, seed=self.seed)  # 处理种子
        return self.status

    def run(self, seed=None):
        while True:
            self.execute(seed)
            # if INTERVAL_SECS > 0:
            #     time.sleep(INTERVAL_SECS)

def main():
    # seed = {
    #     "shop_id": 10026473320097,
    #     "version": "20210427 18:20"
    # }
    start_type = int(os.getenv('START_TYPE', 2))  # 启动类型 0单进程 1多进程 2线程
    start_num = int(os.getenv('START_NUM', 8))  # 启动进线程数 默认为10

    if start_type == 0:
        spider = SkuCommentSpider()
        spider.run()
    elif start_type == 1:
        spider = SkuCommentSpider()
        for i in range(start_num):
            p = mp.Process(target=spider.run, args=())
            p.start()
    elif start_type == 2:
        for i in range(start_num):
            spider = SkuCommentSpider()
            spider.start()


if __name__ == '__main__':
    main()