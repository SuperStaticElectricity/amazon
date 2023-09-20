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
from urllib.parse import urlencode

import requests

from library.constants import Method, Position
from library.base_class.spider import BaseSpider
from library.decorators import log, check_execute
from library.initializer.mysql_pool_client import MysqlPoolClient
from library.initializer import initializer
from library.config import redis_config, mysql_config
from library.util import md5hex

mysql_client = MysqlPoolClient(**mysql_config)
redis_client = initializer.Initializer().init_redis(redis_config)  # redis客户端

REDIS_KEY_TODO = "(jd)list_item_comment:TODO"

INTERVAL_SECS = 1



class ItemReviewSpider(BaseSpider, threading.Thread):

    def __init__(self):
        super(ItemReviewSpider, self).__init__(redis_client=redis_client, mysql_client=mysql_client)
        threading.Thread.__init__(self)
        self.project_name = "JD"  # 项目名字
        self.spider_name = "item_review"  # 爬虫名字
        self.redis_key_todo = REDIS_KEY_TODO

    def spider_config(self):
        page = self.seed.get('page', 1)
        url = "https://wq.jd.com/commodity/comment/getcommentlist"
        params = {
            'callback': 'skuJDEvalA',
            'version': 'v2', 'pagesize': '10',
            'skucomment': '1',
            'sceneval': '2', 'score': '0', 'sku': '3311987',
            'sorttype': '5', 'page': str(page), 't': str(random.random())
        }
        headers = {
            'Host': 'wq.jd.com', 'Connection': 'keep-alive',
            'sec-ch-ua': '"Google Chrome";v="89", "Chromium";v="89", ";Not"A\\Brand";v="99"', 'DNT': '1',
            'sec-ch-ua-mobile': '?1',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Mobile Safari/537.36',
            'Accept': '*/*', 'Sec-Fetch-Site': 'same-site', 'Sec-Fetch-Mode': 'no-cors', 'Sec-Fetch-Dest': 'script',
            'Referer': 'https://item.m.jd.com/', 'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-AU,en;q=0.9',
        }
        proxyMeta = "http://27.155.223.174:4510"
        proxies = {
            "http": proxyMeta,
            "https": proxyMeta,
        }
        for i in range(3):
            try:
                res = requests.get(url, params, headers=headers, verify=False, proxies=proxies, timeout=10)
                txt = res.text
                data_str = re.findall("skuJDEvalA\((.+)\)", txt, re.S)[0]
                # print(data_str.replace(r'\x', ''))
                data = json.loads(data_str.replace(r'\x', ''))
                return data
            except:
                time.sleep(3)
        return False

    @log()
    def parse(self, data):
        data_list = []
        product_id = data["result"]["productCommentSummary"]["ProductId"]
        sku_id = data["result"]["productCommentSummary"]["SkuId"]
        comment_count = data["result"]["productCommentSummary"]["CommentCount"]
        default_good_count = data["result"]["productCommentSummary"]["DefaultGoodCount"]
        comments = data["result"]["comments"]
        for comment in comments:
            r_id = comment["id"]
            guid = comment["guid"]
            creationTime = comment["creationTime"]
            referenceId = comment["referenceId"]
            replyCount = comment["replyCount"]
            score = comment["score"]
            nickname = comment["nickname"]
            productColor = comment["productColor"]
            content = comment["content"]
            created_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            updated_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            try:
                data_list.append(
                    {
                        'product_id': product_id,
                        'sku_id': sku_id,
                        'comment_count': comment_count,
                        'default_good_count': default_good_count,
                        'r_id': r_id,
                        'guid': guid,
                        'creationTime': creationTime,
                        'referenceId': referenceId,
                        'replyCount': replyCount,
                        'score': score,
                        'nickname': nickname,
                        'productColor': productColor,
                        'content': content,
                        'created_at': created_at,
                        'updated_at': updated_at,
                    }
                )
            except Exception as e:
                self.logger.error("解析出错：" + str(e))
            return data_list

    @check_execute("JD", "item_comment", ['gaolizeng@bxtdata.com'])
    def execute(self, seed: dict = None):
        self.seed = self.get_seed(seed=seed)  # 提取种子
        if not self.seed:
            return
        self.logger.info(self.seed)  # 打印种子
        data = self.spider_config()  # 配置爬虫信息
        data_list = self.parse(data)  # 解析
        status = self.insert_or_update(data_list=data_list, table_name="jd_review")
        self.set_seed(status=status, position=Position.RIGHT, seed=self.seed)  # 处理种子
        return self.status

    def run(self, seed=None):
        while True:
            self.execute(seed)
            if INTERVAL_SECS > 0:
                time.sleep(INTERVAL_SECS)


def main():
    # seed = {
    #     "shop_id": 10026473320097,
    #     "version": "20210427 18:20"
    # }
    start_type = int(os.getenv('START_TYPE', 1))  # 启动类型 0单进程 1多进程 2线程
    start_num = int(os.getenv('START_NUM', 2))  # 启动进线程数 默认为10

    if start_type == 0:
        spider = ItemReviewSpider()
        spider.run()
    elif start_type == 1:
        spider = ItemReviewSpider()
        for i in range(start_num):
            p = mp.Process(target=spider.run, args=())
            p.start()
    elif start_type == 2:
        for i in range(start_num):
            spider = ItemReviewSpider()
            spider.start()


if __name__ == '__main__':
    main()
