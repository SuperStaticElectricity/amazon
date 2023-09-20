# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: cc_item_detail.py
@time: 2021-10-21 15:51
@desc:
"""

import datetime
import json
import multiprocessing as mp
import os
import random
import re
import threading
import time
import traceback
from urllib.parse import quote, urlencode

import requests
from pyquery import PyQuery as pq

from library.base_class.spider import BaseSpider
from library.config import mysql_config, redis_config
from library.constants import Method, Position
from library.decorators import check_execute, log
from library.initializer import initializer
from library.initializer.mysql_pool_client import MysqlPoolClient

REDIS_KEY_TODO = "(amazon)list_jiaju_item_review:TODO"
INTERVAL_SECS = 0


class CcItemDetailSpider(BaseSpider, threading.Thread):
    def __init__(self):
        mysql_client = MysqlPoolClient(**mysql_config)
        redis_client = initializer.Initializer().init_redis(redis_config)  # redis客户端
        super(CcItemDetailSpider, self).__init__(redis_client=redis_client, mysql_client=mysql_client)
        threading.Thread.__init__(self)
        self.proxy_redis_client = initializer.Initializer().init_redis(redis_config)
        self.cookies = {}

        self.project_name = "AMAZON"  # 项目名字
        self.spider_name = "jiaju_item_review"  # 爬虫名字
        self.redis_key_todo = REDIS_KEY_TODO
        self.proxyMeta = None

    def spider_config(self):
        try:
            name = self.seed["name"]
            product_id = self.seed["product_id"]
            page = self.seed["page"]
            # https://www.amazon.com/DESINO-Shaped-Computer-Wirting-Workstation/product-reviews/B086HBM2HR/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews
            url = f"https://www.amazon.com/{quote(name).replace(' ','-')}/product-reviews/{product_id}/ref=cm_cr_dp_d_show_all_btm"
            params = {
                "ie": "UTF8",
                "reviewerType": "all_reviews",
                "pageNumber": page
            }

            if not self.proxyMeta:
                self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
            username = "leiyong"
            password = "w9ut6z3t"
            proxies = {
                "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password,
                                                                "proxy": self.proxyMeta},
                "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password,
                                                                 "proxy": self.proxyMeta}
            }

            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-AU,en-US;q=0.9,en;q=0.8,zh-CN;q=0.7,zh;q=0.6,ja;q=0.5,es-ES;q=0.4,es;q=0.3,zh-TW;q=0.2',
                'cache-control': 'max-age=0',
                'cookie': 'session-id=137-2476878-1126147; i18n-prefs=USD; ubid-main=135-6672969-0145109; lc-main=en_US; session-id-time=2082787201l; session-token="zwxugqsHdHrC8MWSSFgC1tlNocsbLhP2sLT6v3CJ/Hy485MduADKhh3pnnFfhm9Ofvl5TgvIypqbSPiliX+n934PBTfGUFeDrJOI8hUf/5G05EndBYbOQjH9Ah+be4XABF01ZrPOfrnm27d5ihJv5KqyWcEptvuSItRQhUyOWcXsNR0LSKV0MKHjYNzhMG/+iW2Jwo3n+tKknBdu8SONFQqiSvZT9q1zdO+MBYYLuno="; sp-cdn="L5Z9:CN"; csm-hit=tb:s-FT0MQHD4F10A2J106Q80|1692530508112&t:1692530508112&adb:adblk_no',
                'dnt': '1', 'downlink': '1.4', 'ect': '3g',
                'referer': url,
                'rtt': '400', 'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
                'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate', 'sec-fetch-site': 'same-origin', 'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'}

            print(url)
            for i in range(1):
                try:
                    res = requests.get(url, params, headers=headers, proxies=proxies, verify=False, timeout=5)
                    txt = res.text
                    # print(txt)
                    try:
                        self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
                    except:
                        self.proxy_redis_client = initializer.Initializer().init_redis(redis_config)
                        self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
                    return txt
                except Exception as e:
                    print(e)
                    time.sleep(3)
                    try:
                        self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
                    except:
                        self.proxy_redis_client = initializer.Initializer().init_redis(redis_config)
                        self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
            return False
        except Exception as e:
            try:
                self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
            except:
                self.proxy_redis_client = initializer.Initializer().init_redis(redis_config)
                self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
            print(e)
            return False

    @log()
    def parse(self, txt):
        if not txt:
            return False
        data_list = []

        try:
            product_id = self.seed['product_id']
            name = self.seed['name']
            page = int(self.seed['page'])
            # print(txt)
            doc = pq(txt)
            review_list = doc("#cm_cr-review_list > div").items()
            for review in review_list:
                # try:
                #div:nth-child(1) > div > div.a-profile-content > span
                # div.genome-widget-row > div.profile-widget-with-avatar > div > div > div.a-profile-content > span
                custom_name = review("div div.a-profile-content > span").text()
                ##customer_review_foreign-RG9YMLC4L24OV > div.a-row.a-spacing-none > i
                score_star = review("div.a-row.a-spacing-none > i > span").text()
                #       div.a-row.a-spacing-none > span.a-size-base.review-title.a-color-base.review-title-content.a-text-bold > span.cr-original-review-content
                briefly = review(
                    ".a-row.a-spacing-none > .a-size-base.review-title.a-color-base.review-title-content.a-text-bold > .cr-original-review-content").text()
                ##customer_review-R195EUBBRLYQ83 > span > font > font
                ##customer_review_foreign-RG9YMLC4L24OV > span
                date_review = review("span.a-size-base.a-color-secondary.review-date").text()
                spec = review("div.a-row.a-spacing-mini.review-data.review-format-strip > a").text()
                content = review("div.a-row.a-spacing-small.review-data").text()
                ##customer_review_foreign-R23DKS7M94GXJ5 > div:nth-child(6) > span.cr-vote > div > span
                is_helpful = review("div.a-row  > span.cr-vote > div > span").text()
                created_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                updated_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                data_list.append({
                    'product_id': product_id,
                    'product_name': name,
                    'custom_name': custom_name,
                    'score_star': score_star,
                    'briefly': briefly,
                    'date_review': date_review,
                    'spec': spec,
                    'content': content,
                    'is_helpful': is_helpful,
                    'created_at': created_at,
                    'updated_at': updated_at,
                })
                # except:
                #     continue
            if data_list:
                new_seed = {**self.seed, **{"page": page + 1}}
                self.push_seeds(seeds=new_seed)
        except Exception as e:
            print(traceback.format_exc())
            print(e)
            return False
        return data_list

    @check_execute("DY", "jiaju_item_detail", ['gaolizeng@bxtdata.com'])
    def execute(self, seed: dict = None):
        # self.seed = self.get_seed(seed=seed)  # 提取种子
        # if not self.seed:
        #     print("没有种子")
        #     time.sleep(10)
        #     return
        self.logger.info(seed)  # 打印种子
        data = self.spider_config()  # 配置爬虫信息
        data_list = self.parse(data)  # 解析
        print(data_list)
        status = self.insert_or_update(data_list=data_list, table_name="bxt.jiaju_amazon_item_review")
        self.set_seed(status=status, position=Position.LEFT, seed=self.seed)  # 处理种子
        return status

    def run(self, seed=None):
        # while True:
        for i in range(30000):
            self.execute(seed)
            if INTERVAL_SECS > 0:
                print("sleep")
                time.sleep(INTERVAL_SECS)
            # break


def main():
    seed = {'product_id': 'B0089ME1K6', 'keyword': '实木床',
            'url': 'https://www.amazon.com/Milliard-Ventilated-Removable-Waterproof-65-Percent/dp/B0089ME1K6/ref=sr_1_13?crid=WNX5M1B8WLSR&keywords=%E5%AE%9E%E6%9C%A8%E5%BA%8A&qid=1646820258&refresh=1&sprefix=shi%2Caps%2C2773&sr=8-13',
            'version': '20220309 19:32'}

    start_type = int(os.getenv('START_TYPE', 0))  # 启动类型 0单进程 1多进程 2线程
    start_num = int(os.getenv('START_NUM', 1))  # 启动进线程数 默认为10

    if start_type == 0:
        spider = CcItemDetailSpider()
        spider.run(seed)
    elif start_type == 1:
        spider = CcItemDetailSpider()
        for i in range(start_num):
            p = mp.Process(target=spider.run, args=())
            p.start()
    elif start_type == 2:
        for i in range(start_num):
            spider = CcItemDetailSpider()
            spider.start()


if __name__ == '__main__':
    # for i in range(3):
    #     p = mp.Process(target=main, args=())
    #     p.start()
    main()
