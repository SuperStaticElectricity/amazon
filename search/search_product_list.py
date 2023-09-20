# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: search_product_list1.py
@time: 2021-12-01 15:57
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
import traceback
from urllib.parse import urlencode
import requests
from library.base_class.spider import BaseSpider
from library.decorators import log, check_execute
from library.initializer.mysql_pool_client import MysqlPoolClient
from library.initializer import initializer
from library.config import redis_config, mysql_config, redis_config_proxy
from pyquery import PyQuery as pq

REDIS_KEY_TODO = "(amazon)list_amazon_search_product:TODO"
INTERVAL_SECS = 1


class SearchProductSpider(BaseSpider, threading.Thread):
    def __init__(self):
        mysql_client = MysqlPoolClient(**mysql_config)
        redis_client = initializer.Initializer().init_redis(redis_config)  # redis客户端
        super(SearchProductSpider, self).__init__(redis_client=redis_client, mysql_client=mysql_client)
        threading.Thread.__init__(self)
        self.proxy_redis_client = initializer.Initializer().init_redis(redis_config_proxy)
        self.cookies = {}
        self.seed_max_failed_count = 30
        self.project_name = "AMAZON"  # 项目名字
        self.spider_name = "amazon_search_product"  # 爬虫名字
        self.redis_key_todo = REDIS_KEY_TODO
        self.proxyMeta = None

    def spider_config(self):
        try:
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
            keyword = self.seed['keyword']
            page = self.seed['page']
            url = "https://www.amazon.com/s"
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'referer': url,
                'accept-language': 'en-AU,en-US;q=0.9,en;q=0.8,zh-CN;q=0.7,zh;q=0.6,ja;q=0.5,es-ES;q=0.4,es;q=0.3,zh-TW;q=0.2',
                'cookie': 'session-id=137-7422431-6191628; ubid-main=135-8815662-3097007; s_fid=3FD2AB5DD249C6FE-391E1932C971FB39; aws-target-data=%7B%22support%22%3A%221%22%7D; aws-target-visitor-id=1620282206080-379780.38_0; s_nr=1622804396495-New; s_vnum=2054804396496%26vn%3D1; s_dslv=1622804396497; session-id-time=2082787201l; regStatus=registering; AMCV_7742037254C95E840A4C98A6%40AdobeOrg=1585540135%7CMCIDTS%7C18978%7CMCMID%7C25488945325638593851824560463516745215%7CMCAAMLH-1640258575%7C3%7CMCAAMB-1640258575%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1639660975s%7CNONE%7CMCAID%7C3049C5AF28FEE1C8-400000C1B0E9EBB3%7CvVersion%7C4.4.0; i18n-prefs=USD; lc-main=en_US; skin=noskin; session-token=2dvcdsRFni8s4Yl9aThzB0Dt13EYzQURhsxmKMVKbSZ1fzIsguC0Di0s9N3RmyKreaD9UpN1hHLah+xYoGVLMcYAecchDA1Nah9HdAmU1BnYxjazPIPNAwCGeCO9FUyrpyU2hGSHpbHjt3kAsXlkBXXewEia4Ms1p6yKNZEpEUQV6ZFnA2afGUeNiGF/DHqq; csm-hit=tb:9F6QW1QE96TTQH719143+sa-KJQH44WA39Z5QVD2N0N2-BZC7135G29G6QYXPQ461|1646817377702&t:1646817377702&adb:adblk_no',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
            }
            if page ==1:
                params = {
                    'k': keyword,
                    # 'page': page,
                    'crid': 'WNX5M1B8WLSR',
                    'qid': str(int(time.time())),
                    'refresh': '1',
                    'sprefix': f'shi,aps,2773',
                    'ref': f'nb_sb_noss'
                }
            else:
                params = {
                    'k': keyword, 'page': page,
                    'crid': 'EBGHBRLOMRXP',
                    'qid': str(int(time.time())),
                    'refresh': '1',
                    # 'sprefix': '%E7%89%9B%E5%A5%B6%2Caps%2C548',
                    'ref': f'sr_pg_{str(page)}'
                }

            # proxies = {}
            for i in range(1):
                try:
                    res = requests.get(url ,params, headers=headers, proxies=proxies, verify=False, timeout=5)
                    txt = res.text
                    # print(txt)
                    try:
                        self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
                    except:
                        self.proxy_redis_client = initializer.Initializer().init_redis(redis_config_proxy)
                        self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
                    return txt
                except Exception as e:
                    print(e)
                    time.sleep(3)
                    try:
                        self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
                    except:
                        self.proxy_redis_client = initializer.Initializer().init_redis(redis_config_proxy)
                        self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
            return False
        except Exception as e:
            try:
                self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
            except:
                self.proxy_redis_client = initializer.Initializer().init_redis(redis_config_proxy)
                self.proxyMeta = self.proxy_redis_client.srandmember('proxy:new_ip_list_5m').decode("utf-8")
            print("请求失败", traceback.format_exc())
            return False

    @log()
    def parse(self, txt):
        try:
            if not txt:
                return False
            if "No results for" in txt:
                print("无结果")
                return []
            data_list = []
            doc = pq(txt)
            item_list = doc(
                "#search > div.s-desktop-width-max.s-desktop-content.s-opposite-dir.sg-row > div.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span:nth-child(4) > div.s-main-slot.s-result-list.s-search-results.sg-row > div").items()
            page = self.seed.get("page", 1)
            for item in item_list:
                try:
                    txt = item("div").text()
                    if "Best Seller" in txt:
                        is_best_sell = 1
                    else:
                        is_best_sell = 0
                    product_id = item("div").attr("data-asin")
                    if not product_id:
                        continue
                    data_uuid = item("div").attr("data-uuid")
                    name = item("div a > span").text()
                    # print(name)
                    url = "https://www.amazon.com" + item("div a").attr("href")
                    country = "美国站"
                    data_index = item("div").attr("data-index")
                    good_index = (page - 1) * 24 + int(data_index)
                    created_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    updated_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    data_list.append({
                        'keyword': self.seed.get("keyword"),
                        'name': name,
                        'url': url,
                        'product_id': product_id,
                        'data_uuid': data_uuid,
                        'is_best_sell': is_best_sell,
                        'country': country,
                        'good_index': good_index,
                        'created_at': created_at,
                        'updated_at': updated_at,
                    })
                except Exception as e:
                    print(e)
                    continue
            new_seed = {**self.seed, **{"page": page + 1}}
            self.push_seeds(seeds=new_seed)
            return data_list
        except Exception as e:
            print(txt)
            print(traceback.format_exc())
            print('parse：total', e)
            return False

    @check_execute("DY", "jiaju_search", ['gaolizeng@bxtdata.com'])
    def execute(self, seed: dict = None):
        self.seed = self.get_seed(seed=seed)  # 提取种子
        if not self.seed:
            print("没有种子")
            time.sleep(180)
            return
        self.logger.info(self.seed)  # 打印种子
        data = self.spider_config()  # 配置爬虫信息
        data_list = self.parse(data)  # 解析
        print(data_list)
        status = self.insert_or_update(data_list=data_list, table_name="bxt.jiaju_amazon_product_list")
        self.set_seed(status=status, seed=self.seed)  # 5
        return status

    def run(self, seed=None):
        while True:
        # for i in range(200):
            self.execute(seed)
            if INTERVAL_SECS > 0:
                print("sleep")
                time.sleep(INTERVAL_SECS)
            # break


def main():
    seed = {
        "sec_uid": "",
        "keyword": '罗永浩',
        "version": "20210818 18:20"
    }
    start_type = int(os.getenv('START_TYPE', 0))  # 启动类型 0单进程 1多进程 2线程
    start_num = int(os.getenv('START_NUM', 3))  # 启动进线程数 默认为10

    if start_type == 0:
        spider = SearchProductSpider()
        spider.run()
    elif start_type == 1:
        spider = SearchProductSpider()
        for i in range(start_num):
            p = mp.Process(target=spider.run, args=())
            p.start()
    elif start_type == 2:
        for i in range(start_num):
            spider = SearchProductSpider()
            spider.start()


if __name__ == '__main__':
    # for i in range(3):
    #     p = mp.Process(target=main, args=())
    #     p.start()
    main()
