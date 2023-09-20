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
import os
import json
import random
import re
import threading
import multiprocessing as mp
import time
import traceback
from urllib.parse import urlencode, unquote

import requests

from library.constants import Method, Position
from library.base_class.spider import BaseSpider
from library.decorators import log, check_execute
from library.initializer.mysql_pool_client import MysqlPoolClient
from library.initializer import initializer
from library.config import redis_config, mysql_config, redis_config_proxy
from pyquery import PyQuery as pq

REDIS_KEY_TODO = "(amazon)list_jiaju_item_detail:TODO"
INTERVAL_SECS = 0


class CcItemDetailSpider(BaseSpider, threading.Thread):
    def __init__(self):
        mysql_client = MysqlPoolClient(**mysql_config)
        redis_client = initializer.Initializer().init_redis(redis_config)  # redis客户端
        super(CcItemDetailSpider, self).__init__(redis_client=redis_client, mysql_client=mysql_client)
        threading.Thread.__init__(self)
        self.proxy_redis_client = initializer.Initializer().init_redis(redis_config_proxy)
        self.cookies = {}

        self.project_name = "AMAZON"  # 项目名字
        self.spider_name = "jiaju_item_detail"  # 爬虫名字
        self.redis_key_todo = REDIS_KEY_TODO
        self.proxyMeta = None

    def spider_config(self):
        try:
            url = self.seed["url"]
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
                # 'referer': url,
                'accept-language': 'en-AU,en-US;q=0.9,en;q=0.8,zh-CN;q=0.7,zh;q=0.6,ja;q=0.5,es-ES;q=0.4,es;q=0.3,zh-TW;q=0.2',
                'cookie': 'session-id=137-7422431-6191628; ubid-main=135-8815662-3097007; s_fid=3FD2AB5DD249C6FE-391E1932C971FB39; aws-target-data=%7B%22support%22%3A%221%22%7D; aws-target-visitor-id=1620282206080-379780.38_0; s_nr=1622804396495-New; s_vnum=2054804396496%26vn%3D1; s_dslv=1622804396497; session-id-time=2082787201l; regStatus=registering; AMCV_7742037254C95E840A4C98A6%40AdobeOrg=1585540135%7CMCIDTS%7C18978%7CMCMID%7C25488945325638593851824560463516745215%7CMCAAMLH-1640258575%7C3%7CMCAAMB-1640258575%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1639660975s%7CNONE%7CMCAID%7C3049C5AF28FEE1C8-400000C1B0E9EBB3%7CvVersion%7C4.4.0; i18n-prefs=USD; lc-main=en_US; skin=noskin; session-token=2dvcdsRFni8s4Yl9aThzB0Dt13EYzQURhsxmKMVKbSZ1fzIsguC0Di0s9N3RmyKreaD9UpN1hHLah+xYoGVLMcYAecchDA1Nah9HdAmU1BnYxjazPIPNAwCGeCO9FUyrpyU2hGSHpbHjt3kAsXlkBXXewEia4Ms1p6yKNZEpEUQV6ZFnA2afGUeNiGF/DHqq; csm-hit=tb:9F6QW1QE96TTQH719143+sa-KJQH44WA39Z5QVD2N0N2-BZC7135G29G6QYXPQ461|1646817377702&t:1646817377702&adb:adblk_no',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
            }

            for i in range(1):
                try:
                    res = requests.get(url, headers=headers, proxies=proxies, verify=False, timeout=5)
                    txt = res.text
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
            print(e)
            return False

    @log()
    def parse(self, txt):
        if not txt:
            return False
        data_list = []

        try:
            product_id = self.seed['product_id']
            # print(txt)
            doc = pq(txt)
            name = doc("#productTitle").text()
            price = doc("#priceblock_saleprice").text()
            if not price:
                price = doc("#corePriceDisplay_desktop_feature_div > div.a-section.a-spacing-none.aok-align-center > span.a-price.aok-align-center.reinventPricePriceToPayPadding.priceToPay > span:nth-child(2)").text()
            if not price:
                price = doc("#corePrice_desktop > div > table > tbody > tr:nth-child(2) > td.a-span12 > span.a-price.a-text-price.a-size-medium.apexPriceToPay > span:nth-child(2)").text()
            if not price:
                price = doc("#corePrice_desktop > div > table > tr > td> span.a-price.a-text-price.a-size-medium.apexPriceToPay").text()
            if not price:
                price = doc("#corePriceDisplay_desktop_feature_div > div").text()
            if not price:
                price = doc("#corePrice_feature_div > div").text()
            if not price:
                price = doc("priceblock_ourprice").text()
            if not price:
                price = doc("#price_inside_buybox").text()
            price = price.replace("$", "")
            stock = doc("#availability").text()
            if not stock:
                stock = doc("#exports_desktop_outOfStock_buybox_message_feature_div > div").text()
                if not stock:
                    stock = doc("#exports_desktop_outOfStock_buybox_message_feature_div > div > span").text()
            comment_count = doc(
                "#reviewsMedley > div > div.a-fixed-left-grid-col.a-col-left > div.a-section.a-spacing-none.a-spacing-top-mini.cr-widget-ACR > div.a-row.a-spacing-medium.averageStarRatingNumerical").text()
            try:
                sku_info_list = re.findall('"variationValues" : (.*?)},', txt)[0]+"}"
                sku_info = sku_info_list
            except:
                sku_info = ""
            try:
                img_list = json.loads(re.findall("'initial':(.+)},", txt)[0])
            except:
                img_list = []
            img = []
            for img_one in img_list:
                img.append(img_one.get("hiRes"))
            img = json.dumps(img, ensure_ascii=False)
            size_list = doc("#variation_flavor_name > ul > li").items()
            size = []
            for size_one in size_list:
                size.append(size_one("span").text())
            size = json.dumps(size, ensure_ascii=False)
            item_about = doc("#feature-bullets > ul").text()
            created_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            updated_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            keyword = self.seed["keyword"]
            data_list.append({
                'keyword': keyword,
                'name': name,
                'price': price,
                'stock': stock,
                'comment_count': comment_count,
                'sku_info': sku_info,
                'size': size,
                'img': img,
                'item_about': item_about,
                'product_id': product_id,
                'created_at': created_at,
                'updated_at': updated_at,
            })
        except Exception as e:
            print(traceback.format_exc())
            print(e)
            return False
        return data_list

    @check_execute("DY", "jiaju_item_detail", ['gaolizeng@bxtdata.com'])
    def execute(self, seed: dict = None):
        self.seed = self.get_seed(seed=seed)  # 提取种子
        if not self.seed:
            print("没有种子")
            time.sleep(10)
            return
        self.logger.info(self.seed)  # 打印种子
        data = self.spider_config()  # 配置爬虫信息
        data_list = self.parse(data)  # 解析
        print(data_list)
        status = self.insert_or_update(data_list=data_list, table_name="bxt.jiaju_amazon_item_detail")
        self.set_seed(status=status, position=Position.LEFT, seed=self.seed)  # 处理种子
        return status

    def run(self, seed=None):
        # while True:
        for i in range(30000):
            self.execute(seed)
            if INTERVAL_SECS > 0:
                print("sleep")
                time.sleep(INTERVAL_SECS)
            break


def main():
    seed = {'product_id': 'B0089ME1K6', 'keyword': '实木床', 'url': 'https://www.amazon.com/Milliard-Ventilated-Removable-Waterproof-65-Percent/dp/B0089ME1K6/ref=sr_1_13?crid=WNX5M1B8WLSR&keywords=%E5%AE%9E%E6%9C%A8%E5%BA%8A&qid=1646820258&refresh=1&sprefix=shi%2Caps%2C2773&sr=8-13', 'version': '20220309 19:32'}

    start_type = int(os.getenv('START_TYPE', 0))  # 启动类型 0单进程 1多进程 2线程
    start_num = int(os.getenv('START_NUM', 5))  # 启动进线程数 默认为10

    if start_type == 0:
        spider = CcItemDetailSpider()
        spider.run()
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
