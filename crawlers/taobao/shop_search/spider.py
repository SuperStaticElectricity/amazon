# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: spider.py
@time: 2021-04-21 15:59
@desc:
"""
import datetime
import json
import multiprocessing
import os
import time
from urllib.parse import quote, unquote
import requests
from crawlers.taobao.shop_search.lib.get_xmini_wua import get_mini_wua
from library.base_class.spider import BaseSpider
from library.config import mysql_config, redis_config
from library.constants import Position
from library.decorators import check_execute
from library.initializer import initializer
from library.initializer.mysql_pool_client import MysqlPoolClient

mysql_client = MysqlPoolClient(**mysql_config)
redis_client = initializer.Initializer().init_redis(redis_config)  # redis客户端
proxy_redis_client = initializer.Initializer().init_redis({
    'host': '114.55.84.165',
    'port': 6379,
    'db': 0,
    'password': 'FNldn.dgdj,gDN,34md',
    'socket_timeout': 600,
})


class ShopSearchSpider(BaseSpider):
    def __init__(self):
        super(ShopSearchSpider, self).__init__(redis_client=redis_client, mysql_client=mysql_client)
        self.project_name = "TB"  # 项目名字
        # self.redis_key_todo = RedisKey.LIST_ITEM_DETAIL_TODO
        self.redis_key_todo = "(tb)list_shop_search:TODO"  # 提取种子的队列
        self.spider_name = "shop_search"  # 爬虫名字
        self.proxyMeta = None
        # 记录爬取成功率
        if self.redis_key_todo == '(tb)list_item_detail:TODO':
            self.seed_log_name = self.project_name + self.spider_name
            self.seed_log = self.init_spider_log(self.seed_log_name)
            self.logger.info('爬取统计开启:' + self.seed_log_name)

    def get_xsign(self, api: str, api_v: str, uid, sid: str, app_key, ttid: str, features, utdid: str,
                  dev_id: str, t, data_str: str):
        url = "http://106.12.212.28:10018/get_xsign"
        data = {
            "api": api,
            "api_v": api_v,
            "uid": uid,
            "sid": sid,
            "app_key": app_key,
            "ttid": ttid,
            "features": features,
            "utdid": utdid,
            "dev_id": dev_id,
            "t": t,
            "data_str": data_str,
        }
        res = requests.post(url, data=json.dumps(data))
        return res.text

    def get_shop(self, seed):
        api = 'mtop.taobao.wsearch.appsearch'
        api_v = '1.0'
        uid = '2208977681596'
        sid = '1241f2e86b4c3008e9322d626e561dcd'
        umt = unquote('rtkApI5LPE8d9wJ47t8NdNMFfN9TLL7B')
        ttid = '1552981757356%40taobao_android_9.25.0'
        dev_id = 'AtQSbADm8Z-dVcrbCmuEvhseHYjYSwglwARc_sBADSBT'
        utdid = 'YAgx90ncwTIDADQRPZDwEBhL'
        features = '27'
        app_key = '21646297'
        time_now = str(time.time())[:10]
        data_str = '{"LBS":"{\\"SG_TMCS_1H_DS\\":\\"{\\\\\\"stores\\\\\\":[]}\\",\\"SG_TMCS_FRESH_MARKET\\":\\"{\\\\\\"stores\\\\\\":[]}\\",\\"TB\\":\\"{\\\\\\"stores\\\\\\":[{\\\\\\"code\\\\\\":\\\\\\"301831119\\\\\\",\\\\\\"bizType\\\\\\":\\\\\\"2\\\\\\",\\\\\\"type\\\\\\":\\\\\\"4\\\\\\"}]}\\",\\"TMALL_MARKET_B2C\\":\\"{\\\\\\"stores\\\\\\":[{\\\\\\"code\\\\\\":\\\\\\"110\\\\\\",\\\\\\"bizType\\\\\\":\\\\\\"REGION_TYPE_CITY\\\\\\",\\\\\\"addrId\\\\\\":\\\\\\"13276663872\\\\\\",\\\\\\"type\\\\\\":\\\\\\"CHOOSE_ADDR\\\\\\"},{\\\\\\"code\\\\\\":\\\\\\"107\\\\\\",\\\\\\"bizType\\\\\\":\\\\\\"REGION_TYPE_REGION\\\\\\",\\\\\\"addrId\\\\\\":\\\\\\"13276663872\\\\\\",\\\\\\"type\\\\\\":\\\\\\"CHOOSE_ADDR\\\\\\"}]}\\",\\"TMALL_MARKET_O2O\\":\\"{\\\\\\"stores\\\\\\":[{\\\\\\"code\\\\\\":\\\\\\"233930124\\\\\\",\\\\\\"bizType\\\\\\":\\\\\\"DELIVERY_TIME_ONE_HOUR\\\\\\",\\\\\\"addrId\\\\\\":\\\\\\"13276663872\\\\\\",\\\\\\"type\\\\\\":\\\\\\"CHOOSE_ADDR\\\\\\"},{\\\\\\"code\\\\\\":\\\\\\"236686073\\\\\\",\\\\\\"bizType\\\\\\":\\\\\\"DELIVERY_TIME_HALF_DAY\\\\\\",\\\\\\"addrId\\\\\\":\\\\\\"13276663872\\\\\\",\\\\\\"type\\\\\\":\\\\\\"CHOOSE_ADDR\\\\\\"}]}\\"}","active_bd":"1",' \
                   '"apptimestamp":"' + time_now + '","areaCode":"CN","brand":"OPPO","canP4pVideoPlay":"true","cityCode":"120100","countryNum":"156","device":"OPPO+R9m","editionCode":"CN","globalLbs":"{\\"biz_common\\":{\\"recommendedAddress\\":{\\"addressId\\":\\"13276663872\\",\\"area\\":\\"萧山区\\",\\"areaDivisionCode\\":\\"330109\\",\\"city\\":\\"杭州市\\",\\"cityDivisionCode\\":\\"330100\\",\\"detailText\\":\\"建设二路528号德圣博奥城6幢402\\",\\"lat\\":\\"30.201523\\",\\"lng\\":\\"120.245533\\",\\"province\\":\\"浙江省\\",\\"provinceDivisionCode\\":\\"330000\\",\\"town\\":\\"北干街道\\",\\"townDivisionCode\\":\\"330109002\\",\\"type\\":\\"deliver\\"}},\\"eleme\\":{\\"storeInfos\\":[{\\"storeId\\":\\"999\\"}]},\\"meeting_place\\":{},\\"on_time_promise\\":{\\"storeInfos\\":[{\\"storeId\\":\\"360544033\\"},{\\"storeId\\":\\"181544296\\"},{\\"storeId\\":\\"179030269\\"},{\\"storeId\\":\\"193672031\\"},{\\"storeId\\":\\"525864366\\"},{\\"storeId\\":\\"286121350\\"},{\\"storeId\\":\\"405405116\\"},{\\"storeId\\":\\"409288026\\"},{\\"storeId\\":\\"202150031\\"},{\\"storeId\\":\\"201185400\\"}]},\\"same_city_buy\\":{},\\"tmall_market_o2o\\":{\\"storeInfos\\":[{\\"storeId\\":\\"233930124\\"},{\\"storeId\\":\\"236686073\\"}]},\\"txd\\":{\\"storeInfos\\":[{\\"storeId\\":\\"414561076\\"}]}}",' \
                                                   '"hasPreposeFilter":"false","homePageVersion":"v7","imei":"862021036824277","imsi":"10988OPPOR94dc1","info":"wifi",' \
                                                   '"isBeta":"false","jarvis_model_version":"mainse_rerank_model.alinn:20210119","latitude":"39.113083","loc":"河南",' \
                                                   '"longitude":"117.216282","n":"10","network":"wifi","page":"' + str(
            '1') + '","pvFeature":"567378376493;640740363271;640740043614;634421567881;641446999348;640651883476",' \
                   '"q":"' + seed.get(
            'search_name') + '","rainbow":"12994,14957,14735,10287578,14393,14071,14960,12433650,14478",' \
                             '"shop_type":"taobao","sort":"common","style":"list","sversion":"12.1","tab":"shop","ttid":"' + ttid + '","utd_id":"' + utdid + '","vm":"nw"}'
        x_mini_wua = unquote(
            get_mini_wua()
        )
        xsign = self.get_xsign(api=api, api_v=api_v, uid=uid, sid=sid, app_key=app_key,
                               features=features, ttid=ttid, utdid=utdid, dev_id=dev_id,
                               t=time_now, data_str=data_str)
        headers = {
            'x-features': '27',
            # 'c-launch-info': '0,0,1618909192110,1618909058000,3',
            'x-page-name': 'com.taobao.android.detail.wrapper.activity.DetailActivity',
            'x-location': '117.216282%2C39.113083',
            'user-agent': 'MTOPSDK%2F3.1.1.7+%28Android%3B5.1%3BOPPO%3BOPPO+R9m%29',
            'x-ttid': quote(ttid),
            # 'cache-control': 'no-cache',
            'x-region-channel': 'CN',
            'x-appkey': app_key, 'x-nq': 'WIFI',
            'x-mini-wua': quote(x_mini_wua),
            'x-c-traceid': 'YAgx90ncwTIDADQRPZDwEBhK16189091921100207117537', 'x-app-conf-v': '0',
            'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'A-SLIDER-Q': 'appKey%3D21646297%26ver%3D1618904404325',
            'x-bx-version': '6.5.27', 'x-pv': '6.2',
            'x-t': time_now, 'x-app-ver': '9.25.0', 'f-refer': 'mtop',
            # 'Cookie': 'x5sec=7b226d746f703b32223a223437613139653564653866323863613566376239633264663837313365613963434e755468595147454f7a6e3663534d743976712b414561447a49794d4467354e7a63324f4445314f5459374d536941424443766863696442513d3d227d',
            'x-sid': sid, 'x-nettype': 'WIFI', 'x-social-attr': '3',
            'x-utdid': quote(utdid), 'x-umt': umt,
            'a-orange-dq': 'appKey=21646297&appVersion=9.25.0&clientAppIndexVersion=1120210420164703099',
            'x-devid': quote(dev_id),
            'x-sign': xsign,
            'x-page-url': 'http%3A%2F%2Fs.m.taobao.com%2Fh5', 'x-uid': uid, 'Host': 'guide-acs.m.taobao.com',
            # 'Accept-Encoding': 'gzip'
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
        url = f'https://acs.m.taobao.com/gw/{api}/{api_v}/?data={quote(data_str)}'
        try:
            res = requests.get(url, headers=headers, proxies=proxies, verify=False, timeout=15)
            print(res.text)
            print(res.headers)
            return res.text
        except Exception as e:
            self.logger.info("请求错误：" + str(e))
            print(self.proxyMeta)
            try:
                self.proxyMeta = proxy_redis_client.srandmember('proxy:new_ip_list').decode("utf-8")

            except Exception as e:
                self.logger.info("提取代理失败：" + str(e))
            time.sleep(1)
            return False


    def parse(self, response):
        if not response:
            return False
        try:
            data = json.loads(response)
            shopList = data["data"]["shopArray"]
            data_list = []
            totalPage = int(data['data'].get('totalPage', 1))
            print(totalPage)
            if totalPage > 1 and int(self.seed.get('page', 1)) == 1:
                seeds_list = []
                for n in range(2, totalPage+1):
                    seeds_list.append({**self.seed, **{"page": n}})
                # print(seeds_list)
                self.push_seeds(seeds=seeds_list, position=Position.RIGHT)
            for shop in shopList:
                created_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                updated_at = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                try:
                    data_list.append(
                        {
                            'shop_id': int(shop['shopId']),  # 商铺ID
                            'shop_name': shop["title"],  # 店铺名
                            'seller_id': int(shop['sellerId']),  # 旺旺ID
                            'shop_sold': shop.get('sold', ''),  # 商品热度
                            'search_name': self.seed.get('search_name', ''),  # 商品热度
                            'cate_name': self.seed.get('cate_name', ''),  # 商品热度
                            'shop_rank': (int(self.seed.get('page', 1)) - 1) * 10 + int(shopList.index(shop)) + 1,  # 商品热度
                            'auctionCount': shop.get('auctionCount', ''),  # 店铺商品数
                            'summaryTips': json.dumps(shop.get('summaryTips', []), ensure_ascii=False),  # 店铺描述
                            'logo': shop.get('logo', ''),  # 商品材质
                            'data_ori': json.dumps(shop, ensure_ascii=False),  # 标题
                            'created_at': created_at,
                            'updated_at': updated_at,
                        }
                    )
                except Exception as e:
                    self.logger.info(str(e) + json.dumps(shop, ensure_ascii=False))
            return data_list
        except Exception as e:
            self.logger.info("解析店铺出错：" + str(e))
            return False

    @check_execute("TB", "shop_search", ['gaolizeng@bxtdata.com'])
    def execute(self, seed: dict = None):
        self.seed = self.get_seed(seed=seed, position='left')  # 提取种子
        if not self.seed:
            self.logger.info("没有种子啊")  # 打印种子
            return
        self.logger.info(self.seed)  # 打印种子
        response = self.get_shop(self.seed)  # http请求验证
        data_list = self.parse(response=response)  # 解析
        print('data_list', data_list)
        # 处理店铺促销信息
        status = self.insert_or_update(data_list=data_list, table_name="tb_shop_info")
        print(status)
        self.set_seed(status=status, seed=self.seed, position='right')
        return self.status

    def run(self):
        # 多线程或多进程使用
        while True:
            self.execute()
            time.sleep(0.8)


def main():
    start_type = int(os.getenv('START_TYPE', 1))  # 启动类型 0单进程 1多进程 2多线程
    start_num = int(os.getenv('START_NUM', 2))  # 启动进线程数 默认为10
    if not start_type:
        # 正常使用
        spider = ShopSearchSpider()
        spider.run()
    else:
        if start_type == 1:
            # 多进程版
            spider = ShopSearchSpider()
            for process_num in range(start_num):
                tb_process = multiprocessing.Process(target=spider.run, args=())
                tb_process.start()


if __name__ == '__main__':
    main()
