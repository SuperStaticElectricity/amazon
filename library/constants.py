# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: constants.py
@time: 2021-04-07 11:12
@desc:
"""

# 种子队列长度现在
QUEUE_LENGTH_LIMIT_MAP = '(queue_config)queue_length_limit_map'
QUEUE_LENGTH_LIMIT = 'QUEUE_LENGTH_LIMIT'

class Position:
    LEFT = 'LEFT',
    RIGHT = 'RIGHT'


class Method:
    GET = 'GET'
    POST = 'POST'


class UserAgent:
    PC_CHROME = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                'Chrome/69.0.3497.100 Safari/537.36'
    PC_FIREFOX = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:62.0) Gecko/20100101 Firefox/62.0'
    ANDROID_CHROME = 'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 ' \
                     '(KHTML, like Gecko) Chrome/69.0.3497.100 Mobile Safari/537.36'
    ANDROID_FIREFOX = 'Mozilla/5.0 (Linux; Android 7.0; SM-G892A Build/NRD90M; wv) AppleWebKit/537.36 ' \
                      '(KHTML, like Gecko) Version/4.0 Chrome/67.0.3396.87 Mobile Safari/537.36'


UserAgentAndroid = [
    'Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 950) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Mobile Safari/537.36 Edge/14.14263',
    # 'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 8.0; Pixel 2 Build/OPD3.170816.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; U; Android 8.1.0; zh-cn; Redmi 6 Pro Build/OPM1.171019.019) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/61.0.3163.128 Mobile Safari/537.36 XiaoMi/MiuiBrowser/10.1.2'

]
UserAgentIOS = [
    'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1'

]

UserAgents = """Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60
Opera/8.0 (Windows NT 5.1; U; en)
Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50
Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 9.50
Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0
Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36
Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11
Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36
Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER
Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER) 
Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)" 
Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)
Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E) 
Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0
Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0) 
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.3.4000 Chrome/30.0.1599.101 Safari/537.36
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36""".split('\n')


class SleepTime:
    # 待爬取队列为空时，睡眠时间
    EMPTY_SEED_SLEEP_SECONDS = 10

    # ip为空
    EMPTY_IP_SLEEP_SECONDS = 10

    # 出现异常睡眠时间
    EXC_SLEEP_SECONDS = 1

    # 店铺属性页的爬取时间间隔
    SHOP_RATE_SLEEP_SECONDS = 0.1

    # 店铺下商品列表页的爬取时间间隔
    SHOP_ITEMS_SLEEP_SECONDS = 0.1

    # 监控器的监控时间间隔
    MONITOR_SLEEP_SECONDS = 60

    EXIT_SLEEP_SECONDS = 35

    # http状态码=429休眠时间
    HTTP_CODE_429 = 30

    # 频率限制后休眠时间
    RATE_LIMIT = 30


class CrawlStatus:
    # 访问的url返回不是200
    HTTP_ERROR = "HTTP_ERROR"

    # 没有解析到关键的字段
    PARSER_ERROR = "PARSER_ERROR"

    # 写数据库错误
    DB_ERROR = "DB_ERROR"

    # 成功
    SUCCESS = "SUCCESS"

    # 失败
    FAILED = "FAILED"


# 邮件组
MailRECEIVERS = ['gaolizeng@bxtdata.com']


class MailReceiver:
    GAOLIZENG = 'gaolizeng@bxtdata.com'


class DataResult:
    SUCCESS = 'success'
    FAILED = 'failed'
    RETRY = 'retry'


class TimeFMT:
    """时间格式"""
    TFM_DATE = '%Y-%m-%d'
    TFM_HOUR = '%Y-%m-%d %H'
    TFM_MINUTE = '%Y-%m-%d %H:%M'
    FTM_SECOND = '%Y-%m-%d %H:%M:%S'


# 清除selenium标志
Injected_JS = """
Object.defineProperty(navigator, 'languages', {
  get: function() {
    return ['zh-cn', 'en'];
  },
});

Object.defineProperty(navigator, 'webdriver', {
  get: function() {
    return null;
  },
});

Object.defineProperty(navigator, 'plugins', {
  get: function() {
    // this just needs to have `length > 0`, but we could mock the plugins too
    return [1, 2, 3, 4, 5];
  },
});
"""
