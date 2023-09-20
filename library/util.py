# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: util.py
@time: 2021-04-07 11:13
@desc:
"""
import json
import time
import base64
import execjs
import random
import hashlib
import string
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from library.constants import TimeFMT, Injected_JS


def random_list(length=11, array=string.digits):
    """列表随机
    以列表为数据源，随机生成length长度的新字符串
    """
    if not isinstance(array, str):  # 防止join时引发TypeError
        array = [str(elem) for elem in array]
    return "".join(random.choices(array, k=length))


def get_version():
    """ 以当前时间戳字符串，作为数据版本号 """
    return time.strftime("%Y%m%d %H:%M", time.localtime())


def get_unix_time(unx):
    """传入指定unix时间戳，格式化输出时间"""
    time_local = time.localtime(unx)
    return time.strftime("%Y-%m-%d %H:%M:%S", time_local)


def sleep(seconds):
    """ 休息一会 """
    r = max(random.random(), 0.5)
    time.sleep(r * seconds)


def get_time(fmt: str = TimeFMT.TFM_MINUTE, **time_type):
    """
    特定时间转字符串
    :param fmt: 时间格式
    :param time_type: 时间加减 {"days": 1}: 日期加一天
    :return: 特定格式的时间字符串
    """
    now_time = datetime.now()
    time_str = (now_time + timedelta(**time_type)).strftime(fmt)
    return time_str


def tolist(x):
    """
    将元素x转换为列表。
    :param x: any, 元素
    :return:
        - None, 如果x是空类型（None/0/[]/{}/""）
        - x, 如果x是list/tuple
        - [x], 其余情况
    """
    if not x:
        return None
    if isinstance(x, (tuple, list)):
        return x
    return [x]



def get_insert_sql(db, table, df):
    """ 根据database、table和DataFrame结构，自动生成insert-sql """
    columns = ','.join(df.columns)
    fmt = ','.join(['%s'] * len(df.columns))
    sql = "INSERT INTO `{}`.`{}` ({}) VALUES({})".format(db, table, columns, fmt)
    return sql


def join(sep="; ", **kwargs):
    buff = list()
    for key in kwargs:
        value = kwargs[key]
        buff.append("{}={}".format(key, value))
    return sep.join(buff)


def parse_int(x):
    try:
        return int(x)
    except (ValueError, TypeError):
        return 0


def bytes_to_string(data):
    """
    bytes 转 string，支持str、dict、list、tuple类型
    :param data:
    :return:
    """
    if isinstance(data, bytes):
        return data.decode('ascii')
    if isinstance(data, dict):
        return dict(map(bytes_to_string, data.items()))
    if isinstance(data, list):
        return list(map(bytes_to_string, data))
    if isinstance(data, tuple):
        return map(bytes_to_string, data)
    return data


def df_to_html(caption, df):
    caption = '<caption>{}</caption>'.format(caption)
    html = ['<table border=1 style="font-family:Arial">\n', caption, '<tr>']
    for column in df.columns:
        html.append('<th>' + column + '</th>')
    html.append('</tr>\n')
    for row in df.values:
        tr = ['<tr>']
        for col in row:
            tr.append('<td>' + str(col) + '</td>')
        tr.append('</tr>\n')
        html.append(''.join(tr))
    html.append('</table>\n')
    return ''.join(html)


def dict_to_html(caption, datas: list):
    caption = '<caption>{}</caption>'.format(caption)
    html = ['<table border=1 style="font-family:Arial">\n', caption, '<tr>']
    for column in datas[0].keys():
        html.append('<th>' + column + '</th>')
    html.append('</tr>\n')
    for data in datas:
        tr = ['<tr>']
        for row in data.values():
            tr.append('<td>' + str(row) + '</td>')
        tr.append('</tr>\n')
        html.append(''.join(tr))
    html.append('</table>\n')
    return ''.join(html)


def dict_to_values(params):
    """将字典转化为sql可读的形式"""
    values = list()
    for k, v in params.items():
        s = '`' + k + '`='
        if type(v) == str:
            v = v.replace('"', '\\"')
            s += '"' + v + '"'
        else:
            s += str(v)
        values.append(s)
    values = ','.join(values)
    return values


def get_part_data(fields: list = None, data_list: list = None):
    """
    从list获取指定数据
    :param fields:
    :param data_list:
    :return:
    """
    if not data_list:
        return data_list

    data_list_temp = list()
    for data in data_list:
        data_temp = dict()
        for field in fields:
            data_temp[field] = data[field]
        data_list_temp.append(data_temp)
    return data_list_temp


def delete_part_data(fields: list = None, data_list: list = None):
    """
    从list删除指定数据
    :param fields:
    :param data_list:
    :return:
    """
    if not data_list:
        return data_list

    data_list_temp = list()
    for data in data_list:
        data_temp = data.copy()
        for field in fields:
            data_temp.pop(field)
        data_list_temp.append(data_temp)
    return data_list_temp


def timer(timer_position: str = ""):
    def get_params(func):
        def save_run_time(*args, **kwargs):
            time_start = time.time()
            date = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            try:
                res = func(*args, **kwargs)
                if res:
                    flag = '成功'
                else:
                    flag = '失败'
                cost_time = time.time() - time_start
                # print(f'{date}:统计位置：{timer_position},消耗时长:{cost_time},结果：{flag}\n')
                return res
            except:
                cost_time = time.time() - time_start
                print(
                    f"{date}-----错误发生位置：" + timer_position + f"消耗时长{cost_time}" + "----具体内容如下：\n" + traceback.format_exc() + "\n")

        return save_run_time

    return get_params


@timer('md5')
def md5hex(word):
    """ MD5加密算法，返回32位小写16进制符号 """
    if isinstance(word, str):
        word = word.encode("utf-8")
    elif not isinstance(word, str):
        word = str(word)
    m = hashlib.md5()
    m.update(word)
    return m.hexdigest()


def format_headers(headers_str):
    """
    str to dict
    :param headers_str: headers的字符串形式
    :return: headers的dict形式
    """
    headers = dict()
    headers_list = headers_str.split('\n')
    for header in headers_list:
        header = header.strip()
        if header == '':
            continue
        header_list = header.split(': ')
        if len(header_list) == 1:
            headers[header_list[0].replace(':', '')] = ''
        elif len(header_list) == 2:
            headers[header_list[0]] = header_list[1]
        else:
            raise Exception('headers 解析出错')
    return headers


def format_redis(redis_queue):
    """
    redis 队列从TODO队列变成DOING
    :param redis_queue:
    :return:
    """
    return redis_queue.replace(')list_', ')hash_').replace(':TODO', ':DOING')


def format_time(time_data, fmt: str = TimeFMT.FTM_SECOND, format_type: int = 1):
    """
    时间戳和字符串转换
    :param time_data: 时间数据, 时间戳精确到ms
    :param fmt: 格式
    :param format_type: 1: 时间戳 to 日期、0: 日期 to 时间戳
    :return:
    """
    return datetime.fromtimestamp(time_data / 1000).strftime(fmt) if format_type == 1 else int(
        time.mktime(time.strptime(time_data, fmt)) * 1000)


def get_cookie(url, ip_data: dict = None, pc: bool = True):
    """
    通过selenium 获取cookie
    :param url:
    :param ip_data:
     :param pc:
    :return:
    """
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        if pc is False:
            # h5页面
            chrome_options.add_argument(
                'user-agent="Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1"')
        driver = webdriver.Chrome(chrome_options=chrome_options)
        driver.get(url=url)
        driver.execute_script(Injected_JS)  # 清除selenium标记
        time.sleep(1)
        driver.refresh()  # 刷新页面
        time.sleep(1)

        cookie = dict()
        for item in driver.get_cookies():
            cookie[item['name']] = item['value']

        driver.quit()
        return cookie
    except:
        return False


def get_token(cookie):
    """
    获取cookie的token
    :param cookie:
    :return:
    """
    try:
        cookie_dict = json.loads(cookie)
        _token = cookie_dict.get('_m_h5_tk').split('_')[0]
        return _token
    except:
        return False


def exec_js_function(js_code, function_name: str = None, *args, **kwargs):
    """
    python 运行JS代码
    :param js_code: js代码
    :param function_name: js的函数名字
    :param args: 参数
    :param kwargs: 参数
    :return:
    """
    ctx = execjs.compile(js_code)  # 编译代码
    return ctx.call(function_name, *args, **kwargs)  # 执行代码


def parse_url(url: str = ''):
    """
    解析url
    :param url: 目标url
    :return: dict()
    """
    if not url:
        return url
    url_parse_data = dict()
    try:
        split_data = url.split('?', maxsplit=1)
        url_parse_data['host'] = split_data[0]
        if len(split_data) == 2:
            for data in split_data[1].split('&'):
                parse_data = data.split('=')
                if len(parse_data) == 2:
                    url_parse_data[parse_data[0]] = parse_data[1]
        return url_parse_data
    except:
        return False


def pop_redis_list(redis_client: None, key: str, position: str = 'RIGHT', size: int = 10):
    """
    从redis队列中提取数据
    :param redis_client: redis链接对象
    :param key：redis key名称
    :param position: 提取方向，RIGHT/LEFT
    :param size: 提取长度
    :return:
    """

    if not redis_client:
        raise ValueError('redis_client must be set.')
    if not key:
        raise ValueError('redis key must be set.')

    pipeline = redis_client.pipeline()
    pipeline.multi()

    if position == 'LEFT':
        pipeline.lrange(key, start=0, end=size - 1)
        pipeline.ltrim(key, start=size, end=-1)
    else:
        pipeline.lrange(key, start=-size, end=-1)
        pipeline.ltrim(key, start=0, end=-size - 1)

    data_list = pipeline.execute()[0]  # 获取lrange的结果

    try:
        return [json.loads(s) for s in data_list]
    except json.decoder.JSONDecodeError:
        return data_list


def schedule_thread_seed(logger: None,
                         redis_client: None,
                         redis_key: str = None,
                         position: str = 'RIGHT',
                         thread_shared_list: list = None,
                         thread_num: int = 100,
                         sleep_sec: float = 0.1):
    """
    多线程种子调度
    :param redis_client: redis链接对象
    :param redis_key：redis key名称
    :param position: 提取方向，RIGHT/LEFT
    :param thread_shared_list: 线程共享的内存种子队列
    :param thread_num: 消费线程数
    :param sleep_sec: 消费速度跟不上时的睡眠秒数
    :return:
    """

    if not redis_client:
        raise ValueError('redis_client must be set.')

    if not isinstance(thread_shared_list, list):
        raise ValueError('thread_shared_seeds_queue must be set.')

    try:
        no_seed_count = 0
        while True:
            if len(thread_shared_list) >= thread_num:
                time.sleep(sleep_sec)
                continue
            data_list = pop_redis_list(redis_client=redis_client,
                                       key=redis_key,
                                       position=position,
                                       size=thread_num)
            if not data_list:
                if no_seed_count < 10:
                    no_seed_count += 1
                time.sleep(sleep_sec * no_seed_count)
                continue
            no_seed_count = 0

            if position == 'RIGHT':
                data_list.reverse()
            thread_shared_list.extend(data_list)

            if logger:
                logger.info(f'从Redis读取{len(data_list)}个种子，共享种子队列长度：{len(thread_shared_list)}')

    except Exception as e:
        # 发生异常时将取出的数据放回原队列
        if len(thread_shared_list) > 0:
            for seed in thread_shared_list:
                if position == 'RIGHT':
                    redis_client.rpush(redis_key, json.dumps(seed))
                else:
                    redis_client.lpush(redis_key, json.dumps(seed))
        raise e


def format_key(back_data: dict):
    """
    格式化返回给后端的数据key为小驼峰命名(变量名不统一的情况无法解决)
    :param back_data: 需要格式化的数据
    :return: 格式化好的数据
    """
    _item_data = dict()
    for key, values in back_data.copy().items():
        new_name = ''
        name_list = key.split("_")
        for name in name_list:
            if new_name == '':
                new_name += name
                continue
            new_name += name.capitalize()
        _item_data[new_name] = back_data.get(key)
    return _item_data


def urlsafe_decode(s):
    """
    url进行base64 编解码
    :param s:
    :return:
    """
    decode_str = base64.urlsafe_b64decode(s=s).decode()
    return decode_str


def time_parser(time_str):
    """对于类似 '2018-09-25T00:06:24.544Z' 的时间解析，解析为时间戳"""
    if not time_str:
        return
    _time = time.strptime(time_str[:-6], '%Y-%m-%dT%H:%M:%S')
    _time = int(time.mktime(_time))
    return _time
