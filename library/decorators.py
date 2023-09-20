# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: decorators.py
@time: 2021-04-07 11:13
@desc:
"""
import time

import traceback

from inspect import isclass
from functools import wraps
from library.log import Log
from library.mail import Mail
from library import mail_log_util
from library.constants import MailReceiver

logger = Log().get_logger()


def retry(tries=3, delay=0, result_num=0):
    """
    重试函数。True/None: 不重试。False: 重试
    :param tries: int, 最大尝试次数, 而不是重试次数
    :param delay: int, 时间间隔(秒)
    :param result_num: 判断第几个结构
    :return: result: 函数运行结果
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = False
            for i in range(tries):
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, tuple) is False:
                        result_tuple = (result,)
                    else:
                        result_tuple = result

                    if result_tuple[result_num] is not False:           # result一定要是元祖(函数返回一个多个结构时，是以元祖的形式)
                        return result                                   # 如满足终止条件，则返回结果
                except Exception as e:
                    logger.exception(e)
                    return result
                if delay > 0:
                    time.sleep(delay)
            return result
        return wrapper
    return decorator


def log(result_num=0, mail_receivers=MailReceiver.GAOLIZENG):
    """
    日志打印。 True/data: 打印成功日志。False: 打印失败日志。 None: 不打印日志
    :param result_num: 判断第几个结构
    :return:
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = False
            # 2020-02-25添加:初始化报警参数
            mail_log = None
            log_mail_name = ''
            try:
                result = func(*args, **kwargs)
                if isinstance(result, tuple) is False:
                    result_tuple = (result,)
                else:
                    result_tuple = result

                if result_tuple[result_num]:                       # 根据函数返回结果，检查是否触发终止条件
                    logger.info('success:: ' + str(func.__name__))
                elif result_tuple[result_num] is False:
                    logger.warn('failed:: ' + str(func.__name__))
                elif result_tuple[result_num] == '':
                    if mail_log and args[0].except_info:
                        # 2020-02-25添加:异常信息保存到loghub
                        mail_log.send(subject='{}运行告警'.format(log_mail_name), content='{}'.format(args[0].except_info))
            except Exception as e:
                logger.exception(e)
                if mail_log:
                    # 2020-02-25添加:异常信息保存到loghub
                    mail_log.send(subject='{}运行异常'.format(log_mail_name), content='{}'.format(traceback.format_exc()))
                return result
            return result  # 如满足终止条件，则返回结果
        return wrapper
    return decorator


def check_execute(project_name: str=None, spider_name: str=None, receivers: list=None, sleep_time: list=[1, 10]):
    """
    监控爬虫执行函数，爬取成功/失败不延时、没有爬取延时15s、出现异常延时1s
    :param project_name: 工程名字，如：TB(淘宝)、JD(京东)
    :param spider_name: 爬虫名字，如：ItemDetail
    :param receivers: 邮件接受者。默认None
    :param sleep_time: 睡眠时间。默认sleep_time[0]: 失败休息时间、sleep_time[1]: 种子为空的休息时间
    :return:
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = False
            try:
                result = func(*args, **kwargs)
                if result is False and hasattr(args[0], 'seed_log') and args[0].seed_log:
                    is_fail_log = args[0].incr_spider_log(key='(FailNum)')    # 记录失败次数
                if result is None:
                    time.sleep(sleep_time[1])
            except KeyboardInterrupt and SystemExit:
                if hasattr(args[0], 'seed_log'):
                    is_fail_log = args[0].incr_spider_log(key='(FailNum)') if hasattr(args[0], 'seed_log') and args[0].seed_log else False  # 记录失败次数
            except Exception as e:
                logger.exception(e)
                is_fail_log = args[0].incr_spider_log(key='(FailNum)') if hasattr(args[0], 'seed_log') and args[0].seed_log else False  # 记录失败次数
                subject = f'{project_name} {spider_name} execute failed'
                content = f'异常信息:: {traceback.format_exc()}'             # 给邮件发送的异常信息包含具体行数
                if receivers:
                    Mail(receivers=receivers).send_mail(subject=subject, content=content)
                    time.sleep(600)
                time.sleep(sleep_time[0])
            finally:
                if result is not None and hasattr(args[0], 'seed_log') and args[0].seed_log:
                    args[0].incr_spider_log(key='(SpiderNum)')

            return result
        return wrapper
    return decorator


def instance(*args, **kwargs):
    def params_instance(cls):
        return cls(*args, **kwargs)  # 带参数的实例
    if args and isclass(args[0]):
        return args[0]()
    return params_instance
