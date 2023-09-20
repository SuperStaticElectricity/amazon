# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: decorators.py
@time: 2021-04-07 11:13
@desc: 爬虫邮件日志发送器
"""
import os
import sys
import json
import functools
import retrying

from library import log

PROJECT_NAME = 'gaolizeng'
LOGHUB_PROJECT = 'data-infra'
LOGHUB_LOG_STORE = 'spider_mail_log'
LOGHUB_TOPIC = 'mail_log'

if hasattr(sys, '_getframe'):
    currentframe = lambda: sys._getframe(3)
else:  # pragma: no cover
    def currentframe():
        """Return the frame object for the caller's stack frame."""
        try:
            raise Exception
        except Exception:
            return sys.exc_info()[2].tb_frame.f_back


class MailLevel:
    WARN = 'WARN'
    INFO = 'INFO'
    ERROR = 'ERROR'


def retry(retries=3, min_wait_secs=0, max_wait_secs=1):
    """函数重试器

    Args:
        retries: 重试次数，默认3次
        min_wait_secs: 随机等待时间最小值（秒）
        max_wait_secs: 随机等待时间最大值（秒）
    Returns:
        被装饰函数的返回结果
    """

    def decorator(func):
        @retrying.retry(stop_max_attempt_number=retries,
                        wait_random_min=min_wait_secs,
                        wait_random_max=max_wait_secs)
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator


class MailLogger(object):
    def __init__(self, logger: None,  mail_receivers: str = None, program_name: str = None):
        """
        Args:
            logger: 日志对象
            mail_receivers: 邮件接收者邮箱，逗号分隔
            program_name: 程序名
        """
        self.logger = logger or log.Log().get_logger()
        self.mail_receivers = mail_receivers
        self.program_name = program_name

    @retry()
    def send(self, receivers: str = None, subject: str = None, content: str = None, level: str = MailLevel.ERROR):
        """发送邮件日志

        Args:
            receivers: 邮件接收者邮箱，逗号分隔
            subject: 邮件主题
            content: 邮件内容
            level: 邮件等级，ERROR,WARN,INFO
        Returns:
            返回成功或失败
        """

        if not receivers and not self.mail_receivers:
            self.logger.warn('mail receiver must be set.')
            return False

        if not subject:
            self.logger.warn('mail subject must be set.')
            return False
        return True

    def send_warning(self, subject, content, receivers: str = None):
        self.send(receivers, subject, content, MailLevel.WARN)

    def send_info(self, subject, content, receivers: str = None):
        self.send(receivers, subject, content, MailLevel.INFO)

    def send_error(self, subject, content, receivers: str = None):
        self.send(receivers, subject, content, MailLevel.ERROR)

    def find_caller(self):
        """
        Find the stack frame of the caller so that we can note the source file name.
        """
        filename = ''
        try:
            f = currentframe()

            if f is None:
                return filename
            f = f.f_back

            while hasattr(f, "f_code"):
                co = f.f_code
                filename = os.path.normcase(co.co_filename)
                if filename == __file__ or 'retrying' in filename or 'decorator' in filename:
                    f = f.f_back
                    continue
                break
        except Exception as e:
            self.logger.exception(e)

        return filename
