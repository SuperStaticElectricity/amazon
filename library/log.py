# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: log.py
@time: 2021-04-07 11:13
@desc:
"""
import logging
from library import config


class Log(object):
    """
        Log Wrapper. Usage:
        >>> from library.log import Log
        >>> logger = Log(filename='app.log').get_logger()
        >>> logger.warn('This is warning')
        >>> logger.info('This is info')
    """
    def __init__(self, log_config: dict=None):
        if log_config is None:
            log_config = config.log_config
        logging.basicConfig(**log_config)

    @staticmethod
    def get_logger(log_name=None):
        logger = logging.getLogger(log_name)
        return logger
