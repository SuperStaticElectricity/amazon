# encoding: utf-8
"""
@author: fengyinws
@contact: fengyinws@163.com
@software: pycharm
@file: mail.py
@time: 2021-04-07 11:13
@desc:
"""
import smtplib
from email.utils import formataddr
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from library import log, util, constants

SENDER = 'gaolizeng@bxtdata.com'    # 发件人邮箱账号
PASSWORD = 'glz6179GLZ'           # 发件人邮箱密码
RECEIVERS = constants.MailRECEIVERS     # 收件人邮箱账号
MAIL_SERVER, MAIL_PORT = "smtp.exmail.qq.com", 465


class Mail(object):
    """
    Usage:
        >>> from library.mail import Mail
        >>> sender = Mail(receivers=['hehhpku@qq.com', 'huihui@zhiyitech.cn'])
        >>> sender.send_mail('title', 'content')
        >>> Mail().send_mail('title', 'content')
    """

    def __init__(self, nickname='spider', sender=SENDER, password=PASSWORD,
                 receivers=RECEIVERS, cc_receivers=None, logger=None, mail_prefix=''):
        """
        :param nickname: str, 发件人昵称
        :param sender: str, 发件人邮箱账号
        :param password: str, 发件人邮箱密码
        :param receivers: str or list, 收件人邮箱账号
        :param logger: object, 日志对象
        :param mail_prefix: str, 邮件标题前缀
        """
        self.nickname = nickname
        self.sender = sender
        self.password = password
        self.receivers = util.tolist(receivers)
        self.cc_receivers = util.tolist(cc_receivers)
        self.logger = log.Log().get_logger() if logger is None else logger
        self.mail_prefix = mail_prefix

    def send_mail(self, subject, content, subtype='html', charset='utf-8', file_path: str = None, filename: str = None):
        """
        发送邮件
        :param subject: str, 邮件标题
        :param content: str, 邮件内容
        :param subtype: str, 邮件格式（例如：plain/html/json等）
        :param charset: str, 邮件字符编码
        :param file_path: str, 附件路径
        :param filename: str, 附近名字(默认：路径后缀)
        :return: bool, 是否发送成功
        """
        try:
            if file_path:
                msg = MIMEMultipart()
                file = MIMEText(open(file_path, 'rb').read(), 'base64', 'utf-8')
                file["Content-Type"] = 'application/octet-stream'
                file["Content-Disposition"] = f'attachment; filename="{filename or file_path.split("/")[-1]}"'
                msg.attach(MIMEText(content, subtype, charset))
                msg.attach(file)
            else:
                msg = MIMEText(content, subtype, charset)
            msg['From'] = formataddr([self.nickname, self.sender])
            if self.receivers is not None:
                msg['To'] = ','.join(self.receivers)
            if self.cc_receivers is not None:
                msg['CC'] = ','.join(self.cc_receivers)
            msg['Subject'] = self.mail_prefix + subject

            # 企业邮箱发送邮件服务器 smtp.exmail.qq.com
            # qq邮箱发送邮件服务器  smtp.qq.com
            server = smtplib.SMTP_SSL(MAIL_SERVER, MAIL_PORT)
            server.login(self.sender, self.password)
            server.sendmail(self.sender, self.receivers, msg.as_string())
            server.quit()
        except Exception as e:
            self.logger.exception(e)
            return False
        return True

    def warn(self, subject, content):
        subject = '[WARN]' + subject
        self.send_mail(subject, content)

    def info(self, subject, content):
        subject = '[INFO]' + subject
        self.send_mail(subject, content)

    def error(self, subject, content):
        subject = '[ERROR]' + subject
        self.send_mail(subject, content)
