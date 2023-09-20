'''
Author: Doller
Date: 2022-08-03 09:56:21
LastEditors: VsCode
LastEditTime: 2022-09-18 21:00:25
Description: 代理工具
'''
import json
import random
import telnetlib
from datetime import datetime

import requests


def getIp(count):
    url = "http://api.shenlongip.com/ip"
    params = {"key":"1yoiwuzf","count":count,"pattern":"json"}
    res = requests.get(url,params)
    if res.status_code == 200:
        data = res.json()
        if data["code"] == 200:
            return True,data
        else:
            return False,data
    else:
        return False,None


def maintainProxy(count):
    flag,res = getIp(count)
    if flag:
        proxy_list = []
        data = res["data"]
        for item in data:
            ip = item["ip"]
            port = item["port"]
            proxy_list.append(ip+":"+str(port))
        with open("proxy.json","w",encoding='utf-8') as f:
            f.truncate()
            f.write(json.dumps(proxy_list,ensure_ascii=False))
        return True,f"添加代理成功 添加数量:{count}"
    else:
        return False,"获取代理出错了"
        

def getProxies():
    # 私密代理
    with open("proxy.json","r") as f:
        data = json.loads(f.read())
    proxy_ip = random.choice(data)
    user = "mliuup"
    password = "tkx3wfxb"
    proxies = {
        "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": user, "pwd": password, "proxy": proxy_ip},
        "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": user, "pwd": password, "proxy": proxy_ip}
    }

    return proxies,proxy_ip.split(":")[0],proxy_ip.split(":")[-1]


def testProxy(ip,port):
    try:
        telnetlib.Telnet(ip,port,timeout=2)
        return True
    except:
        return False


if __name__ == "__main__":
    f,r = maintainProxy(50)
    print(r)

# def dail():
#     # 私密代理
#     conn = OPRedis()
#     proxy_ip = conn.randomOneIp('proxy:new_ip_list_2')
#     print("this is request ip:" + proxy_ip)
#     tid = "leiyong"
#     password = "w9ut6z3t"
#     proxies = {
#         "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": tid, "pwd": password, "proxy": proxy_ip},
#         "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": tid, "pwd": password, "proxy": proxy_ip}
#     }

#     return proxies