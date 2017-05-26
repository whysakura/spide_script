# -*- coding: utf-8 -*-
# @Time    : 2017/5/23 14:33
# @Author  : wrd
import os

import pymysql

main_logging_filename = os.path.join((os.path.dirname(__file__)), "logger",'baidu.log')
base_uk = 2164327417
consume_sleeptime = 0
product_sleeptime = 5
maxsize = 3000
share_url = 'http://pan.baidu.com/pcloud/feed/getsharelist?auth_type={0}&start={1}&limit={2}&query_uk={3}'
follow_url = 'http://pan.baidu.com/pcloud/friend/getfollowlist?start={0}&limit={1}&query_uk={2}'
fan_url = 'http://pan.baidu.com/pcloud/friend/getfanslist?start={0}&limit={1}&query_uk={2}'
# share_url_bak = 'http://pan.baidu.com/pcloud/feed/getsharelist?auth_type=1&query_uk=2164327417&limit=60&start=0'
# follow_url_bak = 'http://pan.baidu.com/pcloud/friend/getfollowlist?query_uk=2164327417&limit=24&start=0'
# fan_url_bak = 'http://pan.baidu.com/pcloud/friend/getfanslist?query_uk=2164327417&limit=24&start=0'

producer_process = 1
producer_logging_filename = os.path.join((os.path.dirname(__file__)), "logger",'producer.log')


http_config = {
    "headers": {
        'accept': "application/json, text/javascript, */*; q=0.01",
        'accept-encoding': "gzip, deflate, sdch",
        'accept-language': "zh-CN,zh;q=0.8",
        'connection': "keep-alive",
        'host': "pan.baidu.com",
        'referer': "http://pan.baidu.com/share/home?uk=2084488645&third=1&view=share",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36",
        'x-requested-with': "XMLHttpRequest",
        'cache-control': "no-cache",
    },
    # "proxy_host":'http://124.251.42.254',
    # "proxy_port": 3128
}
mysql_config = {
    'host': '120.26.214.19',
    'port': 3306,
    'user': 'root',
    'password': 'putao1234',
    'db': 'pt_db',
    'charset': 'utf8',
    'cursorclass': pymysql.cursors.DictCursor
}
redis_conf = {
    'host': '45.76.187.121',
    'port': 6379,
    'password': 'w1314921',
    'db': 1,
    'decode_responses': True
}

def get_http_config():
    return http_config


# spide_ip   的配置

test_url='http://1212.ip138.com/ic.asp'
first_url = 'http://www.xicidaili.com/nn/'
second_url = 'http://www.coobobo.com/free-http-proxy/'
third_url = 'http://www.httpdaili.com/mfdl/'
ip_logging_filename = os.path.join((os.path.dirname(__file__)), "logger",'ips.log')
chkip_logging_filename = os.path.join((os.path.dirname(__file__)), "logger",'check_ips.log')
page_num = 4 # 取5页
sleep_tims = 1200 # 睡眠300s
check_process = 3
ip_http_config = {
    "headers": {
        'accept': "application/json, text/javascript, */*; q=0.01",
        'accept-encoding': "gzip, deflate, sdch",
        'accept-language': "zh-CN,zh;q=0.8",
        'connection': "keep-alive",
        'host': "www.xicidaili.com",
        'referer': "www.xicidaili.com",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36",
        'x-requested-with': "XMLHttpRequest",
        'cache-control': "no-cache",
    },
}


def get_ip_http_config():
    return ip_http_config


sql_logging_filename = os.path.join((os.path.dirname(__file__)), "logger",'query_sql.log')