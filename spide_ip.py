# -*- coding: utf-8 -*-
# Author:wrd
# 抓取页面代理ip
import json
import logging
import re

import time
from bs4 import BeautifulSoup
from tornado import (gen, ioloop)
from tornado.concurrent import Future
from setting import *
from spide_check import delete_proxies
from spide_check import insert_proxies
from spide_util import Spide, MyPyMysql, RedisPool, Logger
from spide_main import mysql_config, redis_conf

@gen.coroutine
def put_ip():
    pmysql = MyPyMysql(**mysql_config)
    sql = """SELECT proxy_host,proxy_port FROM pt_db.spide_proxies_ip order by rand();"""
    result = pmysql.query(sql)
    for i in result:
        r.rpush("proxy_ip_list", json.dumps(i))
    mylog.info('向proxy_ip_list加数据')
    pmysql.close_connect()
    if not result or result is None:
        mylog.info('数据库无代理IP...')
        yield get_first_proxy_data(page_n=2,if_proxy=True)


@gen.coroutine
def get_spide(url,if_proxy=False):
    """
    获取 能用的代理连接
    :return:
    """
    if if_proxy :
        httpconfigs = get_ip_http_config()
        local_ip_nums = r.get('use_local_ip_get_data')
        if int(local_ip_nums) < 10:
            response = yield Spide(url, **httpconfigs).async()
            r.incr('use_local_ip_get_data')
            mylog.info('本地连接获取数据.....{0}次'.format(r.get('use_local_ip_get_data')))
            raise gen.Return(response)
        else:
            # 如果使用自身IP请求过多就等待20分钟
            mylog.info('Wait 20分钟....')
            time.sleep(1200)
            r.decr('use_local_ip_get_data',2)
    delete_list = []
    rlist = r.lrange("proxy_ip_list",0,-1)
    for _ in range(10):
        try:
            if r.llen('proxy_ip_list') == 0:
                yield put_ip()
            i = json.loads(r.blpop("proxy_ip_list", timeout=200)[1])
            httpconfigs = get_ip_http_config()
            # httpconfigs['proxy_host'] = i['proxy_host']
            # httpconfigs['proxy_port'] = i['proxy_port']
            # response = yield Spide(url, **httpconfigs).async_proxy()
            response = yield Spide(url, **httpconfigs).async()
        except Exception as e:
            delete_list.append(i['proxy_host'])
            mylog.error(str(e))
            mylog.error('无法连接... ' + str(len(rlist)) + ' ' + str(i['proxy_host']))
        else:
            mylog.info('连接成功...' + str(len(rlist)) + ' ' + str(i['proxy_host']))
            # delete_proxies(delete_list)
            raise gen.Return(response)
    # delete_proxies(delete_list)  # 获取可用ip比较慢,所以暂时不删除


@gen.coroutine
def get_first_proxy_data(page_n=page_num,if_proxy=False):
    while True:
        try:
            for i in range(1,page_n):
                urls = first_url + str(i)
                response = yield get_spide(urls,if_proxy=if_proxy)
                soup = BeautifulSoup(response.body, 'lxml')
                taglist = soup.find_all('tr', attrs={'class': re.compile("(odd)|()")})
                mylog.info(taglist)
                if not taglist:
                    get_first_proxy_data()
                    raise gen.Return()
                for trtag in taglist:
                    tdlist = trtag.find_all('td')  # 在每个tr标签下,查找所有的td标签
                    proxies_list = {
                        'proxy_host':tdlist[1].string.strip().encode('utf-8'),  # 这里提取IP值
                        'proxy_port':tdlist[2].string.strip().encode('utf-8'),  # 这里提取端口值
                    }
                    r.rpush("proxy_check_ip_list", json.dumps(proxies_list))
                mylog.info(urls)
            yield gen.sleep(sleep_tims)
        except Exception as e:
            mylog.error(e.message)
        finally:
            # 如果是if_proxy 为true  那么就结束循环
            if if_proxy:
                raise gen.Return([])

@gen.coroutine
def get_second_proxy_data():
    while True:
        try:
            for i in range(1, page_num+1):
                urls = second_url + str(i)
                response = yield get_spide(urls)
                soup = BeautifulSoup(response.body, 'lxml')
                taglist = soup.find_all('tr')
                for trtag in taglist[1:]:
                    tdlist = trtag.find_all('td')  # 在每个tr标签下,查找所有的td标签
                    if tdlist:
                        ipc = re.findall(r'(\d+)',tdlist[0].text.strip())
                        ipc = '.'.join(ipc)
                        proxies_list = {
                            'proxy_host':ipc,  # 这里提取IP值
                            'proxy_port':tdlist[1].string.strip().encode('utf-8'),  # 这里提取端口值
                    }
                        r.rpush("proxy_check_ip_list", json.dumps(proxies_list))
                mylog.info(urls)
            yield gen.sleep(sleep_tims)
        except Exception as e:
            mylog.error(e.message)

@gen.coroutine
def get_third_proxy_data():
    while True:
        try:
            response = yield get_spide(third_url)
            soup = BeautifulSoup(response.body, 'lxml')
            taglist = soup.find_all('tr')
            for trtag in taglist[1:12]:
                tdlist = trtag.find_all('td')  # 在每个tr标签下,查找所有的td标签
                if tdlist and tdlist[2].text == u'匿名':
                    ipc = tdlist[0].text.strip()
                    proxies_list = {
                        'proxy_host':ipc,  # 这里提取IP值
                        'proxy_port':tdlist[1].string.strip().encode('utf-8'),  # 这里提取端口值
                    }
                    r.rpush("proxy_check_ip_list", json.dumps(proxies_list))
            mylog.info(third_url)
            yield gen.sleep(sleep_tims)
        except Exception as e:
            mylog.error(e.message)


@gen.coroutine
def main():
    get_first_proxy_data()
    get_second_proxy_data()
    get_third_proxy_data()
    yield Future()



if __name__ == '__main__':
    mylog = Logger(ip_logging_filename)
    mylog.info('获取代理ip....')
    r = RedisPool(**redis_conf).redis()
    r.set('use_local_ip_get_data', '0')
    ioloop.IOLoop.current().run_sync(main)