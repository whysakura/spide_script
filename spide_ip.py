# -*- coding: utf-8 -*-
# Author:wrd
# 抓取页面代理ip
import json
import sys
import os
import logging

import time
from random import choice
from datetime import timedelta

import pymysql
import re
from bs4 import BeautifulSoup
from tornado import (httpclient, gen, ioloop, queues)
from tornado.concurrent import Future
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from spide_util import Spide, MyPyMysql
from spide_main import mysql_config

test_url='http://1212.ip138.com/ic.asp'
first_url = 'http://www.xicidaili.com/nn/'
second_url = 'http://www.coobobo.com/free-http-proxy/'
third_url = 'http://www.httpdaili.com/mfdl/'
logging_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'spide_ip.log')
page_num = 4 # 取3页
sleep_tims = 100 # 睡眠5分钟

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
@gen.coroutine
def get_spide(url=test_url,if_test=True):
    """
    获取 能用的代理连接
    :return:
    """
    pmysql = MyPyMysql(**mysql_config)
    change_proxies = []
    for _ in range(10):
        if q.empty():
            proxy_ip_set.clear()
            logging.info('清空 proxy_ip_set')
            put_ip()
        try:
            j = q.get()
            i = j.result()
            http_config['proxy_host'] = i['proxy_host']
            http_config['proxy_port'] = i['proxy_port']
            response = yield Spide(url, **http_config).async_proxy()
        except Exception as e:
            change_proxies.append(i['proxy_host'])
            logging.error(str(e))
            logging.error('无法连接... '+str(q.qsize())+' '+str(i['proxy_host']))
        else:
            logging.info('连接成功...'+str(q.qsize())+' '+str(i['proxy_host']))
            if not if_test:
                raise gen.Return(response)
    if change_proxies:
        change_proxies = '"'+'","'.join(change_proxies)+'"'
        sql = """delete from pt_db.spide_proxies_ip  where proxy_host in (%s) ;"""
        sql = sql %change_proxies
        result = pmysql.query(sql)
        logging.info('update :'+sql)
    pmysql.close_connect()
    # if not i or i is None:
    #     logging.error('无可用代理ip...')
    # else:
    #     yield get_spide(url)



def insert_proxies(proxies_list):
    if not proxies_list:
        return
    con = MyPyMysql(**mysql_config)
    sql = """ replace into pt_db.spide_proxies_ip (proxy_host,proxy_port,address) values %s """
    con.insert_query(sql, proxies_list)
    con.close_connect()

@gen.coroutine
def get_first_proxy_data():
    while True:
        try:
            proxies_list = []
            for i in range(1,page_num):
                urls = first_url + str(i)
                response = yield get_spide(urls,if_test=False)
                soup = BeautifulSoup(response.body, 'lxml')
                taglist = soup.find_all('tr', attrs={'class': re.compile("(odd)|()")})
                for trtag in taglist:
                    tdlist = trtag.find_all('td')  # 在每个tr标签下,查找所有的td标签
                    proxies_list.append([
                        tdlist[1].string.strip().encode('utf-8'),  # 这里提取IP值
                        tdlist[2].string.strip().encode('utf-8'),  # 这里提取端口值
                        tdlist[3].text.strip().encode('utf-8')  # 这里提取地址
                    ])
                logging.info(urls)
            insert_proxies(proxies_list)
            yield gen.sleep(sleep_tims)
        except Exception as e:
            logging.error(e.message)

@gen.coroutine
def get_second_proxy_data():
    while True:
        try:
            proxies_list = []
            for i in range(1, page_num+1):
                urls = second_url + str(i)
                response = yield get_spide(urls,if_test=False)
                soup = BeautifulSoup(response.body, 'lxml')
                taglist = soup.find_all('tr')
                for trtag in taglist[1:]:
                    tdlist = trtag.find_all('td')  # 在每个tr标签下,查找所有的td标签
                    if tdlist:
                        ipc = re.findall(r'(\d+)',tdlist[0].text.strip())
                        ipc = '.'.join(ipc)
                        proxies_list.append([
                            ipc,  # 这里提取IP值
                            tdlist[1].string.strip().encode('utf-8'),  # 这里提取端口值
                            tdlist[3].text.strip().encode('utf-8').replace('\n','').replace(' ','')  # 这里提取地址
                        ])
                logging.info(urls )
            insert_proxies(proxies_list)
            yield gen.sleep(sleep_tims)
        except Exception as e:
            logging.error(e.message)
@gen.coroutine
def get_third_proxy_data():
    while True:
        try:
            proxies_list = []
            response = yield get_spide(third_url,if_test=False)
            soup = BeautifulSoup(response.body, 'lxml')
            taglist = soup.find_all('tr')
            for trtag in taglist[1:12]:
                tdlist = trtag.find_all('td')  # 在每个tr标签下,查找所有的td标签
                if tdlist and tdlist[2].text == u'匿名':
                    ipc = tdlist[0].text.strip()
                    proxies_list.append([
                        ipc,  # 这里提取IP值
                        tdlist[1].string.strip().encode('utf-8'),  # 这里提取端口值
                        tdlist[3].text.strip().encode('utf-8').replace('\n','').replace(' ','')  # 这里提取地址
                    ])
            logging.info(third_url)
            insert_proxies(proxies_list)
            yield gen.sleep(sleep_tims)
        except Exception as e:
            logging.error(e.message)
@gen.coroutine
def test_proxy():
    while True:
        put_ip()
        yield get_spide(if_test=True)

@gen.coroutine
def main():
    for _ in range(3):
        test_proxy()
    get_first_proxy_data()
    get_second_proxy_data()
    get_third_proxy_data()
    yield Future()


def put_ip():
    pmysql = MyPyMysql(**mysql_config)
    sql = """SELECT proxy_host,proxy_port FROM pt_db.spide_proxies_ip where status = 0 ;"""
    result = pmysql.query(sql)
    for i in result:
        if i['proxy_host'] not in proxy_ip_set:
            proxy_ip_set.add(i['proxy_host'])
            q.put(i)
    pmysql.close_connect()
    if not result:
        os._exit(0)

if __name__ == '__main__':
    logging.basicConfig(filename=logging_filename, level=logging.INFO, datefmt='%Y:%m:%d %H:%M:%S',
                        format='%(asctime)s:%(filename)s[line:%(lineno)d] %(levelname)s %(message)s')

    logging.info('代理ip....')
    q = queues.Queue()
    proxy_ip_set = set()
    ioloop.IOLoop.current().run_sync(main)