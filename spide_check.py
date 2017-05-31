# -*- coding: utf-8 -*-
# @Time    : 2017/5/24 9:25
# @Author  : wrd
import json
import logging
import re
from bs4 import BeautifulSoup
from tornado import (gen, ioloop)
from tornado.concurrent import Future
from setting import *
from spide_util import Spide, MyPyMysql, RedisPool, Logger
from spide_main import mysql_config, redis_conf
from multiprocessing import Pool

mylog = Logger(chkip_logging_filename)

def insert_proxies(proxies_list):
    if not proxies_list:
        return
    con = MyPyMysql(**mysql_config)
    sql = """ replace into pt_db.spide_proxies_ip (proxy_host,proxy_port) values %s """
    con.insert_query(sql, proxies_list)
    mylog.info('insert :' + sql+str(proxies_list))
    con.close_connect()

def delete_proxies(proxies_list):
    if not proxies_list:
        return
    pmysql = MyPyMysql(**mysql_config)
    change_proxies = '"' + '","'.join(proxies_list) + '"'
    sql = """delete from pt_db.spide_proxies_ip  where proxy_host in (%s) ;"""
    sql = sql % change_proxies
    pmysql.query(sql)
    mylog.info('update :' + sql)
    pmysql.close_connect()





class AsySpider(object):
    def __init__(self):
        self.r = RedisPool(**redis_conf).redis()
        self.url = test_url

    @gen.coroutine
    def get_spide(self):
        """
        判断ip是否有效
        :return:
        """
        insert_lists = []
        for _ in range(10):
            rlist = self.r.lrange("proxy_check_ip_list", 0, -1)
            try:
                if self.r.llen('proxy_check_ip_list') == 0:
                    insert_proxies(insert_lists)
                    insert_lists = []
                    mylog.info('proxy_check_ip_list队列无值,等待添加中....')
                i = json.loads(self.r.blpop("proxy_check_ip_list", timeout=100)[1])
                httpconfigs = get_ip_http_config()
                httpconfigs['proxy_host'] = i['proxy_host']
                httpconfigs['proxy_port'] = int(i['proxy_port'])
                yield Spide(self.url, **httpconfigs).async_proxy()
                # yield Spide(self.url, **httpconfigs).async()
            except Exception as e:
                mylog.error(str(e))
                mylog.error('无法连接... ' + str(len(rlist)) + ' ' + str(i['proxy_host']))
            else:
                insert_lists.append([i['proxy_host'],int(i['proxy_port'])])
                mylog.info('连接成功...' + str(len(rlist)) + ' ' + str(i['proxy_host']))
        insert_proxies(insert_lists)

    @gen.coroutine
    def worker(self):
        while True:
            yield self.get_spide()

    def runner(self):
        ioloop.IOLoop.current().run_sync(self.worker)

def test_proxy():
    asy = AsySpider()
    asy.runner()


def main():
    p = Pool(check_process)
    for i in range(check_process):
        p.apply_async(test_proxy)
    p.close()
    p.join()


if __name__ == '__main__':
    mylog.info('检查代理ip....')
    main()