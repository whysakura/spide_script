# -*- coding: utf-8 -*-
# Author:wrd
import json
import sys
import os
import logging

import time
from random import choice
from datetime import timedelta
from setting import *
import pymysql
from tornado import (httpclient, gen, ioloop, queues)
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from spide_util import Spide, MyPyMysql, timestamptotime, Logger

fetched, fetching, sharing = set(), set(), set()
proxy_ip_set = set()  #  代理ip集合 用于去队列重复,增量插入到队列中
ukset = set()  # uk集合 用于去队列重复,增量插入到队列中
q = queues.Queue(maxsize=maxsize) # 生成队列
share_data = queues.Queue(maxsize=maxsize)  # 分享消费队列
proxy_ip_queue = queues.Queue()  # 分享消费队列
now_time = time.strftime('%Y-%m-%d %H:%M:%S')

def put_ip():
    pmysql = MyPyMysql(**mysql_config)
    sql = """SELECT proxy_host,proxy_port FROM pt_db.spide_proxies_ip where status = 0 ;"""
    result = pmysql.query(sql)
    for i in result:
        if i['proxy_host'] not in proxy_ip_set:
            proxy_ip_set.add(i['proxy_host'])
            proxy_ip_queue.put(i)
    pmysql.close_connect()
    if not result or result is None:
        os._exit(0)

def get_all_person(uk):
    """
    获取所有用户当前粉丝数,分享数,关注数   ,代理连接数
    :return:
    """
    pmysql = MyPyMysql(**mysql_config)
    sql = """SELECT proxy_host,proxy_port FROM pt_db.spide_proxies_ip where status = 0 order by rand() desc ;"""
    result = pmysql.query(sql)  # 代理连接数
    sql = """SELECT ifnull(p.follow_nums,0)-ifnull(l.follow_nums,0) as follow_nums
        FROM pt_db.spide_all_person p
        left join pt_db.spide_all_person_log l on p.uk = l.uk
        where p.share_nums !=0 and p.uk = %s """
    all_person = pmysql.query(sql, uk)  #
    uk_data = int(all_person[0]['follow_nums']) if all_person else 0
    pmysql.close_connect()
    return result, uk_data


@gen.coroutine
def get_spide(url):
    """
    获取 能用的代理连接
    :return:
    """
    for _ in range(proxy_ip_queue.qsize()+1):
        if proxy_ip_queue.empty():
            proxy_ip_set.clear()
            mylog.info('清空 proxy_ip_set')
            put_ip()
        try:
            j = proxy_ip_queue.get()
            i = j.result()
            httpconfigs = get_http_config()
            httpconfigs['proxy_host'] = i['proxy_host']
            httpconfigs['proxy_port'] = i['proxy_port']
            response = yield Spide(url, **httpconfigs).async_proxy()
        except Exception as e:
            mylog.info(e)
            mylog.error('无法连接... ' + str(proxy_ip_queue.qsize()) + ' ' + str(i['proxy_host']))
        else:
            mylog.info('连接成功...' + str(proxy_ip_queue.qsize()) + ' ' + str(i['proxy_host']))
            raise gen.Return(response)



@gen.coroutine
def getsharelist(current_uk, start, limit):
    try:

        # yield gen.sleep(consume_sleeptime)
        auth_type = 1
        query_uk = current_uk
        url = share_url.format(auth_type, start, limit, query_uk)
        response = yield get_spide(url)
        list_data = json.loads(response.body)
        if list_data['errno'] != 0:
            yield getsharelist(current_uk, start, limit)
            raise gen.Return(response.body)
        records = list_data['records'] if 'records' in list_data else []
        insert_data = []
        for i in records:
            if i['feed_type'] == 'share':
                for j in i['filelist']:
                    insert_data.append([
                        j['fs_id'],
                        j['category'],
                        'http://pan.baidu.com/s/' if 'shorturl' in i.keys() else 'http://pan.baidu.com/share/link?uk={0}&shareid='.format(current_uk),
                        i['shorturl'].encode("utf-8") if 'shorturl' in i.keys() else i['shareid'].encode("utf-8"),
                        i['public'].encode("utf-8"),
                        j['server_filename'].encode("utf-8"),
                        i['uk'],
                        i['username'].encode("utf-8"),
                        j['size'],
                        timestamptotime(i['feed_time']/1000)
                    ])
        len_insert_data = len(records)
        # print len_insert_data,insert_data
        con = MyPyMysql(**mysql_config)
        if insert_data:
            sql = """ replace into pt_db.spide_shares (fs_id,category,base_url,share_url,`public`,server_filename,uk,username,`size`,share_time) values %s """
            con.insert_query(sql, insert_data)
        # 记录查询的这个人现在分享到多少了
        sql = """ insert into pt_db.spide_all_person_log (uk,share_nums) values (%s,%s) ON DUPLICATE KEY UPDATE share_nums=share_nums+%s , m_time = %s;"""
        con.query(sql, (current_uk, len_insert_data, len_insert_data, now_time))
        con.close_connect()
        mylog.info('sharelist 成功: ' + url)
    except Exception as e:
        mylog.error('sharelist 失败: ' + str(url))
        mylog.error(e)
        raise gen.Return([])
    raise gen.Return([])


@gen.coroutine
def getfanlist(current_uk):
    try:
        start = 0
        limit = 24
        query_uk = current_uk
        url = fan_url.format(start, limit, query_uk)
        mylog.info('fans: ' + url)
        print('fetching %s' % url)
        response = yield get_spide(url)
        list_data = json.loads(response.body)
        if list_data['errno'] != 0:
            yield getfanlist(current_uk)
        total_count = list_data['total_count']
        fans_list = list_data['fans_list']
        uks = []
        for i in fans_list:
            print i['fans_uk'], i['fans_uname'], i['fans_count'], i['follow_count'], i['pubshare_count']
            if i['fans_count'] == 0 and i['follow_count'] == 0:
                continue
            uks.append(i['fans_uk'])
    except Exception as e:
        mylog.error('fanlist error: ' + str(url) + e.message)
        mylog.error(e)
        raise gen.Return([])
    raise gen.Return(uks)


@gen.coroutine
def getfollowlist(current_uk):
    @gen.coroutine
    def followquery(current_uk, start, limit):
        query_uk = current_uk
        url = follow_url.format(start, limit, query_uk)
        mylog.info('follow: ' + url)
        response = yield get_spide(url)
        list_data = json.loads(response.body)
        if list_data['errno'] != 0:
            yield followquery(current_uk, start, limit)
            raise gen.Return(response.body)
        total_count = list_data['total_count']
        follow_list = list_data['follow_list']
        raise gen.Return([total_count, follow_list])

    try:
        start = 0
        limit = 24
        url = follow_url.format(start, limit, current_uk)
        list_data = yield followquery(current_uk, start, limit)
        total_count = list_data[0]
        follow_list = []
        person_data = []
        con = MyPyMysql(**mysql_config)
        sql = """select follow_nums from pt_db.spide_all_person_log where uk = %s """
        query_data = con.query(sql, current_uk)
        query_follows = query_data[0]['follow_nums'] if query_data else 0
        for_follows = total_count - query_follows  # 增量更新,差异多少
        if for_follows > 0:
            for j in range(((for_follows - 1) / limit) + 1):
                start = j * limit
                url = follow_url.format(start, limit, current_uk)
                range_data = yield followquery(current_uk, start, limit)
                follow_list.extend(range_data[1])
        for i in follow_list:
            person_data.append([i['follow_uk'], i['follow_uname'].encode("utf-8"), i['fans_count'], i['follow_count'],
                                i['pubshare_count']])
        # 把所有查到的人添加到数据库
        if person_data:
            sql = """ replace into pt_db.spide_all_person (uk,uk_name,fan_nums,follow_nums,share_nums) values %s """
            con.insert_query(sql, person_data)
        # 记录查询的这个人当前的关注人数
        sql = """ insert into pt_db.spide_all_person_log (uk,follow_nums) values (%s,%s) ON DUPLICATE KEY UPDATE follow_nums=%s , m_time = %s;"""
        con.query(sql, (current_uk, total_count, total_count,now_time))
        # 查询没有fetching的用户uk
        sql = """ SELECT p.uk FROM pt_db.spide_all_person p
                where p.follow_nums !=0
              """
        query_uk = con.query(sql)
        uks = [i['uk'] for i in query_uk]
        con.close_connect()
    except Exception as e:
        mylog.error('followlist 失败: ' + str(url))
        mylog.error(e)
        raise gen.Return([])
    raise gen.Return(uks)


@gen.coroutine
def main():
    @gen.coroutine
    def fetch_url():
        current_uk = yield q.get()
        try:
            if current_uk in fetching:
                raise gen.Return('current_uk is having in fetching')
            mylog.info('生产队列:{0},fetching:{1},fetched:{2},sharing:{3}'.format(q.qsize(), len(fetching), len(fetched),
                                                                                len(sharing)))

            fetching.add(current_uk)
            # proxies, uk_data = get_all_person(current_uk)
            follow_uks = yield getfollowlist(current_uk)
            # fank_uks = yield getfanlist(current_uk,proxies)

            # uks = list(set(follow_uks + fank_uks))
            uks = set(follow_uks) - ukset  # 求最新的集合 - 以前的集合差集
            ukset.update(set(follow_uks))  # 更新到最新的uks集合
            fetched.add(current_uk)
            for uk in uks:
                if uk not in fetched:
                    if q.qsize() <= maxsize - 100:
                        yield q.put(uk)
                    yield share_data.put(uk)
            yield gen.sleep(product_sleeptime)
        except Exception as e:
            mylog.error(e.message)
        finally:
            q.task_done()

    @gen.coroutine
    def worker():
        while True:
            put_ip()
            yield fetch_url()


    @gen.coroutine
    def consumer():
        while True:
            try:
                mylog.info(
                    '消费队列:{0},fetching:{1},fetched:{2},sharing:{3}'.format(share_data.qsize(), len(fetching),
                                                                           len(fetched),
                                                                           len(sharing)))

                current_uk = yield share_data.get()
                if current_uk in sharing:
                    return
                sharing.add(current_uk)  # 添加查询了的uk
                proxies, query_share_nums = get_all_person(current_uk)
                limit = 60
                if query_share_nums > 0:
                    for j in range(((query_share_nums - 1) / limit) + 1):
                        starts = j * limit
                        yield getsharelist(current_uk, starts, limit)

            except Exception as e:
                mylog.error(e)

            finally:
                share_data.task_done()

    start = time.time()
    q.put(base_uk)
    share_data.put(base_uk)
    worker()
    # Start workers, then wait for the work queue to be empty.
    for i in range(3):
        consumer()
    yield q.join()
    yield share_data.join()
    # assert fetching == fetched
    print('Done in %d seconds, fetched %s URLs.' % (
        time.time() - start, len(fetched)))



if __name__ == '__main__':
    mylog = Logger(ip_logging_filename)
    mylog.info('爬虫开始....')
    con = MyPyMysql(**mysql_config)
    sql = """ insert into pt_db.spide_all_person_log (uk,follow_nums,fan_nums,share_nums) values (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE follow_nums=follow_nums , m_time = %s;"""
    con.query(sql, [2164327417, 0, 0, 0,now_time])
    con.close_connect()
    io_loop = ioloop.IOLoop.current().run_sync(main)
