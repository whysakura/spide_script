# -*- coding: utf-8 -*-
# @Time    : 2017/5/25 15:43
# @Author  : wrd
import json
import time
from setting import *
from tornado import (gen, ioloop)
from spide_util import Spide, MyPyMysql, Logger, RedisPool


# @gen.coroutine
# def getfanlist(current_uk):
#     try:
#         start = 0
#         limit = 24
#         query_uk = current_uk
#         url = fan_url.format(start, limit, query_uk)
#         mylog.info('fans: ' + url)
#         print('fetching %s' % url)
#         response = yield get_spide(url)
#         list_data = json.loads(response.body)
#         if list_data['errno'] != 0:
#             yield getfanlist(current_uk)
#         total_count = list_data['total_count']
#         fans_list = list_data['fans_list']
#         uks = []
#         for i in fans_list:
#             print i['fans_uk'], i['fans_uname'], i['fans_count'], i['follow_count'], i['pubshare_count']
#             if i['fans_count'] == 0 and i['follow_count'] == 0:
#                 continue
#             uks.append(i['fans_uk'])
#     except Exception as e:
#         mylog.error('fanlist error: ' + str(url) + e.message)
#         mylog.error(e)
#         raise gen.Return([])
#     raise gen.Return(uks)




class SpProducer(object):
    def __init__(self):
        self.now_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.r = RedisPool(**redis_conf).redis()
        self.con = MyPyMysql(**mysql_config)
        self.first_sql = """ insert into pt_db.spide_all_person_log (uk,follow_nums,fan_nums,share_nums) values (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE follow_nums=follow_nums , m_time = %s;"""
        self.con.query(self.first_sql, [base_uk, 0, 0, 0, self.now_time])
        self.r.rpush("follow_list", str(base_uk))  # 初始化把base_url加入到队列中

    @gen.coroutine
    def getfollowlist(self,current_uk):
        @gen.coroutine
        def followquery(current_uk, start, limit):
            query_uk = current_uk
            url = follow_url.format(start, limit, query_uk)
            mylog.info('follow: ' + url)
            response = yield self.get_spide(url)
            list_data = json.loads(response.body)
            if list_data['errno'] != 0:
                mylog.info(response.body)
                yield followquery(current_uk, start, limit)
            else:
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
            uk_lists = []
            sql = """select follow_nums from pt_db.spide_all_person_log where uk = %s """
            query_data = self.con.query(sql, current_uk)
            query_follows = query_data[0]['follow_nums'] if query_data else 0
            for_follows = total_count - query_follows  # 增量更新,差异多少
            if for_follows > 0:
                for j in range(((for_follows - 1) / limit) + 1):
                    start = j * limit
                    url = follow_url.format(start, limit, current_uk)
                    range_data = yield followquery(current_uk, start, limit)
                    follow_list.extend(range_data[1])
            for i in follow_list:
                uk_lists.append(i['follow_uk'])
                person_data.append(
                    [i['follow_uk'], i['follow_uname'].encode("utf-8"), i['fans_count'], i['follow_count'],
                     i['pubshare_count']])
            # 把所有查到的人添加到数据库
            if person_data:
                sql = """ replace into pt_db.spide_all_person (uk,uk_name,fan_nums,follow_nums,share_nums) values %s """
                self.con.insert_query(sql, person_data)
            # 记录查询的这个人当前的关注人数
            sql = """ insert into pt_db.spide_all_person_log (uk,follow_nums) values (%s,%s) ON DUPLICATE KEY UPDATE follow_nums=%s , m_time = %s;"""
            self.con.query(sql, (current_uk, total_count, total_count, self.now_time))
        except Exception as e:
            mylog.error('followlist 失败: ' + str(url))
            mylog.error(e)
            raise gen.Return([])
        raise gen.Return(uk_lists)

    @gen.coroutine
    def get_spide(self,url):
        """
        获取 能用的代理连接
        :return:
        """
        for _ in range(10):
            rlist = self.r.lrange("proxy_ip_list", 0, -1)
            try:
                if self.r.llen('proxy_ip_list') == 0:
                    mylog.info('proxy_ip_list队列无值,等待添加中....')
                i = json.loads(self.r.blpop("proxy_ip_list", timeout=0)[1])
                httpconfigs = get_http_config()
                httpconfigs['proxy_host'] = i['proxy_host']
                httpconfigs['proxy_port'] = i['proxy_port']
                response = yield Spide(url, **httpconfigs).async_proxy()
                # response = yield Spide(url, **httpconfigs).async()
            except Exception as e:
                mylog.error(str(e))
                mylog.error('无法连接... ' + str(len(rlist)) + ' ' + str(i['proxy_host']))
            else:
                mylog.info('连接成功...' + str(len(rlist)) + ' ' + str(i['proxy_host']))
                raise gen.Return(response)

    @gen.coroutine
    def put_ip(self):
        # 向队列里面添加数据
        sql = """SELECT uk FROM pt_db.spide_all_person p where follow_nums != 0 order by rand() desc ;"""
        result = self.con.query(sql)
        for i in result:
            self.r.rpush("follow_list", str(i))
        mylog.info('向follow_list加数据')
        if not result or result is None:
            mylog.info('spide_all_person无数据...')

    @gen.coroutine
    def fetch_url(self):
        if self.r.llen('follow_list') == 0:
            mylog.info('follow_list队列无值,等待添加中....')
            self.put_ip()
        current_uk = self.r.blpop("follow_list", timeout=0)[1]
        try:
            mylog.info('生产队列:follow_list:{0},followed_set:{1}'.format(self.r.llen('follow_list'), self.r.scard('followed_set')))
            follow_uks = yield self.getfollowlist(current_uk)
            # fank_uks = yield getfanlist(current_uk,proxies)
            # uks = list(set(follow_uks + fank_uks))
            for uk in follow_uks:
                # 判断是否在follow_set 里面,不在才插入到队列中
                if not self.r.sismember('follow_set',uk):
                    self.r.rpush("follow_list", uk)
            yield gen.sleep(product_sleeptime)
        except Exception as e:
            mylog.error(e.message)

    @gen.coroutine
    def worker(self):
        while True:
            yield self.fetch_url()


    def runner(self):
        ioloop.IOLoop.current().run_sync(self.worker)

def main():
    spp = SpProducer()
    spp.runner()


if __name__ == '__main__':
    mylog = Logger(producer_logging_filename)
    mylog.info('爬虫开始....')
    main()