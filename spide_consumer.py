# -*- coding: utf-8 -*-
# @Time    : 2017/5/31 15:43
# @Author  : wrd
import json
import random
import time
from multiprocessing import Pool

from setting import *
from tornado import (gen, ioloop)
from spide_util import Spide, MyPyMysql, Logger, RedisPool, timestamptotime


class SpProducer(object):
    def __init__(self):
        self.now_time = time.strftime('%Y-%m-%d %H:%M:%S')
        self.r = RedisPool(**redis_conf).redis()
        self.r.rpush("share_list", str(base_uk))  # 初始化把base_url加入到队列中
        self.con = MyPyMysql(**mysql_config)

    def add_most_person(self):
        ntime = time.time()
        last_time = self.r.get('if_add_time')
        if not last_time or last_time is None:
            self.r.set('if_add_time', str(ntime))
        else:
            if ntime - float(last_time) > 86400:
                self.r.set('if_add_time', str(ntime))
                # 向队列里面添加数据
                sql = """SELECT uk FROM pt_db.spide_all_person p where p.share_nums !=0 order by share_nums desc limit 100; """
                result = self.con.query(sql)
                for i in result:
                    self.r.rpush("share_list", str(i['uk']))
                mylog.info('向share_list添加前分享前100数据')

    @gen.coroutine
    def getsharelist(self,current_uk, start, limit):
        try:
            auth_type = 1
            query_uk = current_uk
            url = share_url.format(auth_type, start, limit, query_uk)
            response = yield self.get_spide(url)
            list_data = json.loads(response.body)
            if list_data['errno'] != 0:
                yield self.getsharelist(current_uk, start, limit)
            else:
                records = list_data['records'] if 'records' in list_data else []
                insert_data = []
                for i in records:
                    if i['feed_type'] == 'share':
                        for j in i['filelist']:
                            insert_data.append([
                                j['fs_id'],
                                j['category'],
                                'http://pan.baidu.com/s/' if 'shorturl' in i.keys() else 'http://pan.baidu.com/share/link?uk={0}&shareid='.format(
                                    current_uk),
                                i['shorturl'].encode("utf-8") if 'shorturl' in i.keys() else i['shareid'].encode("utf-8"),
                                i['public'].encode("utf-8"),
                                j['server_filename'].encode("utf-8"),
                                i['uk'],
                                i['username'].encode("utf-8"),
                                j['size'],
                                timestamptotime(i['feed_time'] / 1000)
                            ])
                len_insert_data = len(records)
                # print len_insert_data,insert_data
                if insert_data:
                    sql = """ insert ignore into pt_db.spide_shares (fs_id,category,base_url,share_url,`public`,server_filename,uk,username,`size`,share_time) values %s """
                    self.con.insert_query(sql, insert_data)
                # 记录查询的这个人现在分享到多少了
                sql = """ insert into pt_db.spide_all_person_log (uk,share_nums) values (%s,%s) ON DUPLICATE KEY UPDATE share_nums=share_nums+%s , m_time = %s;"""
                self.con.query(sql, (current_uk, len_insert_data, len_insert_data, time.strftime('%Y-%m-%d %H:%M:%S')))
                mylog.info('sharelist 成功: ' + url)
        except Exception as e:
            mylog.error('sharelist 失败: ' + str(url))
            mylog.error(e)
        else:
            raise gen.Return([])

    @gen.coroutine
    def get_spide(self,url):
        """
        获取 能用的代理连接
        :return:
        """
        for _ in range(10):
            rlist = self.r.lrange("proxy_ip_list", 0, -1)
            try:
                # if self.r.llen('proxy_ip_list') == 0:
                #     self.put_ip()
                #     mylog.info('proxy_ip_list队列无值,等待添加中....')
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

    def put_ip(self):
        sql = """SELECT proxy_host,proxy_port FROM pt_db.spide_proxies_ip order by rand();"""
        result = self.con.query(sql)
        for i in result:
            self.r.rpush("proxy_ip_list", json.dumps(i))
        mylog.info('向proxy_ip_list加数据')

    def put_share_list(self):
        # 向队列里面添加数据
        sql = """SELECT uk FROM pt_db.spide_all_person p where p.share_nums !=0 order by share_nums desc; """
        result = self.con.query(sql)
        for i in result:
            self.r.rpush("share_list", str(i['uk']))
        mylog.info('向share_list加数据')
        if not result or result is None:
            mylog.info('share_list数据库无数据...')


    def get_all_person(self,uk):
        """
        获取用户当前的
        :return:
        """
        sql = """SELECT ifnull(p.share_nums,0)-ifnull(l.share_nums,0) as share_nums
            FROM pt_db.spide_all_person p
            left join pt_db.spide_all_person_log l on p.uk = l.uk
            where p.share_nums !=0 and p.uk = %s """
        all_person = self.con.query(sql, uk)  #
        uk_data = (all_person[0]['share_nums']) if all_person  else 0
        return uk_data

    @gen.coroutine
    def worker(self):
        while True:
            try:
                self.add_most_person()
                if self.r.llen('share_list') == 0:
                    mylog.info('share_list队列无值,等待添加中....')
                    self.put_share_list()
                mylog.info('消费队列:share_list:{0}'.format(self.r.llen('share_list')))
                current_uk = (self.r.blpop("share_list", timeout=200)[1])
                query_share_nums = self.get_all_person(current_uk)
                limit = 60
                if query_share_nums > 0:
                    for j in range(((query_share_nums - 1) / limit) + 1):
                        starts = j * limit
                        yield self.getsharelist(current_uk, starts, limit)
            except Exception as e:
                mylog.error(e.message)


    def runner(self):
        ioloop.IOLoop.current().run_sync(self.worker)

def main():
    spp = SpProducer()
    spp.runner()


if __name__ == '__main__':
    mylog = Logger(consumer_logging_filename)
    mylog.info('获取分享数据开始....')
    yanshi = random.randint(0, 60)
    mylog.info(yanshi)
    time.sleep(yanshi)
    p = Pool(consumer_process)
    for i in range(consumer_process):
        p.apply_async(main)
    p.close()
    p.join()