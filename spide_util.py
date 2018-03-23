# -*- coding: utf-8 -*-
# Author:wrd
import json
import logging

import pymysql
import time
import logging.handlers
import redis
from tornado import gen
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.httpclient import HTTPRequest, AsyncHTTPClient

from my_logger import TimedRotatingFileHandler_MP
from setting import redis_conf, mysql_config, get_ip_http_config, sql_logging_filename


class Spide(object):
    def __init__(self, url, **kwargs):
        self.request = HTTPRequest(url, **kwargs)

    @gen.coroutine
    def async(self, **kwargs):
        http_c = AsyncHTTPClient()
        response = yield http_c.fetch(self.request, **kwargs)
        raise gen.Return(response)

    @gen.coroutine
    def async_proxy(self, **kwargs):
        http_c = CurlAsyncHTTPClient()
        response = yield http_c.fetch(self.request, **kwargs)
        raise gen.Return(response)

class Logger(logging.Logger):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls,'_inst'):
            cls._inst = super(Logger,cls).__new__(cls,*args,**kwargs)
        return cls._inst

    def __init__(self, filename=None):
        super(Logger, self).__init__(self)
        # 日志文件名
        if filename is None:
            filename = 'test.log'
        self.filename = filename

        # 创建一个handler，用于写入日志文件 (每天生成1个，保留30天的日志)
        fh = TimedRotatingFileHandler_MP(self.filename, 'D', 1, 1)
        fh.suffix = "%Y%m%d.log"
        fh.setLevel(logging.DEBUG)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # 定义handler的输出格式
        formatter = logging.Formatter('%(asctime)s:%(filename)s[Line:%(lineno)d][thread:%(thread)s][process:%(process)s] %(levelname)s %(message)s',datefmt='%Y:%m:%d %H:%M:%S')
        fh.setFormatter(formatter)
        #ch.setFormatter(formatter)

        # 给logger添加handler
        self.addHandler(fh)
        self.addHandler(ch)


mylog = Logger(sql_logging_filename)

class MyPyMysql(object):
    def __init__(self,**kwargs):
        try:
            self.connection = pymysql.connect(**kwargs)
        except Exception as e:
            mylog.error('数据库连接失败...')
            mylog.error(e)

    def query(self,sql,data=None):
        data = [] if data is None else data
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql,data)
                mylog.info(cursor._last_executed)
                self.connection.commit()
                result = cursor.fetchall()
                return result
        except Exception as e:
            self.connection.rollback()
            mylog.error('回滚: '+sql+str(data))
            mylog.error(e)

    @staticmethod
    def sql_splice(data):
        """
        sql 拼接
        :param data:
        :return:
        eg:
            sql_splice(data=[['1','2','3'],['a','b','c']])
            "('1', '2', '3'),('a', 'b', 'c')"
        """
        sql = []
        for i in data:
            tp = '("' + '","'.join(map(str, i)) + '")'
            sql.append(tp)

        return ','.join(sql)

    def insert_query(self,sql,data):
        data = MyPyMysql.sql_splice(data)
        self.query(sql %(data))

    def close_connect(self):
        self.connection.close()

def timestamptotime(num):
    """
    时间戳转时间
    :param num:
    :return:
    """
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(num))

class RedisPool(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls,'_inst'):
            cls._inst = super(RedisPool,cls).__new__(cls,*args,**kwargs)
        return cls._inst

    def __init__(self,**kwargs):
        self.pool = redis.ConnectionPool(**kwargs)

    def redis(self):
        rd = redis.Redis(connection_pool=self.pool)
        return rd



if __name__=="__main__":

    # from multiprocessing import Pool
    # p = Pool(1)
    # func = [Lee,Frank,Marlon]
    # print id(p)
    # for i in func:
    #     p.apply_async(i)
    # p.close()
    # print "%s" % (os.getpid())
    # p.join()
    # print 'end!!!'
    r = RedisPool(**redis_conf).redis()
    # a = r.llen('proxy_ip_list')
    # print r.lrange("proxy_ip_list",0,-1)
    # while r.llen('proxy_ip_list'):
    #     print r.blpop("proxy_ip_list", timeout=0)
    # print 'a'
    # pmysql = MyP125yMysql(**mysql_config)
    sql = """SELEC123T proxy_host,proxy_port FROM pt_db.spide_proxies_ip;"""
    # result = p123mysql.query(sql)
    # for i in result:
    #     r.rpush("proxy_ip_list", json.dumps(i))
    #     print(r.lrange("proxy_ip_list", 0, -1))
    # print('向proxy_ip_list加数据')
    # pmysql.close_connect()
