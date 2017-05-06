# -*- coding: utf-8 -*-
# Author:wrd
import logging

import pymysql
from tornado import gen
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.httpclient import HTTPRequest, AsyncHTTPClient


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


class MyPyMysql(object):
    def __init__(self,**kwargs):
        try:
            self.connection = pymysql.connect(**kwargs)
        except Exception as e:
            logging.error('数据库连接失败...')
            logging.error(e)

    def query(self,sql,data=None):
        data = [] if data is None else data
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql,data)
                self.connection.commit()
                result = cursor.fetchall()
                return result
        except Exception as e:
            self.connection.rollback()
            logging.error('回滚: '+sql+str(data))

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


if __name__=="__main__":
    data = MyPyMysql.sql_splice([['1','2','3'],['a','b','c']])
    print data
