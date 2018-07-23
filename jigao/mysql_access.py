#!/usr/bin/python
# encoding:utf-8

"""
@Version:V0.1
@Author: Chunrui Guo
@Email:chunrui.guo@moji.com
@File:mysql_access.py
@Time:2016/6/1 18:14
"""
import MySQLdb
#from MySQLdb.cursors import DictCursor
from DBUtils.PooledDB import PooledDB


class MysqlAccess:
    """
    mysql read/write
    """
    __pool = None

    def __init__(self, _param):
        self.conn_str = _param

    def __get_conn(self):
        if self.__pool is None:
            self.__pool = PooledDB(creator=MySQLdb, mincached=1, maxcached=20,
                              host=self.conn_str['host'], port=self.conn_str['port'], 
                              user=self.conn_str['user'], passwd=self.conn_str['passwd'] ,  
                              db=self.conn_str['dbname'], use_unicode=False, charset="utf8")#, 
                              #cursorclass=DictCursor)
            #print "self.__pool is None"
        return self.__pool.connection()


    def query_data(self, sql_text, *args):
        conn = self.__get_conn()
        #print 'query conn:', conn
        try:
            cur = conn.cursor()
            if args and len(args) > 0:  # sql_text maybe include sql format
                cur.execute(sql_text, args)
            else:
                cur.execute(sql_text)
            _data_table = cur.fetchall()
            cur.close()
            return _data_table
        except Exception, e:
            print "database access occur an exception:", e, sql_text
        finally:
            conn.close()


    def insert_data(self, sql, *data_list):
        assert len(sql) > 0, "missing insert statement!"
        _sql = sql.strip()
        assert _sql.startswith('insert') or _sql.startswith('call '), "sql that is executed to insert data is invalid!"
        try:
            conn = self.__get_conn()
            #print 'insert conn:', conn   # test
            cur = conn.cursor()
            use_batch_inst = data_list is not None and len(data_list) > 0
            affected_rows = cur.executemany(_sql, data_list) if use_batch_inst else cur.execute(_sql)
            conn.commit()
        except Exception, e:
            print "when inserted data, occur an exception:", e, sql[:30]
        else:
            return -1 if affected_rows is None else affected_rows
        finally:
            if cur:
                cur.close()
            conn.close()

    def insert_or_update_data(self, sql_text, *data_list):
        conn = self.__get_conn()
        try:
            cur = conn.cursor()
            #cur.execute('set names utf8')
            affected_rows = cur.executemany(sql_text, data_list) \
                if data_list is not None and len(data_list) > 0 else cur.execute(sql_text)
            conn.commit()
        except Exception, e:
            print "database access occur an exception:", e, sql_text
        else:
            return -1 if affected_rows is None else affected_rows
        finally:
            conn.close()
