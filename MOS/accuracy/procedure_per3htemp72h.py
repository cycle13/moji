#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/12/19
description: Python调用存储过程
"""
import MySQLdb,datetime
if __name__ == "__main__":
    start='2018-01-01 12:00:00'
    starttime=datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
    
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',3307)
    cursor=db.cursor()
    for i in range(90):
        initialtime=starttime+datetime.timedelta(days=i)
        initial=datetime.datetime.strftime(initialtime,'%Y-%m-%d %H:%M:%S')
        cursor.callproc('proc_est_3h_temp_in_72h',(initial,3,0))
        cursor.execute('select @_proc_est_3h_temp_in_72h_1,@_proc_est_3h_temp_in_72h_2')
        data=cursor.fetchall()
        print i,data,initial