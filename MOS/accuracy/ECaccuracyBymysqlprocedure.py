#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/9/12
description:
"""
import MySQLdb,datetime
if __name__ == "__main__":
    print ''
    start='2018-05-01 12:00:00'
    starttime=datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
    for i in range(124):
        pdatetime=starttime+datetime.timedelta(days=i)
        db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',3307)
        cursor=db.cursor()
        args=(pdatetime,2,0)
        #cursor.callproc('proc_est_24h_avg_temp2',args)
        cursor.callproc('proc_est_24h_max_min_temp2',args)
        cursor.callproc('proc_est_3h_temp_in_72h2',args)