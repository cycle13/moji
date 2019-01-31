#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/12/19
description: Python调用存储过程
"""
import MySQLdb,datetime
if __name__ == "__main__":
    start='2018-11-13 12:00:00'
    starttime=datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
    
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',3307)
    cursor=db.cursor()
    for i in range(17):
        initialtime=starttime+datetime.timedelta(days=i)
        initial=datetime.datetime.strftime(initialtime,'%Y-%m-%d %H:%M:%S')
        cursor.callproc('proc_make_ec_forecast_accuracy_mos_dem',(initial,1,240,30,3))
        cursor.execute('select @_proc_make_ec_forecast_accuracy_mos_dem_1,@_proc_make_ec_forecast_accuracy_mos_dem_2')
        data=cursor.fetchall()
        print i,data,initial