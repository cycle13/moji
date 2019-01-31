#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2019/1/8
description:
"""
import MySQLdb, datetime

if __name__ == "__main__":
    
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    start = '2018-12-01 12:00:00'
    starttime = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    cursor = db.cursor()
    for i in range(31):
        initialtime = starttime + datetime.timedelta(days=i)
        initial = datetime.datetime.strftime(initialtime, '%Y-%m-%d %H:%M:%S')
        cursor.callproc('proc_make_ec_forecast_accuracy_sunrain',(initial, 1, 120, 30, 1.33))
        cursor.execute('select @_proc_make_ec_forecast_accuracy_sunrain_1,@_proc_make_ec_forecast_accuracy_sunrain_2')
        data = cursor.fetchall()
        print i, data, initial
    db.commit()
    db.close()
