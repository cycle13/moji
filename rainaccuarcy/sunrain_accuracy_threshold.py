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
    cursor001=db.cursor()
    for k in range(1,20,1):
        start = '2018-01-01 12:00:00'
        starttime = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        cursor = db.cursor()
        for i in range(59):
            initialtime = starttime + datetime.timedelta(days=i)
            initial = datetime.datetime.strftime(initialtime, '%Y-%m-%d %H:%M:%S')
            cursor.callproc('proc_make_ec_forecast_accuracy_sunrain',(initial, 1, 120, 30, 0.2*k))
            cursor.execute('select @_proc_make_ec_forecast_accuracy_sunrain_1,@_proc_make_ec_forecast_accuracy_sunrain_2')
            data = cursor.fetchall()
            print i, data, initial
        sql="SELECT avg(sun_rain_accuracy) from t_r_ec_forecast_accuracy_s where start_time BETWEEN '"+start+"' and '"+initial+"'"
        print sql
        cursor.execute(sql)
        data=cursor.fetchall()
        accu=0.0
        for row in data:
            accu=row[0]
            break
        insql="replace into precipitation_result_threshold(threshold,seasons,accuracy)values ('"+str(0.2*k)+"','winter','"+str(accu)+"')"
        print insql
        cursor001.execute(insql)
        db.commit()
    db.close()