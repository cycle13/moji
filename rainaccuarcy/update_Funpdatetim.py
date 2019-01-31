#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2019/1/15
description:
"""
import MySQLdb,datetime
if __name__ == "__main__":
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor001=db.cursor()
    update001='2019-01-14 16:00:00'
    start001='2018-12-05 12:00:00'
    for i in range(27):
        starttime001=datetime.datetime.strptime(start001,'%Y-%m-%d %H:%M:%S')
        updatetime001=datetime.datetime.strptime(update001,'%Y-%m-%d %H:%M:%S')
        starttime=starttime001+datetime.timedelta(days=i)
        updatetime=updatetime001+datetime.timedelta(days=i)
        start=datetime.datetime.strftime(starttime,'%Y-%m-%d %H:%M:%S')
        update=datetime.datetime.strftime(updatetime,'%Y-%m-%d %H:%M:%S')
        sql="UPDATE t_r_ec_forecast_accuracy SET Fupdate_time='"+update+"' where start_time='"+start+"' and city_id='0' and forecast_aging_range='06~10'"
        cursor001.execute(sql)
    db.commit()
    db.close()
