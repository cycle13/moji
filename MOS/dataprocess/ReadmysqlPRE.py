#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

author:     yetao.lu
date:       2017/12/27
description:
"""
import datetime
import MySQLdb

firstdate='20160601 00:00:00'
pdatetime=datetime.datetime.strptime(firstdate,'%Y%m%d %H:%M:%S')

db=MySQLdb.connect('192.168.1.84','admin','moji_China_123','moge')
cursor = db.cursor()
for i in range(30):
    odatetime = pdatetime + datetime.timedelta(days=i)
    filename = '/Users/yetao.lu/Documents/PRECSV/pre' + datetime.datetime.strftime(
        odatetime, '%Y%m%d') + '.csv'
    csvfile = open(filename, 'w')
    for j in range(8):
        pyear=odatetime.year
        pmonth=odatetime.month
        pday=odatetime.day
        #print pyear,pmonth,pday
        datetime0=odatetime+datetime.timedelta(hours=3*j+0)
        datetime1=odatetime+datetime.timedelta(hours=3*j+1)
        datetime2=odatetime+datetime.timedelta(hours=3*j+2)
        #print datetime0,datetime1,datetime2
        pdatetime0=datetime.datetime.strftime(datetime0,'%Y-%m-%d %H:%M:%S')
        pdatetime1 = datetime.datetime.strftime(datetime1, '%Y-%m-%d %H:%M:%S')
        pdatetime2 = datetime.datetime.strftime(datetime2, '%Y-%m-%d %H:%M:%S')
        sql='SELECT a.v01000 , a.vdate,a.v05001,a.v06001,a.v07001 , IF(a.v_PRE_1h<>"999999"||b.v_PRE_1h<>"999999"||c.v_PRE_1h<>"999999",a.v_PRE_1h + b.v_PRE_1h + c.v_PRE_1h,"999999") AS v_PRE_3H FROM( SELECT p.v01000 , p.v_PRE_1h , p.vdate, s.v05001,s.v06001,s.v07001 FROM `t_r_hourly_surf_live_ele` p,`t_p_station_cod` s WHERE vdate = "'+pdatetime0+'"and p.v01000=s.v01000) a ,( SELECT v01000 , v_PRE_1h , vdate FROM `t_r_hourly_surf_live_ele` WHERE vdate = "'+pdatetime1+'") b ,( SELECT v01000 , v_PRE_1h , vdate FROM `t_r_hourly_surf_live_ele` WHERE vdate = "'+pdatetime2+'") c WHERE a.v01000 = b.v01000 AND a.v01000 = c.v01000 GROUP BY a.v01000'
        print sql
        cursor.execute(sql)
        rows=cursor.fetchall()
        for row in rows:
            print str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3])+','+str(row[4])+','+str(row[5])
            csvfile.write(str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3])+','+str(row[4])+','+str(row[5]))
            csvfile.write('\n')
    csvfile.close()
db.close()
