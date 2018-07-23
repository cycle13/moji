#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

author:     yetao.lu
date:       2017/12/27
description:
"""
import datetime
import MySQLdb

firstdate='20170601 12:00:00'
pdatetime=datetime.datetime.strptime(firstdate,'%Y%m%d %H:%M:%S')

db=MySQLdb.connect('192.168.1.84','admin','moji_China_123','moge')
cursor = db.cursor()
for i in range(92):
    odatetime = pdatetime + datetime.timedelta(days=i)
    filename = '/Users/yetao.lu/Desktop/mos/maxmin' + datetime.datetime.strftime(
        odatetime, '%Y%m%d') + '.csv'
    csvfile = open(filename, 'w')

    datetime0=odatetime+datetime.timedelta(days=1)
    odatetime0=datetime.datetime.strftime(odatetime,'%Y-%m-%d %H:%M:%S')
    pdatetime0=datetime.datetime.strftime(datetime0,'%Y-%m-%d %H:%M:%S')
    print odatetime0,pdatetime0
    sql='select v01000, max(v_TEM),min(v_TEM),AVG(v_TEM) from t_r_hourly_surf_live_ele where VDate>= " '+odatetime0+'" and VDate< "'+pdatetime0+'" and v_TEM<>999999 GROUP BY v01000'
    #sql='select v01000, max(v_TEM),min(v_TEM) from t_r_hourly_surf_live_ele where VDate>='+odatetime0+' and VDate<'2017-06-02 12:00:00' and v_TEM<>'999999' GROUP BY v01000'
    print sql
    cursor.execute(sql)
    rows=cursor.fetchall()
    for row in rows:
        print str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3])
        csvfile.write(str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3]))
        csvfile.write('\n')
    csvfile.close()
db.close()
