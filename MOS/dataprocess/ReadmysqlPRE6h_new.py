#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

author:     yetao.lu
date:       2017/12/27
description:
"""
import datetime
import MySQLdb

firstdate='20150601 00:00:00'
pdatetime=datetime.datetime.strptime(firstdate,'%Y%m%d %H:%M:%S')

db=MySQLdb.connect('192.168.1.84','admin','moji_China_123','moge')
filename = '/Users/yetao.lu/Documents/PRECSV/pre2015.csv'
csvfile = open(filename, 'w')
cursor = db.cursor()
for i in range(92):
    odatetime = pdatetime + datetime.timedelta(days=i)
    for j in range(4):
        pyear=odatetime.year
        pmonth=odatetime.month
        pday=odatetime.day
        #print pyear,pmonth,pday
        datetime0=odatetime+datetime.timedelta(hours=6*j)
        datetime6=odatetime+datetime.timedelta(hours=6*(j+1))
        #print datetime0,datetime1,datetime2
        pdatetime0=datetime.datetime.strftime(datetime0,'%Y-%m-%d %H:%M:%S')
        pdatetime6 = datetime.datetime.strftime(datetime6, '%Y-%m-%d %H:%M:%S')
        sql='SELECT Station_Id_d , ANY_VALUE(Lat) , ANY_VALUE(Lon) , ANY_VALUE(Alti) , SUM(IF(PRE_1h > "99999" , "0" , PRE_1h)) AS presum_6h FROM cimiss_mos WHERE vdate >= "'+pdatetime0+'" AND vdate < "'+pdatetime6+'" GROUP BY Station_Id_d'
        print sql
        cursor.execute(sql)
        rows=cursor.fetchall()
        for row in rows:
            #print str(row[0])+','+pdatetime0+','+str(row[1])+','+str(row[2])+','+str(row[3])+','+str(row[4])
            csvfile.write(str(row[0])+','+pdatetime0+','+str(row[1])+','+str(row[2])+','+str(row[3])+','+str(row[4]))
            csvfile.write('\n')
csvfile.close()
db.close()
