#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

author:     yetao.lu
date:       2017/12/27
description:
"""
import datetime
import MySQLdb

firstdate='20170601 00:00:00'
pdatetime=datetime.datetime.strptime(firstdate,'%Y%m%d %H:%M:%S')

db=MySQLdb.connect('192.168.1.84','admin','moji_China_123','moge')
cursor = db.cursor()
#气温是1天一个文件
for i in range(92):
    #print  i
    odatetime=pdatetime+datetime.timedelta(days=i)
    pyear=odatetime.year
    pmonth=odatetime.month
    pday=odatetime.day
    print pyear,pmonth,pday
    filename='/Users/yetao.lu/Documents/tempCSV/temp'+datetime.datetime.strftime(odatetime,'%Y%m%d')+'.csv'
    csvfile=open(filename,'w')
    sql='select a.v01000,a.v_TEM,a.vdate,b.v05001,b.v06001,b.v07001 from `t_r_hourly_surf_live_ele` a,`t_p_station_cod` b where a.v04001='+str(pyear)+' and a.v04002='+str(pmonth)+' and a.v04003='+str(pday)+' and a.v04004%3=0 and a.v01000=b.v01000'
    print sql
    cursor.execute(sql)
    rows=cursor.fetchall()
    for row in rows:
        #print row
        csvfile.write(str(row[0])+','+str(row[2])+','+str(row[3])+','+str(row[4])+','+str(row[5])+','+str(row[1]))
        csvfile.write('\n')
    csvfile.close()
db.close()