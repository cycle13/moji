#!/usr/bin/python
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

db=MySQLdb.connect(host='bj28',user='admin',passwd='moji_China_123',db='moge',port=3307,charset='utf8')
cursor = db.cursor()
for i in range(93):
    #print  i
    odatetime=pdatetime+datetime.timedelta(days=i)
    odatetime1=pdatetime+datetime.timedelta(days=i+1)
    pyear=odatetime.year
    pmonth=odatetime.month
    pday=odatetime.day
    #print pyear,pmonth,pday
    filename='/tmp/tempcsv/temp'+datetime.datetime.strftime(odatetime,'%Y%m%d')+'.csv'
    csvfile=open(filename,'w')
    pdatetime1=datetime.datetime.strftime(odatetime,'%Y-%m-%d %H:%M:%S')
    pdatetime2 = datetime.datetime.strftime(odatetime1, '%Y-%m-%d %H:%M:%S')
    sql='select v01000,VDate,v05001,v06001,v07001,TEM from t_h_surf_hour_from_cimiss_ele where VDate>="'+pdatetime1+'" and VDate<"'+pdatetime2+'"and v04004%3=0'
    cursor.execute(sql)
    rows=cursor.fetchall()
    for row in rows:
        #print row
        csvfile.write(str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3])+','+str(row[4])+','+str(row[5]))
        csvfile.write('\n')
    csvfile.close()
db.close()
