#!/usr/bin/python
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
#db=MySQLdb.connect(host='192.168.1.84',user='admin',passwd='moji_China_123',db='moge',port=3306,charset='utf8')
db=MySQLdb.connect(host='bj28',user='admin',passwd='moji_China_123',db='moge',port=3307,charset='utf8')
cursor = db.cursor()
for i in range(92):
    print i
    odatetime = pdatetime + datetime.timedelta(days=i)
    filename = '/home/wlan_dev/pre6h/pre' + datetime.datetime.strftime(
        odatetime, '%Y%m%d') + '.csv'
    csvfile = open(filename, 'w')
    odatetime2 = pdatetime + datetime.timedelta(days=i+1)
    pdatetime1=datetime.datetime.strftime(odatetime,'%Y-%m-%d %H:%M:%S')
    pdatetime2=datetime.datetime.strftime(odatetime2,'%Y-%m-%d %H:%M:%S')
    sql='SELECT p.v01000 , p.vv , p.v_pre , q.v05001 , q.v06001 , q.v07001 FROM( SELECT v01000 , vv , sum(PRE_1h) AS v_pre FROM( SELECT t.v01000 , t.vdate , t.v04004 , t.PRE_1h , CASE WHEN t.v04004 BETWEEN 0 AND 5 THEN 0 WHEN t.v04004 BETWEEN 6 AND 11 THEN 1 WHEN t.v04004 BETWEEN 12 AND 17 THEN 2 ELSE 3 END vv FROM t_h_surf_hour_from_cimiss_ele t WHERE t.VDate >= "'+pdatetime1+'" AND t.VDate < "'+pdatetime2+'") tt GROUP BY v01000 , vv) p , t_p_station_cod q WHERE p.v01000 = q.v01000;'
    print(sql)
    cursor.execute(sql)
    rows=cursor.fetchall()
    for row in rows:
        timedes = odatetime + datetime.timedelta(hours=3*int(row[1]))
        timestring=datetime.datetime.strftime(timedes,'%Y-%m-%d %H:%M:%S')
        csvfile.write(str(row[0])+','+timestring+','+str(row[2])+','+str(row[3])+','+str(row[4])+','+str(row[5]))
        csvfile.write('\n')
    csvfile.close()
db.close()