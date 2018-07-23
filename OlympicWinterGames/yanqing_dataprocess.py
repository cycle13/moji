#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/4/24
description:处理延庆站数据
"""
import MySQLdb,datetime
def processyanqing():
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor=db.cursor()
    adatetime='2005-01-01 00:00:00'
    pdate=datetime.datetime.strptime(adatetime,'%Y-%m-%d %H:%M:%S')
    #print datestring
    #pdatetime=pdate+datetime.timedelta(hours=1)
    csvfile='/Users/yetao.lu/Desktop/冬奥会/tmp/a.csv'
    csvfw=open(csvfile,'w')
    for i in range(864):
        pdate01 = pdate + datetime.timedelta(hours=i)
        odate01 = pdate + datetime.timedelta(hours=(i+1))
        #print pdate01,odate01
        startdate=datetime.datetime.strftime(pdate01,'%Y-%m-%d %H:%M:%S')
        enddate=datetime.datetime.strftime(odate01,'%Y-%m-%d %H:%M:%S')
        sql='SELECT AVG(temp),MAX(temp),MIN(temp),AVG(ws_2m),MAX(ws_2m),AVG(rh),AVG(prs),SUM(pre_1h) from temptable where observatedate>="'+startdate+'" and observatedate<"'+enddate+'"and stationID="'+stationid+'"'
        #print sql
        cursor.execute(sql)
        rows=cursor.fetchall()
        for row in rows:
            csvfw.write(startdate+','+str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3])+','+str(row[4])+','+str(row[5])+','+str(row[6]))
            csvfw.write('\n')
    csvfw.close()
if __name__ == "__main__":
    print ''
    processyanqing()
