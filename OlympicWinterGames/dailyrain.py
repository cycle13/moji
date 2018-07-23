#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/2
description:处理便携站的数据
"""
import MySQLdb,datetime
def processportable():
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor=db.cursor()
    adatetime='2014-10-01 00:00:00'
    stationid='A1492'
    pdate=datetime.datetime.strptime(adatetime,'%Y-%m-%d %H:%M:%S')
    #print datestring
    #pdatetime=pdate+datetime.timedelta(hours=1)
    csvfile='/Users/yetao.lu/Desktop/冬奥会/tmp/'+stationid+'rain.csv'
    csvfw=open(csvfile,'w')
    for i in range(1500):
        pdate01 = pdate + datetime.timedelta(days=i)
        odate01 = pdate + datetime.timedelta(days=(i+1))
        #print pdate01,odate01
        startdate=datetime.datetime.strftime(pdate01,'%Y-%m-%d %H:%M:%S')
        enddate=datetime.datetime.strftime(odate01,'%Y-%m-%d %H:%M:%S')
        sql='SELECT SUM(pre_1h) from Hourly_6Ele_qc where observatedate>="'+startdate+'" and observatedate<"'+enddate+'"and pre_1h<>"999999" and stationID="'+stationid+'"'
        print sql
        cursor.execute(sql)
        rows=cursor.fetchall()
        for row in rows:
            csvfw.write(stationid+','+startdate+','+str(row[0]))
            csvfw.write('\n')
    csvfw.close()
if __name__ == "__main__":
    print ''
    processportable()
