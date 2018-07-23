#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/17
description:
"""
import MySQLdb,datetime
def process():
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor = db.cursor()
    csvfw=open('/Users/yetao.lu/snow1.csv','w')
    for i in range(59):
        adatetime='1959-07-01'
        bdatetime='1960-06-30'
        pdatetime=datetime.datetime.strptime(adatetime,'%Y-%m-%d')
        cdate=pdatetime.year+i
        ddate=pdatetime.year+i+1
        cdatetime=datetime.datetime(cdate,pdatetime.month,pdatetime.day)
        ddatetime=datetime.datetime(ddate,pdatetime.month,pdatetime.day)
 
        xdatetime=datetime.datetime.strftime(cdatetime,'%Y-%m-%d')
        ydatetime=datetime.datetime.strftime(ddatetime,'%Y-%m-%d')
        sql='SELECT max(snowprs) from snowprs where vdate>="'+xdatetime+'" and vdate<"'+ydatetime+'" and snowprs<>"999999"'
        cursor.execute(sql)
        rows=cursor.fetchall()
        for row in rows:
            csvfw.write(xdatetime+'~'+ydatetime+','+str(row[0]))
            csvfw.write('\n')
    csvfw.close()
    
if __name__ == "__main__":
    print ''
    process()
