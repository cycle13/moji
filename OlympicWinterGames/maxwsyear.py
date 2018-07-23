#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/17
description:
"""
import datetime,MySQLdb,time
def maxwsperyear():
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor = db.cursor()
    year01=2005
    csvfw=open('/Users/yetao.lu/ws2.csv','w')
    for i in range(13):
        year=year01+i
        print year
        #sql='SELECT YEAR , vdate , ws , t , prs FROM hour_54406 WHERE ws =( SELECT max(ws) maxws FROM hour_54406 WHERE ws <> "999999" AND YEAR = "'+str(year)+'") AND YEAR = "'+str(year)+'"'
        sql='SELECT YEAR , times , ws , t , prs FROM yanqingJ WHERE ws =( SELECT max(ws) maxws FROM yanqingJ WHERE ws <> "999999" AND YEAR = "'+str(year)+'") AND YEAR = "'+str(year)+'"'
        print sql
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            csvfw.write(str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3])+','+str(row[4]))
            csvfw.write('\n')
    csvfw.close()
if __name__ == "__main__":
    print ''
    maxwsperyear()
