#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/16
description:按天统计模式结果来取均值，最高，最低气温并存入数据库
"""
import datetime,MySQLdb
if __name__ == "__main__":
    firstdate = '20150601 12:00:00'
    pdatetime = datetime.datetime.strptime(firstdate, '%Y%m%d %H:%M:%S')
    
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'moge')
    cursor = db.cursor()
    cursor001 = db.cursor()
    for i in range(92):
        odatetime = pdatetime + datetime.timedelta(days=i)
        filename = '/Users/yetao.lu/Documents/maxmin' + datetime.datetime.strftime(
            odatetime, '%Y%m%d') + '.csv'
        csvfile = open(filename, 'w')
        datetime0 = odatetime + datetime.timedelta(days=1)
        odatetime0 = datetime.datetime.strftime(odatetime, '%Y%m%d%H')

        sql='SELECT t.stationid , avg(preds) , max(preds) , min(preds) , avg(origin) , max(origin) , min(origin) , avg(test) , max(test) , min(test) , t.fore FROM( SELECT stationid , origintime , preds , origin , test , CASE WHEN foretimes BETWEEN 1 AND 8 THEN 1 WHEN foretimes BETWEEN 9 AND 16 THEN 2 WHEN foretimes BETWEEN 17 AND 24 THEN 3 WHEN foretimes BETWEEN 25 AND 32 THEN 4 WHEN foretimes BETWEEN 33 AND 40 THEN 5 WHEN foretimes BETWEEN 41 AND 48 THEN 6 WHEN foretimes BETWEEN 49 AND 52 THEN 7 WHEN foretimes BETWEEN 53 AND 56 THEN 8 WHEN foretimes BETWEEN 57 AND 60 THEN 9 WHEN foretimes BETWEEN 53 AND 66 THEN 10 ELSE 0 END fore FROM mosresult WHERE origintime = '+odatetime0+') t GROUP BY stationid , t.fore'
        cursor.execute(sql)
        rows=cursor.fetchall()
        allli=[]
        for row in rows:
            listll = []
            #print len(row)
            for i in range(len(row)):
                listll.append(row[i])
            fdatetime = odatetime + datetime.timedelta(days=int(row[10]))
            fdatetime0=datetime.datetime.strftime(odatetime, '%Y%m%d%H')
            listll.append(odatetime0)
            listll.append(fdatetime0)
            allli.append(listll)
        print len(allli)
        sql='INSERT INTO mosdayly values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor001.executemany(sql,allli)
    db.commit()
    db.close()
    
        
        
        
        
        


