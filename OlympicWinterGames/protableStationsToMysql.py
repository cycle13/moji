#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/4/8
description:便携站数据入库
"""
import os, MySQLdb,threadpool
def pushintomysql(filepath):
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor = db.cursor()
    fileread = open(filepath, 'r')
    while True:
        perline = fileread.readline()
        perArray = perline.split()
        if (len(perArray)) > 3:
            stationid = perArray[1]
            observationtime = perArray[0]
            if perArray[2] <> '///':
                wd_2m = int(perArray[2])
            else:
                wd_2m = '999999'
            if perArray[3] <> '///':
                ws_2m = float(perArray[3]) * 0.1
            else:
                ws_2m = '999999'
            if perArray[4] <> '////':
                pre_1h = float(perArray[4]) * 0.1
            else:
                pre_1h = '999999'
            if perArray[5] <> '////':
                temp = float(perArray[5]) * 0.1
            else:
                temp = '999999'
            if perArray[6] <> '///':
                rh = int(perArray[6])
            else:
                rh = '999999'
            if perArray[7] <> '/////':
                prs = float(perArray[7]) * 0.1
            else:
                prs = '999999'
            year=observationtime[0:4]
            mon=observationtime[4:6]
            day=observationtime[6:8]
            hours=observationtime[8:10]
            min=observationtime[10:12]
            sql = 'insert into portable (stationID,observatedate,wd_2m,ws_2m,pre_1h,temp,rh,prs,year,mon,day,hours,minute)VALUES ("' + stationid + '","' + observationtime + '","' + str(
                wd_2m) + '","' + str(ws_2m) + '","' + str(
                pre_1h) + '","' + str(temp) + '","' + str(
                rh) + '","' + str(prs)+ '","' + str(year)+ '","' + str(mon)+ '","' + str(day)+ '","' + str(hours)+ '","' + str(min) + '")'
            print sql
            cursor.execute(sql)
            db.commit()
        if not perline:
            break
    db.close()
path = '/Users/yetao.lu/Desktop/冬奥会/data2/mobile'
pool = threadpool.ThreadPool(40)
listname=[]
for rootpath, dirs, files in os.walk(path):
    for file in files:
        if file[-3:] == 'txt':
            filepath = os.path.join(rootpath, file)
            listname.append(filepath)
requests = threadpool.makeRequests(pushintomysql, listname)
[pool.putRequest(req) for req in requests]
pool.wait()

