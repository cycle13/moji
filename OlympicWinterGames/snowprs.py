#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/14
description:
"""
import datetime,MySQLdb
def readsnowprs(filename):
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor = db.cursor()
    files=open(filename,'r')
    firstline=files.readline()
    while True:
        line=files.readline()
        perarray=line.split()
        print perarray
        for i in range(len(perarray)):
            if perarray[i]=='/' or perarray[i]=='*':
                perarray[i]='999999'
        if len(perarray)>4:
            pdate=datetime.datetime(int(perarray[1]),int(perarray[2]),int(perarray[3]))
            pdatestr=datetime.datetime.strftime(pdate,'%Y-%m-%d %H:%M:%S')
            sql='insert into snowprs(stationid,year,mon,day,snowdepth,snowprs,vdate)VALUES ("'+perarray[0]+'","'+perarray[1]+'","'+perarray[2]+'","'+perarray[3]+'","'+perarray[4]+'","'+perarray[5]+'","'+pdatestr+'")'
            cursor.execute(sql)
            db.commit()
        if not line:
            break
    db.close()
if __name__ == "__main__":
    print ''
    filename='/Users/yetao.lu/Desktop/冬奥会/data3/DAY.TXT'
    readsnowprs(filename)