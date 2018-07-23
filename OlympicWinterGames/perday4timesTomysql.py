#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/14
description:
"""
import os,MySQLdb,datetime
def readfiletomysql(filename):
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor = db.cursor()
    file=open(filename,'r')
    firstline=file.readline()
    print firstline
    while True:
        line=file.readline()
        perarray=line.split()
        print perarray
        if len(perarray)>9:
            year=int(perarray[1])
            mon=int(perarray[2])
            day=int(perarray[3])
            hour=int(perarray[4])
            vdate=datetime.datetime(year,mon,day,hour)
            print vdate
            wd_int=perarray[10]
            for i in range(len(perarray)):
                if perarray[i]=='/':
                    perarray[i]='999999'
            vdatestring=datetime.datetime.strftime(vdate,'%Y-%m-%d %H:%M:%S')
            print vdatestring
            suntime='999999'
            print
            sql='insert into hour_54406(stationID,year,mon,day,hour,t,rh,prs,suntime,ws,wd,wd_int,vdate)VALUES ("'+perarray[0]+'","'+perarray[1]+'","'+perarray[2]+'","'+perarray[3]+'","'+perarray[4]+'","'+perarray[5]+'","'+perarray[6]+'","'+perarray[7]+'","'+suntime+'","'+perarray[8]+'","'+perarray[9]+'","'+perarray[10]+'","'+vdatestring+'")'
            print sql
            cursor.execute(sql)
            db.commit()
        if not line:
            break
    db.close()
def readfile2tomysql(filename2):
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor = db.cursor()
    files=open(filename2,'r')
    firstline=files.readline()
    while True:
        line=files.readline()
        perarray=line.split()
        print perarray
        if len(perarray)>9:
            year=int(perarray[1])
            mon=int(perarray[2])
            day=int(perarray[3])
            hour=int(perarray[4])
            vdate=datetime.datetime(year,mon,day,hour)
            vdatestring=datetime.datetime.strftime(vdate,'%Y-%m-%d %H:%M:%S')
            print vdate
            wd_int=perarray[10]
            for i in range(len(perarray)):
                if perarray[i]=='/':
                    perarray[i]='999999'
            sql = 'insert into hour_54406(stationID,year,mon,day,hour,t,rh,prs,suntime,ws,wd,wd_int,dt,pre,vdate)VALUES ("' + \
                  perarray[0] + '","' + perarray[1] + '","' + perarray[2] + '","' + \
                  perarray[3] + '","' + perarray[4] + '","' + perarray[5] + '","' + \
                  perarray[6] + '","' + perarray[7] +'","' + \
                  perarray[8] + '","' + perarray[9] + '","' + perarray[
                      10] + '","'+perarray[11]+'","'+perarray[12]+'","'+perarray[13]+'","' + vdatestring + '")'
            print sql
            cursor.execute(sql)
            db.commit()
        if not line:
            break
    db.close()
if __name__ == "__main__":
    print ''
    filename='/Users/yetao.lu/Desktop/冬奥会/data3/HOUR.TXT'
    filename2='/Users/yetao.lu/Desktop/冬奥会/data3/Houly_54406.TXT'
    #ruku 59-79
    readfiletomysql(filename)
    #ruku 80-05
    #readfile2tomysql(filename2)
    