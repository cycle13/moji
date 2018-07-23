#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/4/17
description:
"""
import os,datetime,MySQLdb,threading
def Readyanqing30(filename):
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor=db.cursor()
    fileread=open(filename,'r')
    while True:
        line=fileread.readline()
        larray=line.split()
        print len(larray)
        if len(larray)>=8:
            year=larray[0]
            mon=larray[1]
            day=larray[2]
            qc=larray[3]
            vdate=datetime.datetime(int(year),int(mon),int(day))
            if larray[4]<>'32766':
                rain=float(larray[4])*0.1
            else:
                rain=32766
            sql = 'insert into yanqing_rain30 (year,mon,day,qc,rain,stationid,vdate)VALUES("' + year + '","' + str(
                mon) + '","' + str(day) + '","' + str(
                qc) + '","' + str(rain) + '","'+'54406'+ '","'+ str(vdate) + '")'
            cursor.execute(sql)
            db.commit()
        if not line:
            break
    db.close()
def Read2004_2015(filename):
    db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'olympic')
    cursor=db.cursor()
    fileread=open(filename,'r')
    print filename
    while True:
        line=fileread.readline()
        larray=line.split()
        if len(larray)>=3:
            stationid=larray[0]
            vdate=larray[1]
            rain=float(larray[2])*0.1
            year=vdate[0:4]
            mon=vdate[4:6]
            day=vdate[6:8]
            hours=vdate[8:10]
            min=vdate[10:12]
            print year,mon,day,hours,min
            sql='insert into yanqing_rain_minute (stationid,vdate,rain,year,mon,days,hours,minute)VALUES("' +'54406'+ '","'+ str(vdate) +'","'+ str(rain)+'","'+ str(year)+'","'+ str(mon)+'","'+ str(day)+'","'+ str(hours)+'","'+ str(min)+'")'
            print sql
            cursor.execute(sql)
            db.commit()
        if not line:
            break
    db.close()
if __name__ == "__main__":
    filename='/Users/yetao.lu/Desktop/冬奥会/data2/WY/PRE/1min/1980_2004/R1544061980010120041231.DAT'
    path='/Users/yetao.lu/Desktop/冬奥会/data2/WY/PRE/1min/2005_2017/54406'
    #Readyanqing30(filename)
    print 'aaaaaaaaaaaaa'
    #Read2004_2015(path)
    for root,dirs,files in os.walk(path):
        for file in files:
            filename=os.path.join(root,file)
            t=threading.Thread(target=Read2004_2015,args=(filename,))
            t.start()