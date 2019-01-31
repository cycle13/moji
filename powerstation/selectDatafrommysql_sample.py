#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/20
description:
"""
import MySQLdb,datetime
from scipy import stats
def writecsvfile(csvfile,name):
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor = db.cursor()
    starttime001=datetime.datetime.strptime('2018-10-13 00:00:00','%Y-%m-%d %H:%M:%S')
    initialtime001=datetime.datetime.strptime('2018-10-11 12:00:00','%Y-%m-%d %H:%M:%S')
    csvwr=open(csvfile,'w')
    for i in range(19):
        initialtime=initialtime001+datetime.timedelta(days=i)
        starttime=starttime001+datetime.timedelta(days=i)
        for j in range(96):
            minutetime=starttime+datetime.timedelta(minutes=15*j)
            initial=datetime.datetime.strftime(initialtime,'%Y-%m-%d %H:%M:%S')
            minstring=datetime.datetime.strftime(minutetime,'%Y-%m-%d %H:%M:%S')
            sql='select * from powerplant_radiation where city_id="'+name+'" and initial_time="'+initial+'" and forecast_time="'+minstring+'"'
            print sql
            cursor.execute(sql)
            rows=cursor.fetchall()
            for row in rows:
                if float(row[4])<10:
                    ra='0'
                else:
                    ra=str(row[4])
                csvwr.write(initial+','+minstring+','+ra)
                csvwr.write('\n')
    csvwr.close()
'''
直接计算相关系数，实现自动化
'''
def get_xibanya_live_list(csvfile):
    csvread=open(csvfile,'r')
    list_xibanya=[]
    list_live=[]
    list_moji=[]

    firstline=csvread.readline()
    while True:
        line=csvread.readline()
        linearray=line.split(',')
        if len(linearray)>6:
            list_xibanya.append(float(linearray[4]))
            list_live.append(float(linearray[5]))
            list_moji.append(float(linearray[6]))
        if not line:
            break
def hunhecalculate(name,csvfile,savefile):
    list_moji=[]
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor = db.cursor()
    starttime001=datetime.datetime.strptime('2018-10-13 00:00:00','%Y-%m-%d %H:%M:%S')
    initialtime001=datetime.datetime.strptime('2018-10-11 12:00:00','%Y-%m-%d %H:%M:%S')
    m=0
    for i in range(19):
        initialtime=initialtime001+datetime.timedelta(days=i)
        starttime=starttime001+datetime.timedelta(days=i)
        for j in range(96):
            minutetime=starttime+datetime.timedelta(minutes=15*j)
            initial=datetime.datetime.strftime(initialtime,'%Y-%m-%d %H:%M:%S')
            minstring=datetime.datetime.strftime(minutetime,'%Y-%m-%d %H:%M:%S')
            sql='select * from powerplant_radiation where city_id="'+name+'" and initial_time="'+initial+'" and forecast_time="'+minstring+'"'
            print sql
            cursor.execute(sql)
            rows=cursor.fetchall()
            for row in rows:
                m = m + 1
                print m
                if float(row[4])<10:
                    ra='0'
                else:
                    ra=str(row[4])
                list_moji.append(float(ra))
    csvread=open(csvfile,'r')
    list_xibanya=[]
    list_live=[]
    firstline=csvread.readline()
    while True:
        line=csvread.readline()
        linearray=line.split(',')
        if len(linearray)>5:
            list_xibanya.append(float(linearray[4]))
            list_live.append(float(linearray[5]))
        if not line:
            break
    print len(list_moji),len(list_live),len(list_xibanya)
    wrcsv=open(savefile,'w')
    for ii in range(len(list_xibanya)):
        wrcsv.write(str(list_xibanya[ii])+','+str(list_live[ii])+','+str(list_moji[ii]))
        wrcsv.write('\n')
    wrcsv.close()
    #计算相关系数
    a=stats.pearsonr(list_xibanya,list_live)
    b=stats.pearsonr(list_moji,list_live)
    print a,b

    
if __name__ == "__main__":
    # csvfile001 = '/opt/meteo/cluster/data/temp/powersun/result/XJHMSK.csv'
#     # name001='XJHMSK'
#     # csvfile002 = '/opt/meteo/cluster/data/temp/powersun/result/YNCXDZ.csv'
#     # name002='YNCXDZ'
#     # writecsvfile(csvfile001,name001)
#     # writecsvfile(csvfile002,name002)
    csvfile001='/opt/meteo/cluster/data/temp/powersun/XJHMSK.csv'
    csvfile002='/opt/meteo/cluster/data/temp/powersun/YNCXDZ.csv'
    savefile='/opt/meteo/cluster/data/temp/powersun/result/save.csv'
    hunhecalculate('XJHMSK',csvfile001,'/opt/meteo/cluster/data/temp/powersun/result/save01.csv')
    hunhecalculate('YNCXDZ',csvfile002,'/opt/meteo/cluster/data/temp/powersun/result/save02.csv')