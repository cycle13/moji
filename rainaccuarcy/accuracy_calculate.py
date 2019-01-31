#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2019/1/7
description:
"""
import MySQLdb,datetime
def sunrain_accuracy(initial_time,th):
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor=db.cursor()
    #stationcsv='/Users/yetao.lu/2167.csv'
    stationcsv='/home/wlan_dev/2167.csv'
    csvfile=open(stationcsv,'r')
    stationlist=[]
    while True:
        line=csvfile.readline()
        if len(line)>0:
            id=int(line[0:5])
            stationlist.append(id)
        if not line:
            break
    L=[]
    LL=[]
    #insertsql = 'replace into precipitation_forecast_accuracy_temp(city_id,start_time,forecast_aging,sun_rain_accuracy)values (%s,%s,%s,%s)'
    sql_re='insert into precipitation_ec_forecast_accuracy(city_id,start_time,forecast_aging_range,sun_rain_accuracy)values (%s,%s,%s,%s)'
    for i in range(len(stationlist)):
        stationid=stationlist[i]
        accu5=0.0
        ll=[]
        for k in range(5):
            l=[]
            forecast_time=initial_time+datetime.timedelta(days=k+1)
            forecast=datetime.datetime.strftime(forecast_time,'%Y-%m-%d %H:%M:%S')
            n=0
            for j in range(30):
                initialtime=initial_time+datetime.timedelta(days=j)
                initial=datetime.datetime.strftime(initialtime,'%Y-%m-%d %H:%M:%S')
                sql="SELECT rain_fore,rain_live from precipitation_accuracy_temp where initial_time='"+initial+"' and forecast_time='"+forecast+"' and city_id='"+str(stationid)+"'"
                print sql
                cursor.execute(sql)
                rows=cursor.fetchall()
                for row in rows:
                    forerain=float(row[0])
                    liverain=float(row[1])
                    if (forerain>th and liverain>=0.1) or (forerain<=th and liverain<0.1):
                        n=n+1
            accu=round(n/30,2)
            # l.append(stationid)
            # l.append(initial)
            # l.append(k+1)
            # l.append(accu)
            # L.append(l)
            if k==0:
                accu5=0.35*accu
            elif k==1:
                accu5=accu5+0.3*accu
            elif k==2:
                accu5=accu5+0.25*accu
            elif k==3:
                accu5=accu5+0.07*accu
            elif k==4:
                accu5=accu5+0.03*accu
        ll.append(stationid)
        ll.append(initial)
        ll.append('01~05')
        ll.append(accu5)
        LL.append(ll)
    #cursor.executemany(insertsql,L)
    cursor.executemany(sql_re,LL)
    db.commit()
    db.close()
def accuracy_result():

if __name__ == "__main__":
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor=db.cursor()
    initial='2018-01-01 12:00:00'
    initial_time001=datetime.datetime.strptime(initial,'%Y-%m-%d %H:%M:%S')
    A=[]
    sql_th = 'replace into precipitation_result_threshold(threshold,seasons,accuracy)values (%s,%s,%s)'
    for k in range(20):
        end=''
        a=[]
        for i in range(59):
            initial_time=initial_time001+datetime.timedelta(days=i)
            end=datetime.datetime.strftime(initial_time,'%Y-%m-%d %H:%M:%S')
            sunrain_accuracy(initial_time,0.1*k)
        sql="select avg(sun_rain_accuracy) from precipitation_ec_forecast_accuracy where start_time>='"+initial+"' and start_time<='"+end+"'"
        cursor.execute(sql)
        rows=cursor.fetchall()
        accu001=0.0
        for row in rows:
            accu001=float(row[0])

        a.append(0.2*k)
        a.append('winter')
        a.append(accu001)
        A.append(a)
    cursor.executemany(sql_th,A)
    db.commit()
    db.close()