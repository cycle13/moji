#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2019/1/3
description:
"""
import MySQLdb,datetime
def threshold_rain(initialtime):
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor001=db.cursor()
    cursor002=db.cursor()
    cursor_live=db.cursor()
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
    #24小时降水实况2167个站点
    L = []
    for i in range(len(stationlist)):
        stationid=stationlist[i]
        #30个样本的准确率
        for k in range(5):
            for j in range(30):
                #预报：每个站点从起始时间往后滚30天
                l=[]
                initialtime001= initialtime + datetime.timedelta(days=j)
                initial=datetime.datetime.strftime(initialtime001,'%Y-%m-%d %H:%M:%S')
                #每一天都有5天的预报, 起报时间initialtime001
                starttime=initialtime001+datetime.timedelta(days=k)
                endtime=starttime+datetime.timedelta(days=1)
                start = datetime.datetime.strftime(starttime,'%Y-%m-%d %H:%M:%S')
                end = datetime.datetime.strftime(endtime, '%Y-%m-%d %H:%M:%S')
                sql001="SELECT precipitation from t_r_ec_city_forecast_ele where initial_time='"+initial+"' and forecast_time='"+start+"' and city_id='"+str(stationid)+"'"
                sql002="SELECT precipitation from t_r_ec_city_forecast_ele where initial_time='"+initial+"' and forecast_time='"+end+"' and city_id='"+str(stationid)+"'"
                cursor001.execute(sql001)
                cursor002.execute(sql002)
                data001=cursor001.fetchall()
                data002=cursor002.fetchall()
                #24小时降水
                p1=0.0
                p2=0.0
                for row001 in data001:
                    a=row001[0]
                    p1=a
                    break
                for row002 in data002:
                    b=row002[0]
                    p2=b
                    break
                #预报24小时降水
                precipitation=p2-p1
                #实况的降水
                sql_live="SELECT SUM(IF(v_PRE_1h<1000,v_PRE_1h,0)) as v_PRE_24h from t_r_hourly_surf_live_ele where vdate>'"+start+"' and vdate<='"+end+"' and v01000='"+str(stationid)+"'"
                #print sql_live
                cursor_live.execute(sql_live)
                #实况降水和预报降水的差
                cursor_live.execute(sql_live)
                data_live=cursor_live.fetchall()
                pre_live=0.0
                for row_live in data_live:
                    #print row_live[0]
                    pre_live=float(row_live[0])
                    break
                sql_in="replace into precipitation_accuracy_temp(city_id,initial_time,forecast_time,dayindex,rain_fore,rain_live) values (%s,%s,%s,%s,%s,%s)"
                #sql_in="replace into precipitation_accuracy_temp(city_id,initial_time,forecast_time,dayindex,rain_fore,rain_live) values ('"+str(stationid)+"','"+initial+"','"+end+"','"+str(k+1)+"','"+str(precipitation)+"','"+str(pre_live)+"')"
                l.append(str(stationid))
                l.append(initial)
                l.append(end)
                l.append(str(k+1))
                l.append(str(precipitation))
                l.append(str(pre_live))
                L.append(l)
    cursor.executemany(sql_in,L)
    db.commit()
    db.close()
if __name__ == "__main__":
    initial001='2018-04-01 12:00:00'
    initialtime=datetime.datetime.strptime(initial001,'%Y-%m-%d %H:%M:%S')
    for m in range(30):
        initial_time=initialtime+datetime.timedelta(days=m)
        threshold_rain(initial_time)
