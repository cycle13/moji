#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/10/11
description:
"""
import datetime,MySQLdb,math
def calculate_accuacy_mopan_old(db, initial, pdate, odate, ii):
    initialtime = datetime.datetime.strptime(initial, '%Y-%m-%d %H:%M:%S')
    year002 = initialtime.year
    month002 = initialtime.month
    day002 = initialtime.day
    hour002 = initialtime.hour
    cursor001 = db.cursor()
    sql = 'select city_id,initial_time,avg(temperature),max(temperature),min(temperature) from t_r_ec_city_forecast_ele_mos where initial_time="' + initial + '" and forecast_time>"' + pdate + '" and forecast_time<="' + odate + '" group by city_id'
    print sql
    cursor001.execute(sql)
    rows = cursor001.fetchall()
    
if __name__ == "__main__":
    print ''
    starttime='2018-06-01 12:00:00'
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',3307)
    #取30天的实况
    csvfile = '/home/wlan_dev/stations.csv'
    csvread = open(csvfile, 'r')
    while True:
        line = csvread.readline()
        linearray = line.split(',')
        if (len(linearray)) > 1:
            stationid=linearray[0]
            nn = 0
            mm = 0
            for n in range(31):
                initialtime = datetime.datetime.strptime(starttime,
                                                         '%Y-%m-%d %H:%M:%S')
                pdatetime = initialtime + datetime.timedelta(days=n)
                pdate = datetime.datetime.strftime(pdatetime,
                                                   '%Y-%m-%d %H:%M:%S')
                odatetime = initialtime + datetime.timedelta(days=n + 1)
                odate = datetime.datetime.strftime(odatetime,
                                                   '%Y-%m-%d %H:%M:%S')
                rowdict = {}
                sql_live = 'select v01000,AVG(TEM),MAX(TEM_Max),MIN(TEM_Min) from t_r_surf_hour_ele where v01000="'+stationid+'" and vdate>"' + pdate + '" and vdate<="' + odate + '" and TEM<90 and TEM_Max<90 and TEM_Min<90 ;'

                cursor = db.cursor()
                cursor.execute(sql_live)
                rows_live = cursor.fetchall()
                
                sql_fore='select city_id,avg(temperature),max(temperature),min(temperature) from t_r_ec_city_forecast_ele_mos where city_id="'+stationid+'" and initial_time="' + starttime + '" and forecast_time>"' + pdate + '" and forecast_time<="' + odate + '" '
                print sql_live,sql_fore
                cursor_fore=db.cursor()
                cursor_fore.execute(sql_fore)
                rows_fore=cursor_fore.fetchall()
                print rows_live,rows_live
                #准确率计算
                if rows_live[0]<>None or rows_fore<>None:
                    nn=nn+1
                    if abs(float(rows_live[1])-float(rows_fore[1]))<3:
                        mm=mm+1
                
                        
        if not line:
            break

    
