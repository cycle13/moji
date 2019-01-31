#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/12/12
description:
"""
import psycopg2
if __name__ == "__main__":
    conn = psycopg2.connect(database='adc2018', user='postgres',
                            password='mojichina',
                            host='192.168.1.21', port='5432',connect_timeout=99999)
    cursor=conn.cursor()
    sql = "insert into location(lat,lon,pv,geomm)values (%s,%s,%s,ST_SetSRID(ST_MakePoint(%s, %s), 4326))"
    csvfile = '/Users/yetao.lu/Documents/定位/loc.csv'
    csvread=open(csvfile,'r')
    csvread.readline()
    latlon=[]
    while True:
        params=[]
        line=csvread.readline()
        linearray=line.split(',')
        if len(linearray)>1:
            lat=float(linearray[0])
            lon=float(linearray[1])
            pv=int(linearray[2])
            # loc='POINT('+linearray[1]+' '+linearray[0]+'),4326'
            params.append(lat)
            params.append(lon)
            params.append(pv)
            params.append(lon)
            params.append(lat)
            params001=tuple(params)
            latlon.append(params001)
        if not line:
            break
    for per in latlon:
        cursor.execute(sql,per)
        conn.commit()
    conn.close()

