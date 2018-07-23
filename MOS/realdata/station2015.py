#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/2/7
description:
"""
import os,json,datetime,time
listdirs=os.walk('/Users/yetao.lu/Documents/SURF_HOUR_N/2015')
csvfile='/Users/yetao.lu/Documents/temp2015.csv'
csvfile2='/Users/yetao.lu/Documents/pre2015.csv'
wf=open(csvfile,'w')
wf2=open(csvfile2,'w')
for root,dirs,files in listdirs:
    for f in files:
        if f[-5:]=='.json':
            jsonfile=os.path.join(root,f)
            print jsonfile
            fileRead=open(jsonfile,'r')
            jsonstring=fileRead.read()
            #data=json.dumps(jsonstring)
            datastring=json.loads(jsonstring)
            #print datastring['DS']
            datalist=datastring['DS']
            for i in range(len(datalist)):
                per2=datalist[i]
                stationid=per2['Station_Id_d']
                lat=per2['Lat']
                lon=per2['Lon']
                alti=per2['Alti']
                year=per2['Year']
                mon=per2['Mon']
                day=per2['Day']
                hour=per2['Hour']
                temp=per2['TEM']
                pre_1h=per2['PRE_1h']
                print stationid,lat,lon,alti,year,mon,day,hour,temp,pre_1h
                pdatetime=datetime.datetime(int(year),int(mon),int(day),int(hour),0,0)
                if int(hour)%3==0:
                    timestring=datetime.datetime.strftime(pdatetime,'%Y-%m-%d %H:%M:%S')
                    wf.write(stationid+','+timestring+','+lat+','+lon+','+alti+','+temp)
                    wf2.write(stationid+','+timestring+','+lat+','+lon+','+alti+','+pre_1h)
                    wf.write('\n')
                    wf2.write('\n')
    wf.close()
    wf2.close()

