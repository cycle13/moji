#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
author:     yetao.lu
date:       2018/1/5
description:
"""
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json, datetime, os
#import MySQLdb
import xlwt

start=datetime.datetime.now()
m = 1
# weatherfile = '/Users/yetao.lu/Documents/t_p_weather_cod.csv'
# weatherread = open(weatherfile, 'r')
# dict = {}
# while True:
#     line = weatherread.readline()
#     llist = line.split(',')
#     if (len(llist) == 5):
#         dict[llist[4]] = llist[2]
#     if not line:
#         break
# cityfile='/Users/yetao.lu/Documents/city14.csv'
# cityRead=open(cityfile,'r')
# stationlist=[]
# inlist=[]
# stationnameDict={}
# while True:
#     perline=cityRead.readline().decode('gbk')
#     perlist=perline.split(',')
#     # for i in range(len(perlist)):
#     #     templist.append(perlist[i])
#     stationlist.append(perlist[0])
#     stationnameDict[perlist[0]]=perlist
#     if not perline:
#         break
# print stationlist,stationnameDict
#stationlist=['58203','52889','57178']
#stationnameDict={'58203':'阜阳','52889':'兰州','57178':'南阳'}
wbk = xlwt.Workbook(encoding='utf-8', style_compression=0)
sheet = wbk.add_sheet('sheet 1', cell_overwrite_ok=True)
# jsonfile='/Users/yetao.lu/Documents/SURF_HOUR_N/2017/SHOUR_20170901.json'
# pdate=jsonfile[-13:-5]
# print pdate
# pdatetime=datetime.datetime.strptime(pdate,'%Y%m%d')
# print pdatetime
listdirs = os.walk('/Users/yetao.lu/Documents/surf_data/2015')
for root, dirs, files in listdirs:
    for f in files:
        if f[-4:]=='json':
            maxlist1=[]
            minlist1=[]
            templist=[]
            maxtemp1 = -999998
            mintemp1 = 999998
            jsonfile = os.path.join(root, f)
            fileRead = open(jsonfile, 'r')
            jsonstring = fileRead.read()
            datastring = json.loads(jsonstring)
            datalist = datastring['DS']
            sheet.write(0, 0, '时间'.decode('utf-8'))
            sheet.write(0, 1, '最高气温'.decode('utf-8'))
            sheet.write(0, 2, '最低气温'.decode('utf-8'))
            sheet.write(0, 3, '站号'.decode('utf-8'))
            sheet.write(0, 4, '站名'.decode('utf-8'))
            for i in range(len(datalist)):
                per2 = datalist[i]
                stationid = per2['Station_Id_d']
                if stationid =='57178':
                    stationname = per2['Station_Name']
                    city = per2['City']
                    tem_max = per2['TEM_Max']
                    tem_min = per2['TEM_Min']
                    temperature=per2['TEM']
                    year = per2['Year']
                    mon = per2['Mon']
                    day = per2['Day']
                    hour = per2['Hour']
                    odatetime = datetime.datetime(int(year), int(mon), int(day),0, 0)
                    dd = datetime.datetime.strftime(odatetime, '%Y-%m-%d %H:%M:%S')
                    if temperature<>'999998':
                        templist.append(temperature)
            if len(templist)<>0:
                for i in range(len(templist)):
                    if float(templist[i])>maxtemp1:
                        maxtemp1=float(templist[i])
                    if float(templist[i])<mintemp1:
                        mintemp1=float(templist[i])
            else:
                maxtemp1=999998
                mintemp1=999998
            sheet.write(m, 0, dd)
            sheet.write(m, 1, maxtemp1)
            sheet.write(m, 2, mintemp1)
            sheet.write(m, 3, '57178')
            sheet.write(m, 4, stationname)
            m=m+1
wbk.save('/Users/yetao.lu/Documents/南阳2015.xls')
end=datetime.datetime.now()
print (end-start).seconds
