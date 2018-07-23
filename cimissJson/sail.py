#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/5
description:
"""
import json, datetime, os
import MySQLdb
import xlwt

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
# print dict
wbk = xlwt.Workbook(encoding='utf-8', style_compression=0)
sheet = wbk.add_sheet('sheet 1', cell_overwrite_ok=True)
# jsonfile='/Users/yetao.lu/Documents/SURF_HOUR_N/2017/SHOUR_20170901.json'
# pdate=jsonfile[-13:-5]
# print pdate
# pdatetime=datetime.datetime.strptime(pdate,'%Y%m%d')
# print pdatetime
listdirs = os.walk('/Users/yetao.lu/Documents/2016')
for root, dirs, files in listdirs:
    for f in files:
        jsonfile = os.path.join(root, f)
        print jsonfile
        fileRead = open(jsonfile, 'r')
        jsonstring = fileRead.read()
        # data=json.dumps(jsonstring)
        datastring = json.loads(jsonstring)
        # print datastring['DS']
        datalist = datastring['DS']
        # print len(datalist)
        # db = MySQLdb.connect('192.168.1.84', 'admin', 'moji_China_123', 'moge')
        # cursor = db.cursor()
        sheet.write(0, 0, '时间'.decode('utf-8'))
        sheet.write(0, 1, '风速'.decode('utf-8'))
        sheet.write(0, 2, '风向'.decode('utf-8'))
        sheet.write(0, 3, '气温'.decode('utf-8'))
        for i in range(len(datalist)):
            per2 = datalist[i]
            # akey=per.keys()
            stationname = per2['Station_Name']
            city = per2['City']
            stationid = per2['Station_Id_d']
            lat = per2['Lat']
            lon = per2['Lon']
            alti = per2['Alti']
            year = per2['Year']
            mon = per2['Mon']
            day = per2['Day']
            hour = per2['Hour']
            prs = per2['PRS']
            temp = per2['TEM']
            ws = per2['WIN_S_Avg_10mi']
            rhu = per2['RHU']
            pre_1h = per2['PRE_1h']
            wd = per2['WIN_D_Avg_10mi']
            weather = per2['WEP_Now']
            # print stationname,city,stationid,lat,lon,alti,year,mon,day,hour,prs,ws,rhu,pre_1h,wd,weather
            # sql='insert into test (Station_Name,City,Station_Id_d,Lat,Lon,Alti,Year,Mon,Day,Hour,PRS,WIN_S_Avg_10mi,RHU,PRE_1h,WIN_D_Avg_10mi,WEP_Now) VALUES("'+stationname+'","'+city+'",'+str(stationid)+','+str(lat)+','+str(lon)+','+str(alti)+','+str(year)+','+str(mon)+','+str(day)+','+str(hour)+','+str(prs)+','+str(ws)+','+str(rhu)+','+str(pre_1h)+','+str(wd)+','+str(weather)+')'
            if stationid == '59288':
                # 数据写到excel里面；
                print year, mon, day, hour
                odatetime = datetime.datetime(int(year), int(mon), int(day),
                                              int(hour), 0)
                dd = datetime.datetime.strftime(odatetime, '%Y-%m-%d %H:%M:%S')
                #weath = dict.get(str(weather) + '\n')
                #print weath
                sheet.write(m, 0, dd)
                sheet.write(m, 1, ws)
                sheet.write(m, 2, wd)
                sheet.write(m, 3, temp)
                m = m + 1
wbk.save('/Users/yetao.lu/Documents/weather59288.xls')










