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
import MySQLdb
import xlwt

m = 1
weatherfile = '/Users/yetao.lu/Documents/AQI/t_p_weather_cod.csv'
weatherread = open(weatherfile, 'r')
dict = {}
while True:
    line = weatherread.readline()
    llist = line.split(',')
    if (len(llist) == 5):
        dict[llist[4]] = llist[2]
    if not line:
        break
cityfile='/Users/yetao.lu/Documents/AQI/city14.csv'
cityRead=open(cityfile,'r')
stationlist=[]
inlist=[]
stationnameDict={}
while True:
    perline=cityRead.readline().decode('gbk')
    perlist=perline.split(',')
    # for i in range(len(perlist)):
    #     templist.append(perlist[i])
    stationlist.append(perlist[0])
    stationnameDict[perlist[0]]=perlist
    if not perline:
        break
print stationlist,stationnameDict
wbk = xlwt.Workbook(encoding='utf-8', style_compression=0)
sheet = wbk.add_sheet('sheet 1', cell_overwrite_ok=True)
# jsonfile='/Users/yetao.lu/Documents/SURF_HOUR_N/2017/SHOUR_20170901.json'
# pdate=jsonfile[-13:-5]
# print pdate
# pdatetime=datetime.datetime.strptime(pdate,'%Y%m%d')
# print pdatetime
listdirs = os.walk('/Users/yetao.lu/Documents/SURF_HOUR_N/2019')
for root, dirs, files in listdirs:
    for f in files:
        jsonfile = os.path.join(root, f)
        print jsonfile
        fileRead = open(jsonfile, 'r')
        jsonstring = fileRead.read()
        # data=json.dumps(jsonstring)
        datastring = json.loads(jsonstring)
        # print datastring['DS']
        datalist = datastring['DS']#!/usr/bin/python
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
import MySQLdb
import xlwt

m = 1
weatherfile = '/Users/yetao.lu/Documents/AQI/t_p_weather_cod.csv'
weatherread = open(weatherfile, 'r')
dict = {}
while True:
    line = weatherread.readline()
    llist = line.split(',')
    if (len(llist) == 5):
        dict[llist[4]] = llist[2]
    if not line:
        break
cityfile='/Users/yetao.lu/Documents/AQI/city14.csv'
cityRead=open(cityfile,'r')
stationlist=[]
inlist=[]
stationnameDict={}
while True:
    perline=cityRead.readline().decode('gbk')
    perlist=perline.split(',')
    # for i in range(len(perlist)):
    #     templist.append(perlist[i])
    stationlist.append(perlist[0])
    stationnameDict[perlist[0]]=perlist
    if not perline:
        break
print stationlist,stationnameDict
wbk = xlwt.Workbook(encoding='utf-8', style_compression=0)
sheet = wbk.add_sheet('sheet 1', cell_overwrite_ok=True)
# jsonfile='/Users/yetao.lu/Documents/SURF_HOUR_N/2017/SHOUR_20170901.json'
# pdate=jsonfile[-13:-5]
# print pdate
# pdatetime=datetime.datetime.strptime(pdate,'%Y%m%d')
# print pdatetime
listdirs = os.walk('/Users/yetao.lu/Documents/SURF_HOUR_N/2019')
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
        sheet.write(0, 1, '气压'.decode('utf-8'))
        sheet.write(0, 2, '风速'.decode('utf-8'))
        sheet.write(0, 3, '风向'.decode('utf-8'))
        sheet.write(0, 4, '相对湿度'.decode('utf-8'))
        sheet.write(0, 5, '降水'.decode('utf-8'))
        sheet.write(0, 6, '气温'.decode('utf-8'))
        sheet.write(0, 7, '天气现象'.decode('utf-8'))
        sheet.write(0, 8, '站号'.decode('utf-8'))
        sheet.write(0, 9, '站名'.decode('utf-8'))
        sheet.write(0, 10, '区县'.decode('utf-8'))
        sheet.write(0, 11, '省'.decode('utf-8'))
        sheet.write(0, 12, '市'.decode('utf-8'))
        sheet.write(0,13,'天气代码'.decode('utf-8'))
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
            if stationid in stationlist:
                if stationid not in inlist:
                    inlist.append(stationid)
                # 数据写到excel里面；
                print year, mon, day, hour
                odatetime = datetime.datetime(int(year), int(mon), int(day),
                                              int(hour), 0)
                dd = datetime.datetime.strftime(odatetime, '%Y-%m-%d %H:%M:%S')
                weath = dict.get(str(weather) + '\n')
                namelist=stationnameDict.get(stationid)
                sname=namelist[1]
                town=namelist[2]
                province=namelist[3]
                city=namelist[4]
                print sname,town,province,city
                sheet.write(m, 0, dd)
                sheet.write(m, 1, prs)
                sheet.write(m, 2, ws)
                sheet.write(m, 3, wd)
                sheet.write(m, 4, rhu)
                sheet.write(m, 5, pre_1h)
                sheet.write(m, 6, temp)
                sheet.write(m, 7, weath)
                sheet.write(m, 8, stationid)
                sheet.write(m, 9, namelist[1])
                sheet.write(m, 10, namelist[2])
                sheet.write(m, 11, namelist[3])
                sheet.write(m, 12, namelist[4])
                sheet.write(m, 13, weather)
                m = m + 1
wbk.save('/Users/yetao.lu/Documents/weatherCity14.xls')
print inlist,len(inlist)









