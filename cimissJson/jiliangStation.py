#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/17
description:
"""
import shapefile,datetime

csvfile='/Users/yetao.lu/Downloads/shared/rain/raingauge.csv'
readfile=open(csvfile,'r')
firstline=readfile.readline()
listall=[]
while True:
    perline=readfile.readline()
    perlist=perline.split(',')
    if len(perlist)>5:
        listall.append(perlist)
    if not perline or len(perlist)<5:
        break
print len(listall)
timestring='2018-01-16 00:00:00'
odatetime=datetime.datetime.strptime(timestring,'%Y-%m-%d %H:%M:%S')
for t in range(17):
    filename = '/Users/yetao.lu/Downloads/shared/rain0/rain'+str(t)+".shp"
    w = shapefile.Writer(shapefile.POINT)
    # ensures gemoetry and attributes match
    w.autoBalance=1
    w.field('id')
    w.field('station')
    w.field('rain','F',10,5)# float
    w.field('rain_time')
    w.field('flat','F',10,5)
    w.field('flon','F',10,5)
    pdatetime=odatetime+datetime.timedelta(hours=t)
    ptimestring=datetime.datetime.strftime(pdatetime,'%Y-%m-%d %H:%M:%S')
    print len(listall)
    for i in range(len(listall)):
        print listall[i][3],ptimestring
        if listall[i][3]==ptimestring:
            print i
            w.point(float(listall[i][5]),float(listall[i][4]))
            print listall[i][0],listall[i][1],listall[i][2],listall[i][3],listall[i][4],listall[i][5]
            w.record(listall[i][0],listall[i][1],listall[i][2],listall[i][3],listall[i][4],listall[i][5])
    w.save(filename)
    prj = open("%s.prj" % filename[:-4], "w")
    epsg = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
    prj.write(epsg)
    prj.close()