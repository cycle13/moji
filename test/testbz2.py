#!/usr/bin/python
# -*- coding: utf-8 -*-

import pygrib

file_path = '/Users/yetao.lu/Desktop/mos/temp/D1D04180000041812001.grib'
grbs=pygrib.open(file_path)
for g in grbs:
    print g
    #print g.level,g.name,g.validDate,g.analDate,g.typeOfLevel
# tmpmsgs = grbs.select(name='Temperature')
# print tmpmsgs
names=['Maximum temperature at 2 metres in the last 6 hours','Minimum temperature at 2m in the last 6 hours','2 metre temperature','2 metre dewpoint temperature','10 metre U wind component','10 metre V wind component','Total cloud cover','Low cloud cover','Relative humidity']
for name in names:
    grb = grbs.select(name=name)
    print grb, type(grb), len(grb)
    if name=='Relative humidity':
        grb=grbs.select(name='Relative humidity',level=500)

        

    # values=grb[0].values
    # print values.shape,values.min(),values.max()
# lats,lons=grb[0].latlons()
# print lats.shape,lats.min(),lats.max(),lons.shape,lons.min(),lons.max()
# for i in range(len(lats)):
#     lat=lats[i]
# for j in range(len(lons)):
#     lon=lons[i]
#      prin print lon
# grb=grbs.message(2)
# print grb
# g=grbs[1]
# print g.messagenumber
# print g.keys
