#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/2/1
description:
"""
import os,Nio,bz2,netCDF4,numpy,datetime,shutil,logging
#logging.basicConfig(filename='/home/wlan_dev/lognc.log',level=logging.INFO)
logging.basicConfig(filename='/Users/yetao.lu/Desktop/lognc.log',level=logging.INFO)
gvariables=['2T_GDS0_SFC', '10V_GDS0_SFC', '10U_GDS0_SFC', 'LCC_GDS0_SFC' ]
templist=[]
u10list=[]
v10list=[]
lcclist=[]
timeslist=[]
fullfilelist=[]
bz2filepath='/Users/yetao.lu/Documents/DD'
timestring='2018-01-16 00:00:00'
#filename='D1D01020000010203001.bz2'

pdatetime=datetime.datetime.strptime(timestring,'%Y-%m-%d %H:%M:%S')
mdh=datetime.datetime.strftime(pdatetime,'%m%d%H')
mdhm=datetime.datetime.strftime(pdatetime,'%m%d%H%M')
filenames='D1D'+mdhm+mdh+'011.bz2'
filenamepath=os.path.join(bz2filepath,filenames)
print filenamepath
fullfilelist.append(filenamepath)
#这里的数字为个数-1
for i in range(23):
    print i
    odatetime=pdatetime+datetime.timedelta(hours=3*(i+1))
    pmdhm=datetime.datetime.strftime(odatetime,'%m%d%H%M')
    filenames='D1D'+mdhm+pmdhm+'1.bz2'
    filenamepath=os.path.join(bz2filepath,filenames)
    print filenamepath
    fullfilelist.append(filenamepath)
    #print len(fullfilelist),fullfilelist
#写成NC4格式数据
dataset=netCDF4.Dataset('/Users/yetao.lu/Downloads/tmp/ECtest.nc','w',format='NETCDF4_CLASSIC')
#isobaric=dataset.createDimension('isobaric',1)
lat=dataset.createDimension('lat',1441)
lon=dataset.createDimension('lon',2880)
time=dataset.createDimension('time',24)
times=dataset.createVariable('time',numpy.float64,('time',))
#isobarics=dataset.createVariable('isobaric',numpy.int32,('isobaric',))
latitudes=dataset.createVariable('latitude',numpy.float32,('lat',))
longitudes=dataset.createVariable('longitude',numpy.float32,('lon',))
temp=dataset.createVariable('2T_GDS0_SFC',numpy.float32,('time','lat','lon'))
u10=dataset.createVariable('10U_GDS0_SFC',numpy.float32,('time','lat','lon'))
v10=dataset.createVariable('10V_GDS0_SFC',numpy.float32,('time','lat','lon'))
lcc=dataset.createVariable('LCC_GDS0_SFC',numpy.float32,('time','lat','lon'))
for i in range(len(fullfilelist)):
        logging.info(fullfilelist[i])
        a = bz2.BZ2File(fullfilelist[i],'rb')
        temp=os.path.join(bz2filepath+'temp.grib')
        b = open(temp, 'wb')
        b.write(a.read())
        a.close()
        b.close()
        #读文件
        gribfile=Nio.open_file(temp,'r')
        names=gribfile.variables.keys()
        #print names
        latlist=gribfile.variables['g0_lat_0']
        lonlist=gribfile.variables['g0_lon_1']
        templl=gribfile.variables['2T_GDS0_SFC']
        u10ll = gribfile.variables['10U_GDS0_SFC']
        v10ll = gribfile.variables['10V_GDS0_SFC']
        lccll = gribfile.variables['LCC_GDS0_SFC']
        timesattr=getattr(templl,'forecast_time')
        #print len(latlist),len(lonlist),len(templl),len(u10ll)
        dataset.variables['time'][i]=timesattr
        print len(templl),times,i
        dataset.variables['2T_GDS0_SFC'][i] = templl
        dataset.variables['10U_GDS0_SFC'][i] = u10ll
        dataset.variables['10V_GDS0_SFC'][i] = v10ll
        dataset.variables['LCC_GDS0_SFC'][i] = lccll
#写NC属性
latitudes.units='degrees north'
longitudes.units='degress east'
times.units = "hours since 0001-01-01 00:00:00.0"
times.calendar = "gregorian"
times.long_name=timestring
dataset.close()

