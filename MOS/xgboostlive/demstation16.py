#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/7/9
description:
"""
import os,numpy
from osgeo import gdal
from decimal import Decimal
def DemProcessing(dempath,latitude,longitude,vstring):
    #获取DEM的文件名
    # #首先计算EC数据中经纬度对应格点的索引，
    indexlat_ec = int((60 - latitude) / 0.125)
    indexlon_ec = int((longitude - 60) / 0.125)
    #EC网格的数据和DEM数据的象元格式不一样的，因此需要将16个点重新落在DEM上取DEM值
    #依次取16个点--左上角点
    lat=60-indexlat_ec*0.125
    lon=60+indexlon_ec*0.125
    #16个点不一定在一张TIF上
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 1,demfile,lat,lon
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int(int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601))))
        indexlon=int(int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601))))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #依次取16个点--右上角点
    lat=60-indexlat_ec*0.125
    lon=60+(indexlon_ec+1)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 2,demfile,lat,lon
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #依次取16个点--右下角点
    lat=60-(indexlat_ec+1)*0.125
    lon=60+(indexlon_ec+1)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 3,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    lat=60-(indexlat_ec+1)*0.125
    lon=60+(indexlon_ec)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 4,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #5
    lat=60-(indexlat_ec-1)*0.125
    lon=60+(indexlon_ec-1)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 5,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #6
    lat=60-(indexlat_ec-1)*0.125
    lon=60+(indexlon_ec)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 6,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #7
    lat=60-(indexlat_ec-1)*0.125
    lon=60+(indexlon_ec+1)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 7,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #8
    lat=60-(indexlat_ec-1)*0.125
    lon=60+(indexlon_ec+2)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 8,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #9
    lat=60-(indexlat_ec)*0.125
    lon=60+(indexlon_ec+2)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 9,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #10
    lat=60-(indexlat_ec+1)*0.125
    lon=60+(indexlon_ec+2)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 10,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #11
    lat=60-(indexlat_ec+2)*0.125
    lon=60+(indexlon_ec+2)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 11,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #12
    lat=60-(indexlat_ec+2)*0.125
    lon=60+(indexlon_ec+1)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 12,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #13
    lat=60-(indexlat_ec+2)*0.125
    lon=60+(indexlon_ec)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 13,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #14
    lat=60-(indexlat_ec+2)*0.125
    lon=60+(indexlon_ec-1)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 14,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    #15
    lat=60-(indexlat_ec+1)*0.125
    lon=60+(indexlon_ec-1)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 15,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:

        vstring.append(0)
    #16
    lat=60-(indexlat_ec)*0.125
    lon=60+(indexlon_ec-1)*0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex<100:
        lonindex ='0'+str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    #print 16,demfile
    if os.path.exists(demfile):
        dataset=gdal.Open(demfile)
        myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        #dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat=int((float(lat)+float(1/3601*0.5)-int(lat))/float(Decimal(1)/Decimal(3601)))
        indexlon=int((float(lon)+float(1/3601*0.5)-int(lon))/float(Decimal(1)/Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    return vstring
def perstationvalue(vstring,latlonArray,indexlat,indexlon):
    vstring.append(latlonArray[indexlat][indexlon])
    vstring.append(latlonArray[indexlat][indexlon + 1])
    vstring.append(latlonArray[indexlat + 1][indexlon + 1])
    vstring.append(latlonArray[indexlat + 1][indexlon])
    vstring.append(latlonArray[indexlat - 1][indexlon - 1])
    vstring.append(latlonArray[indexlat - 1][indexlon])
    vstring.append(latlonArray[indexlat - 1][indexlon + 1])
    vstring.append(latlonArray[indexlat - 1][indexlon + 2])
    vstring.append(latlonArray[indexlat][indexlon + 2])
    vstring.append(latlonArray[indexlat + 1][indexlon + 2])
    vstring.append(latlonArray[indexlat + 2][indexlon + 2])
    vstring.append(latlonArray[indexlat + 2][indexlon + 1])
    vstring.append(latlonArray[indexlat + 2][indexlon])
    vstring.append(latlonArray[indexlat + 2][indexlon - 1])
    vstring.append(latlonArray[indexlat + 1][indexlon - 1])
    vstring.append(latlonArray[indexlat][indexlon - 1])
    return vstring
if __name__ == "__main__":
    # csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
    # dempath = '/Users/yetao.lu/Desktop/mos/tif'
    csvfile = '/home/wlan_dev/stations.csv'
    dempath = '/mnt/data/tif'
    fileread = open(csvfile, 'r')
    fileread.readline()
    iii = 0
    #demcsv='/Users/yetao.lu/Desktop/mos/dem.csv'
    demcsv='/home/wlan_dev/dem.csv'
    filewrite=open(demcsv,'w')
    while True:
        iii = iii + 1
        line = fileread.readline()
        perlist = line.split(',')
        print perlist[0]
        if len(perlist) >= 4:
            #stationlist.append(perlist)
            latitude = float(perlist[1])
            longitude = float(perlist[2])
            alti = float(perlist[3])
            vstring=[]
            vstring=DemProcessing(dempath,latitude,longitude,vstring)
            filewrite.write(perlist[0]+',')
            for j in range(len(vstring)):
                if j<>len(vstring)-1:
                    filewrite.write(str(vstring[j])+',')
                else:
                    filewrite.write(str(vstring[j]))
            filewrite.write('\n')
        if not line:
            break
    filewrite.close()
    fileread.close()