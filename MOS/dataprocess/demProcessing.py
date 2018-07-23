#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/11
description:
"""
#Sorry, it's a limitation of Pillow. We don't have the ability to read multichannel images that are more than 8 bit per channel.
# import cv2
tiffile='/Users/yetao.lu/Downloads/shared/dem_resample1.tif'
# image=cv2.imread(tiffile,-1)
# image.dtype
# print image.dtype
# print image.shape
import numpy,os
from osgeo import gdal
from PIL import Image
def GetTiffile(inputpath,latitude,longitude):
    lat=str(int(latitude))
    lon=str(int(longitude))
    filename='ASTGTM2_N'+lat+'E'+lon+'_dem.tif'
    filepath=os.path.join(inputpath,filename)
    return filepath
    
def DemProcess(tiffile,latitude,longitude,stationid):
    featuredic={}
    dataset=gdal.Open(tiffile)
    myarray=numpy.array(dataset.GetRasterBand(1).ReadAsArray())
    print myarray.shape
    print myarray
    indexlat=int((60-latitude)/0.125)
    indexlon=int((longitude-60)/0.125)
    #直接根据索引来取16个栅格数据的值；
    arraylist=[]
    varikey = stationid
    latlonArray = myarray
    arraylist.append(latlonArray[indexlat][indexlon])
    arraylist.append(latlonArray[indexlat][indexlon + 1])
    arraylist.append(latlonArray[indexlat + 1][indexlon + 1])
    arraylist.append(latlonArray[indexlat + 1][indexlon])
    arraylist.append(latlonArray[indexlat - 1][indexlon - 1])
    arraylist.append(latlonArray[indexlat - 1][indexlon])
    arraylist.append(latlonArray[indexlat - 1][indexlon + 1])
    arraylist.append(latlonArray[indexlat - 1][indexlon + 2])
    arraylist.append(latlonArray[indexlat][indexlon + 2])
    arraylist.append(latlonArray[indexlat + 2][indexlon + 2])
    arraylist.append(latlonArray[indexlat + 2][indexlon + 1])
    arraylist.append(latlonArray[indexlat + 2][indexlon])
    arraylist.append(latlonArray[indexlat + 2][indexlon - 1])
    arraylist.append(latlonArray[indexlat + 1][indexlon - 1])
    arraylist.append(latlonArray[indexlat][indexlon - 1])
    featuredic[varikey] = arraylist
    print featuredic
    return featuredic
filepath='/Users/yetao.lu/Desktop/mos/anonymous'
tiffile=GetTiffile(filepath,30.5,100.8)
print tiffile
DemProcess(tiffile,30.5,100.8,'54511')