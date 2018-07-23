#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/4
description:线上的最新最高气温、最低气温、气温预测,晴雨预测
该是根据DEM模型来进行预测的
20180709修改：把DEM数据独立出来，
总降水也要减去1.87进行预测
"""
import datetime, os, xgboost, numpy, bz2, \
    multiprocessing, sys, MySQLdb, pygrib, logging
from osgeo import gdal
from decimal import Decimal
from sklearn.externals import joblib


def DemProcessing(dempath, latitude, longitude, vstring):
    # 获取DEM的文件名
    # #首先计算EC数据中经纬度对应格点的索引，
    indexlat_ec = int((60 - latitude) / 0.125)
    indexlon_ec = int((longitude - 60) / 0.125)
    # EC网格的数据和DEM数据的象元格式不一样的，因此需要将16个点重新落在DEM上取DEM值
    # 依次取16个点--左上角点
    lat = 60 - indexlat_ec * 0.125
    lon = 60 + indexlon_ec * 0.125
    # 16个点不一定在一张TIF上
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 1,demfile,lat,lon
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int(int(
            (float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
                Decimal(1) / Decimal(3601))))
        indexlon = int(int(
            (float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
                Decimal(1) / Decimal(3601))))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 依次取16个点--右上角点
    lat = 60 - indexlat_ec * 0.125
    lon = 60 + (indexlon_ec + 1) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 2,demfile,lat,lon
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 依次取16个点--右下角点
    lat = 60 - (indexlat_ec + 1) * 0.125
    lon = 60 + (indexlon_ec + 1) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 3,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    lat = 60 - (indexlat_ec + 1) * 0.125
    lon = 60 + (indexlon_ec) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 4,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 5
    lat = 60 - (indexlat_ec - 1) * 0.125
    lon = 60 + (indexlon_ec - 1) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 5,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 6
    lat = 60 - (indexlat_ec - 1) * 0.125
    lon = 60 + (indexlon_ec) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 6,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 7
    lat = 60 - (indexlat_ec - 1) * 0.125
    lon = 60 + (indexlon_ec + 1) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 7,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 8
    lat = 60 - (indexlat_ec - 1) * 0.125
    lon = 60 + (indexlon_ec + 2) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 8,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 9
    lat = 60 - (indexlat_ec) * 0.125
    lon = 60 + (indexlon_ec + 2) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 9,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 10
    lat = 60 - (indexlat_ec + 1) * 0.125
    lon = 60 + (indexlon_ec + 2) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 10,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 11
    lat = 60 - (indexlat_ec + 2) * 0.125
    lon = 60 + (indexlon_ec + 2) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 11,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 12
    lat = 60 - (indexlat_ec + 2) * 0.125
    lon = 60 + (indexlon_ec + 1) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 12,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 13
    lat = 60 - (indexlat_ec + 2) * 0.125
    lon = 60 + (indexlon_ec) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 13,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 14
    lat = 60 - (indexlat_ec + 2) * 0.125
    lon = 60 + (indexlon_ec - 1) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 14,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    # 15
    lat = 60 - (indexlat_ec + 1) * 0.125
    lon = 60 + (indexlon_ec - 1) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 15,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        
        vstring.append(0)
    # 16
    lat = 60 - (indexlat_ec) * 0.125
    lon = 60 + (indexlon_ec - 1) * 0.125
    latindex = (int(lat))
    lonindex = (int(lon))
    if lonindex < 100:
        lonindex = '0' + str(lonindex)
    demfile = os.path.join(dempath, 'ASTGTM2_N' + str(latindex) + 'E' + str(
        lonindex) + '_dem.tif')
    # print 16,demfile
    if os.path.exists(demfile):
        dataset = gdal.Open(demfile)
        myarray = numpy.array(dataset.GetRasterBand(1).ReadAsArray())
        # dem是3601*3601的网格因此求经纬度最邻近的索引
        indexlat = int((float(lat) + float(1 / 3601 * 0.5) - int(lat)) / float(
            Decimal(1) / Decimal(3601)))
        indexlon = int((float(lon) + float(1 / 3601 * 0.5) - int(lon)) / float(
            Decimal(1) / Decimal(3601)))
        vstring.append(myarray[indexlat][indexlon])
    else:
        vstring.append(0)
    return vstring


def perstationvalue(vstring, latlonArray, indexlat, indexlon):
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
def perstationvalue_p(vstring,latlonArray,platlonArray,indexlat,indexlon):
    if 1000 * (
            latlonArray[indexlat][indexlon] - platlonArray[indexlat][
        indexlon])>1.87:
        vstring.append(1000 * (
                latlonArray[indexlat][indexlon] - platlonArray[indexlat][
            indexlon])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (
            latlonArray[indexlat][indexlon + 1] - platlonArray[indexlat][
        indexlon + 1])>1.87:
        vstring.append(1000 * (
                latlonArray[indexlat][indexlon + 1] - platlonArray[indexlat][
            indexlon + 1])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (latlonArray[indexlat + 1][indexlon + 1] -
                           platlonArray[indexlat + 1][indexlon + 1])>1.87:
        vstring.append(1000 * (latlonArray[indexlat + 1][indexlon + 1] -
                               platlonArray[indexlat + 1][indexlon + 1])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (
            latlonArray[indexlat + 1][indexlon] - platlonArray[indexlat + 1][
        indexlon])>1.87:
        vstring.append(1000 * (
                latlonArray[indexlat + 1][indexlon] - platlonArray[indexlat + 1][
            indexlon])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (latlonArray[indexlat - 1][indexlon - 1] -
                           platlonArray[indexlat - 1][indexlon - 1])>1.87:
        vstring.append(1000 * (latlonArray[indexlat - 1][indexlon - 1] -
                               platlonArray[indexlat - 1][indexlon - 1])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (
            latlonArray[indexlat - 1][indexlon] - platlonArray[indexlat - 1][
        indexlon])>1.87:
        vstring.append(1000 * (
                latlonArray[indexlat - 1][indexlon] - platlonArray[indexlat - 1][
            indexlon])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (latlonArray[indexlat - 1][indexlon + 1] -
                           platlonArray[indexlat - 1][indexlon + 1])>1.87:
        vstring.append(1000 * (latlonArray[indexlat - 1][indexlon + 1] -
                               platlonArray[indexlat - 1][indexlon + 1])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (latlonArray[indexlat - 1][indexlon + 2] -
                           platlonArray[indexlat - 1][indexlon + 2])>1.87:
        vstring.append(1000 * (latlonArray[indexlat - 1][indexlon + 2] -
                               platlonArray[indexlat - 1][indexlon + 2])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (
            latlonArray[indexlat][indexlon + 2] - platlonArray[indexlat][
        indexlon + 2])>1.87:
        vstring.append(1000 * (
                latlonArray[indexlat][indexlon + 2] - platlonArray[indexlat][
            indexlon + 2])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (latlonArray[indexlat + 1][indexlon + 2] -
                           platlonArray[indexlat + 1][indexlon + 2])>1.87:
        vstring.append(1000 * (latlonArray[indexlat + 1][indexlon + 2] -
                               platlonArray[indexlat + 1][indexlon + 2])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (latlonArray[indexlat + 2][indexlon + 2] -
                           platlonArray[indexlat + 2][indexlon + 2])>1.87:
        vstring.append(1000 * (latlonArray[indexlat + 2][indexlon + 2] -
                               platlonArray[indexlat + 2][indexlon + 2])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (latlonArray[indexlat + 2][indexlon + 1] -
                           platlonArray[indexlat + 2][indexlon + 1])>1.87:
        vstring.append(1000 * (latlonArray[indexlat + 2][indexlon + 1] -
                               platlonArray[indexlat + 2][indexlon + 1])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (
            latlonArray[indexlat + 2][indexlon] - platlonArray[indexlat + 2][
        indexlon])>1.87:
        vstring.append(1000 * (
                latlonArray[indexlat + 2][indexlon] - platlonArray[indexlat + 2][
            indexlon])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (latlonArray[indexlat + 2][indexlon - 1] -
                           platlonArray[indexlat + 2][indexlon - 1])>1.87:
        vstring.append(1000 * (latlonArray[indexlat + 2][indexlon - 1] -
                               platlonArray[indexlat + 2][indexlon - 1])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (latlonArray[indexlat + 1][indexlon - 1] -
                           platlonArray[indexlat + 1][indexlon - 1])>1.87:
        vstring.append(1000 * (latlonArray[indexlat + 1][indexlon - 1] -
                               platlonArray[indexlat + 1][indexlon - 1])-1.87)
    else:
        vstring.append(0.0)
    if 1000 * (
            latlonArray[indexlat][indexlon - 1] - platlonArray[indexlat][
        indexlon - 1])>1.87:
        vstring.append(1000 * (
                latlonArray[indexlat][indexlon - 1] - platlonArray[indexlat][
            indexlon - 1])-1.87)
    else:
        vstring.append(0.0)
    return vstring
def demdatefromcsvTodict(demcsv):
    dictdem = {}
    fileread = open(demcsv, 'r')
    while True:
        line = fileread.readline()
        linearray = line.split(',')
        if linearray > 10:
            stationid001 = linearray[0]
            vstring = []
            for i in range(1, len(linearray)):
                vstring.append(linearray[i])
            dictdem[stationid001] = vstring
        if not line:
            break
    return dictdem
def calculateStationVariable(hours,tempvariablelist, maxtempvariablelist,
                             mintempvariablelist, rainvariablelist, inputfile,previouspath,
                             stationlist, csvfile, demcsv):
    if inputfile[-4:] == 'grib' and previouspath[-4:]=='grib':
        #前一个时次只取降水
        pregrbs=pygrib.open(previouspath)
        pregrb=pregrbs.select(name='Total precipitation')
        pretpArray=pregrb[0].values
        pregrb=pregrbs.select(name='Convective precipitation')
        precpArray=pregrb[0].values
        
        grbs = pygrib.open(inputfile)
        # grbs.seek(0)
        # for grb in grbs:
        #     print grb
        # 把数据矩阵都拿出来
        grb = grbs.select(
            name='Maximum temperature at 2 metres in the last 6 hours')
        maxtempArray = grb[0].values
        grb = grbs.select(
            name='Minimum temperature at 2 metres in the last 6 hours')
        mintempArray = grb[0].values
        grb = grbs.select(name='2 metre temperature')
        tempArray = grb[0].values
        grb = grbs.select(name='2 metre dewpoint temperature')
        dewpointArray = grb[0].values
        grb = grbs.select(name='10 metre U wind component')
        u10Array = grb[0].values
        grb = grbs.select(name='10 metre V wind component')
        v10Array = grb[0].values
        grb = grbs.select(name='Total cloud cover')
        tccArray = grb[0].values
        grb = grbs.select(name='Low cloud cover')
        lccArray = grb[0].values
        grb = grbs.select(name='Relative humidity', level=500)
        rh500Array = grb[0].values
        grb = grbs.select(name='Relative humidity', level=850)
        rh850Array = grb[0].values
        grb = grbs.select(name='Total precipitation')
        tpArray = grb[0].values
        grb = grbs.select(name='Convective precipitation')
        cpArray = grb[0].values
        grb = grbs.select(name='U component of wind', level=500)
        u500Array = grb[0].values
        grb = grbs.select(name='V component of wind', level=500)
        v500Array = grb[0].values
        grb = grbs.select(name='U component of wind', level=850)
        u850Array = grb[0].values
        grb = grbs.select(name='V component of wind', level=850)
        v850Array = grb[0].values
        grb = grbs.select(name='Geopotential')
        geoArray = grb[0].values
        idlist = []
        fileread = open(csvfile, 'r')
        fileread.readline()
        iii = 0
        while True:
            iii = iii + 1
            line = fileread.readline()
            perlist = line.split(',')
            if len(perlist) >= 4:
                stationlist.append(perlist)
                station_id = perlist[0]
                latitude = float(perlist[1])
                longitude = float(perlist[2])
                alti = float(perlist[3])
                idlist.append(perlist[0])
                # 经纬度索引
                indexlat = int((90 - latitude) / 0.1)
                indexlon = int((longitude + 180) / 0.1)
                maxlist = []
                minlist = []
                templist = []
                rainlist = []
                vstring = []
                # 各类要素按照要素训练的顺序保持一致:根据索引取值
                # 每个站点的经纬度来获取周边16个格点得值
                perstationvalue(vstring, maxtempArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    #minlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, mintempArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    #maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                # 气温预测时把站点经度纬度和高度加上,一定保持数据顺序的一致性
                maxlist.append(perlist[1])
                minlist.append(perlist[1])
                maxlist.append(perlist[2])
                minlist.append(perlist[2])
                maxlist.append(perlist[3])
                minlist.append(perlist[3])
                vstring = []
                if hours <= 3:
                    perstationvalue(vstring, tpArray, indexlat,
                                    indexlon)
                    for i in range(len(vstring)):
                        rainlist.append(vstring[i])
                    vstring = []
                    perstationvalue(vstring, cpArray, indexlat,
                                    indexlon)
                    # 虽不需要减前一个时次，但需要变换单位
                    for ii in range(len(vstring)):
                        rainlist.append(1000 * vstring[ii])
                else:
                    perstationvalue_p(vstring, tpArray, pretpArray, indexlat,
                                      indexlon)
                    for i in range(len(vstring)):
                        rainlist.append(vstring[i])
                    vstring = []
                    perstationvalue_p(vstring, cpArray, precpArray, indexlat,
                                      indexlon)
                    for ii in range(len(vstring)):
                        rainlist.append(vstring[ii])
                vstring = []
                perstationvalue(vstring, u10Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, v10Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, tempArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, dewpointArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, u10Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, v10Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, tccArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, lccArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, geoArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, rh500Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, rh850Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                    templist.append(vstring[i])
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, u500Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, v500Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, u850Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, v850Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])

                # 站点的纬度经度高度加到矩阵中
                templist.append(latitude)
                templist.append(longitude)
                templist.append(alti)
                # dem16个点加载到矩阵中
                # vstring=DemProcessing(dempath,latitude,longitude,vstring)
                demdict = {}
                demdict = demdatefromcsvTodict(demcsv)
                vstring = []
                vstring = demdict[station_id]
                #print vstring
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                    maxlist.append(vstring[i])
                    minlist.append(vstring[i])
                # print perlist[1],perlist[2],perlist[3]
            # 添加到总的矩阵中
            tempvariablelist.append(templist)
            maxtempvariablelist.append(maxlist)
            mintempvariablelist.append(minlist)
            rainvariablelist.append(rainlist)
            if not line:
                break
def calculateStationVariable2(hours,tempvariablelist, rainvariablelist, inputfile,previouspath,
                              stationlist, csvfile, demcsv):
    if inputfile[-4:] == 'grib' and previouspath[-4:]=='grib':
        #前一个时次只取降水
        pregrbs=pygrib.open(previouspath)
        pregrb=pregrbs.select(name='Total precipitation')
        pretpArray=pregrb[0].values
        pregrb=pregrbs.select(name='Convective precipitation')
        precpArray=pregrb[0].values
        grbs = pygrib.open(inputfile)
        # grbs.seek(0)
        # for grb in grbs:
        #     print grb
        # 把数据矩阵都拿出来
        grb = grbs.select(name='2 metre temperature')
        tempArray = grb[0].values
        grb = grbs.select(name='2 metre dewpoint temperature')
        dewpointArray = grb[0].values
        grb = grbs.select(name='10 metre U wind component')
        u10Array = grb[0].values
        grb = grbs.select(name='10 metre V wind component')
        v10Array = grb[0].values
        grb = grbs.select(name='Total cloud cover')
        tccArray = grb[0].values
        grb = grbs.select(name='Low cloud cover')
        lccArray = grb[0].values
        grb = grbs.select(name='Relative humidity', level=500)
        rh500Array = grb[0].values
        grb = grbs.select(name='Relative humidity', level=850)
        rh850Array = grb[0].values
        grb = grbs.select(name='Total precipitation')
        tpArray = grb[0].values
        grb = grbs.select(name='Convective precipitation')
        cpArray = grb[0].values
        grb = grbs.select(name='U component of wind', level=500)
        u500Array = grb[0].values
        grb = grbs.select(name='V component of wind', level=500)
        v500Array = grb[0].values
        grb = grbs.select(name='U component of wind', level=850)
        u850Array = grb[0].values
        grb = grbs.select(name='V component of wind', level=850)
        v850Array = grb[0].values
        grb = grbs.select(name='Geopotential')
        geoArray = grb[0].values
        idlist = []
        fileread = open(csvfile, 'r')
        fileread.readline()
        iii = 0
        while True:
            iii = iii + 1
            line = fileread.readline()
            perlist = line.split(',')
            if len(perlist) >= 4:
                stationlist.append(perlist)
                station_id = perlist[0]
                latitude = float(perlist[1])
                longitude = float(perlist[2])
                alti = float(perlist[3])
                idlist.append(perlist[0])
                # 经纬度索引
                indexlat = int((90 - latitude) / 0.1)
                indexlon = int((longitude + 180) / 0.1)
                # 定义气温要素训练的一个站点的一条记录为templist，存多个训练因子
                templist = []
                rainlist = []
                vstring = []
                # 各类要素按照要素训练的顺序保持一致:根据索引取值
                # 总降水
                if hours<=3:
                    perstationvalue(vstring, tpArray, indexlat,
                                      indexlon)
                    for i in range(len(vstring)):
                        rainlist.append(vstring[i])
                    vstring = []
                    perstationvalue(vstring, cpArray, indexlat,
                                      indexlon)
                    #虽不需要减前一个时次，但需要变换单位
                    for i in range(len(vstring)):
                        rainlist.append(1000*vstring[i])
                else:
                    perstationvalue_p(vstring,tpArray,pretpArray,indexlat,indexlon)
                    for i in range(len(vstring)):
                        rainlist.append(vstring[i])
                    vstring=[]
                    perstationvalue_p(vstring,cpArray,precpArray,indexlat,indexlon)
                    for i in range(len(vstring)):
                        rainlist.append(vstring[i])
                vstring = []
                # 10米U
                perstationvalue(vstring, u10Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                # 10米
                perstationvalue(vstring, v10Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                # 气温数据的获取从这里开始：顺序是：
                # 2T,2D,10u,10v,tcc,lcc,z,500Rh,850rh,lat,lon,alti,dem16个点。
                # 气温
                perstationvalue(vstring, tempArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                    rainlist.append(vstring[i])
                vstring = []
                # 露点温度
                perstationvalue(vstring, dewpointArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                    rainlist.append(vstring[i])
                vstring = []
                # 10
                perstationvalue(vstring, u10Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 10V
                perstationvalue(vstring, v10Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 总云量
                perstationvalue(vstring, tccArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 低云量
                perstationvalue(vstring, lccArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 位势高度
                perstationvalue(vstring, geoArray, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                vstring = []
                # 500hpaRH
                perstationvalue(vstring, rh500Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                    rainlist.append(vstring[i])
                vstring = []
                # 850hPaRH
                perstationvalue(vstring, rh850Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, u500Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, v500Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, u850Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                perstationvalue(vstring, v850Array, indexlat, indexlon)
                for i in range(len(vstring)):
                    rainlist.append(vstring[i])
                vstring = []
                # 站点的纬度经度高度加到矩阵中
                templist.append(latitude)
                templist.append(longitude)
                templist.append(alti)
                # dem16个点加载到矩阵中
                # vstring=DemProcessing(dempath,latitude,longitude,vstring)
                demdict = {}
                demdict = demdatefromcsvTodict(demcsv)
                vstring = demdict[station_id]
                #print vstring
                for i in range(len(vstring)):
                    templist.append(vstring[i])
                # print perlist[1],perlist[2],perlist[3]
            # 添加到总的矩阵中
            #print type(rainlist),rainlist
            tempvariablelist.append(templist)
            rainvariablelist.append(rainlist)
            if not line:
                break
        # 关闭grib文件
        grbs.close()


def Predict(hours,outfilename,previouspath, modelname, maxmodel, minmodel, rainmodelfile,
            premodelfile, tempscalerfile, maxscalerfile, minscalerfile,
            rainscalerfile, prescalerfile, origintime, foretime, outpath,
            csvfile, demcsv):
    try:
        logname=os.path.split(outfilename)[1][:-5]+'.log'
        #多进程不支持写一个日志，所以每个进程一个日志
        logger = logging.getLogger('learing1.logger')
        # 指定logger输出格式
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-8s: %(message)s')
        # 文件日志learning
        logfile = os.path.join(outpath,logname)
        # logfile='/Users/yetao.lu/Desktop/mos/temp/loging.log'
        file_handler = logging.FileHandler(logfile)
        file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
        # 控制台日志
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter  # 也可以直接给formatter赋值
    
        # 为logger添加的日志处理器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
        # 指定日志的最低输出级别，默认为WARN级别
        logger.setLevel(logging.INFO)
        
        logger.info('hours=' + str(hours) + ';' + outfilename)
        # 取气温、最高气温、最低气温的训练训练矩阵。读文件费劲，连降水一起取了。晴雨、降水一块训
        tempvariablelist = []
        maxtempvariablelist = []
        mintempvariablelist = []
        rainvariablelist = []
        stationlist = []
        calculateStationVariable(hours,tempvariablelist, maxtempvariablelist,
                                 mintempvariablelist, rainvariablelist,
                                 outfilename,previouspath,stationlist, csvfile, demcsv)
        # 加载训练模型
        params = {
            'booster': 'gbtree',
            'objective': 'reg:linear',  # 线性回归
            'gamma': 0.2,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
            'max_depth': 12,  # 构建树的深度，越大越容易过拟合
            'lambda': 2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
            'subsample': 0.7,  # 随机采样训练样本
            'colsample_bytree': 0.7,  # 生成树时进行的列采样
            'min_child_weight': 3,
            # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
            # ，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
            # 这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
            'silent': 0,  # 设置成1则没有运行信息输出，最好是设置为0.
            'eta': 0.02,  # 如同学习率
            'seed': 1000,
            # 'nthread': 3,  # cpu 线程数
            # 'eval_metric': 'auc'
            'scale_pos_weight': 1
        }
        bst = xgboost.Booster(params)
        bst.load_model(modelname)
        # 气温模型预测
        ecvaluelist = numpy.array(tempvariablelist)
        # ecvaluelist=ecvaluelist.astype('float64')
        # logger.info('ecvaluelist')
        # logger.info(ecvaluelist)
        # 加载标准化预处理文件，对数据进行与模型一致的标准化
        scaler = joblib.load(tempscalerfile)
        # transform后必须重新复制，原来矩阵是不变的
        ecvaluelist_t = scaler.transform(ecvaluelist)
        # logger.info(ecvaluelist)
        # logger.info(ecvaluelist_t)
        xgbtrain = xgboost.DMatrix(ecvaluelist_t)
        result = bst.predict(xgbtrain)
        # logger.info('result')
        # logger.info(result)
        # 最高气温
        # 加载训练模型
        params01 = {
            'booster': 'gbtree',
            'objective': 'reg:linear',  # 线性回归
            'gamma': 0.2,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
            'max_depth': 12,  # 构建树的深度，越大越容易过拟合
            'lambda': 2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
            'subsample': 0.7,  # 随机采样训练样本
            'colsample_bytree': 0.7,  # 生成树时进行的列采样
            'min_child_weight': 3,
            # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
            # ，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
            # 这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
            'silent': 0,  # 设置成1则没有运行信息输出，最好是设置为0.
            'eta': 0.1,  # 如同学习率
            'seed': 1000,
            # 'nthread': 3,  # cpu 线程数
            # 'eval_metric': 'auc'
            'scale_pos_weight': 1
        }
        # 最低气温
        params02 = {
            'booster': 'gbtree',
            'objective': 'reg:linear',  # 线性回归
            'gamma': 0.2,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
            'max_depth': 12,  # 构建树的深度，越大越容易过拟合
            'lambda': 2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
            'subsample': 0.7,  # 随机采样训练样本
            'colsample_bytree': 0.7,  # 生成树时进行的列采样
            'min_child_weight': 3,
            # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
            # ，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
            # 这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
            'silent': 0,  # 设置成1则没有运行信息输出，最好是设置为0.
            'eta': 0.1,  # 如同学习率
            'seed': 1000,
            # 'nthread': 3,  # cpu 线程数
            # 'eval_metric': 'auc'
            'scale_pos_weight': 1
        }
        bst01 = xgboost.Booster(params01)
        bst01.load_model(maxmodel)
        bst02 = xgboost.Booster(params02)
        bst02.load_model(minmodel)
        # 最高最低气温模型预测
        maxtempvariablelist = numpy.array(maxtempvariablelist)
        maxscaler = joblib.load(maxscalerfile)
        #logger.info(maxscalerfile)
        maxtempvariablelist_t = maxscaler.transform(maxtempvariablelist)
        xgbtrain01 = xgboost.DMatrix(maxtempvariablelist_t)
        result01 = bst01.predict(xgbtrain01)
        #logger.info(result01.shape)
        #logger.info('min')
        mintempvariablelist = numpy.array(mintempvariablelist)
        minscaler = joblib.load(minscalerfile)
        mintempvariablelist_t = minscaler.transform(mintempvariablelist)
        xgbtrain02 = xgboost.DMatrix(mintempvariablelist_t)
        result02 = bst02.predict(xgbtrain02)
        #logger.info(result02.shape)
        # 晴雨预测
        params_rain = {
            'booster': 'gbtree',
            'objective': 'multi:softmax',  # 分类
            'num_class': 3,  # 分3类
            'gamma': 0.2,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
            'max_depth': 12,  # 构建树的深度，越大越容易过拟合
            'lambda': 2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
            'subsample': 0.7,  # 随机采样训练样本
            'colsample_bytree': 0.7,  # 生成树时进行的列采样
            'min_child_weight': 3,
            # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
            # ，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
            # 这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
            'silent': 0,  # 设置成1则没有运行信息输出，最好是设置为0.
            'eta': 0.1,  # 如同学习率
            'seed': 1000,
            # 'nthread': 20,  # cpu 线程数
            # 'eval_metric': 'auc'
            'scale_pos_weight': 1
        }
        rainvariablelist=numpy.array(rainvariablelist)
        rainbst = xgboost.Booster(params_rain)
        rainbst.load_model(rainmodelfile)
        #logger.info(rainmodelfile)
        rainscaler = joblib.load(rainscalerfile)
        #print rainvariablelist
        rainvariablelist_t = rainscaler.transform(rainvariablelist)
        #logger.info('hello')
        #logger.info(rainvariablelist_t)
        raintrain = xgboost.DMatrix(rainvariablelist_t)
        rainresult = rainbst.predict(raintrain)
        #logger.info(rainresult)
        # 降水预测：需要把有雨的站点挑出来进行降水预测
        params_pre = {
            'booster': 'gbtree',
            'objective': 'reg:linear',  # 线性回归
            'gamma': 0.2,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
            'max_depth': 12,  # 构建树的深度，越大越容易过拟合
            'lambda': 2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
            'subsample': 0.7,  # 随机采样训练样本
            'colsample_bytree': 0.7,  # 生成树时进行的列采样
            'min_child_weight': 3,
            # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
            # ，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
            # 这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
            'silent': 0,  # 设置成1则没有运行信息输出，最好是设置为0.
            'eta': 0.1,  # 如同学习率
            'seed': 1000,
            # 'nthread': 20,  # cpu 线程数
            # 'eval_metric': 'auc'
            'scale_pos_weight': 1
        }
        # 挑选有雨的预报集
        rainstationlist = []
        prevariablelist = []
        for k in range(len(stationlist)):
            stationid = stationlist[k][0]
            rainstate = rainresult[k]
            if rainstate == 1:
                rainstationlist.append(stationlist[k])
                prevariablelist.append(rainvariablelist[k])
        prevariablelist=numpy.array(prevariablelist)
        prebst = xgboost.Booster(params_pre)
        prebst.load_model(premodelfile)
        prescaler = joblib.load(prescalerfile)
        prevariablelist_t = prescaler.transform(prevariablelist)
        pretrain = xgboost.DMatrix(prevariablelist_t)
        preresult = prebst.predict(pretrain)
        #logger.info(preresult)
        # 预测结果入库
        db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',3307)
        #db = MySQLdb.connect('192.168.10.84', 'admin', 'moji_China_123','moge')
        cursor = db.cursor()
        origin = datetime.datetime.strftime(origintime, '%Y-%m-%d %H:%M:%S')
        forecast = datetime.datetime.strftime(foretime, '%Y-%m-%d %H:%M:%S')
        forecast_year = foretime.year
        forecast_month = foretime.month
        forecast_day = foretime.day
        forecast_hour = foretime.hour
        forecast_minute = foretime.minute
        timestr = datetime.datetime.strftime(origintime, '%Y%m%d%H%M%S')
        # csv = os.path.join(outpath, origin+'_'+forecast + '.csv')
        # csvfile = open(csv, 'w')
        sql = 'replace into t_r_ec_city_forecast_ele_mos_dem (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature,temp_max_6h,temp_min_6h,rainstate,precipitation)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        #print sql
        L = []
        for j in range(len(stationlist)):
            perstationlist = []
            stationid = stationlist[j][0]
            # 遍历降水预测中是否有该站点
            prevalue = 0
            for m in range(len(rainstationlist)):
                if stationid in rainstationlist[m]:
                    prevalue = preresult[m]
            temp = result[j]
            maxtemp = result01[j]
            mintemp = result02[j]
            rainstate = rainresult[j]
            # 每个站点存储
            perstationlist.append(stationid)
            perstationlist.append(origin)
            perstationlist.append(forecast)
            perstationlist.append(forecast_year)
            perstationlist.append(forecast_month)
            perstationlist.append(forecast_day)
            perstationlist.append(forecast_hour)
            perstationlist.append(temp)
            perstationlist.append(maxtemp)
            perstationlist.append(mintemp)
            perstationlist.append(rainstate)
            perstationlist.append(prevalue)
            L.append(perstationlist)
            # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
            # sql = 'insert into t_r_ec_city_forecast_ele_mos (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature,temp_max_6h,temp_min_6h,rainstate,precipitation)VALUES ("' + stationid + '","' + origin + '","' + str(
            #     forecast) + '","' + str(forecast_year) + '","' + str(
            #     forecast_month) + '","' + str(forecast_day) + '","' + str(
            #     forecast_hour) + '","' + str(temp) + '","' + str(maxtemp)+ '","' + str(mintemp)+'","' + str(rainstate)+'","' + str(prevalue)+ '")'
            # csvfile.write(stationid + '","' + origin + '","' + str(
            #     forecast) + '","' + str(forecast_year) + '","' + str(
            #     forecast_month) + '","' + str(forecast_day) + '","' + str(
            #     forecast_hour) + '","' + str(forecast_minute) + '","' + str(
            #     temp)+ '","' + str(maxtemp)+ '","' + str(mintemp)+'","' + str(rainstate)+'","' + str(prevalue))
            # csvfile.write('\n')
            # print sql
            # cursor.execute(sql)
        cursor.executemany(sql, L)
        db.commit()
        db.close()
        # csvfile.close()
        os.remove(outfilename)
        logger.info(outfilename)
    except Exception as e:
        logger.info(e.message)
    logger.removeHandler(file_handler)
# 不被6整除的时次，本来想写在一个函数里，但预报要素多，为了提高效率还是分开，后期可以再加3h的最高最低
def Predict2(hours,outfilename,previouspath, modelname, rainmodelfile, premodelfile,
             tempscalerfile, rainscalerfile, prescalerfile, origintime,
             foretime,outpath, csvfile, demcsv):
    try:
        logname = os.path.split(outfilename)[1][:-5] + '.log'
        # 多进程不支持写一个日志，所以每个进程一个日志
        logger = logging.getLogger('learing2.logger')
        # 指定logger输出格式
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
        # 文件日志learning
        logfile = os.path.join(outpath, logname)
        # logfile='/Users/yetao.lu/Desktop/mos/temp/loging.log'
        file_handler = logging.FileHandler(logfile)
        file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
        # 控制台日志
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter  # 也可以直接给formatter赋值
    
        # 为logger添加的日志处理器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
        # 指定日志的最低输出级别，默认为WARN级别
        logger.setLevel(logging.INFO)
        logger.info('hours='+str(hours)+';'+outfilename)
        # 取气温、最高气温、最低气温的训练训练矩阵。读文件费劲，连降水一起取了。晴雨、降水一块训
        tempvariablelist = []
        rainvariablelist = []
        stationlist = []
        calculateStationVariable2(hours,tempvariablelist, rainvariablelist,
                                  outfilename,previouspath, stationlist, csvfile, demcsv)
        # 加载训练模型
        params = {
            'booster': 'gbtree',
            'objective': 'reg:linear',  # 线性回归
            'gamma': 0.2,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
            'max_depth': 12,  # 构建树的深度，越大越容易过拟合
            'lambda': 2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
            'subsample': 0.7,  # 随机采样训练样本
            'colsample_bytree': 0.7,  # 生成树时进行的列采样
            'min_child_weight': 3,
            # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
            # ，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
            # 这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
            'silent': 0,  # 设置成1则没有运行信息输出，最好是设置为0.
            'eta': 0.02,  # 如同学习率
            'seed': 1000,
            # 'nthread': 3,  # cpu 线程数
            # 'eval_metric': 'auc'
            'scale_pos_weight': 1
        }
        bst = xgboost.Booster(params)
        bst.load_model(modelname)
        # 气温模型预测
        ecvaluelist = numpy.array(tempvariablelist)
        # ecvaluelist=ecvaluelist.astype('float64')
        #print 'ecvaluelist',ecvaluelist
        # 加载标准化预处理文件，对数据进行与模型一致的标准化
        scaler = joblib.load(tempscalerfile)
        # transform后必须重新复制，原来矩阵是不变的
        ecvaluelist_t = scaler.transform(ecvaluelist)
        #print ecvaluelist, ecvaluelist_t
        xgbtrain = xgboost.DMatrix(ecvaluelist_t)
        result = bst.predict(xgbtrain)
        #print 'result',result
        # 晴雨预测
        params_rain = {
            'booster': 'gbtree',
            'objective': 'multi:softmax',  # 分类
            'num_class': 3,  # 分3类
            'gamma': 0.2,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
            'max_depth': 12,  # 构建树的深度，越大越容易过拟合
            'lambda': 2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
            'subsample': 0.7,  # 随机采样训练样本
            'colsample_bytree': 0.7,  # 生成树时进行的列采样
            'min_child_weight': 3,
            # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
            # ，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
            # 这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
            'silent': 0,  # 设置成1则没有运行信息输出，最好是设置为0.
            'eta': 0.1,  # 如同学习率
            'seed': 1000,
            # 'nthread': 20,  # cpu 线程数
            # 'eval_metric': 'auc'
            'scale_pos_weight': 1
        }
        #print 'hello'
        #rainvariablelist=list(rainvariablelist)
        rainvariablelist=numpy.array(rainvariablelist)
        # print '===========\n'
        # print rainvariablelist
        # print rainvariablelist.shape
        # print type(rainvariablelist)
        rainbst = xgboost.Booster(params_rain)
        rainbst.load_model(rainmodelfile)
        rainscaler = joblib.load(rainscalerfile)
        #print'aaaaaaa',type(rainvariablelist)
        rainvariablelist_t = rainscaler.transform(rainvariablelist)
        #print 'bbbbbbbbb'
        raintrain = xgboost.DMatrix(rainvariablelist_t)
        rainresult = rainbst.predict(raintrain)
        # 降水预测：需要把有雨的站点挑出来进行降水预测
        params_pre = {
            'booster': 'gbtree',
            'objective': 'reg:linear',  # 线性回归
            'gamma': 0.2,  # 用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
            'max_depth': 12,  # 构建树的深度，越大越容易过拟合
            'lambda': 2,  # 控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
            'subsample': 0.7,  # 随机采样训练样本
            'colsample_bytree': 0.7,  # 生成树时进行的列采样
            'min_child_weight': 3,
            # 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
            # ，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
            # 这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。
            'silent': 0,  # 设置成1则没有运行信息输出，最好是设置为0.
            'eta': 0.1,  # 如同学习率
            'seed': 1000,
            # 'nthread': 20,  # cpu 线程数
            # 'eval_metric': 'auc'
            'scale_pos_weight': 1
        }

        # 挑选有雨的预报集
        rainstationlist = []
        prevariablelist = []
        for k in range(len(stationlist)):
            stationid = stationlist[k][0]
            rainstate = rainresult[k]
            if rainstate == 1:
                rainstationlist.append(stationlist[k])
                prevariablelist.append(rainvariablelist[k])
        prevariablelist=numpy.array(prevariablelist)
        prebst = xgboost.Booster(params_pre)
        prebst.load_model(premodelfile)
        prescaler = joblib.load(prescalerfile)
        prevariablelist_t = prescaler.transform(prevariablelist)
        pretrain = xgboost.DMatrix(prevariablelist_t)
        preresult = prebst.predict(pretrain)
        #print preresult
        # 预测结果入库
        db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',3307)
        #db = MySQLdb.connect('192.168.10.84', 'admin', 'moji_China_123','moge')
        cursor = db.cursor()
        origin = datetime.datetime.strftime(origintime, '%Y-%m-%d %H:%M:%S')
        forecast = datetime.datetime.strftime(foretime, '%Y-%m-%d %H:%M:%S')
        forecast_year = foretime.year
        forecast_month = foretime.month
        forecast_day = foretime.day
        forecast_hour = foretime.hour
        forecast_minute = foretime.minute
        timestr = datetime.datetime.strftime(origintime, '%Y%m%d%H%M%S')
        # csv = os.path.join(outpath,origin+'_'+forecast + '.csv')
        # csvfile = open(csv, 'w')
        sql = 'replace into t_r_ec_city_forecast_ele_mos_dem (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature,rainstate,precipitation)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        L = []
        for j in range(len(stationlist)):
            perstationlist = []
            stationid = stationlist[j][0]
            # 遍历降水预测中是否有该站点
            prevalue = 0
            for m in range(len(rainstationlist)):
                if stationid in rainstationlist[m]:
                    prevalue = preresult[m]
            temp = result[j]
            rainstate = rainresult[j]
            perstationlist.append(stationid)
            perstationlist.append(origin)
            perstationlist.append(forecast)
            perstationlist.append(forecast_year)
            perstationlist.append(forecast_month)
            perstationlist.append(forecast_day)
            perstationlist.append(forecast_hour)
            perstationlist.append(temp)
            perstationlist.append(rainstate)
            perstationlist.append(prevalue)
            L.append(perstationlist)
            # sql='insert into t_r_ec_mos_city_forecast_ele(city_id,initial_time,forecast_time,forecsat_year,forecast_month,forecast_day,forecast_hour,temperature)VALUES ()'
            # sql = 'insert into t_r_ec_city_forecast_ele_mos (city_id,initial_time,forecast_time,forecast_year,forecast_month,forecast_day,forecast_hour,temperature,rainstate,precipitation)VALUES ("' + stationid + '","' + origin + '","' + str(
            #     forecast) + '","' + str(forecast_year) + '","' + str(
            #     forecast_month) + '","' + str(forecast_day) + '","' + str(
            #     forecast_hour) + '","' + str(temp) +'","' + str(rainstate)+'","' + str(prevalue)+ '")'
            # csvfile.write(stationid + '","' + origin + '","' + str(
            #     forecast) + '","' + str(forecast_year) + '","' + str(
            #     forecast_month) + '","' + str(forecast_day) + '","' + str(
            #     forecast_hour) + '","' + str(forecast_minute) + '","' + str(
            #     temp)+'","' + str(rainstate)+'","' + str(prevalue))
            # csvfile.write('\n')
            # print sql
            # cursor.execute(sql)
        cursor.executemany(sql, L)
        db.commit()
        db.close()
        # csvfile.close()
        os.remove(outfilename)
        logger.info(outfilename)
    except Exception, e:
        logger.info(e.message)
    logger.removeHandler(file_handler)
if __name__ == "__main__":
    # 加日志
    logger = logging.getLogger('learing.logger')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    logfile = '/home/wlan_dev/lea.log'
    #logfile='/Users/yetao.lu/Desktop/mos/temp/loging.log'
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值
    
    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    starttime = datetime.datetime.now()
    # 遍历所有文件，预测历史数据
    # path = '/Users/yetao.lu/Desktop/mos/new'
    # outpath = '/Users/yetao.lu/Desktop/mos/temp'
    # csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
    # demcsv = '/Users/yetao.lu/Desktop/mos/dem.csv'
    # 遍历2867个站点
    path = '/home/wlan_dev/tmp'
    outpath = '/home/wlan_dev/result'
    csvfile = '/home/wlan_dev/stations.csv'
    demcsv = '/mnt/data/dem.csv'
    bz2list = []
    rootpath = ''
    pool=multiprocessing.Pool(processes=8)
    for root, dirs, files in os.walk(path):
        rootpath = root
        for file in files:
            if file[-4:] == '.bz2' and file[:3] == 'D1D':
                filename = os.path.join(root, file)
                bz2list.append(file)
    bz2list001 = sorted(bz2list)
    print bz2list001
    for i in range(len(bz2list001)):
        file = bz2list[i]
        bz2file = os.path.join(root, file)
        # 降水需要取前一个时次的文件
        start = file[3:9]
        end = file[11:17]
        print start, end
        start001 = str(starttime.year) + start
        end001 = str(starttime.year) + end
        if start001 > end001:
            end001 = str(starttime.year + 1) + end
        starttime = datetime.datetime.strptime(start001, '%Y%m%d%H')
        origintime = datetime.datetime.strptime(str(starttime.year) + start,
                                                '%Y%m%d%H')
        endtime = datetime.datetime.strptime(end001, '%Y%m%d%H')
        d = (endtime - starttime).days
        f = (endtime - starttime).seconds / 3600
        hours = (d * 24 + (endtime - starttime).seconds / 3600)
        logger.info('hours=' + str(hours))
        # 气温和降水的ID获取,把0排除掉了,如果hours<=3则降水不用减前一个时次的
        if hours <= 144 and hours > 0:
            id = hours / 3
        elif hours > 144:
            id = 48 + (hours - 144) / 6
        elif hours == 0:
            continue
        # 判断时间，取前一个时次文件
        previousfile = ''
        if i > 0:
            previousfile = bz2list001[i - 1]
            #print 'previousfile======'+previousfile
        else:
            previousfile = bz2list001[i]
        # 当前文件
        newfile = file[:-4] + '.grib'
        newfilepath = os.path.join(rootpath, newfile)
        # 上一个文件
        previousgribfile = previousfile[:-4] + '.grib'
        previouspath = os.path.join(rootpath, previousgribfile)
        logger.info(newfilepath+','+previouspath)
        if not os.path.exists(newfilepath) :
            a = bz2.BZ2File(bz2file, 'rb')
            b = open(newfilepath, 'wb')
            b.write(a.read())
            a.close()
            b.close()
        elif os.path.getsize(newfilepath)<100000:
            os.remove(newfilepath)
            a = bz2.BZ2File(bz2file, 'rb')
            b = open(newfilepath, 'wb')
            b.write(a.read())
            a.close()
            b.close()
        if not os.path.exists(previouspath) :
            a = bz2.BZ2File(bz2file, 'rb')
            b = open(previouspath, 'wb')
            b.write(a.read())
            a.close()
            b.close()
        elif os.path.getsize(previouspath)<100000:
            os.remove(previouspath)
            a = bz2.BZ2File(bz2file, 'rb')
            b = open(previouspath, 'wb')
            b.write(a.read())
            a.close()
            b.close()
        #filename的路径，这里是grib数据
        filename = newfilepath
        modelname = '/mnt/data/demtemp/tmodel' + str(id) + '.model'
        tempscalerfile = '/mnt/data/demtemp/tscale' + str(id) + '.save'
        rainmodelfile = '/mnt/data/xrain/x_rain' + str(id) + '.model'
        premodelfile = '/mnt/data/xrain/x_pre' + str(id) + '.model'
        rainscalerfile = '/mnt/data/xrain/x_rainscale' + str(id) + '.save'
        prescalerfile = '/mnt/data/xrain/x_prescale' + str(id) + '.save'
        # modelname = '/Users/yetao.lu/Desktop/mos/model/demtemp/tmodel2.model'
        # tempscalerfile = '/Users/yetao.lu/Desktop/mos/model/demtemp/tscale2.save'
        # rainmodelfile = '/Users/yetao.lu/Desktop/mos/model/xrain/x_rain' + str(id) + '.model'
        # premodelfile = '/Users/yetao.lu/Desktop/mos/model/xrain/x_pre' + str(id) + '.model'
        # rainscalerfile = '/Users/yetao.lu/Desktop/mos/model/xrain/x_rainscale' + str(id) + '.save'
        # prescalerfile = '/Users/yetao.lu/Desktop/mos/model/xrain/x_prescale' + str(id) + '.save'
        # 6小时最高气温和最低气温ID获取，判断数据里有没有6小时最高最低气温要素
        if hours % 6 == 0 and hours / 6 <> 0:
            # 初始场数据中也没有6小时预报
            j = hours / 6
            j = j - 1
            logger.info('j=' + str(j) + 'id=' + str(id) + 'hours=' + str(hours))
            maxmodel = '/mnt/data/demmaxmin/dem_maxtemp' + str(
                j) + '.model'
            minmodel = '/mnt/data/demmaxmin/dem_mintemp' + str(
                j) + '.model'
            maxscalerfile = '/mnt/data/demmaxmin/dem_maxscale' + str(
                j) + '.save'
            minscalerfile = '/mnt/data/demmaxmin/dem_minscale' + str(
                j) + '.save'
            # maxmodel = '/Users/yetao.lu/Desktop/mos/model/demmaxmin/demmax_temp0.model'
            # minmodel = '/Users/yetao.lu/Desktop/mos/model/demmaxmin/demmin_temp0.model'
            # maxscalerfile = '/Users/yetao.lu/Desktop/mos/model/demmaxmin/demscaler_max0.save'
            # minscalerfile = '/Users/yetao.lu/Desktop/mos/model/demmaxmin/demscaler_min0.save'
            # logger.info(filename)
            # Predict(hours,filename,previouspath, modelname, maxmodel, minmodel,
            #         rainmodelfile, premodelfile, tempscalerfile,
            #         maxscalerfile, minscalerfile, rainscalerfile,
            #         prescalerfile, origintime, endtime, outpath,
            #         csvfile,demcsv)
            # args= 不能省略
            pool.apply_async(Predict, args=(hours,
            filename,previouspath, modelname, maxmodel, minmodel,
            rainmodelfile, premodelfile, tempscalerfile,
            maxscalerfile, minscalerfile, rainscalerfile,
            prescalerfile, origintime, endtime, outpath,
            csvfile, demcsv))
        else:
            #Predict2(hours,filename,previouspath,modelname,rainmodelfile,premodelfile,tempscalerfile,rainscalerfile,prescalerfile, origintime, endtime,outpath,csvfile,demcsv)
            pool.apply_async(Predict2, args=(hours,
            filename,previouspath, modelname, rainmodelfile, premodelfile,
            tempscalerfile, rainscalerfile, prescalerfile, origintime,
            endtime,outpath, csvfile, demcsv))
    pool.close()
    pool.join()
    endtime = datetime.datetime.now()
    logger.info((endtime - starttime).seconds)
    
