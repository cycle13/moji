#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2017/12/28
description:
"""
import Nio,datetime
inputfile='/Users/yetao.lu/Documents/mosdata/sfc_20150601_12.grib'
inputfile2='/Users/yetao.lu/Documents/mosdata/pl_20150601_12.grib'
file=Nio.open_file(inputfile,'r')
names=file.variables.keys()
fnames=file.variables.values()
t=getattr(file.variables[names[1]],'initial_time')
odatetime=datetime.datetime.strptime(t,'%m/%d/%Y (%H:%M)')
#print odatetime
# for attrib in file.variables[names[1]].attributes.keys():
#     print attrib
#     t=getattr(file.variables[names[1]],'initial_time')
#     print  t
# tempArray=file.variables['2T_GDS0_SFC']
# tempa=tempArray[:]
# for i in range(len(tempArray)):
#     latlonArray=tempArray[i]
#     for j in range(len(latlonArray)):
#         lonarray=latlonArray[j]
#         for k in range(len(lonarray)):
#             perlonarray=lonarray[k]
#             print k,perlonarray[0],perlonarray[len(perlonarray)-1]
#variables:['SP_GDS0_SFC', 'Z_GDS0_SFC', 'TCC_GDS0_SFC', 'g0_lat_1', 'SD_GDS0_SFC', '10V_GDS0_SFC', 'TP_GDS0_SFC', '2D_GDS0_SFC', 'MSL_GDS0_SFC', 'HCC_GDS0_SFC', 'CP_GDS0_SFC', 'MCC_GDS0_SFC', 'forecast_time0', '10U_GDS0_SFC', '2T_GDS0_SFC', 'SSTK_GDS0_SFC', 'g0_lon_2', 'LCC_GDS0_SFC', 'SKT_GDS0_SFC']
#按照该顺序设置变量顺序，除去时间、纬度、经度三个变量(维度)
def calculateRaster(inputfile,longitude,latitude):
    file = Nio.open_file(inputfile, 'r')
    names = file.variables.keys()
    varinames=['SP_GDS0_SFC', 'Z_GDS0_SFC', 'TCC_GDS0_SFC', 'SD_GDS0_SFC', '10V_GDS0_SFC', 'TP_GDS0_SFC', '2D_GDS0_SFC', 'MSL_GDS0_SFC', 'HCC_GDS0_SFC', 'CP_GDS0_SFC', 'MCC_GDS0_SFC', '10U_GDS0_SFC', '2T_GDS0_SFC', 'SSTK_GDS0_SFC', 'LCC_GDS0_SFC', 'SKT_GDS0_SFC']
    #首先计算经纬度对应格点的索引，
    indexlat=int((60-latitude)/0.125)
    indexlon=int((longitude-60)/0.125)
    print indexlat,indexlon
    #则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
    vstring=[]
    timeArray=file.variables['forecast_time0']
    # for t in range(len(timeArray)):
    #     hh=int(timeArray[t])
    #     pdatetime=odatetime+datetime.timedelta(hours=hh)
    for i in range(len(varinames)):
        #print varinames[i]
        vstring.append(varinames[i])
        parray=file.variables[varinames[i]]
        for j in range(len(parray)):
            #print j,'65'
            hh=int(timeArray[j])
            pdatetime=odatetime+datetime.timedelta(hours=hh)
            vstring.append(pdatetime)
            latlonArray=parray[j]
            vstring.append(latlonArray[indexlat][indexlon])
            vstring.append(latlonArray[indexlat][indexlon + 1])
            vstring.append(latlonArray[indexlat+1][indexlon + 1])
            vstring.append(latlonArray[indexlat+1][indexlon])
            vstring.append(latlonArray[indexlat-1][indexlon - 1])
            vstring.append(latlonArray[indexlat-1][indexlon])
            vstring.append(latlonArray[indexlat-1][indexlon + 1])
            vstring.append(latlonArray[indexlat-1][indexlon + 2])
            vstring.append(latlonArray[indexlat][indexlon + 2])
            vstring.append(latlonArray[indexlat+1][indexlon + 2])
            vstring.append(latlonArray[indexlat+2][indexlon + 2])
            vstring.append(latlonArray[indexlat+2][indexlon + 1])
            vstring.append(latlonArray[indexlat+2][indexlon])
            vstring.append(latlonArray[indexlat+2][indexlon - 1])
            vstring.append(latlonArray[indexlat+1][indexlon - 1])
            vstring.append(latlonArray[indexlat][indexlon - 1])
    #循环遍历vsting
    vdescirbe=''
    for p in range(len(vstring)):
        vdescirbe=vdescirbe+','+str(vstring[p])
    print vdescirbe
    del vstring
    del latlonArray
#test
#calculateRaster(inputfile,80,30)
#写成dictionary，要素和时间为key,16个要素的列表为value
def calculateRasterTodict(inputfile,longitude,latitude):
    file = Nio.open_file(inputfile, 'r')
    names = file.variables.keys()
    varinames=['SP_GDS0_SFC', 'Z_GDS0_SFC', 'TCC_GDS0_SFC', 'SD_GDS0_SFC', '10V_GDS0_SFC', 'TP_GDS0_SFC', '2D_GDS0_SFC', 'MSL_GDS0_SFC', 'HCC_GDS0_SFC', 'CP_GDS0_SFC', 'MCC_GDS0_SFC', '10U_GDS0_SFC', '2T_GDS0_SFC', 'SSTK_GDS0_SFC', 'LCC_GDS0_SFC', 'SKT_GDS0_SFC']
    #首先计算经纬度对应格点的索引，
    indexlat=int((60-latitude)/0.125)
    indexlon=int((longitude-60)/0.125)
    #则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
    timeArray=file.variables['forecast_time0']
    featuredic={}
    for i in range(len(varinames)):
        parray=file.variables[varinames[i]]
        for j in range(len(parray)):
            arraylist=[]
            #print j,'65'
            hh=int(timeArray[j])
            pdatetime=odatetime+datetime.timedelta(hours=hh)
            timestr=datetime.datetime.strftime(pdatetime,'%Y%m%d %H:%M:%S')
            varikey=varinames[i]+'_'+timestr
            latlonArray=parray[j]
            arraylist.append(latlonArray[indexlat][indexlon])
            arraylist.append(latlonArray[indexlat][indexlon + 1])
            arraylist.append(latlonArray[indexlat+1][indexlon + 1])
            arraylist.append(latlonArray[indexlat+1][indexlon])
            arraylist.append(latlonArray[indexlat-1][indexlon - 1])
            arraylist.append(latlonArray[indexlat-1][indexlon])
            arraylist.append(latlonArray[indexlat-1][indexlon + 1])
            arraylist.append(latlonArray[indexlat-1][indexlon + 2])
            arraylist.append(latlonArray[indexlat][indexlon + 2])
            arraylist.append(latlonArray[indexlat+2][indexlon + 2])
            arraylist.append(latlonArray[indexlat+2][indexlon + 1])
            arraylist.append(latlonArray[indexlat+2][indexlon])
            arraylist.append(latlonArray[indexlat+2][indexlon - 1])
            arraylist.append(latlonArray[indexlat+1][indexlon - 1])
            arraylist.append(latlonArray[indexlat][indexlon - 1])
            featuredic[varikey]=arraylist
    return featuredic
#calculateRasterTodict(inputfile,90,15)
#给经纬度列表
# csvfile='/Users/yetao.lu/Desktop/mos/t_p_station_cod.csv'
# fileread=open(csvfile,'r')
# stationlist=[]
# firstline=fileread.readline()
# while True:
#     line=fileread.readline()
#     perlist=line.split(',')
#     print perlist
#     dic=calculateRasterTodict(inputfile,float(perlist[5]),float(perlist[4]))
#     stationlist.append(dic)
#     if not line or line=='':
#         break
# print stationlist
def calculateRaster2(inputfile,inputfile2,longitude,latitude):
    file = Nio.open_file(inputfile, 'r')
    names = file.variables.keys()
    #varinames1=['SP_GDS0_SFC', 'Z_GDS0_SFC', 'TCC_GDS0_SFC', 'SD_GDS0_SFC', '10V_GDS0_SFC', 'TP_GDS0_SFC', '2D_GDS0_SFC', 'MSL_GDS0_SFC', 'HCC_GDS0_SFC', 'CP_GDS0_SFC', 'MCC_GDS0_SFC', '10U_GDS0_SFC', '2T_GDS0_SFC', 'SSTK_GDS0_SFC', 'LCC_GDS0_SFC', 'SKT_GDS0_SFC']
    varinames=['2T_GDS0_SFC','2D_GDS0_SFC','10U_GDS0_SFC','10V_GDS0_SFC','TCC_GDS0_SFC','LCC_GDS0_SFC','HCC_GDS0_SFC','MCC_GDS0_SFC','TP_GDS0_SFC','CP_GDS0_SFC']
    #首先计算经纬度对应格点的索引，
    indexlat=int((60-latitude)/0.125)
    indexlon=int((longitude-60)/0.125)
    #则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
    vstring=[]
    timeArray=file.variables['forecast_time0']
    # for t in range(len(timeArray)):
    #     hh=int(timeArray[t])
    #     pdatetime=odatetime+datetime.timedelta(hours=hh)
    for i in range(len(varinames)):
        vstring.append(varinames[i])
        parray=file.variables[varinames[i]]
        for j in range(len(parray)):
            hh=int(timeArray[j])
            pdatetime=odatetime+datetime.timedelta(hours=hh)
            vstring.append(pdatetime)
            latlonArray=parray[j]
            vstring.append(latlonArray[indexlat][indexlon])
            vstring.append(latlonArray[indexlat][indexlon + 1])
            vstring.append(latlonArray[indexlat+1][indexlon + 1])
            vstring.append(latlonArray[indexlat+1][indexlon])
            vstring.append(latlonArray[indexlat-1][indexlon - 1])
            vstring.append(latlonArray[indexlat-1][indexlon])
            vstring.append(latlonArray[indexlat-1][indexlon + 1])
            vstring.append(latlonArray[indexlat-1][indexlon + 2])
            vstring.append(latlonArray[indexlat][indexlon + 2])
            vstring.append(latlonArray[indexlat+1][indexlon + 2])
            vstring.append(latlonArray[indexlat+2][indexlon + 2])
            vstring.append(latlonArray[indexlat+2][indexlon + 1])
            vstring.append(latlonArray[indexlat+2][indexlon])
            vstring.append(latlonArray[indexlat+2][indexlon - 1])
            vstring.append(latlonArray[indexlat+1][indexlon - 1])
            vstring.append(latlonArray[indexlat][indexlon - 1])
    #读高空的文件获取500hpa和850hpa的相对湿度
    gribfile=Nio.open_file(inputfile2,'r')
    names2=gribfile.variables.keys()
    print names2
    variablenames=['R_GDS0_ISBL']
    tt = getattr(gribfile.variables[names2[1]], 'initial_time')
    oodatetime=datetime.datetime.strptime(tt,'%m/%d/%Y (%H:%M)')
    timeArray = gribfile.variables['forecast_time0']
    levelArray=gribfile.variables['lv_ISBL1']
    print levelArray
    for i in range(len(variablenames)):
        variableArray=gribfile.variables[variablenames[i]]
        vstring.append(variablenames[i])
        for j in range(len(variableArray)):
            pArray=variableArray[j]
            hh=int(timeArray[j])
            pdatetime=oodatetime+datetime.timedelta(hours=hh)
            vstring.append(pdatetime)
            for k in range(len(pArray)):
                phaArray=pArray[k]
                llArray=phaArray
                pha=levelArray[k]
                vstring.append(str(pha)+'hpa')
                vstring.append(llArray[indexlat][indexlon])
                vstring.append(llArray[indexlat][indexlon + 1])
                vstring.append(llArray[indexlat + 1][indexlon + 1])
                vstring.append(llArray[indexlat + 1][indexlon])
                vstring.append(llArray[indexlat - 1][indexlon - 1])
                vstring.append(llArray[indexlat - 1][indexlon])
                vstring.append(llArray[indexlat - 1][indexlon + 1])
                vstring.append(llArray[indexlat - 1][indexlon + 2])
                vstring.append(llArray[indexlat][indexlon + 2])
                vstring.append(llArray[indexlat + 1][indexlon + 2])
                vstring.append(llArray[indexlat + 2][indexlon + 2])
                vstring.append(llArray[indexlat + 2][indexlon + 1])
                vstring.append(llArray[indexlat + 2][indexlon])
                vstring.append(llArray[indexlat + 2][indexlon - 1])
                vstring.append(llArray[indexlat + 1][indexlon - 1])
                vstring.append(llArray[indexlat][indexlon - 1])
    #循环遍历vsting
    vdescirbe=''
    for p in range(len(vstring)):
        vdescirbe=vdescirbe+','+str(vstring[p])
    print vdescirbe
    del vstring
    del latlonArray
#calculateRaster2(inputfile,inputfile2,80,30)
def calculateRasterTodict2(inputfile,inputfile2,longitude,latitude):
    file = Nio.open_file(inputfile, 'r')
    names = file.variables.keys()
    varinames=['SP_GDS0_SFC', 'Z_GDS0_SFC', 'TCC_GDS0_SFC', 'SD_GDS0_SFC', '10V_GDS0_SFC', 'TP_GDS0_SFC', '2D_GDS0_SFC', 'MSL_GDS0_SFC', 'HCC_GDS0_SFC', 'CP_GDS0_SFC', 'MCC_GDS0_SFC', '10U_GDS0_SFC', '2T_GDS0_SFC', 'SSTK_GDS0_SFC', 'LCC_GDS0_SFC', 'SKT_GDS0_SFC']
    #首先计算经纬度对应格点的索引，
    indexlat=int((60-latitude)/0.125)
    indexlon=int((longitude-60)/0.125)
    #则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
    timeArray=file.variables['forecast_time0']
    featuredic={}
    for i in range(len(varinames)):
        parray=file.variables[varinames[i]]
        print parray.shape
        for j in range(len(parray)):
            arraylist=[]
            #print j,'65'
            hh=int(timeArray[j])
            pdatetime=odatetime+datetime.timedelta(hours=hh)
            timestr=datetime.datetime.strftime(pdatetime,'%Y%m%d %H:%M:%S')
            varikey=varinames[i]+'_'+timestr
            latlonArray=parray[j]
            arraylist.append(latlonArray[indexlat][indexlon])
            arraylist.append(latlonArray[indexlat][indexlon + 1])
            arraylist.append(latlonArray[indexlat+1][indexlon + 1])
            arraylist.append(latlonArray[indexlat+1][indexlon])
            arraylist.append(latlonArray[indexlat-1][indexlon - 1])
            arraylist.append(latlonArray[indexlat-1][indexlon])
            arraylist.append(latlonArray[indexlat-1][indexlon + 1])
            arraylist.append(latlonArray[indexlat-1][indexlon + 2])
            arraylist.append(latlonArray[indexlat][indexlon + 2])
            arraylist.append(latlonArray[indexlat+2][indexlon + 2])
            arraylist.append(latlonArray[indexlat+2][indexlon + 1])
            arraylist.append(latlonArray[indexlat+2][indexlon])
            arraylist.append(latlonArray[indexlat+2][indexlon - 1])
            arraylist.append(latlonArray[indexlat+1][indexlon - 1])
            arraylist.append(latlonArray[indexlat][indexlon - 1])
            featuredic[varikey]=arraylist
    # 读高空的文件获取500hpa和850hpa的相对湿度
    gribfile = Nio.open_file(inputfile2, 'r')
    names2 = gribfile.variables.keys()
    print names2
    variablenames = ['R_GDS0_ISBL']
    tt = getattr(gribfile.variables[names2[1]], 'initial_time')
    oodatetime = datetime.datetime.strptime(tt, '%m/%d/%Y (%H:%M)')
    timeArray = gribfile.variables['forecast_time0']
    levelArray = gribfile.variables['lv_ISBL1']
    #print levelArray
    for i in range(len(variablenames)):
        variableArray = gribfile.variables[variablenames[i]]
        for j in range(len(variableArray)):
            pArray = variableArray[j]
            hh = int(timeArray[j])
            pdatetime = oodatetime + datetime.timedelta(hours=hh)
            timestring=datetime.datetime.strftime(pdatetime,'%Y%m%d %H:%M:%S')
            for k in range(len(pArray)):
                phalist=[]
                phaArray = pArray[k]
                llArray = phaArray
                pha = levelArray[k]
                fkey=str(variablenames[i])+'_'+timestring+'_'+str(pha)+'hpa'
                phalist.append(llArray[indexlat][indexlon])
                phalist.append(llArray[indexlat][indexlon + 1])
                phalist.append(llArray[indexlat + 1][indexlon + 1])
                phalist.append(llArray[indexlat + 1][indexlon])
                phalist.append(llArray[indexlat - 1][indexlon - 1])
                phalist.append(llArray[indexlat - 1][indexlon])
                phalist.append(llArray[indexlat - 1][indexlon + 1])
                phalist.append(llArray[indexlat - 1][indexlon + 2])
                phalist.append(llArray[indexlat][indexlon + 2])
                phalist.append(llArray[indexlat + 1][indexlon + 2])
                phalist.append(llArray[indexlat + 2][indexlon + 2])
                phalist.append(llArray[indexlat + 2][indexlon + 1])
                phalist.append(llArray[indexlat + 2][indexlon])
                phalist.append(llArray[indexlat + 2][indexlon - 1])
                phalist.append(llArray[indexlat + 1][indexlon - 1])
                phalist.append(llArray[indexlat][indexlon - 1])
                featuredic[fkey]=phalist
    print featuredic
calculateRasterTodict2(inputfile,inputfile2,120,70)
