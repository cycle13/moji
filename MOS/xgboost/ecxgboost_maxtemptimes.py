#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/7/24
description:
"""
import Nio, datetime, os, xgboost, numpy, math, string
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import accuracy_score
from sklearn import preprocessing
from sklearn.feature_selection import f_regression, mutual_info_regression
from osgeo import gdal
from decimal import Decimal
from sklearn.externals import joblib
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

def temp6hmaxminTodict(tempcsv):
    stationdict = {}
    for srootpath, sdirs, stationfile in os.walk(tempcsv):
        for i in range(len(stationfile)):
            if stationfile[i][-4:] == '.csv':
                stationfilepath = os.path.join(srootpath, stationfile[i])
                fileR = open(stationfilepath, 'r')
                while True:
                    line = fileR.readline()
                    linearray = line.split(',')
                    if len(linearray) > 4:
                        sdictId = linearray[0] + linearray[1]
                        pdatetime = datetime.datetime.strptime(linearray[1],
                                                               '%Y-%m-%d %H:%M:%S')
                        timestring = datetime.datetime.strftime(pdatetime,
                                                                '%Y%m%d%H%M%S')
                        sdictId = linearray[0] + '_' + timestring
                        stationdict[sdictId] = linearray
                    if not line:
                        break
    return stationdict
def GetOnetimeFromEC(n, max_varinames,max_varinames001,min_varinames,min_varinames001, h6file,h3file, sfc_varinames1, sfc_file,
                     pl_varinames, pl_file,
                     trainlabellist,maxecvaluelist,minecvaluelist,latitude,longitude,dempath):
    maxvstring = []
    minvstring=[]
    # #首先计算经纬度对应格点的索引，
    indexlat = int((60 - latitude) / 0.125)
    indexlon = int((longitude - 60) / 0.125)
    levelArray = pl_file.variables['lv_ISBL1']
    #n为1时，也就是第一个6小时预报，需要增加0时和3时的预报
    if n>1:
        #增加0时次也就是初始场的数据
        for i in range(len(max_varinames001)):
            variArray=h6file.variables[max_varinames001[i]]
            latlonArray=variArray[0]
            maxvstring.append((latlonArray[indexlat][indexlon]))
            maxvstring.append((latlonArray[indexlat][indexlon + 1]))
            maxvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
            maxvstring.append((latlonArray[indexlat + 1][indexlon]))
            maxvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
            maxvstring.append((latlonArray[indexlat - 1][indexlon]))
            maxvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
            maxvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
            maxvstring.append((latlonArray[indexlat][indexlon + 2]))
            maxvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
            maxvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
            maxvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
            maxvstring.append((latlonArray[indexlat + 2][indexlon]))
            maxvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
            maxvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
            maxvstring.append((latlonArray[indexlat][indexlon - 1]))
        for i in range(len(min_varinames001)):
            variArray = h6file.variables[min_varinames001[i]]
            latlonArray = variArray[0]
            minvstring.append((latlonArray[indexlat][indexlon]))
            minvstring.append((latlonArray[indexlat][indexlon + 1]))
            minvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
            minvstring.append((latlonArray[indexlat + 1][indexlon]))
            minvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
            minvstring.append((latlonArray[indexlat - 1][indexlon]))
            minvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
            minvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
            minvstring.append((latlonArray[indexlat][indexlon + 2]))
            minvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
            minvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
            minvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
            minvstring.append((latlonArray[indexlat + 2][indexlon]))
            minvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
            minvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
            minvstring.append((latlonArray[indexlat][indexlon - 1]))
    #0时次的其他因子
    mm=n*2-2
    for ii in range(len(sfc_varinames1)):
        variArray = sfc_file.variables[sfc_varinames1[ii]]
        # print len(variArray)
        latlonArray = variArray[mm]
        maxvstring.append((latlonArray[indexlat][indexlon]))
        maxvstring.append((latlonArray[indexlat][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat][indexlon - 1]))
        minvstring.append((latlonArray[indexlat][indexlon]))
        minvstring.append((latlonArray[indexlat][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon]))
        minvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        minvstring.append((latlonArray[indexlat - 1][indexlon]))
        minvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        minvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        minvstring.append((latlonArray[indexlat][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 2][indexlon]))
        minvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        minvstring.append((latlonArray[indexlat][indexlon - 1]))
    for j in range(len(pl_varinames)):
        pl_variArray = pl_file.variables[pl_varinames[j]]
        print(len(pl_variArray))
        phaArray = pl_variArray[mm]
        for k in range(len(phaArray)):
            llArray = phaArray[k]
            pha = levelArray[k]
            # print pha
            if pha == 500 or pha == 850:
                maxvstring.append(llArray[indexlat][indexlon])
                maxvstring.append(llArray[indexlat][indexlon + 1])
                maxvstring.append(llArray[indexlat + 1][indexlon + 1])
                maxvstring.append(llArray[indexlat + 1][indexlon])
                maxvstring.append(llArray[indexlat - 1][indexlon - 1])
                maxvstring.append(llArray[indexlat - 1][indexlon])
                maxvstring.append(llArray[indexlat - 1][indexlon + 1])
                maxvstring.append(llArray[indexlat - 1][indexlon + 2])
                maxvstring.append(llArray[indexlat][indexlon + 2])
                maxvstring.append(llArray[indexlat + 1][indexlon + 2])
                maxvstring.append(llArray[indexlat + 2][indexlon + 2])
                maxvstring.append(llArray[indexlat + 2][indexlon + 1])
                maxvstring.append(llArray[indexlat + 2][indexlon])
                maxvstring.append(llArray[indexlat + 2][indexlon - 1])
                maxvstring.append(llArray[indexlat + 1][indexlon - 1])
                maxvstring.append(llArray[indexlat][indexlon - 1])
                minvstring.append(llArray[indexlat][indexlon])
                minvstring.append(llArray[indexlat][indexlon + 1])
                minvstring.append(llArray[indexlat + 1][indexlon + 1])
                minvstring.append(llArray[indexlat + 1][indexlon])
                minvstring.append(llArray[indexlat - 1][indexlon - 1])
                minvstring.append(llArray[indexlat - 1][indexlon])
                minvstring.append(llArray[indexlat - 1][indexlon + 1])
                minvstring.append(llArray[indexlat - 1][indexlon + 2])
                minvstring.append(llArray[indexlat][indexlon + 2])
                minvstring.append(llArray[indexlat + 1][indexlon + 2])
                minvstring.append(llArray[indexlat + 2][indexlon + 2])
                minvstring.append(llArray[indexlat + 2][indexlon + 1])
                minvstring.append(llArray[indexlat + 2][indexlon])
                minvstring.append(llArray[indexlat + 2][indexlon - 1])
                minvstring.append(llArray[indexlat + 1][indexlon - 1])
                minvstring.append(llArray[indexlat][indexlon - 1])
    #增加3小时的数据
    for i in range(len(max_varinames)):
        variArray=h3file.variables[max_varinames[i]]
        latlonArray=variArray[0]
        maxvstring.append((latlonArray[indexlat][indexlon]))
        maxvstring.append((latlonArray[indexlat][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat][indexlon - 1]))
    for i in range(len(min_varinames)):
        variArray = h3file.variables[min_varinames[i]]
        latlonArray = variArray[0]
        minvstring.append((latlonArray[indexlat][indexlon]))
        minvstring.append((latlonArray[indexlat][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon]))
        minvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        minvstring.append((latlonArray[indexlat - 1][indexlon]))
        minvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        minvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        minvstring.append((latlonArray[indexlat][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 2][indexlon]))
        minvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        minvstring.append((latlonArray[indexlat][indexlon - 1]))
    #3小时的数据
    mm=n*2-1
    for ii in range(len(sfc_varinames1)):
        variArray = sfc_file.variables[sfc_varinames1[ii]]
        # print len(variArray)
        latlonArray = variArray[mm]
        maxvstring.append((latlonArray[indexlat][indexlon]))
        maxvstring.append((latlonArray[indexlat][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat][indexlon - 1]))
        minvstring.append((latlonArray[indexlat][indexlon]))
        minvstring.append((latlonArray[indexlat][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon]))
        minvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        minvstring.append((latlonArray[indexlat - 1][indexlon]))
        minvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        minvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        minvstring.append((latlonArray[indexlat][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 2][indexlon]))
        minvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        minvstring.append((latlonArray[indexlat][indexlon - 1]))
    for j in range(len(pl_varinames)):
        pl_variArray = pl_file.variables[pl_varinames[j]]
        print(len(pl_variArray))
        phaArray = pl_variArray[mm]
        for k in range(len(phaArray)):
            llArray = phaArray[k]
            pha = levelArray[k]
            # print pha
            if pha == 500 or pha == 850:
                maxvstring.append(llArray[indexlat][indexlon])
                maxvstring.append(llArray[indexlat][indexlon + 1])
                maxvstring.append(llArray[indexlat + 1][indexlon + 1])
                maxvstring.append(llArray[indexlat + 1][indexlon])
                maxvstring.append(llArray[indexlat - 1][indexlon - 1])
                maxvstring.append(llArray[indexlat - 1][indexlon])
                maxvstring.append(llArray[indexlat - 1][indexlon + 1])
                maxvstring.append(llArray[indexlat - 1][indexlon + 2])
                maxvstring.append(llArray[indexlat][indexlon + 2])
                maxvstring.append(llArray[indexlat + 1][indexlon + 2])
                maxvstring.append(llArray[indexlat + 2][indexlon + 2])
                maxvstring.append(llArray[indexlat + 2][indexlon + 1])
                maxvstring.append(llArray[indexlat + 2][indexlon])
                maxvstring.append(llArray[indexlat + 2][indexlon - 1])
                maxvstring.append(llArray[indexlat + 1][indexlon - 1])
                maxvstring.append(llArray[indexlat][indexlon - 1])
                minvstring.append(llArray[indexlat][indexlon])
                minvstring.append(llArray[indexlat][indexlon + 1])
                minvstring.append(llArray[indexlat + 1][indexlon + 1])
                minvstring.append(llArray[indexlat + 1][indexlon])
                minvstring.append(llArray[indexlat - 1][indexlon - 1])
                minvstring.append(llArray[indexlat - 1][indexlon])
                minvstring.append(llArray[indexlat - 1][indexlon + 1])
                minvstring.append(llArray[indexlat - 1][indexlon + 2])
                minvstring.append(llArray[indexlat][indexlon + 2])
                minvstring.append(llArray[indexlat + 1][indexlon + 2])
                minvstring.append(llArray[indexlat + 2][indexlon + 2])
                minvstring.append(llArray[indexlat + 2][indexlon + 1])
                minvstring.append(llArray[indexlat + 2][indexlon])
                minvstring.append(llArray[indexlat + 2][indexlon - 1])
                minvstring.append(llArray[indexlat + 1][indexlon - 1])
                minvstring.append(llArray[indexlat][indexlon - 1])
    for i in range(len(max_varinames)):
        variArray = h6file.variables[max_varinames[i]]
        #print len(variArray),n-1
        if n-1<0:
            return
        #注意这里的索引，外面是n里面是n-1,这样实况和预报数据才能对上
        latlonArray = variArray[n-1]
        maxvstring.append((latlonArray[indexlat][indexlon]))
        maxvstring.append((latlonArray[indexlat][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat][indexlon - 1]))
        minvstring.append((latlonArray[indexlat][indexlon]))
        minvstring.append((latlonArray[indexlat][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon]))
        minvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        minvstring.append((latlonArray[indexlat - 1][indexlon]))
        minvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        minvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        minvstring.append((latlonArray[indexlat][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 2][indexlon]))
        minvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        minvstring.append((latlonArray[indexlat][indexlon - 1]))
    # 影响因子
    # 这里气温、露点温度等是65维的数据，其预报时次和最高最低不是一样的索引，两者之间的关系
    # n<=24时，气温的索引n*2，最高气温的索引是n-1
    if n <= 24:
        mm = n * 2
    else:
        mm = (n - 24) + 48
    for ii in range(len(sfc_varinames1)):
        variArray = sfc_file.variables[sfc_varinames1[ii]]
        #print len(variArray)
        latlonArray = variArray[mm]
        maxvstring.append((latlonArray[indexlat][indexlon]))
        maxvstring.append((latlonArray[indexlat][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon]))
        maxvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        maxvstring.append((latlonArray[indexlat][indexlon - 1]))
        minvstring.append((latlonArray[indexlat][indexlon]))
        minvstring.append((latlonArray[indexlat][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon]))
        minvstring.append((latlonArray[indexlat - 1][indexlon - 1]))
        minvstring.append((latlonArray[indexlat - 1][indexlon]))
        minvstring.append((latlonArray[indexlat - 1][indexlon + 1]))
        minvstring.append((latlonArray[indexlat - 1][indexlon + 2]))
        minvstring.append((latlonArray[indexlat][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 1][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 2][indexlon + 2]))
        minvstring.append((latlonArray[indexlat + 2][indexlon + 1]))
        minvstring.append((latlonArray[indexlat + 2][indexlon]))
        minvstring.append((latlonArray[indexlat + 2][indexlon - 1]))
        minvstring.append((latlonArray[indexlat + 1][indexlon - 1]))
        minvstring.append((latlonArray[indexlat][indexlon - 1]))
    for j in range(len(pl_varinames)):
        pl_variArray = pl_file.variables[pl_varinames[j]]
        print(len(pl_variArray))
        phaArray = pl_variArray[mm]
        for k in range(len(phaArray)):
            llArray = phaArray[k]
            pha = levelArray[k]
            # print pha
            if pha == 500 or pha == 850:
                maxvstring.append(llArray[indexlat][indexlon])
                maxvstring.append(llArray[indexlat][indexlon + 1])
                maxvstring.append(llArray[indexlat + 1][indexlon + 1])
                maxvstring.append(llArray[indexlat + 1][indexlon])
                maxvstring.append(llArray[indexlat - 1][indexlon - 1])
                maxvstring.append(llArray[indexlat - 1][indexlon])
                maxvstring.append(llArray[indexlat - 1][indexlon + 1])
                maxvstring.append(llArray[indexlat - 1][indexlon + 2])
                maxvstring.append(llArray[indexlat][indexlon + 2])
                maxvstring.append(llArray[indexlat + 1][indexlon + 2])
                maxvstring.append(llArray[indexlat + 2][indexlon + 2])
                maxvstring.append(llArray[indexlat + 2][indexlon + 1])
                maxvstring.append(llArray[indexlat + 2][indexlon])
                maxvstring.append(llArray[indexlat + 2][indexlon - 1])
                maxvstring.append(llArray[indexlat + 1][indexlon - 1])
                maxvstring.append(llArray[indexlat][indexlon - 1])
                minvstring.append(llArray[indexlat][indexlon])
                minvstring.append(llArray[indexlat][indexlon + 1])
                minvstring.append(llArray[indexlat + 1][indexlon + 1])
                minvstring.append(llArray[indexlat + 1][indexlon])
                minvstring.append(llArray[indexlat - 1][indexlon - 1])
                minvstring.append(llArray[indexlat - 1][indexlon])
                minvstring.append(llArray[indexlat - 1][indexlon + 1])
                minvstring.append(llArray[indexlat - 1][indexlon + 2])
                minvstring.append(llArray[indexlat][indexlon + 2])
                minvstring.append(llArray[indexlat + 1][indexlon + 2])
                minvstring.append(llArray[indexlat + 2][indexlon + 2])
                minvstring.append(llArray[indexlat + 2][indexlon + 1])
                minvstring.append(llArray[indexlat + 2][indexlon])
                minvstring.append(llArray[indexlat + 2][indexlon - 1])
                minvstring.append(llArray[indexlat + 1][indexlon - 1])
                minvstring.append(llArray[indexlat][indexlon - 1])
    # 把站点经度纬度和高度加上
    maxvstring.append(trainlabellist[2])
    maxvstring.append(trainlabellist[3])
    maxvstring.append(trainlabellist[4])
    minvstring.append(trainlabellist[2])
    minvstring.append(trainlabellist[3])
    minvstring.append(trainlabellist[4])
    #增加DEM数据
    demdict = demdatefromcsvTodict(dempath)
    demlist=[]
    station_id=trainlabellist[0]
    demlist = demdict[station_id]
    for i in range(len(demlist)):
        maxvstring.append(demlist[i])
        minvstring.append(demlist[i])
    maxecvaluelist.append(maxvstring)
    minecvaluelist.append(minvstring)
def GetStationsAndOnetimesFromEC(i, max_varinames,max_varinames001,min_varinames,min_varinames001, h6file,h3file, sfc_varinames1,
                                 sfc_file, pl_varinames, pl_file, inputfile,
                                 maxecvaluelist,minecvaluelist, stationdict,
                                 stationlist, dict01, maxtemplist,
                                 mintemplist,dempath):
    for j in range(len(stationlist)):
        # 根据文件名来建立索引获取实况数据气温的值
        strarray = inputfile.split('_')
        odatetime = datetime.datetime.strptime((strarray[1] + strarray[2][:2]),'%Y%m%d%H')
        if i <= 40:
            fdatetime = odatetime + datetime.timedelta(hours=i * 6)
        fdateStr = datetime.datetime.strftime(fdatetime, '%Y%m%d%H%M%S')
        # 这里只能是起报时间加预报时效，不能用预报时间。因预报时间有重复
        dictid = stationlist[j][0] + '_' + strarray[1] + strarray[2][:2] + '_' + str(i)
        #根据站号+实况数据的索引来获取实况数据
        kid = stationlist[j][0] + '_' + fdateStr
        trainlebellist = stationdict.get(kid)
        #print trainlebellist
        if trainlebellist <> None:
            maxvalue = float(trainlebellist[5])
            minvalue = float(trainlebellist[6])
            # 判断该实况数据是否是有效值（不为99999或者None）,若有效再计算16个格点值，将其一起添加到训练样本
            if maxvalue < 9999 and minvalue < 9999:
                latitude = float(stationlist[j][1])
                longitude = float(stationlist[j][2])
                # 则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
                GetOnetimeFromEC(i, max_varinames,max_varinames001,min_varinames,min_varinames001,h6file,h3file,
                                                  sfc_varinames1, sfc_file,
                                                  pl_varinames, pl_file,maxecvaluelist,minecvaluelist,
                                                  trainlebellist,latitude,longitude,dempath)
                #根据站号和时间，对应预报数据和实况数据，形成矩阵和label标签
                maxtemplist.append(maxvalue)
                mintemplist.append(minvalue)
    #print 'ecvaluelist', len(ecvaluelist), 'maxtemplist', len(maxtemplist), 'mintemplist', len(mintemplist)
def modelprocess(stationdict, stationlist, ll, allpath, dempath,forehours):
    max_varinames = ['MX2T6_GDS0_SFC_1']
    max_varinames001=['MX2T6_GDS0_SFC']
    min_varinames = ['MN2T6_GDS0_SFC_1']
    min_varinames001=['MN2T6_GDS0_SFC']
    sfc_varinames1 = ['2T_GDS0_SFC', '2D_GDS0_SFC', '10U_GDS0_SFC',
        '10V_GDS0_SFC', 'TCC_GDS0_SFC', 'LCC_GDS0_SFC', 'Z_GDS0_SFC']
    pl_varinames = ['R_GDS0_ISBL']
    # print max_varinames
    dict01 = {}
    # 遍历文件
    maxecvaluelist = []
    minecvaluelist=[]
    maxtemplist = []
    mintemplist = []
    # 遍历所有的文件，
    for rootpath, dirs, files in os.walk(allpath):
        for file in files:
            # print file
            if file[:3] == 'sfc' and file[-5:] == '.grib' and (
                    string.find(file, '2017') == -1):
                inputfile = os.path.join(rootpath, file)
                inputfile2 = inputfile.replace('sfc', 'pl')
                inputfile3 = inputfile.replace('sfc', '6h')
                inputfile4=inputfile.replace('sfc','3h')
                sfcfile = Nio.open_file(inputfile, 'r')
                plfile = Nio.open_file(inputfile2, 'r')
                h6file = Nio.open_file(inputfile3, 'r')
                h3file=Nio.open_file(inputfile4,'r')
                # 参数0是指第0个时次的预报,这里只是一个文件的2000个站的列表。
                GetStationsAndOnetimesFromEC(ll, max_varinames,max_varinames001,min_varinames,min_varinames001, h6file,h3file,
                                             sfc_varinames1, sfcfile,
                                             pl_varinames, plfile, inputfile,
                                             maxecvaluelist,minecvaluelist, stationdict,
                                             stationlist, dict01,
                                             maxtemplist,
                                             mintemplist, dempath)
    #ecvaluelist = numpy.array(ecvaluelist)
    maxtemplist = numpy.array(maxtemplist)
    mintemplist = numpy.array(mintemplist)
    # class_train, class_test = train_test_split(ecvaluelist, test_size=0.33,random_state=7)
    # 数据训练前进行标准化
    #ecvaluelist = ecvaluelist.astype('float32')
    maxtemplist = maxtemplist.astype('float32')
    mintemplist = mintemplist.astype('float32')
    # print ecvaluelist.shape, maxtemplist.shape
    # x_scaled = preprocessing.scale(ecvaluelist)
    # ecvaluelist = x_scaled
    # max、min分开
    #ecvaluelist = numpy.array(ecvaluelist)
    maxecvaluelist=ecvaluelist
    # print minecvaluelist.shape
    # 为了统计准确率，提前分割数据集:这里只是为了计算训练前的准确率，训练后的准确率计算用不到这个分割；
    max_train, max_test,min_train,min_test = train_test_split(maxtemplist,mintemplist,test_size=0.33,random_state=7)
    # 数据集标准化
    x_scaler = preprocessing.StandardScaler().fit(maxecvaluelist)
    max_scaler = x_scaler.transform(maxecvaluelist)
    maxecvaluelist = max_scaler
    maxscaler_file = os.path.join(outpath, 'dem_maxscale' + str(forehours) + '.save')
    joblib.dump(x_scaler, maxscaler_file)
    # y_scaler = preprocessing.StandardScaler().fit(minecvaluelist)
    # min_scaler = y_scaler.transform(minecvaluelist)
    # minecvaluelist = min_scaler
    # minscaler_file = os.path.join(outpath, 'dem_minscale' + str(forehours) + '.save')
    # joblib.dump(y_scaler, minscaler_file)
    # xgboost，训练集和预测集分割：这里已经是标准化完的矩阵；
    x_train, x_test, y_train, y_test = train_test_split(maxecvaluelist,
                                                        maxtemplist,
                                                        test_size=0.33,
                                                        random_state=7)
    # u_train, u_test, v_train, v_test = train_test_split(minecvaluelist,
    #                                                     mintemplist,
    #                                                     test_size=0.33,
    #                                                     random_state=7)
    xgbtrain = xgboost.DMatrix(x_train, label=y_train)
    xgbtest = xgboost.DMatrix(x_test, label=y_test)
    # xgbtrain01 = xgboost.DMatrix(u_train, label=v_train)
    # xgbtest01 = xgboost.DMatrix(u_test, label=v_test)
    # xgbtrain.save_binary('train.buffer')
    # 特征选址
    # print x_train.shape, x_test.shape
    ff, pp = f_regression(x_train, y_train)
    # print ff, pp
    # 训练和验证的错误率
    watchlist = [(xgbtrain, 'xgbtrain'), (xgbtest, 'xgbeval')]
    watchlist01 = [(xgbtrain01, 'xgbtrain01'), (xgbtest01, 'xgbeval01')]
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
        'eta': 0.1,  # 如同学习率
        'seed': 1000,
        # 'nthread': 3,  # cpu 线程数
        # 'eval_metric': 'auc'
        'scale_pos_weight': 1
    }
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
    plst = list(params.items())
    plst01 = list(params01.items())
    num_rounds = 99999
    # early_stopping_rounds当设置的迭代次数较大时,early_stopping_rounds 可在一定的迭代次数内准确率没有提升就停止训练
    model = xgboost.train(plst, xgbtrain, num_rounds, watchlist,
                          early_stopping_rounds=500)
    model01 = xgboost.train(plst01, xgbtrain01, num_rounds, watchlist01,
                            early_stopping_rounds=500)
    # print model,watchlist
    preds = model.predict(xgbtest, ntree_limit=model.best_iteration)
    #preds01 = model01.predict(xgbtest01, ntree_limit=model.best_iteration)
    model.save_model(os.path.join(outpath, 'dem_maxtemp' + str(forehours) + '.model'))
    #model01.save_model(os.path.join(outpath, 'dem_mintemp' + str(forehours) + '.model'))
    # print preds, preds01
    # 训练后高温的RMSE，MAE
    mse = mean_squared_error(y_test, preds)
    rmse = math.sqrt(mse)
    mae = mean_absolute_error(y_test, preds, multioutput='uniform_average')
    bias = numpy.mean(y_test - preds)
    print("训练后最高气温MSE: %.4f" % mse)
    print("训练后最高气温RMSE: %.4f" % rmse)
    print("训练后最高气温MAE: %.4f" % mae)
    print("训练后最高气温bias: %.4f" % bias)
    # 训练后最高气温2度以内的准确率
    n = 0
    for x, y in zip(y_test, preds):
        if abs(x - y) < 2:
            n = n + 1
    accuracy2_after = float(n) / float(len(y_test))
    print ("训练后最高气温2度的accuracy: %.4f" % accuracy2_after)
    n = 0
    for x, y in zip(y_test, preds):
        if abs(x - y) < 3:
            n = n + 1
    accuracy3_after = float(n) / float(len(y_test))
    print ("训练后最高气温3度的accuracy: %.4f" % accuracy3_after)
    # 训练前高温的RMSE,MAE
    class_test = max_test.astype('float64')
    max_ec = (class_test[:, 0]) - 273.15
    mse0 = mean_squared_error(max_ec, y_test)
    rmse0 = math.sqrt(mse0)
    mae0 = mean_absolute_error(max_ec, y_test, multioutput='uniform_average')
    bias0 = numpy.mean(y_test - max_ec)
    print("训练前最高气温MSE: %.4f" % mse0)
    print("训练前最高气温RMSE: %.4f" % rmse0)
    print("训练前最高气温MAE: %.4f" % mae0)
    print("训练前最高气温BIAS: %.4f" % bias0)
    n = 0
    for x, y in zip(y_test, max_ec):
        if abs(x - y) < 2:
            n = n + 1
    accuracy2_before = float(n) / float(len(y_test))
    print ("训练前最高气温2度的accuracy: %.4f" % accuracy2_before)
    n = 0
    for x, y in zip(y_test, max_ec):
        if abs(x - y) < 3:
            n = n + 1
    accuracy3_before = float(n) / float(len(y_test))
    print ("训练前最高气温3度的accuracy: %.4f" % accuracy3_before)
    # 训练后低温的RMSE|MAE
    min_mse = mean_squared_error(v_test, preds01)
    min_rmse = math.sqrt(min_mse)
    min_mae = mean_absolute_error(v_test, preds01,
                                  multioutput='uniform_average')
    min_bias = numpy.mean(v_test - preds01)
    print("训练后最低气温MSE: %.4f" % min_mse)
    print("训练后最低气温RMSE: %.4f" % min_rmse)
    print("训练后最低气温MAE: %.4f" % min_mae)
    print("训练后最低气温BIAS: %.4f" % min_bias)
    # 训练后最低气温2度以内的准确率
    n = 0
    for x, y in zip(v_test, preds01):
        if abs(x - y) < 2:
            n = n + 1
    min_accuracy2_after = float(n) / float(len(v_test))
    print ("训练后最低气温2度的accuracy: %.4f" % min_accuracy2_after)
    n = 0
    for x, y in zip(v_test, preds01):
        if abs(x - y) < 3:
            n = n + 1
    min_accuracy3_after = float(n) / float(len(v_test))
    print ("训练后最低气温3度的accuracy: %.4f" % min_accuracy3_after)
    # 训练前低温的RMSE,MAE
    min_ec = min_test[:, 0] - 273.15
    min_mse0 = mean_squared_error(min_ec, v_test)
    min_rmse0 = math.sqrt(min_mse0)
    min_mae0 = mean_absolute_error(min_ec, v_test,
                                   multioutput='uniform_average')
    min_bias0 = numpy.mean(v_test - min_ec)
    print("训练前最低气温MSE: %.4f" % min_mse0)
    print("训练前最低气温RMSE: %.4f" % min_rmse0)
    print("训练前最低气温MAE: %.4f" % min_mae0)
    print("训练前最低气温BIAS: %.4f" % min_bias0)
    n = 0
    for x, y in zip(v_test, min_ec):
        if abs(x - y) < 2:
            n = n + 1
    min_accuracy2_before = float(n) / float(len(v_test))
    print ("训练前最低气温2度的accuracy: %.4f" % min_accuracy2_before)
    n = 0
    for x, y in zip(v_test, min_ec):
        if abs(x - y) < 3:
            n = n + 1
    min_accuracy3_before = float(n) / float(len(v_test))
    print ("训练前最低气温3度的accuracy: %.4f" % min_accuracy3_before)
    print  str(rmse) + ',' + str(mae) + ',' + str(accuracy2_after) + ',' + str(
        accuracy3_after) + ',' + str(rmse0) + ',' + str(mae0) + ',' + str(
        accuracy2_before) + ',' + str(accuracy3_before) + ',' + str(
        bias) + ',' + str(bias0) + ',' + str(
        min_rmse) + ',' + str(min_mae) + ',' + str(
        min_accuracy2_after) + ',' + str(min_accuracy3_after) + ',' + str(
        min_rmse0) + ',' + str(min_mae0) + ',' + str(
        min_accuracy2_before) + ',' + str(min_accuracy3_before) + ',' + str(
        min_bias) + ',' + str(min_bias0)
    maxfile = os.path.join(outpath, 'demmaxt' + str(ll) + '.csv')
    maxtfw = open(maxfile, 'w')
    for pp in range(len(y_test)):
        maxtfw.write(
            str(max_ec[pp]) + ',' + str(preds[pp]) + ',' + str(y_test[pp]))
        maxtfw.write('\n')
    maxtfw.close()
    # 输出最低气温
    minfile = os.path.join(outpath, 'demmint' + str(ll) + '.csv')
    mintfw = open(minfile, 'w')
    for qq in range(len(v_test)):
        mintfw.write(
            str(min_ec[qq]) + ',' + str(preds01[qq]) + ',' + str(v_test[qq]))
        mintfw.write('\n')
    mintfw.close()
if __name__ == "__main__":
    starttime = datetime.datetime.now()
    outpath = '/Users/yetao.lu/Desktop/mos/anonymous'
    #outpath = '/home/wlan_dev/model'
    ll = 1
    stationdict = {}
    # 站点列表数据
    stationlist = []
    csvfile = '/Users/yetao.lu/Desktop/mos/stations.csv'
    #csvfile = '/home/wlan_dev/stations.csv'
    fileread = open(csvfile, 'r')
    firstline = fileread.readline()
    while True:
        line = fileread.readline()
        perlist = line.split(',')
        if len(perlist) >= 4:
            stationlist.append(perlist)
        if not line or line == '':
            break
    # print stationlist
    # 处理站点6小时最高气温、最低气温实况数据
    maxmin6hcsv = '/Users/yetao.lu/Desktop/mos/data/temmaxmin6h'
    #maxmin6hcsv = '/home/wlan_dev/temmaxmin6h'
    stationdict = temp6hmaxminTodict(maxmin6hcsv)
    #print 'dict', len(stationdict)
    allpath = '/Users/yetao.lu/Desktop/mos/testdata'
    #allpath = '/mnt/data/MOS/'
    forehours=ll*6
    demcsvfile='/Users/yetao.lu/Desktop/mos/dem.csv'
    modelprocess(stationdict, stationlist, ll, allpath,demcsvfile,forehours)
    endtime = datetime.datetime.now()
    # print len(dict)
    print(endtime - starttime).seconds

