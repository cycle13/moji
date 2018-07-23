#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/30
description:
"""
import Nio,datetime,os
allpath='/Users/yetao.lu/Documents/mosdata'
#遍历文件
inputfile=''
inputfile2=''
for rootpath,dirs,files in os.walk(allpath):
    for file in files:
        #print file[:3],file,file[-5:]
        if file[:3]=='sfc' and file[-5:]=='.grib':
            inputfile=os.path.join(rootpath,file)
            print inputfile
        elif file[:2]=='pl' and file[-5:]=='.grib':
            inputfile2=os.path.join(rootpath,file)
            print inputfile2
file=Nio.open_file(inputfile,'r')
names=file.variables.keys()
fnames=file.variables.values()
t=getattr(file.variables[names[1]],'initial_time')
odatetime=datetime.datetime.strptime(t,'%m/%d/%Y (%H:%M)')
def calculateRaster2(inputfile,inputfile2,longitude,latitude):
    file = Nio.open_file(inputfile, 'r')
    names = file.variables.keys()
    #varinames1=['SP_GDS0_SFC', 'Z_GDS0_SFC', 'TCC_GDS0_SFC', 'SD_GDS0_SFC', '10V_GDS0_SFC', 'TP_GDS0_SFC', '2D_GDS0_SFC', 'MSL_GDS0_SFC', 'HCC_GDS0_SFC', 'CP_GDS0_SFC', 'MCC_GDS0_SFC', '10U_GDS0_SFC', '2T_GDS0_SFC', 'SSTK_GDS0_SFC', 'LCC_GDS0_SFC', 'SKT_GDS0_SFC']
    #varinames=['2T_GDS0_SFC','2D_GDS0_SFC','10U_GDS0_SFC','10V_GDS0_SFC','TCC_GDS0_SFC','LCC_GDS0_SFC','HCC_GDS0_SFC','MCC_GDS0_SFC','TP_GDS0_SFC','CP_GDS0_SFC']
    varinames = ['2T_GDS0_SFC', '2D_GDS0_SFC', '10U_GDS0_SFC', '10V_GDS0_SFC',
        'TCC_GDS0_SFC', 'LCC_GDS0_SFC']
    print len(varinames)
    #首先计算经纬度对应格点的索引，
    indexlat=int((60-latitude)/0.125)
    indexlon=int((longitude-60)/0.125)
    #则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
    vstring=[]
    timeArray=file.variables['forecast_time0']
    # for t in range(len(timeArray)):
    #     hh=int(timeArray[t])
    #     pdatetime=odatetime+datetime.timedelta(hours=hh)
    #首先按照要素变量循环
    for i in range(len(varinames)):
        #vstring.append(varinames[i])
        parray=file.variables[varinames[i]]
        #然后是时间维度的循环遍历,时间维度的循环遍历就分布建立64个矩阵来存储数据；
        for j in range(len(parray)):
            hh=int(timeArray[j])
            print len(parray),j,timeArray[j]
            pdatetime=odatetime+datetime.timedelta(hours=hh)
            #vstring.append(pdatetime)
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
    #print names2
    variablenames=['R_GDS0_ISBL']
    tt = getattr(gribfile.variables[names2[1]], 'initial_time')
    oodatetime=datetime.datetime.strptime(tt,'%m/%d/%Y (%H:%M)')
    timeArray = gribfile.variables['forecast_time0']
    levelArray=gribfile.variables['lv_ISBL1']
    #print levelArray
    #要素
    for i in range(len(variablenames)):
        variableArray=gribfile.variables[variablenames[i]]
        vstring.append(variablenames[i])
        #时间维度的循环遍历
        for j in range(len(variableArray)):
            pArray=variableArray[j]
            hh=int(timeArray[j])
            pdatetime=oodatetime+datetime.timedelta(hours=hh)
            #vstring.append(pdatetime)
            for k in range(len(pArray)):
                phaArray=pArray[k]
                llArray=phaArray
                pha=levelArray[k]
                #vstring.append(str(pha)+'hpa')
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
    #print '------'
    #print len(vstring)
    #print len(vdescirbe)
    del vstring
    del latlonArray
    return vdescirbe
calculateRaster2(inputfile,inputfile2,80,30)