#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/3
description: decode Grib data at live on OSS
"""
import Nio,datetime,bz2file,bz2,os,time
#先解压
bz2filepath='/Users/yetao.lu/Desktop/mos/mosdata/d/D1D04180000041812001.bz2'
newfile=bz2filepath[:-4]+'.grib'
if not os.path.exists(newfile):
    a = bz2.BZ2File(bz2filepath,'rb')
    b = open(newfile, 'wb')
    b.write(a.read())
    a.close()
    b.close()

gribfile=Nio.open_file(newfile,'r')
names=gribfile.variables.keys()
print names,len(names)
def calculateRasterlive(inputfile,longitude,latitude):
    file = Nio.open_file(inputfile, 'r')
    names = file.variables.keys()
    #线上要素要分2维和3维，
    #2D要素集合=['SP_GDS0_SFC', 'STL2_GDS0_DBLY', 'STL1_GDS0_DBLY', 'MCC_GDS0_SFC', 'W_GDS0_ISBL', 'SSTK_GDS0_SFC', 'lv_ISBL2', 'STL3_GDS0_DBLY', 'STL4_GDS0_DBLY', '10V_GDS0_SFC', 'SWVL4_GDS0_DBLY', 'CP_GDS0_SFC', '2T_GDS0_SFC', 'SWVL1_GDS0_DBLY', 'TCC_GDS0_SFC', 'T_GDS0_ISBL', 'SD_GDS0_SFC', 'TP_GDS0_SFC', '2D_GDS0_SFC', 'MSL_GDS0_SFC', 'SF_GDS0_SFC', 'HCC_GDS0_SFC', '10U_GDS0_SFC', 'R_GDS0_ISBL', 'SWVL2_GDS0_DBLY', 'LCC_GDS0_SFC', 'LSP_GDS0_SFC', 'RSN_GDS0_SFC', 'Z_GDS0_SFC', 'V_GDS0_ISBL', 'VIS_GDS0_SFC', 'Q_GDS0_ISBL', 'GH_GDS0_ISBL', 'SWVL3_GDS0_DBLY', 'U_GDS0_ISBL', 'SKT_GDS0_SFC']
    #3D要素集合=['SP_GDS0_SFC', 'STL2_GDS0_DBLY', 'STL1_GDS0_DBLY', 'MCC_GDS0_SFC', 'W_GDS0_ISBL', 'SSTK_GDS0_SFC', 'lv_ISBL2', 'STL3_GDS0_DBLY', 'STL4_GDS0_DBLY', '10V_GDS0_SFC', 'SWVL4_GDS0_DBLY', 'CP_GDS0_SFC', '2T_GDS0_SFC', 'SWVL1_GDS0_DBLY', 'TCC_GDS0_SFC', 'T_GDS0_ISBL', 'SD_GDS0_SFC', 'TP_GDS0_SFC', '2D_GDS0_SFC', 'MSL_GDS0_SFC', 'SF_GDS0_SFC', 'HCC_GDS0_SFC', '10U_GDS0_SFC', 'R_GDS0_ISBL', 'SWVL2_GDS0_DBLY', 'LCC_GDS0_SFC', 'LSP_GDS0_SFC', 'RSN_GDS0_SFC', 'Z_GDS0_SFC', 'V_GDS0_ISBL', 'VIS_GDS0_SFC', 'Q_GDS0_ISBL', 'GH_GDS0_ISBL', 'SWVL3_GDS0_DBLY', 'U_GDS0_ISBL', 'SKT_GDS0_SFC']
    varinames=['SP_GDS0_SFC', 'STL2_GDS0_DBLY', 'STL1_GDS0_DBLY', 'MCC_GDS0_SFC', 'W_GDS0_ISBL', 'SSTK_GDS0_SFC', 'lv_ISBL2', 'STL3_GDS0_DBLY', 'STL4_GDS0_DBLY', '10V_GDS0_SFC', 'SWVL4_GDS0_DBLY', 'CP_GDS0_SFC', '2T_GDS0_SFC', 'SWVL1_GDS0_DBLY', 'TCC_GDS0_SFC', 'T_GDS0_ISBL', 'SD_GDS0_SFC', 'TP_GDS0_SFC', '2D_GDS0_SFC', 'MSL_GDS0_SFC', 'SF_GDS0_SFC', 'HCC_GDS0_SFC', '10U_GDS0_SFC', 'R_GDS0_ISBL', 'SWVL2_GDS0_DBLY', 'LCC_GDS0_SFC', 'LSP_GDS0_SFC', 'RSN_GDS0_SFC', 'Z_GDS0_SFC', 'V_GDS0_ISBL', 'VIS_GDS0_SFC', 'Q_GDS0_ISBL', 'GH_GDS0_ISBL', 'SWVL3_GDS0_DBLY', 'U_GDS0_ISBL', 'SKT_GDS0_SFC']
    #则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
    vstring=[]
    latArray=file.variables['g0_lat_0']
    lonArray=file.variables['g0_lon_1']
    print '--------------'
    print latArray
    print '--------------'
    print lonArray
    #首先计算经纬度对应格点的索引，
    indexlat=int((90-latitude)/0.125)
    indexlon=int((longitude)/0.125)
    print indexlat,indexlon
    for i in range(len(varinames)):
        #print varinames[i]
        vstring.append(varinames[i])
        parray=file.variables[varinames[i]]
        latlonArray=parray
        if len(parray)==1441:
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
        elif len(parray)==25:
            for k in range(len(parray)):
                latlonArray=parray[k]
    #循环遍历vsting
    vdescirbe=''
    for p in range(len(vstring)):
        vdescirbe=vdescirbe+','+str(vstring[p])
    print vdescirbe
    del vstring
    del latlonArray
calculateRasterlive(newfile,179,5)
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






