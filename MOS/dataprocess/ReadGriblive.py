#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/3
description: decode Grib data at live on OSS
"""
import Nio,datetime,bz2file,bz2,os
#先解压
bz2filepath='/Users/yetao.lu/Documents/mosdata/D1D01020000010203001.bz2'
newfile=bz2filepath[:-4]+'.grib'
if not os.path.exists(newfile):
    a = bz2.BZ2File(bz2filepath,'rb')
    b = open(newfile, 'wb')
    b.write(a.read())
    a.close()
    b.close()
# gribfile=Nio.open_file(newfile,'r')
# names=gribfile.variables.keys()
# print names,len(names)
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

calculateRasterlive(newfile,120,35)




