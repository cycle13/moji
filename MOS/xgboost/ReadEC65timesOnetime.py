#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/2/12
description:
"""
import Nio,os,numpy,datetime
#站点气温实况数据
tempcsv='/Users/yetao.lu/Desktop/mos/data/tempCSV1'
stationdict={}
def stationHistoryprocessing(path):
    for srootpath,sdirs,stationfile in os.walk(tempcsv):
        for i in range(len(stationfile)):
            if stationfile[i][-4:]=='.csv':
                stationfilepath=os.path.join(srootpath,stationfile[i])
                fileR=open(stationfilepath,'r')
                while True:
                    line=fileR.readline()
                    linearray=line.split(',')
                    if len(linearray)>4:
                        sdictId=linearray[0]+linearray[1]
                        pdatetime=datetime.datetime.strptime(linearray[1],'%Y-%m-%d %H:%M:%S')
                        timestring=datetime.datetime.strftime(pdatetime,'%Y%m%d%H%M%S')
                        sdictId = linearray[0] + '_'+timestring
                        if float(linearray[5])<>999999 or float(linearray[5])<>None:
                            stationdict[sdictId]=float(linearray[5])
                    if not line:
                        break
    return stationdict
#站点列表数据
stationlist=[]
csvfile='/Users/yetao.lu/Desktop/mos/t_p_station_cod.csv'
def stationlistprocessing(path):
    fileread=open(csvfile,'r')
    firstline=fileread.readline()
    while True:
        line=fileread.readline()
        perlist=line.split(',')
        if len(perlist)>4:
            stationlist.append(perlist)
        if not line or line=='':
            break
    return stationlist
def ECvalueFromlatlon(latlonArray,latitude,longitude):
    vstring=[]
    # #    #首先计算经纬度对应格点的索引，
    indexlat = int((60 - latitude) / 0.125)
    indexlon = int((longitude - 60) / 0.125)
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

#EC数据解析
allpath='/Users/yetao.lu/Documents/testdata'
sfc_varinames = ['2T_GDS0_SFC', '2D_GDS0_SFC', '10U_GDS0_SFC','10V_GDS0_SFC','TCC_GDS0_SFC', 'LCC_GDS0_SFC']
pl_varinames = ['R_GDS0_ISBL']
for rootpath, dirs, files in os.walk(allpath):
    for file in files:
        if file[:3] == 'sfc' and file[-5:] == '.grib':
            inputfile = os.path.join(rootpath, file)
            inputfile2=inputfile.replace('sfc','pl')
            sfcfile=Nio.open_file(inputfile,'r')
            plfile=Nio.open_file(inputfile2,'r')
            for i in range(len(sfc_varinames)):
                #取要素层维度
                sfclist=sfcfile.variables[sfc_varinames[i]]
                print len(sfclist)
                #遍历时间维度
                for j in range(len(sfclist)):
                    latlonlist=sfclist[j]
                    print len(latlonlist)
                    #遍历国家观测站点获取2千多站点的EC值
                    for k in range(len(stationlist)):
                        latitude=float(stationlist[k][4])
                        longitude=float(stationlist[k][5])
                        #根据站点经纬度取所在格点的值，该方法封装成函数，用可能用插值方法替换
                        featurelist=ECvalueFromlatlon(latlonlist,latitude,longitude)
                        
                    
                    
                    
                    