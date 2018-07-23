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
#该函数将EC数据65个预报时次的数据按要素都取出来
def CalculateArray(inputfile,inputfile2,longitude,latitude):
    #两个文件同时读取
    sfcfile= Nio.open_file(inputfile, 'r')
    plfile=Nio.open_file(inputfile2,'r')
    sfc_varinames = ['2T_GDS0_SFC', '2D_GDS0_SFC', '10U_GDS0_SFC', '10V_GDS0_SFC',
        'TCC_GDS0_SFC', 'LCC_GDS0_SFC']
    pl_varinames=['R_GDS0_ISBL']
    #    #首先计算经纬度对应格点的索引，
    indexlat=int((60-latitude)/0.125)
    indexlon=int((longitude-60)/0.125)
    #则依次取周边16个点的索引为[indexlat,indexlon+1][indexlat+1,indexlon+1][indexlat+1,indexlon]...(顺时针)
    #要素个数不同，不可能实现同时循环，因此这里把PL的循环扔到SFC文件读取里面，毕竟pl要素少，只有1个
    allArray=[]
    csvfile='/Users/yetao.lu/Downloads/tmp/a.csv'
    filewrite=open(csvfile,'w')
    #第二个文件的数组先取出来,这里要素必须是1个，不然需要重新多定义几个
    pl_variableArray = plfile.variables['R_GDS0_ISBL']
    #把高空数据的高度层取出来
    levelArray=plfile.variables['lv_ISBL1']
    for i in range(len(sfc_varinames)):
        # vstring.append(sfc_varinames[i])
        parray = file.variables[sfc_varinames[i]]
    #时间维度的循环遍历,时间维度的循环遍历不依赖要素，不然没发写矩阵
    for j in range(65):
        vstring=[]
        vdescirbe = ''
        #hh=int(timeArray[j])
        #pdatetime=odatetime+datetime.timedelta(hours=hh)
        #vstring.append(pdatetime)
        vstring.append(j)
        for i in range(len(sfc_varinames)):
            # vstring.append(sfc_varinames[i])
            parray = file.variables[sfc_varinames[i]]
            latlonArray=parray[j]
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
        #取高空的数据矩阵
        phaArray=pl_variableArray[j]
        for k in range(len(phaArray)):
            llArray=phaArray[k]
            pha = levelArray[k]
            #print pha
            if pha == 500 or pha == 850:
                # vstring.append(str(pha)+'hpa')
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
        print vstring
        for p in range(len(vstring)):
            vdescirbe = vdescirbe + ',' + str(vstring[p])
        filewrite.write(vdescirbe)
        filewrite.write('\n')
CalculateArray(inputfile,inputfile2,80,30)