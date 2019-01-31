#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/9/17
description:
"""
import pygrib,numpy,math,MySQLdb,os
#根据经纬度取最邻近点的EC数据的值
#计算辐照度：根据3小时的插值为15分钟
def calculateRadiation(plist):
    alist=[]
    alist.append(plist[0])
    for i in range(len(plist)):
        if i==len(plist)-1:
            ra=plist[i]
        else:
            N=12*i
            for j in range(12):
                ra=(plist[i+1]-plist[i])*(N+j)/12+((N+12)*plist[i]-N*plist[i+1])/12
                alist.append(ra)
    return alist
#累计的转为离散的,可以理解为瞬时值
def totaltosiple(alist):
    blist=[]
    for i in range(len(alist)):
        if i==0:
            blist.append(alist[0]/(3*3600))
        else:
            blist.append((alist[i]-alist[i-1])/(3*3600))
    return blist
def calculatwinddirect(u,v):
    if u > 0 and v > 0:
        fx = 270 - math.atan(v / u) * 180 / math.pi
    elif u < 0 and v > 0:
        fx = 90 - math.atan(v / u) * 180 / math.pi
    elif u < 0 and v < 0:
        fx = 90 - math.atan(v / u) * 180 / math.pi
    elif u > 0 and v < 0:
        fx = 270 - math.atan(v / u) * 180 / math.pi
    elif u == 0 and v > 0:
        fx = 180
    elif u == 0 and v < 0:
        fx = 0
    elif u > 0 and v == 0:
        fx = 90
    elif u == 0 and v == 0:
        fx = -9999
    return fx
def linearForECvalue(parray,lon,lat):
    lonindex=int((lon+0.05-60)/0.1)
    latindex=int((60-(lat+0.05))/0.1)
    return parray[latindex][lonindex]
def weatherfeatureFromEC(ecfile,lon,lat,powerstationname,txtpath):
    grbs=pygrib.open(ecfile)
    # grbs.seek(0)
    # for grb in grbs:
    #     print grb
    #定义list存放72小时的逐3小时的预报数据共25个时次
    plist_totalradiation=[]
    plist_straightradiation=[]
    plist_surfacedradiation=[]
    # test_radiation=[]
    plist_ws=[]
    plist_wd=[]
    plist_t=[]
    plist_rh=[]
    plist_p=[]
    #总辐射
    grb001=grbs.select(name='Surface solar radiation downwards')
    totalradiationarray=grb001[0].values
    #print totalradiationarray
    #散射辐射是计算出来的，总辐射-水平面直接辐射
    #法直辐射：总辐射和法直辐射并没有什么关系：
    grb003=grbs.select(name='Direct solar radiation')
    #straightradiationarray=grb003[0].values
    #print straightradiationarray
    #暂时任务该变量为水平面直接辐射
    grb002=grbs.select(name='Total sky direct solar radiation at surface')
    #平均风速
    grb004=grbs.select(name='10 metre U wind component')
    #u10array=grb004[0].values
    #print u10array
    grb005=grbs.select(name='10 metre V wind component')
    #v10array=grb005[0].values
    #print v10array
    #空气温度
    grb006=grbs.select(name='2 metre temperature')
    #temperaturearray=grb006[0].values
    #print temperaturearray
    #露点温度
    grb007=grbs.select(name='2 metre dewpoint temperature')
    #dewpointarray=grb007[0].values
    #print dewpointarray
    #气压
    grb008=grbs.select(name='Surface pressure')
    #airpressurearray=grb008[0].values
    #print airpressurearray
    #print len(airpressurearray)
    #parray=numpy.array(airpressurearray)
    #print parray.shape
    # csvfile='/Users/yetao.lu/2017/1.csv'
    # csvwf=open(csvfile,'w')
    #获取25个预报时次的数据矩阵,然后根据该点的经纬度获取最邻近点的要素值
    for i in range(25):
        #总辐射
        perarray001=grb001[i].values
        perarray001=numpy.array(perarray001)
        pvalue001=linearForECvalue(perarray001,lon,lat)
        plist_totalradiation.append(pvalue001)
        #法直辐射
        perarray003=grb003[i].values
        perarray003=numpy.array(perarray003)
        pvalue003=linearForECvalue(perarray003,lon,lat)
        plist_straightradiation.append(pvalue003)
        #散射辐射=总辐射-水平面直接辐射
        #这里是水平面直接辐射
        perarray002=grb002[i].values
        pvalue002=linearForECvalue(perarray002,lon,lat)
        plist_surfacedradiation.append(pvalue002)
        # testarray002=grb002[i].values
        # testvalue002=linearForECvalue(testarray002,lon,lat)
        # test_radiation.append(testvalue002)
        
        #print pvalue001,pvalue003,testvalue002
        # csvwf.write(str(pvalue001)+','+str(pvalue003)+','+str(testvalue002))
        # csvwf.write('\n')
        #平均风速
        #u分量
        perarray004=grb004[i].values
        perarray004 = numpy.array(perarray004)
        #v分量
        perarray005=grb005[i].values
        perarray005 = numpy.array(perarray005)
        pvalue004=linearForECvalue(perarray004,lon,lat)
        pvalue005=linearForECvalue(perarray005,lon,lat)
        ws=math.sqrt(pvalue004*pvalue004+pvalue005*pvalue005)
        plist_ws.append(ws)
        wd=calculatwinddirect(pvalue004,pvalue005)
        plist_wd.append(wd)
        #气温
        perarray006=grb006[i].values
        perarray006=numpy.array(perarray006)
        t=linearForECvalue(perarray006,lon,lat)-273.15
        plist_t.append(t)
        #湿度
        #根据气温和露点温度计算
        #露点温度
        perarray007=grb007[i].values
        perarray007=numpy.array(perarray007)
        dt=linearForECvalue(perarray007,lon,lat)-273.15
        rh=100*math.exp((17.625*dt)/(243.04+dt))/math.exp((17.625*t)/(243.04+t))
        plist_rh.append(rh)
        #气压
        perarray008=grb008[i].values
        perarray008=numpy.array(perarray008)
        p=linearForECvalue(perarray008,lon,lat)/100
        plist_p.append(p)
    #csvwf.close()
    #为了测试写的文件，瞬时值计算
    # a1=totaltosiple(plist_totalradiation)
    # a2=totaltosiple(plist_straightradiation)
    # a3=totaltosiple(test_radiation)
    # csvfile2='/Users/yetao.lu/2017/2.csv'
    # cvw=open(csvfile2,'w')
    # for p in range(len(a1)):
    #     cvw.write(str(a1[p])+','+str(a2[p])+','+str(a3[p]))
    #     cvw.write('\n')
    # cvw.close()

    #总辐射
    pplist_total=totaltosiple(plist_totalradiation)
    perlist_total=calculateRadiation(pplist_total)
    #print pplist_total
    #法直辐射
    pplist_straight=totaltosiple(plist_straightradiation)
    perlist_straight=calculateRadiation(pplist_straight)
    #print pplist_straight
    #散射辐射=总辐射减去水平面直接辐射的值，这里还是水平面直接辐射的插值
    pplist_surface=totaltosiple(plist_surfacedradiation)
    perlist_fdir=calculateRadiation(pplist_surface)
    
    #风速
    perlist_ws=calculateRadiation(plist_ws)

    #风向
    perlist_wd=calculateRadiation(plist_wd)
    
    #气温
    perlist_t=calculateRadiation(plist_t)

    #相对湿度
    perlist_rh=calculateRadiation(plist_rh)
    #气压
    perlist_p=calculateRadiation(plist_p)
    #写文件
    #首先确定文件名称
    txtfile = os.path.join(txtpath, powerstationname + '.txt')
    wfile = open(txtfile, 'w')
    L=[]
    linelist=[]
    #定义散射辐射列表
    perlist_scattered=[]
    for i in range(len(perlist_total)):
        #print perlist_total[i],perlist_straight[i],perlist_scattered[i]
        scatteredvalue=perlist_total[i]-perlist_fdir[i]
        if scatteredvalue<0:
            scatteredvalue=0
        perlist_scattered.append(scatteredvalue)
        wfile.write(str(perlist_total[i])+'  '+str(perlist_straight[i])+' '+str(scatteredvalue)+'    '+str(perlist_ws[i])+'    '+str(perlist_wd[i])+'  '+str(perlist_t[i])+'    '+str(perlist_rh[i])+'   '+str(perlist_p[i]))
        wfile.write('\n')
        L.append((powerstationname,'2018-03-20 12:00:00',str(i),str(perlist_total[i]),str(perlist_straight[i]),str(scatteredvalue),str(perlist_ws[i]),str(perlist_wd[i]),str(perlist_t[i]),str(perlist_rh[i]),str(perlist_p[i])))
    wfile.close()
    #数据入库
    print L
    print len(L)
    db = MySQLdb.connect('192.168.1.20', 'meteou1', '1iyHUuq3', 'moge',3345)
    cursor = db.cursor()
    sql='insert ignore into t_r_powerplant_radiation(city_id,initial_time,forecast_time,total_radiation,straight_radiation,scattered_radiation,wind_speed,wind_direction,temperature,humidity,air_pressure)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
    cursor.executemany(sql,L)
    db.commit()
    db.close()
if __name__ == "__main__":
    ecfile='/Users/yetao.lu/Downloads/_mars-atls04-70e05f9f8ba4e9d19932f1c45a7be8d8-eQOWUm.grib'
    #ecfile='/Users/yetao.lu/Downloads/_mars-atls14-98f536083ae965b31b0d04811be6f4c6-B1XMxA.grib'
    #weatherfeatureFromEC(ecfile,115.1725,28.2214,txtfile)
    powerstationname='TZHCS'
    txtpath='/Users/yetao.lu/2017/'
    weatherfeatureFromEC(ecfile, 114.0352, 40.4638, powerstationname,txtpath)
