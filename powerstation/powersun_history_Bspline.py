#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/9/17
description:
"""
import pygrib, numpy, math, MySQLdb, os, datetime, logging, subprocess, shutil, \
    sys, time
import logging.handlers
import scipy.signal
from scipy import interpolate
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor


# 根据系统时间获取EC文件目录，获取EC文件列表
def getecfilelist(ecfilepath, pdatetime):
    ecfilelist = []
    # 文件列表
    month = pdatetime.month
    day = pdatetime.day
    hour = pdatetime.hour
    if month < 10:
        mmstr = '0' + str(month)
    else:
        mmstr = str(month)
    if day < 10:
        ddstr = '0' + str(day)
    else:
        ddstr = str(day)
    if hour == 12:
        hhstr = '12'
    else:
        hhstr = '00'
    for i in range(25):
        foretime = pdatetime + datetime.timedelta(hours=i * 3)
        month_new = foretime.month
        day_new = foretime.day
        hour_new = foretime.hour
        if month_new < 10:
            mmstr_new = '0' + str(month_new)
        else:
            mmstr_new = str(month_new)
        if day_new < 10:
            ddstr_new = '0' + str(day_new)
        else:
            ddstr_new = str(day_new)
        if hour_new < 10:
            hhstr_new = '0' + str(hour_new)
        else:
            hhstr_new = str(hour_new)
        if i == 0:
            ecfilename = 'D1D' + mmstr + ddstr + hhstr + '00' + mmstr_new + ddstr_new + hhstr_new + '011'
        else:
            ecfilename = 'D1D' + mmstr + ddstr + hhstr + '00' + mmstr_new + ddstr_new + hhstr_new + '001'
        ecfullname = os.path.join(ecfilepath, ecfilename)
        ecfilelist.append(ecfullname)
    return ecfilelist


def getECfilelistFromsystemtime2(starttime, ecrootpath):
    # 根据系统时间获取EC数据所在的目录
    yearstr = starttime.year
    monthstr = starttime.month
    daystr = starttime.day
    hourstr = starttime.hour
    datestring = datetime.datetime.strftime(starttime, '%Y-%m-%d')
    ecfilepath = ecrootpath + '/' + str(yearstr) + '/' + datestring
    ecfilelist = getecfilelist(ecfilepath, starttime)
    return ecfilelist


# 解压EC数据获取EC数据列表
def unzipECdata(ecfilelist, tmppath):
    unzipfilelist = []
    for i in range(len(ecfilelist)):
        if os.path.exists(ecfilelist[i]):
            logger.info(ecfilelist[i] + '文件不存在')
        shutil.copy(ecfilelist[i], tmppath)
        filename = os.path.split(ecfilelist[i])[1]
        # 拷贝后的EC 文件全路径
        copyfilepath = os.path.join(tmppath, filename)
        # print copyfilepath
        unzipfilelist.append(copyfilepath[:-4])
        subprocess.call('bzip2 -d -k ' + copyfilepath, shell=True)
    return unzipfilelist


# 计算辐照度：根据3小时的插值为15分钟
def calculateRadiationbyInterplote(plist):
    alist = []
    for i in range(len(plist)):
        if i == len(plist) - 1:
            ra = plist[i]
        else:
            N = 12 * i
            for j in range(12):
                ra = (plist[i + 1] - plist[i]) * (N + j) / 12 + (
                            (N + 12) * plist[i] - N * plist[i + 1]) / 12
                alist.append(ra)
    return alist
# 计算辐照度：根据逐小时的插值为15分钟
def calculateRadiationbyInterplote_1h(plist):
    alist = []
    for i in range(len(plist)):
        if i == len(plist) - 1:
            ra = plist[i]
        else:
            N = 4 * i
            for j in range(4):
                ra = (plist[i + 1] - plist[i]) * (N + j) / 4 + ((N + 4) * plist[i] - N * plist[i + 1]) / 4
                alist.append(ra)
    return alist
def calculateRadiationbyBspline_1h(X,plist):
    xnew=numpy.linspace(1,313,313)
    # 根据kind创建插值对象interp1d
    f = interpolate.interp1d(X, plist, kind=5)
    ynew = f(xnew)  # 计算插值结果
    return ynew
# 累计的转为离散的,可以理解为瞬时值
def totaltosiple_3h(alist):
    blist = []
    for i in range(len(alist)):
        if i == 0:
            blist.append(alist[0] / (3 * 3600))
        else:
            blist.append((alist[i] - alist[i - 1]) / (3 * 3600))
    return blist
def totaltosiple_1h(alist):
    blist = []
    for i in range(len(alist)):
        if i == 0:
            blist.append(alist[0] / (3600))
        else:
            blist.append((alist[i] - alist[i - 1]) / (3600))
    return blist

def calculatwinddirect(u, v):
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


# 根据经纬度取最邻近点的EC数据的值
def linearForECvalue(parray, lon, lat):
    lonindex = int((lon + 0.05 - 60) / 0.1)
    latindex = int((60 - (lat + 0.05)) / 0.1)
    return parray[latindex][lonindex]


def weatherfeatureFromEC(starttime, lon, lat, powerstationname, txtpath):
    ecfilelist = getECfilelistFromsystemtime2(starttime, ecrootpath)
    
    # 判断文件是否存在
    flag = True
    for i in range(len(ecfilelist)):
        if not os.path.exists(ecfilelist[i]):
            print ecfilelist[i] + '文件不存在'
            return -1
    yearstr = starttime.year
    monthstr = starttime.month
    daystr = starttime.day
    hourstr = starttime.hour
    initialtimestring = ''
    if hourstr < 17:
        pdatetime = datetime.datetime(yearstr, monthstr, daystr, 12, 0,
                                      0) + datetime.timedelta(days=-1)
        year_t = pdatetime.year
        pdate_t = datetime.datetime.strftime(pdatetime, '%Y-%m-%d')
        initial_txt = datetime.datetime.strftime(pdatetime,
                                                 '%Y-%m-%d %H:%M:%S')
        odatetime = pdatetime + datetime.timedelta(hours=8)
        initialtimestring = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    else:
        pdatetime = datetime.datetime(yearstr, monthstr, daystr)
        year_t = pdatetime.year
        pdate_t = datetime.datetime.strftime(pdatetime, '%Y-%m-%d')
        initial_txt = datetime.datetime.strftime(pdatetime,
                                                 '%Y-%m-%d %H:%M:%S')
        odatetime = pdatetime + datetime.timedelta(hours=8)
        initialtimestring = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    # 定义list存放72小时的逐3小时的预报数据共25个时次
    plist_totalradiation = []
    plist_straightradiation = []
    plist_surfacedradiation = []
    # test_radiation=[]
    plist_ws = []
    plist_wd = []
    plist_t = []
    plist_rh = []
    plist_p = []
    for i in range(len(ecfilelist)):
        ecfile = ecfilelist[i]
        grbs = pygrib.open(ecfile)
        # grbs001=pygrib.open(testfile)
        #
        # grbs001.seek(0)
        # for grb in grbs001:
        #     print grb
        # 总辐射
        grb001 = grbs.select(name='Surface solar radiation downwards')
        totalradiationarray = grb001[0].values
        # print totalradiationarray
        # 散射辐射是计算出来的，总辐射-水平面直接辐射
        # 法直辐射：总辐射和法直辐射并没有什么关系：
        grb003 = grbs.select(name='Direct solar radiation')
        # straightradiationarray=grb003[0].values
        # print straightradiationarray
        # 暂时任务该变量为水平面直接辐射
        grb002 = grbs.select(
            name='Total sky direct solar radiation at surface')
        # 平均风速
        grb004 = grbs.select(name='10 metre U wind component')
        # u10array=grb004[0].values
        # print u10array
        grb005 = grbs.select(name='10 metre V wind component')
        # v10array=grb005[0].values
        # print v10array
        # 空气温度
        grb006 = grbs.select(name='2 metre temperature')
        # temperaturearray=grb006[0].values
        # print temperaturearray
        # 露点温度
        grb007 = grbs.select(name='2 metre dewpoint temperature')
        # dewpointarray=grb007[0].values
        # print dewpointarray
        # 气压
        grb008 = grbs.select(name='Surface pressure')
        # airpressurearray=grb008[0].values
        # print airpressurearray
        # print len(airpressurearray)
        # parray=numpy.array(airpressurearray)
        # print parray.shape
        # csvfile='/Users/yetao.lu/2017/1.csv'
        # csvwf=open(csvfile,'w')
        # 获取25个预报时次的数据矩阵,然后根据该点的经纬度获取最邻近点的要素值

        # 总辐射
        perarray001 = grb001[0].values
        perarray001 = numpy.array(perarray001)
        pvalue001 = linearForECvalue(perarray001, lon, lat)
        plist_totalradiation.append(pvalue001)
        # 法直辐射
        perarray003 = grb003[0].values
        perarray003 = numpy.array(perarray003)
        pvalue003 = linearForECvalue(perarray003, lon, lat)
        plist_straightradiation.append(pvalue003)
        # 散射辐射=总辐射-水平面直接辐射
        # 这里是水平面直接辐射
        perarray002 = grb002[0].values
        pvalue002 = linearForECvalue(perarray002, lon, lat)
        plist_surfacedradiation.append(pvalue002)
        # testarray002=grb002[i].values
        # testvalue002=linearForECvalue(testarray002,lon,lat)
        # test_radiation.append(testvalue002)

        # print pvalue001,pvalue003,testvalue002
        # csvwf.write(str(pvalue001)+','+str(pvalue003)+','+str(testvalue002))
        # csvwf.write('\n')
        # 平均风速
        # u分量
        perarray004 = grb004[0].values
        perarray004 = numpy.array(perarray004)
        # v分量
        perarray005 = grb005[0].values
        perarray005 = numpy.array(perarray005)
        pvalue004 = linearForECvalue(perarray004, lon, lat)
        pvalue005 = linearForECvalue(perarray005, lon, lat)
        ws = math.sqrt(pvalue004 * pvalue004 + pvalue005 * pvalue005)
        plist_ws.append(ws)
        wd = calculatwinddirect(pvalue004, pvalue005)
        plist_wd.append(wd)
        # 气温
        perarray006 = grb006[0].values
        perarray006 = numpy.array(perarray006)
        t = linearForECvalue(perarray006, lon, lat) - 273.15
        plist_t.append(t)
        # 湿度
        # 根据气温和露点温度计算
        # 露点温度
        perarray007 = grb007[0].values
        perarray007 = numpy.array(perarray007)
        dt = linearForECvalue(perarray007, lon, lat) - 273.15
        rh = 100 * math.exp((17.625 * dt) / (243.04 + dt)) / math.exp(
            (17.625 * t) / (243.04 + t))
        plist_rh.append(rh)
        # 气压
        perarray008 = grb008[0].values
        perarray008 = numpy.array(perarray008)
        p = linearForECvalue(perarray008, lon, lat) / 100
        plist_p.append(p)
    # csvwf.close()
    # 为了测试写的文件，瞬时值计算
    # a1=totaltosiple(plist_totalradiation)
    # a2=totaltosiple(plist_straightradiation)
    # a3=totaltosiple(test_radiation)
    # csvfile2='/Users/yetao.lu/2017/2.csv'
    # cvw=open(csvfile2,'w')
    # for p in range(len(a1)):
    #     cvw.write(str(a1[p])+','+str(a2[p])+','+str(a3[p]))
    #     cvw.write('\n')
    # cvw.close()

    # 总辐射
    pplist_total = totaltosiple_3h(plist_totalradiation)
    print pplist_total
    perlist_total = calculateRadiationbyInterplote(pplist_total)
    print perlist_total
    # 法直辐射
    pplist_straight = totaltosiple_3h(plist_straightradiation)
    perlist_straight = calculateRadiationbyInterplote(pplist_straight)
    # print pplist_straight
    # 散射辐射=总辐射减去水平面直接辐射的值，这里还是水平面直接辐射的插值
    pplist_surface = totaltosiple_3h(plist_surfacedradiation)
    perlist_fdir = calculateRadiationbyInterplote(pplist_surface)
    # 风速
    perlist_ws = calculateRadiationbyInterplote(plist_ws)
    # 风向
    perlist_wd = calculateRadiationbyInterplote(plist_wd)
    # 气温
    perlist_t = calculateRadiationbyInterplote(plist_t)
    # 相对湿度
    perlist_rh = calculateRadiationbyInterplote(plist_rh)
    # 气压
    perlist_p = calculateRadiationbyInterplote(plist_p)
    # 平滑过滤,所有的要素都平滑过滤了
    # print perlist_total
    perlist_total = scipy.signal.savgol_filter(perlist_total, 5, 2)
    perlist_straight = scipy.signal.savgol_filter(perlist_straight, 5, 2)
    perlist_fdir = scipy.signal.savgol_filter(perlist_fdir, 5, 2)
    perlist_ws = scipy.signal.savgol_filter(perlist_ws, 5, 3)
    perlist_wd = scipy.signal.savgol_filter(perlist_ws, 5, 2)
    perlist_t = scipy.signal.savgol_filter(perlist_t, 5, 2)
    perlist_rh = scipy.signal.savgol_filter(perlist_rh, 5, 2)
    perlist_p = scipy.signal.savgol_filter(perlist_p, 5, 2)
    # print perlist_total
    # 写文件
    # 首先确定文件名称
    txtpath001 = txtpath + '/' + str(year_t) + '/' + pdate_t
    txtfile = os.path.join(txtpath001,
                           powerstationname + initialtimestring + '.txt')
    wfile = open(txtfile, 'w')
    L = []
    linelist = []
    # 定义散射辐射列表
    perlist_scattered = []
    for i in range(len(perlist_total)):
        endtime = odatetime + datetime.timedelta(minutes=i * 15)
        endtimestring = datetime.datetime.strftime(endtime, '%Y%m%d%H%M')
        endtimestring001 = datetime.datetime.strftime(endtime,
                                                      '%Y-%m-%d %H:%M:%S')
        # print perlist_total[i],perlist_straight[i],perlist_scattered[i]
        scatteredvalue = perlist_total[i] - perlist_fdir[i]
        if perlist_total[i] < 0:
            perlist_total[i] = 0
        if perlist_straight[i] < 0:
            perlist_straight[i] = 0
        if scatteredvalue < 0:
            scatteredvalue = 0
        perlist_scattered.append(scatteredvalue)
        print len(perlist_total), len(perlist_straight), len(perlist_ws), len(
            perlist_wd), len(perlist_t), len(perlist_p), len(perlist_rh)
        wfile.write(
            endtimestring + ' ' + str("%.2f" % perlist_total[i]) + '  ' + str(
                "%.2f" % perlist_straight[i]) + '   ' + str(
                "%.2f" % scatteredvalue) + ' ' + str(
                "%.2f" % perlist_ws[i]) + '  ' + str(
                "%.2f" % perlist_wd[i]) + ' ' + str(
                "%.2f" % perlist_t[i]) + '    ' + str(
                "%.2f" % perlist_rh[i]) + '  ' + str("%.2f" % perlist_p[i]))
        wfile.write('\n')
        # print len(perlist_total),len(perlist_straight),len(perlist_ws),len(perlist_wd),len(perlist_t),len(perlist_p),len(perlist_rh)
        L.append((powerstationname, initial_txt, endtimestring001,
        str(perlist_total[i]), str(scatteredvalue), str(perlist_straight[i]),
        str(perlist_ws[i]), str(perlist_wd[i]), str(perlist_t[i]),
        str(perlist_rh[i]), str(perlist_p[i])))
    wfile.close()

    # 数据入库
    # db = MySQLdb.connect('192.168.1.20', 'meteou1', '1iyHUuq3', 'moge',3345)
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor = db.cursor()
    sql = 'insert ignore into t_r_powerplant_radiation(city_id,initial_time,forecast_time,total_radiation,straight_radiation,scattered_radiation,wind_speed,wind_direction,temperature,humidity,air_pressure)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
    cursor.executemany(sql, L)
    db.commit()
    db.close()
    # 判断文件是否生成成功
    if not os.path.exists(txtfile):
        return -1
    # 判断入库是否完成
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor = db.cursor()
    sql = 'select count(*) from t_r_powerplant_radiation where initial_time="' + initial_txt + '"'
    cursor.execute(sql)
    data = cursor.fetchall()
    dataint = int(data[0])
    db.close()
    if dataint != 289:
        return -1
    return 0

def powersun_check(ecfile,lon, lat, name, txtpath):
    namearray=os.path.split(ecfile)
    ecname=namearray[1]
    ectime=ecname[4:12]
    #这里是EC预报的起报时间，为世界时
    starttime=datetime.datetime(int(ecname[4:8]),int(ecname[8:10]),int(ecname[10:12]),int(ecname[13:15]),0,0)
    print starttime
    initialtime=starttime
    grbs=pygrib.open(ecfile)
    # for grb in grbs:
    #     print grb
    # grb001 = grbs.select(name='Surface solar radiation downwards')
    # grbarray001=grb001[0].values
    # grbarray002=grb001[124].values
    # print grbarray001,grbarray002
    # print len(grb001)
    # 总辐射
    grb001 = grbs.select(name='Surface solar radiation downwards')
    # totalradiationarray = grb001[0].values
    # print totalradiationarray
    # 散射辐射是计算出来的，总辐射-水平面直接辐射
    # 法直辐射：总辐射和法直辐射并没有什么关系：
    grb003 = grbs.select(name='Direct solar radiation')
    # straightradiationarray=grb003[0].values
    # print straightradiationarray
    # 暂时任务该变量为水平面直接辐射
    grb002 = grbs.select(
        name='Total sky direct solar radiation at surface')
    # 平均风速
    grb004 = grbs.select(name='10 metre U wind component')
    # u10array=grb004[0].values
    # print u10array
    grb005 = grbs.select(name='10 metre V wind component')
    # v10array=grb005[0].values
    # print v10array
    # 空气温度
    grb006 = grbs.select(name='2 metre temperature')
    # temperaturearray=grb006[0].values
    # print temperaturearray
    # 露点温度
    grb007 = grbs.select(name='2 metre dewpoint temperature')
    # dewpointarray=grb007[0].values
    # print dewpointarray
    # 气压
    grb008 = grbs.select(name='Surface pressure')
    #定义list来存储不同要素的逐小时数据
    plist_totalradiation = []
    plist_straightradiation = []
    plist_surfacedradiation = []
    # test_radiation=[]
    plist_ws = []
    plist_wd = []
    plist_t = []
    plist_rh = []
    plist_p = []
    for i in range(12,91,1):
        # 总辐射
        perarray001 = grb001[i].values
        perarray001 = numpy.array(perarray001)
        pvalue001 = linearForECvalue(perarray001, lon, lat)
        plist_totalradiation.append(pvalue001)
        # 法直辐射
        perarray003 = grb003[i].values
        perarray003 = numpy.array(perarray003)
        pvalue003 = linearForECvalue(perarray003, lon, lat)
        plist_straightradiation.append(pvalue003)
        # 散射辐射=总辐射-水平面直接辐射
        # 这里是水平面直接辐射
        perarray002 = grb002[i].values
        pvalue002 = linearForECvalue(perarray002, lon, lat)
        plist_surfacedradiation.append(pvalue002)
        # testarray002=grb002[i].values
        # testvalue002=linearForECvalue(testarray002,lon,lat)
        # test_radiation.append(testvalue002)

        # print pvalue001,pvalue003,testvalue002
        # csvwf.write(str(pvalue001)+','+str(pvalue003)+','+str(testvalue002))
        # csvwf.write('\n')
        # 平均风速
        # u分量
        perarray004 = grb004[i].values
        perarray004 = numpy.array(perarray004)
        # v分量
        perarray005 = grb005[i].values
        perarray005 = numpy.array(perarray005)
        pvalue004 = linearForECvalue(perarray004, lon, lat)
        pvalue005 = linearForECvalue(perarray005, lon, lat)
        ws = math.sqrt(pvalue004 * pvalue004 + pvalue005 * pvalue005)
        plist_ws.append(ws)
        wd = calculatwinddirect(pvalue004, pvalue005)
        plist_wd.append(wd)
        # 气温
        perarray006 = grb006[i].values
        perarray006 = numpy.array(perarray006)
        t = linearForECvalue(perarray006, lon, lat) - 273.15
        plist_t.append(t)
        # 湿度
        # 根据气温和露点温度计算
        # 露点温度
        perarray007 = grb007[i].values
        perarray007 = numpy.array(perarray007)
        dt = linearForECvalue(perarray007, lon, lat) - 273.15
        rh = 100 * math.exp((17.625 * dt) / (243.04 + dt)) / math.exp(
            (17.625 * t) / (243.04 + t))
        plist_rh.append(rh)
        # 气压
        perarray008 = grb008[i].values
        perarray008 = numpy.array(perarray008)
        p = linearForECvalue(perarray008, lon, lat) / 100
        plist_p.append(p)
    print len(plist_p),plist_totalradiation
    #其中太阳辐射的值是累积值，把他变为瞬时值（这种说法不完全正确）
    #针对各个要素进行线性插值
    # 总辐射
    pplist_total = totaltosiple_1h(plist_totalradiation)
    perlist_total = calculateRadiationbyInterplote_1h(pplist_total)
    print pplist_total,len(pplist_total)
    print perlist_total,len(perlist_total)
    # 法直辐射
    pplist_straight = totaltosiple_1h(plist_straightradiation)
    perlist_straight = calculateRadiationbyInterplote_1h(pplist_straight)
    # print pplist_straight
    # 散射辐射=总辐射减去水平面直接辐射的值，这里还是水平面直接辐射的插值
    pplist_surface = totaltosiple_1h(plist_surfacedradiation)
    perlist_fdir = calculateRadiationbyInterplote_1h(pplist_surface)
    # 风速
    perlist_ws = calculateRadiationbyInterplote_1h(plist_ws)
    # 风向
    perlist_wd = calculateRadiationbyInterplote_1h(plist_wd)
    # 气温
    perlist_t = calculateRadiationbyInterplote_1h(plist_t)
    # 相对湿度
    perlist_rh = calculateRadiationbyInterplote_1h(plist_rh)
    # 气压
    perlist_p = calculateRadiationbyInterplote_1h(plist_p)
    # 平滑过滤,所有的要素都平滑过滤了
    perlist_total = scipy.signal.savgol_filter(perlist_total, 5, 2)
    perlist_straight = scipy.signal.savgol_filter(perlist_straight, 5, 2)
    perlist_fdir = scipy.signal.savgol_filter(perlist_fdir, 5, 2)
    perlist_ws = scipy.signal.savgol_filter(perlist_ws, 5, 3)
    perlist_wd = scipy.signal.savgol_filter(perlist_ws, 5, 2)
    perlist_t = scipy.signal.savgol_filter(perlist_t, 5, 2)
    perlist_rh = scipy.signal.savgol_filter(perlist_rh, 5, 2)
    perlist_p = scipy.signal.savgol_filter(perlist_p, 5, 2)
    # 写文件
    # 首先确定文件名称
    #txtpath001 = txtpath + '/' + str(year_t) + '/' + pdate_t
    txtpath001=txtpath+'/'+ecname[4:8]
    if not os.path.exists(txtpath001):
        os.mkdir(txtpath001)
    initialtimestring=datetime.datetime.strftime(initialtime,'%Y%m%d%H')
    txtfile = os.path.join(txtpath001,
                           name + initialtimestring + '.txt')
    print txtfile
    wfile = open(txtfile, 'w')
    L = []
    linelist = []
    # 定义散射辐射列表，initialtime是起报时间世界时
    #但是文件的起始时间不是起报时间，文件的起始时间向后推了12个小时，转为北京时间再+8
    filetime=initialtime+datetime.timedelta(hours=20)
    perlist_scattered = []
    for i in range(len(perlist_total)):
        endtime = filetime + datetime.timedelta(minutes=i * 15)
        endtimestring = datetime.datetime.strftime(endtime, '%Y%m%d%H%M')
        endtimestring001 = datetime.datetime.strftime(endtime,
                                                      '%Y-%m-%d %H:%M:%S')
        # print perlist_total[i],perlist_straight[i],perlist_scattered[i]
        scatteredvalue = perlist_total[i] - perlist_fdir[i]
        if perlist_total[i] < 0:
            perlist_total[i] = 0
        if perlist_straight[i] < 0:
            perlist_straight[i] = 0
        if scatteredvalue < 0:
            scatteredvalue = 0
        perlist_scattered.append(scatteredvalue)
        print len(perlist_total), len(perlist_straight), len(perlist_ws), len(
            perlist_wd), len(perlist_t), len(perlist_p), len(perlist_rh)
        wfile.write(
            endtimestring + ' ' + str("%.2f" % perlist_total[i]) + '  ' + str(
                "%.2f" % perlist_straight[i]) + '   ' + str(
                "%.2f" % scatteredvalue) + ' ' + str(
                "%.2f" % perlist_ws[i]) + '  ' + str(
                "%.2f" % perlist_wd[i]) + ' ' + str(
                "%.2f" % perlist_t[i]) + '    ' + str(
                "%.2f" % perlist_rh[i]) + '  ' + str("%.2f" % perlist_p[i]))
        wfile.write('\n')
        # print len(perlist_total),len(perlist_straight),len(perlist_ws),len(perlist_wd),len(perlist_t),len(perlist_p),len(perlist_rh)
        initial_txt = datetime.datetime.strftime(initialtime,
                                                 '%Y-%m-%d %H:%M:%S')
        L.append((name, initial_txt, endtimestring001,
        str(perlist_total[i]), str(scatteredvalue), str(perlist_straight[i]),
        str(perlist_ws[i]), str(perlist_wd[i]), str(perlist_t[i]),
        str(perlist_rh[i]), str(perlist_p[i])))
    wfile.close()
    # 数据入库
    # db = MySQLdb.connect('192.168.1.20', 'meteou1', '1iyHUuq3', 'moge',3345)
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor = db.cursor()
    sql = 'replace into powerplant_radiation(city_id,initial_time,forecast_time,total_radiation,straight_radiation,scattered_radiation,wind_speed,wind_direction,temperature,humidity,air_pressure)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
    cursor.executemany(sql, L)
    db.commit()
    db.close()
    # 判断文件是否生成成功
    if not os.path.exists(txtfile):
        return -1
    # 判断入库是否完成
    # db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
    #                      3307)
    # cursor = db.cursor()
    # sql = 'select count(*) from powerplant_radiation where initial_time="' + initial_txt + '"'
    # cursor.execute(sql)
    # data = cursor.fetchall()
    # dataint = int(data[0])
    # db.close()
    # if dataint != 289:
    #     return -1
    return 0
def powersun_check_Bspline(ecfile,lon, lat, name, txtpath):
    namearray=os.path.split(ecfile)
    ecname=namearray[1]
    ectime=ecname[4:12]
    #这里是EC预报的起报时间，为世界时
    starttime=datetime.datetime(int(ecname[4:8]),int(ecname[8:10]),int(ecname[10:12]),int(ecname[13:15]),0,0)
    print starttime
    initialtime=starttime
    grbs=pygrib.open(ecfile)
    # for grb in grbs:
    #     print grb
    # grb001 = grbs.select(name='Surface solar radiation downwards')
    # grbarray001=grb001[0].values
    # grbarray002=grb001[124].values
    # print grbarray001,grbarray002
    # print len(grb001)
    # 总辐射
    grb001 = grbs.select(name='Surface solar radiation downwards')
    # totalradiationarray = grb001[0].values
    # print totalradiationarray
    # 散射辐射是计算出来的，总辐射-水平面直接辐射
    # 法直辐射：总辐射和法直辐射并没有什么关系：
    grb003 = grbs.select(name='Direct solar radiation')
    # straightradiationarray=grb003[0].values
    # print straightradiationarray
    # 暂时任务该变量为水平面直接辐射
    grb002 = grbs.select(
        name='Total sky direct solar radiation at surface')
    # 平均风速
    grb004 = grbs.select(name='10 metre U wind component')
    # u10array=grb004[0].values
    # print u10array
    grb005 = grbs.select(name='10 metre V wind component')
    # v10array=grb005[0].values
    # print v10array
    # 空气温度
    grb006 = grbs.select(name='2 metre temperature')
    # temperaturearray=grb006[0].values
    # print temperaturearray
    # 露点温度
    grb007 = grbs.select(name='2 metre dewpoint temperature')
    # dewpointarray=grb007[0].values
    # print dewpointarray
    # 气压
    grb008 = grbs.select(name='Surface pressure')
    #定义list来存储不同要素的逐小时数据
    plist_totalradiation = []
    plist_straightradiation = []
    plist_surfacedradiation = []
    # test_radiation=[]
    plist_ws = []
    plist_wd = []
    plist_t = []
    plist_rh = []
    plist_p = []
    X=[]
    for i in range(12,91,1):
        X.append(1+4*(i-12))
        # 总辐射
        perarray001 = grb001[i].values
        perarray001 = numpy.array(perarray001)
        pvalue001 = linearForECvalue(perarray001, lon, lat)
        plist_totalradiation.append(pvalue001)
        # 法直辐射
        perarray003 = grb003[i].values
        perarray003 = numpy.array(perarray003)
        pvalue003 = linearForECvalue(perarray003, lon, lat)
        plist_straightradiation.append(pvalue003)
        # 散射辐射=总辐射-水平面直接辐射
        # 这里是水平面直接辐射
        perarray002 = grb002[i].values
        pvalue002 = linearForECvalue(perarray002, lon, lat)
        plist_surfacedradiation.append(pvalue002)
        # testarray002=grb002[i].values
        # testvalue002=linearForECvalue(testarray002,lon,lat)
        # test_radiation.append(testvalue002)

        # print pvalue001,pvalue003,testvalue002
        # csvwf.write(str(pvalue001)+','+str(pvalue003)+','+str(testvalue002))
        # csvwf.write('\n')
        # 平均风速
        # u分量
        perarray004 = grb004[i].values
        perarray004 = numpy.array(perarray004)
        # v分量
        perarray005 = grb005[i].values
        perarray005 = numpy.array(perarray005)
        pvalue004 = linearForECvalue(perarray004, lon, lat)
        pvalue005 = linearForECvalue(perarray005, lon, lat)
        ws = math.sqrt(pvalue004 * pvalue004 + pvalue005 * pvalue005)
        plist_ws.append(ws)
        wd = calculatwinddirect(pvalue004, pvalue005)
        plist_wd.append(wd)
        # 气温
        perarray006 = grb006[i].values
        perarray006 = numpy.array(perarray006)
        t = linearForECvalue(perarray006, lon, lat) - 273.15
        plist_t.append(t)
        # 湿度
        # 根据气温和露点温度计算
        # 露点温度
        perarray007 = grb007[i].values
        perarray007 = numpy.array(perarray007)
        dt = linearForECvalue(perarray007, lon, lat) - 273.15
        rh = 100 * math.exp((17.625 * dt) / (243.04 + dt)) / math.exp(
            (17.625 * t) / (243.04 + t))
        plist_rh.append(rh)
        # 气压
        perarray008 = grb008[i].values
        perarray008 = numpy.array(perarray008)
        p = linearForECvalue(perarray008, lon, lat) / 100
        plist_p.append(p)
    #其中太阳辐射的值是累积值，把他变为瞬时值（这种说法不完全正确）
    #针对各个要素进行线性插值
    # 总辐射
    pplist_total = totaltosiple_1h(plist_totalradiation)
    perlist_total = calculateRadiationbyBspline_1h(X,pplist_total)
    #print len(pplist_total),pplist_total
    # print len(perlist_total),perlist_total
    # 法直辐射
    pplist_straight = totaltosiple_1h(plist_straightradiation)
    perlist_straight = calculateRadiationbyBspline_1h(X,pplist_straight)
    # print pplist_straight
    # 散射辐射=总辐射减去水平面直接辐射的值，这里还是水平面直接辐射的插值
    pplist_surface = totaltosiple_1h(plist_surfacedradiation)
    perlist_fdir = calculateRadiationbyBspline_1h(X,pplist_surface)
    # 风速
    perlist_ws = calculateRadiationbyBspline_1h(X,plist_ws)
    # 风向
    perlist_wd = calculateRadiationbyBspline_1h(X,plist_wd)
    # 气温
    perlist_t = calculateRadiationbyBspline_1h(X,plist_t)
    # 相对湿度
    perlist_rh = calculateRadiationbyBspline_1h(X,plist_rh)
    # 气压
    perlist_p = calculateRadiationbyBspline_1h(X,plist_p)
    # 写文件
    # 首先确定文件名称
    #txtpath001 = txtpath + '/' + str(year_t) + '/' + pdate_t
    txtpath001=txtpath+'/'+ecname[4:8]
    if not os.path.exists(txtpath001):
        os.mkdir(txtpath001)
    initialtimestring=datetime.datetime.strftime(initialtime,'%Y%m%d%H')
    txtfile = os.path.join(txtpath001,
                           name + initialtimestring + '.txt')
    wfile = open(txtfile, 'w')
    L = []
    linelist = []
    # 定义散射辐射列表，initialtime是起报时间世界时
    #但是文件的起始时间不是起报时间，文件的起始时间向后推了12个小时，转为北京时间再+8
    filetime=initialtime+datetime.timedelta(hours=20)
    perlist_scattered = []
    for i in range(len(perlist_total)):
        endtime = filetime + datetime.timedelta(minutes=i * 15)
        endtimestring = datetime.datetime.strftime(endtime, '%Y%m%d%H%M')
        endtimestring001 = datetime.datetime.strftime(endtime,
                                                      '%Y-%m-%d %H:%M:%S')
        # print perlist_total[i],perlist_straight[i],perlist_scattered[i]
        scatteredvalue = perlist_total[i] - perlist_fdir[i]
        if perlist_total[i] < 0:
            perlist_total[i] = 0
        if perlist_straight[i] < 0:
            perlist_straight[i] = 0
        if scatteredvalue < 0:
            scatteredvalue = 0
        perlist_scattered.append(scatteredvalue)
        wfile.write(
            endtimestring + ' ' + str("%.2f" % perlist_total[i]) + '  ' + str(
                "%.2f" % perlist_straight[i]) + '   ' + str(
                "%.2f" % scatteredvalue) + ' ' + str(
                "%.2f" % perlist_ws[i]) + '  ' + str(
                "%.2f" % perlist_wd[i]) + ' ' + str(
                "%.2f" % perlist_t[i]) + '    ' + str(
                "%.2f" % perlist_rh[i]) + '  ' + str("%.2f" % perlist_p[i]))
        wfile.write('\n')
        # print len(perlist_total),len(perlist_straight),len(perlist_ws),len(perlist_wd),len(perlist_t),len(perlist_p),len(perlist_rh)
        initial_txt = datetime.datetime.strftime(initialtime,
                                                 '%Y-%m-%d %H:%M:%S')
        L.append((name, initial_txt, endtimestring001,
        str(perlist_total[i]), str(scatteredvalue), str(perlist_straight[i]),
        str(perlist_ws[i]), str(perlist_wd[i]), str(perlist_t[i]),
        str(perlist_rh[i]), str(perlist_p[i])))
    wfile.close()
    # 数据入库
    # db = MySQLdb.connect('192.168.1.20', 'meteou1', '1iyHUuq3', 'moge',3345)
    db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
                         3307)
    cursor = db.cursor()
    sql = 'replace into powerplant_radiation(city_id,initial_time,forecast_time,total_radiation,straight_radiation,scattered_radiation,wind_speed,wind_direction,temperature,humidity,air_pressure)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
    cursor.executemany(sql, L)
    db.commit()
    db.close()
    # 判断文件是否生成成功
    if not os.path.exists(txtfile):
        return -1
    # 判断入库是否完成
    # db = MySQLdb.connect('172.16.8.28', 'admin', 'moji_China_123', 'moge',
    #                      3307)
    # cursor = db.cursor()
    # sql = 'select count(*) from powerplant_radiation where initial_time="' + initial_txt + '"'
    # cursor.execute(sql)
    # data = cursor.fetchall()
    # dataint = int(data[0])
    # db.close()
    # if dataint != 289:
    #     return -1
    return 0
    
if __name__ == "__main__":
    # 日志模块
    logpath = '/moji/meteo/cluster/data/log'
    #logpath = '/Users/yetao.lu/Downloads/tmp/SSRA'
    logging.basicConfig()
    logger = logging.getLogger("apscheduler.executors.default")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
        '%a, %d %b %Y %H:%M:%S', )
    logfile = os.path.join(logpath, 'power.log')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    hdlr = logging.handlers.TimedRotatingFileHandler(logfile, when='D',
                                                     interval=1,
                                                     backupCount=40)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # 程序部分
    ecrootpath = '/opt/meteo/cluster/data/sample/2018/10'
    txtpath='/opt/meteo/cluster/data/temp/powersun/result'
    # tmppath = '/opt/meteo/cluster/data/ecmwf/tmp'
   #ecfile = '/Users/yetao.lu/Downloads/sfc_20181031_12.grib'
    # ecfile='/opt/meteo/cluster/data/ecmwf/orig/2018/2018-09-29/D1D09290000092903001'
    #starttime = datetime.datetime.now()
    powerstationname001 = 'XJHMSK'
    # txtpath = '/Users/yetao.lu/2017'
    # powersun_check_Bspline(ecfile,93.9203, 42.2439,powerstationname001,txtpath)
    
    #文件夹循环遍历
    for root,dirs,files in os.walk(ecrootpath):
        for file in files:
            filename=os.path.join(root,file)
            if file[:3]=='sfc' and file[13:15]=='12':
                powersun_check_Bspline(filename, 93.9203, 42.2439, powerstationname001,txtpath)
                powersun_check_Bspline(filename,101.8403, 24.7708, 'YNCXDZ', txtpath)
    
    # txtpath='/opt/meteo/cluster/data/ecmwf/powerplant/'
    # yearstr = starttime.year
    # monthstr = starttime.month
    # daystr = starttime.day
    # hourstr = starttime.hour
    # hourstr = starttime.hour
    # initialtimestring = ''
    # if hourstr < 17:
    #     pdatetime = datetime.datetime(yearstr, monthstr, daystr, 12, 0,
    #                                   0) + datetime.timedelta(days=-1)
    #     year_t = pdatetime.year
    #     pdate_t = datetime.datetime.strftime(pdatetime, '%Y-%m-%d')
    #     initial_txt = datetime.datetime.strftime(pdatetime,
    #                                              '%Y-%m-%d %H:%M:%S')
    #     odatetime = pdatetime + datetime.timedelta(hours=8)
    #     initialtimestring = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    # else:
    #     pdatetime = datetime.datetime(yearstr, monthstr, daystr)
    # weatherfeatureFromEC(pdatetime,114.0352, 40.4638, 'TZHCS', txtpath)
    # weatherfeatureFromEC(pdatetime,93.9203, 42.2439, 'XJHMSK', txtpath)
    #powersun_check(pdatetime, 101.8403, 24.7708, 'YNCXDZ', txtpath)
    # #定时任务部分
    # executors = {
    #     'default': ThreadPoolExecutor(20),
    #     'processpool': ProcessPoolExecutor(5)
    # }
    # job_defaults = {
    #     'coalesce': False,
    #     'max_instances': 4,
    #     'misfire_grace_time':300
    # }
    # # 创建后台执行的schedulers
    # scheduler = BackgroundScheduler(executors=executors,job_defaults=job_defaults)
    # # 添加调度任务
    # # 调度方法为timeTask,触发器选择定时，
    # scheduler.add_job(weatherfeatureFromEC,'cron',hour='5,17',minute='41',args=(114.0352, 40.4638, 'TZHCS', txtpath))
    # scheduler.add_job(weatherfeatureFromEC,'cron',hour='5,17',minute='41',args=(93.9203, 42.2439, 'XJHMSK', txtpath))
    # scheduler.add_job(weatherfeatureFromEC,'cron',hour='5,17',minute='41',args=(101.8403, 24.7708, 'YNCXDZ', txtpath))
    # #scheduler.add_job(weatherfeatureFromEC,'cron',minute='*/2',args=(114.0352, 40.4638, 'TZHCS', txtpath,starttime))
    # scheduler.start()
    # try:
    #     while True:
    #         time.sleep(2)
    # except Exception as e:
    #     print e.message
