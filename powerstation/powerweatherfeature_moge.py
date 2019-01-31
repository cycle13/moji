#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/9/17
description: python3.6
线上版本，Python3.6，类，集成于模式中心
由EC逐3小时插值修改为EC逐小时插值
"""
import pygrib, numpy, math, os, datetime,logging,sys,logging.handlers
import scipy.signal


class Powersun_base(object):
    def __init__(self, logger, db):
        self.logger = logger
        self.db = db

    # 根据系统时间获取EC文件目录，获取EC文件列表
    #1512
    def getecfilelist(self, ecfilepath, pdatetime):
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
        #预报序列
        for i in range(3,29,1):
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
            print(ecfullname)
            ecfilelist.append(ecfullname)
        return ecfilelist
    def getecfilelist_new(self, ecfilepath, pdatetime):
        print(pdatetime)
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
        #预报序列:逐小时是从12个小时
        for i in range(11,85,1):
            foretime = pdatetime + datetime.timedelta(hours=i)
            #print(pdatetime,foretime)
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
                ecfilename = 'D2S' + mmstr + ddstr + hhstr + '00' + mmstr_new + ddstr_new + hhstr_new + '011'
            else:
                ecfilename = 'D2S' + mmstr + ddstr + hhstr + '00' + mmstr_new + ddstr_new + hhstr_new + '001'
            ecfullname = os.path.join(ecfilepath, ecfilename)
            ecfilelist.append(ecfullname)
        return ecfilelist

    # def getECfilelistFromsystemtime2(self, starttime, ecfilepath):
    #     # 根据系统时间获取EC数据所在的目录
    #     # yearstr = starttime.year
    #     # monthstr = starttime.month
    #     # daystr = starttime.day
    #     # hourstr = starttime.hour
    #     # datestring = datetime.datetime.strftime(starttime, '%Y-%m-%d')
    #     # ecfilepath = ecrootpath + '/' + str(yearstr) + '/' + datestring
    #     ecfilelist = self.getecfilelist(ecfilepath, starttime)
    #     ecfilelist_prevous=s
    #     return ecfilelist

    # 计算辐照度：根据3小时的插值为15分钟
    def calculateRadiationbyInterplote(self, plist):
        alist = []
        alist.append(plist[0])
        for i in range(len(plist)):
            if i == len(plist) - 1:
                ra = plist[i]
            else:
                N = 12 * i
                for j in range(12):
                    ra = (plist[i + 1] - plist[i]) * (N + j) / 12 + ((N + 12) * plist[i] - N * plist[i + 1]) / 12
                    alist.append(ra)
        return alist

    # 计算辐照度：根据逐小时的插值为15分钟
    def calculateRadiationbyInterplote_1h(self,plist):
        alist = []
        for i in range(len(plist)):
            if i == len(plist) - 1:
                ra = plist[i]
            else:
                N = 4 * i
                for j in range(4):
                    ra = (plist[i + 1] - plist[i]) * (N + j) / 4 + (
                                (N + 4) * plist[i] - N * plist[i + 1]) / 4
                    alist.append(ra)
        return alist

    # 3小时累计的转为离散的,可以理解为瞬时值
    def totaltosiple(self, alist):
        blist = []
        for i in range(len(alist)):
                blist.append((alist[i]) / (3*3600))
        return blist

    #逐小时累计的转为离散的,可以理解为瞬时值
    def totaltosiple_1h(self,alist):
        blist = []
        #这里的alist已经是本时次减去上一个时次的数据，所以直接除以3600
        for i in range(len(alist)):
            blist.append(alist[i]/3600)
        return blist
    def calculatwinddirect(self, u, v):
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
    def linearForECvalue(self, parray, lon, lat):
        lonindex = int((lon + 0.05 - 60) / 0.1)
        latindex = int((60 - (lat + 0.05)) / 0.1)
        return parray[latindex][lonindex]

    # def weatherfeatureFromEC(self, starttime, lon, lat, powerstationname, txtpath, ec_file_dir):
    #     if starttime.hour not in (0, 12):
    #         return -1
    #     ecfilelist = self.getecfilelist(ec_file_dir,starttime)
    #     # 判断文件是否存在
    #     flag = True
    #     for i in range(len(ecfilelist)):
    #         if not os.path.exists(ecfilelist[i]):
    #             self.logger.info(ecfilelist[i] + '文件不存在')
    #             return -1
    #     yearstr = starttime.year
    #     monthstr = starttime.month
    #     daystr = starttime.day
    #     hourstr = starttime.hour
    #     if hourstr == 12:
    #         pdatetime = datetime.datetime(yearstr, monthstr, daystr, 12, 0, 0)
    #         initial_txt = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
    #         odatetime = pdatetime + datetime.timedelta(hours=20)
    #         initialtimestring = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    #     else:
    #         pdatetime = datetime.datetime(yearstr, monthstr, daystr)
    #         initial_txt = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
    #         odatetime = pdatetime + datetime.timedelta(hours=20)
    #         initialtimestring = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    #     # 定义list存放72小时的逐3小时的预报数据共25个时次
    #     plist_totalradiation = []
    #     plist_straightradiation = []
    #     plist_surfacedradiation = []
    #     # test_radiation=[]
    #     plist_ws = []
    #     plist_wd = []
    #     plist_t = []
    #     plist_rh = []
    #     plist_p = []
    #     for i in range(len(ecfilelist)-1):
    #         ecfile = ecfilelist[i+1]
    #         ecfile_pre=ecfilelist[i]
    #         grbs = pygrib.open(ecfile)
    #         grbs_pre=pygrib.open(ecfile_pre)
    #         #print 'ec-------',ecfile
    #         # 总辐射
    #         grb001 = grbs.select(name='Surface solar radiation downwards')
    #         grb001_pre=grbs_pre.select(name='Surface solar radiation downwards')
    #         totalradiationarray = grb001[0].values
    #         # 散射辐射是计算出来的，总辐射-水平面直接辐射
    #         # 法直辐射：总辐射和法直辐射并没有什么关系：
    #         grb003 = grbs.select(name='Direct solar radiation')
    #         grb003_pre = grbs_pre.select(name='Direct solar radiation')
    #         # 暂时认为该变量为水平面直接辐射
    #         grb002 = grbs.select(name='Total sky direct solar radiation at surface')
    #         grb002_pre = grbs_pre.select(name='Total sky direct solar radiation at surface')
    #         # 平均风速
    #         grb004 = grbs.select(name='10 metre U wind component')
    #         grb005 = grbs.select(name='10 metre V wind component')
    #         # 空气温度
    #         grb006 = grbs.select(name='2 metre temperature')
    #         # 露点温度
    #         grb007 = grbs.select(name='2 metre dewpoint temperature')
    #         # 气压
    #         grb008 = grbs.select(name='Surface pressure')
    #         # 获取25个预报时次的数据矩阵,然后根据该点的经纬度获取最邻近点的要素值
    #         # 总辐射
    #         perarray001 = grb001[0].values
    #         print('===========',perarray001.shape)
    #         perarray001_pre=grb001_pre[0].values
    #         perarray001 = numpy.array(perarray001)
    #         perarray001_pre=numpy.array(perarray001_pre)
    #         pvalue001 = self.linearForECvalue(perarray001, lon, lat)
    #         pvalue001_pre=self.linearForECvalue(perarray001_pre, lon, lat)
    #         #print pvalue001,pvalue001_pre
    #         plist_totalradiation.append(pvalue001-pvalue001_pre)
    #         # 法直辐射
    #         perarray003 = grb003[0].values
    #         perarray003_pre=grb003_pre[0].values
    #         perarray003 = numpy.array(perarray003)
    #         perarray003_pre=numpy.array(perarray003_pre)
    #         pvalue003 = self.linearForECvalue(perarray003, lon, lat)
    #         pvalue003_pre=self.linearForECvalue(perarray003_pre,lon,lat)
    #         plist_straightradiation.append(pvalue003-pvalue003_pre)
    #         # 散射辐射=总辐射-水平面直接辐射
    #         # 这里是水平面直接辐射
    #         perarray002 = grb002[0].values
    #         perarray002_pre=grb002_pre[0].values
    #         pvalue002 = self.linearForECvalue(perarray002, lon, lat)
    #         pvalue002_pre=self.linearForECvalue(perarray002_pre,lon,lat)
    #         plist_surfacedradiation.append(pvalue002-pvalue002_pre)
    #         # 平均风速
    #         # u分量
    #         perarray004 = grb004[0].values
    #         perarray004 = numpy.array(perarray004)
    #         # v分量
    #         perarray005 = grb005[0].values
    #         perarray005 = numpy.array(perarray005)
    #         pvalue004 = self.linearForECvalue(perarray004, lon, lat)
    #         pvalue005 = self.linearForECvalue(perarray005, lon, lat)
    #         ws = math.sqrt(pvalue004 * pvalue004 + pvalue005 * pvalue005)
    #         plist_ws.append(ws)
    #         wd = self.calculatwinddirect(pvalue004, pvalue005)
    #         plist_wd.append(wd)
    #         # 气温
    #         perarray006 = grb006[0].values
    #         perarray006 = numpy.array(perarray006)
    #         t = self.linearForECvalue(perarray006, lon, lat) - 273.15
    #         plist_t.append(t)
    #         # 湿度
    #         # 根据气温和露点温度计算
    #         # 露点温度
    #         perarray007 = grb007[0].values
    #         perarray007 = numpy.array(perarray007)
    #         dt = self.linearForECvalue(perarray007, lon, lat) - 273.15
    #         rh = 100 * math.exp((17.625 * dt) / (243.04 + dt)) / math.exp((17.625 * t) / (243.04 + t))
    #         plist_rh.append(rh)
    #         # 气压
    #         perarray008 = grb008[0].values
    #         perarray008 = numpy.array(perarray008)
    #         p = self.linearForECvalue(perarray008, lon, lat) / 100
    #         plist_p.append(p)
    #     # csvwf.close()
    #     # 为了测试写的文件，瞬时值计算
    #     # a1=totaltosiple(plist_totalradiation)
    #     # a2=totaltosiple(plist_straightradiation)
    #     # a3=totaltosiple(test_radiation)
    #     # csvfile2='/Users/yetao.lu/2017/2.csv'
    #     # cvw=open(csvfile2,'w')
    #     # for p in range(len(a1)):
    #     #     cvw.write(str(a1[p])+','+str(a2[p])+','+str(a3[p]))
    #     #     cvw.write('\n')
    #     # cvw.close()
    #
    #     # 总辐射
    #     pplist_total = self.totaltosiple(plist_totalradiation)
    #     perlist_total = self.calculateRadiationbyInterplote(pplist_total)
    #     # print pplist_total
    #     # 法直辐射
    #     pplist_straight = self.totaltosiple(plist_straightradiation)
    #     perlist_straight = self.calculateRadiationbyInterplote(pplist_straight)
    #     # print pplist_straight
    #     # 散射辐射=总辐射减去水平面直接辐射的值，这里还是水平面直接辐射的插值
    #     pplist_surface = self.totaltosiple(plist_surfacedradiation)
    #     perlist_fdir = self.calculateRadiationbyInterplote(pplist_surface)
    #     # 风速
    #     perlist_ws = self.calculateRadiationbyInterplote(plist_ws)
    #     # 风向
    #     perlist_wd = self.calculateRadiationbyInterplote(plist_wd)
    #     # 气温
    #     perlist_t = self.calculateRadiationbyInterplote(plist_t)
    #     # 相对湿度
    #     perlist_rh = self.calculateRadiationbyInterplote(plist_rh)
    #     # 气压
    #     perlist_p = self.calculateRadiationbyInterplote(plist_p)
    #     # 平滑过滤,所有的要素都平滑过滤了
    #     # print perlist_total
    #     perlist_total = scipy.signal.savgol_filter(perlist_total, 5, 2)
    #     perlist_straight = scipy.signal.savgol_filter(perlist_straight, 5, 2)
    #     perlist_fdir = scipy.signal.savgol_filter(perlist_fdir, 5, 2)
    #     perlist_ws = scipy.signal.savgol_filter(perlist_ws, 5, 3)
    #     perlist_wd = scipy.signal.savgol_filter(perlist_ws, 5, 2)
    #     perlist_t = scipy.signal.savgol_filter(perlist_t, 5, 2)
    #     perlist_rh = scipy.signal.savgol_filter(perlist_rh, 5, 2)
    #     perlist_p = scipy.signal.savgol_filter(perlist_p, 5, 2)
    #     # print perlist_total
    #     # 写文件
    #     # 首先确定文件名称
    #     # txtpath001 = txtpath + '/' + str(year_t) + '/' + pdate_t
    #     txtfile = os.path.join(txtpath, powerstationname + initialtimestring + '.txt')
    #     wfile = open(txtfile, 'w')
    #     L = []
    #     linelist = []
    #     # 定义散射辐射列表
    #     perlist_scattered = []
    #     for i in range(len(perlist_total)):
    #         endtime = pdatetime + datetime.timedelta(hours=8,minutes=i * 15)
    #         #print pdatetime,endtime
    #         endtimestring = datetime.datetime.strftime(endtime, '%Y%m%d%H%M')
    #         endtimestring001 = datetime.datetime.strftime(endtime, '%Y-%m-%d %H:%M:%S')
    #         # print perlist_total[i],perlist_straight[i],perlist_scattered[i]
    #         scatteredvalue = perlist_total[i] - perlist_fdir[i]
    #         if perlist_total[i] < 0:
    #             perlist_total[i] = 0
    #         if perlist_straight[i] < 0:
    #             perlist_straight[i] = 0
    #         if scatteredvalue < 0:
    #             scatteredvalue = 0
    #         perlist_scattered.append(scatteredvalue)
    #         # print len(perlist_total),len(perlist_straight),len(perlist_ws),len(perlist_wd),len(perlist_t),len(perlist_p),len(perlist_rh)
    #         # wfile.write(endtimestring + ' ' + str(perlist_total[i]) + '  ' + str(perlist_straight[i]) + '   ' + str(
    #         #     scatteredvalue) + ' ' + str(perlist_ws[i]) + '  ' + str(perlist_wd[i]) + ' ' + str(
    #         #     perlist_t[i]) + '    ' + str(perlist_rh[i]) + '  ' + str(perlist_p[i]))
    #         wfile.write(endtimestring + ' ' + str("%.2f" % perlist_total[i]) + '  ' + str(
    #             "%.2f" % perlist_straight[i]) + '   ' + str("%.2f" % scatteredvalue) + ' ' + str(
    #             "%.2f" % perlist_ws[i]) + '  ' + str("%.2f" % perlist_wd[i]) + ' ' + str(
    #             "%.2f" % perlist_t[i]) + '    ' + str("%.2f" % perlist_rh[i]) + '  ' + str("%.2f" % perlist_p[i]))
    #         wfile.write('\n')
    #         # print len(perlist_total),len(perlist_straight),len(perlist_ws),len(perlist_wd),len(perlist_t),len(perlist_p),len(perlist_rh)
    #         L.append((powerstationname, initial_txt, endtimestring001, str(perlist_total[i]), str(scatteredvalue),
    #                   str(perlist_straight[i]), str(perlist_ws[i]), str(perlist_wd[i]), str(perlist_t[i]),
    #                   str(perlist_rh[i]), str(perlist_p[i])))
    #     wfile.close()
    #
    #     if len(L) == 0:
    #         self.logger.warning("[%s]no data for powersun!", starttime.strftime('%Y%m%d%H'))
    #         return -1
    #
    #     # 数据入库
    #     sql = 'insert ignore into t_r_powerplant_radiation(city_id,initial_time,forecast_time,total_radiation,straight_radiation,scattered_radiation,wind_speed,wind_direction,temperature,humidity,air_pressure)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
    #     result = self.db.write_data(sql, *L)
    #
    #     if result is not None:
    #         if result > 0:
    #             self.logger.info('文件录入数据库成功')
    #         else:
    #             self.logger.warning('文件录入数据库失败')
    #     return 0
    '''
        starttime为EC预报起始时间
        txtpath为结果输出路径
        #ec_file_dir为EC数据所存在的文件路径带年和日期
    '''
    def weatherfeatureFromEC_new(self, starttime, lon, lat, powerstationname, txtpath, ec_file_dir):
        if starttime.hour not in (0, 12):
            return -1
        ecfilelist = self.getecfilelist_new(ec_file_dir,starttime)
        # 判断文件是否存在
        flag = True
        for i in range(len(ecfilelist)):
            if not os.path.exists(ecfilelist[i]):
                self.logger.info(ecfilelist[i] + '文件不存在')
                return -1
        yearstr = starttime.year
        monthstr = starttime.month
        daystr = starttime.day
        hourstr = starttime.hour
        if hourstr == 12:
            pdatetime = datetime.datetime(yearstr, monthstr, daystr, 12, 0, 0)
            initial_txt = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
            odatetime = pdatetime + datetime.timedelta(hours=20)
            initialtimestring = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
        else:
            pdatetime = datetime.datetime(yearstr, monthstr, daystr)
            initial_txt = datetime.datetime.strftime(pdatetime, '%Y-%m-%d %H:%M:%S')
            odatetime = pdatetime + datetime.timedelta(hours=20)
            initialtimestring = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
        # 定义list存放72小时的逐小时的预报数据共72个时次
        plist_totalradiation = []
        plist_straightradiation = []
        plist_surfacedradiation = []
        # test_radiation=[]
        plist_ws = []
        plist_wd = []
        plist_t = []
        plist_rh = []
        plist_p = []
        #print(ecfilelist)
        for i in range(1,len(ecfilelist),1):
            ecfile = ecfilelist[i]
            ecfile_pre=ecfilelist[i-1]
            grbs = pygrib.open(ecfile)
            grbs_pre=pygrib.open(ecfile_pre)
            # 总辐射
            grb001 = grbs.select(name='Surface solar radiation downwards')
            grb001_pre=grbs_pre.select(name='Surface solar radiation downwards')
            totalradiationarray = grb001[0].values
            # 散射辐射是计算出来的，总辐射-水平面直接辐射
            # 法直辐射：总辐射和法直辐射并没有什么关系：
            grb003 = grbs.select(name='Direct solar radiation')
            grb003_pre = grbs_pre.select(name='Direct solar radiation')
            # 暂时认为该变量为水平面直接辐射
            grb002 = grbs.select(name='Total sky direct solar radiation at surface')
            grb002_pre = grbs_pre.select(name='Total sky direct solar radiation at surface')
            # 平均风速
            grb004 = grbs.select(name='10 metre U wind component')
            grb005 = grbs.select(name='10 metre V wind component')
            # 空气温度
            grb006 = grbs.select(name='2 metre temperature')
            # 露点温度
            grb007 = grbs.select(name='2 metre dewpoint temperature')
            # 气压
            grb008 = grbs.select(name='Surface pressure')
            # 获取72个预报时次的数据矩阵,然后根据该点的经纬度获取最邻近点的要素值
            # 总辐射
            perarray001 = grb001[0].values
            perarray001_pre=grb001_pre[0].values
            # print(perarray001.shape,perarray001_pre.shape)
            perarray001 = numpy.array(perarray001)
            perarray001_pre=numpy.array(perarray001_pre)
            pvalue001 = self.linearForECvalue(perarray001, lon, lat)
            pvalue001_pre=self.linearForECvalue(perarray001_pre, lon, lat)
            plist_totalradiation.append(pvalue001-pvalue001_pre)
            #print(pvalue001_pre,pvalue001,pvalue001-pvalue001_pre,ecfile_pre,ecfile)
            # 法直辐射
            perarray003 = grb003[0].values
            perarray003_pre=grb003_pre[0].values
            perarray003 = numpy.array(perarray003)
            perarray003_pre=numpy.array(perarray003_pre)
            pvalue003 = self.linearForECvalue(perarray003, lon, lat)
            pvalue003_pre=self.linearForECvalue(perarray003_pre,lon,lat)
            plist_straightradiation.append(pvalue003-pvalue003_pre)
            # 散射辐射=总辐射-水平面直接辐射
            # 这里是水平面直接辐射
            perarray002 = grb002[0].values
            perarray002_pre=grb002_pre[0].values
            pvalue002 = self.linearForECvalue(perarray002, lon, lat)
            pvalue002_pre=self.linearForECvalue(perarray002_pre,lon,lat)
            plist_surfacedradiation.append(pvalue002-pvalue002_pre)
            # 平均风速
            # u分量
            perarray004 = grb004[0].values
            perarray004 = numpy.array(perarray004)
            # v分量
            perarray005 = grb005[0].values
            perarray005 = numpy.array(perarray005)
            pvalue004 = self.linearForECvalue(perarray004, lon, lat)
            pvalue005 = self.linearForECvalue(perarray005, lon, lat)
            ws = math.sqrt(pvalue004 * pvalue004 + pvalue005 * pvalue005)
            plist_ws.append(ws)
            wd = self.calculatwinddirect(pvalue004, pvalue005)
            plist_wd.append(wd)
            # 气温
            perarray006 = grb006[0].values
            perarray006 = numpy.array(perarray006)
            t = self.linearForECvalue(perarray006, lon, lat) - 273.15
            plist_t.append(t)
            # 湿度
            # 根据气温和露点温度计算
            # 露点温度
            perarray007 = grb007[0].values
            perarray007 = numpy.array(perarray007)
            dt = self.linearForECvalue(perarray007, lon, lat) - 273.15
            rh = 100 * math.exp((17.625 * dt) / (243.04 + dt)) / math.exp((17.625 * t) / (243.04 + t))
            plist_rh.append(rh)
            # 气压
            perarray008 = grb008[0].values
            perarray008 = numpy.array(perarray008)
            p = self.linearForECvalue(perarray008, lon, lat) / 100
            plist_p.append(p)
        # csvwf.close()
        # 为了测试写的文件，瞬时值计算
        # a1=self.totaltosiple_1h(plist_totalradiation)
        # a2=self.totaltosiple_1h(plist_straightradiation)
        # a3=self.totaltosiple_1h(plist_surfacedradiation)
        # csvfile2='/Users/yetao.lu/2017/2.csv'
        # cvw=open(csvfile2,'w')
        # for p in range(len(a1)):
        #     cvw.write(str(a1[p])+','+str(a2[p])+','+str(a3[p]))
        #     cvw.write('\n')
        # cvw.close()
        # 总辐射
        pplist_total = self.totaltosiple_1h(plist_totalradiation)
        perlist_total = self.calculateRadiationbyInterplote_1h(pplist_total)
        print(plist_totalradiation)
        print(pplist_total)
        print(perlist_total)
        # 法直辐射
        pplist_straight = self.totaltosiple_1h(plist_straightradiation)
        perlist_straight = self.calculateRadiationbyInterplote_1h(pplist_straight)
        # print pplist_straight
        # 散射辐射=总辐射减去水平面直接辐射的值，这里还是水平面直接辐射的插值
        pplist_surface = self.totaltosiple_1h(plist_surfacedradiation)
        perlist_fdir = self.calculateRadiationbyInterplote_1h(pplist_surface)
        # 风速
        perlist_ws = self.calculateRadiationbyInterplote_1h(plist_ws)
        # 风向
        perlist_wd = self.calculateRadiationbyInterplote_1h(plist_wd)
        # 气温
        perlist_t = self.calculateRadiationbyInterplote_1h(plist_t)
        # 相对湿度
        perlist_rh = self.calculateRadiationbyInterplote_1h(plist_rh)
        # 气压
        perlist_p = self.calculateRadiationbyInterplote_1h(plist_p)
        # 平滑过滤,所有的要素都平滑过滤了
        # print perlist_total
        perlist_total = scipy.signal.savgol_filter(perlist_total, 5, 2)
        perlist_straight = scipy.signal.savgol_filter(perlist_straight, 5, 2)
        perlist_fdir = scipy.signal.savgol_filter(perlist_fdir, 5, 2)
        perlist_ws = scipy.signal.savgol_filter(perlist_ws, 5, 2)
        perlist_wd = scipy.signal.savgol_filter(perlist_ws, 5, 2)
        perlist_t = scipy.signal.savgol_filter(perlist_t, 5, 2)
        perlist_rh = scipy.signal.savgol_filter(perlist_rh, 5, 2)
        perlist_p = scipy.signal.savgol_filter(perlist_p, 5, 2)
        # print perlist_total
        # 写文件
        # 首先确定文件名称
        # txtpath001 = txtpath + '/' + str(year_t) + '/' + pdate_t
        txtfile = os.path.join(txtpath, powerstationname + initialtimestring + '.txt')
        wfile = open(txtfile, 'w')
        L = []
        linelist = []
        # 定义散射辐射列表
        perlist_scattered = []
        print(len(perlist_total))
        for i in range(len(perlist_total)):
            endtime = pdatetime + datetime.timedelta(hours=20,minutes=i * 15)
            endtimestring = datetime.datetime.strftime(endtime, '%Y%m%d%H%M')
            endtimestring001 = datetime.datetime.strftime(endtime, '%Y-%m-%d %H:%M:%S')
            scatteredvalue = perlist_total[i] - perlist_fdir[i]
            print(pdatetime, endtime, perlist_total[i], perlist_straight[i],scatteredvalue)
            if perlist_total[i] < 10:
                perlist_total[i] = 0
            if perlist_straight[i] < 10:
                perlist_straight[i] = 0
            if scatteredvalue < 10:
                scatteredvalue = 0
            perlist_scattered.append(scatteredvalue)
            # print len(perlist_total),len(perlist_straight),len(perlist_ws),len(perlist_wd),len(perlist_t),len(perlist_p),len(perlist_rh)
            wfile.write(endtimestring + ' ' + str("%.2f" % perlist_total[i]) + '  ' + str(
                "%.2f" % perlist_straight[i]) + '   ' + str("%.2f" % scatteredvalue) + ' ' + str(
                "%.2f" % perlist_ws[i]) + '  ' + str("%.2f" % perlist_wd[i]) + ' ' + str(
                "%.2f" % perlist_t[i]) + '    ' + str("%.2f" % perlist_rh[i]) + '  ' + str("%.2f" % perlist_p[i]))
            wfile.write('\n')
            #print(len(perlist_total),len(perlist_straight),len(perlist_ws),len(perlist_wd),len(perlist_t),len(perlist_p),len(perlist_rh))
            L.append((powerstationname, initial_txt, endtimestring001, str(perlist_total[i]), str(scatteredvalue),
                      str(perlist_straight[i]), str(perlist_ws[i]), str(perlist_wd[i]), str(perlist_t[i]),
                      str(perlist_rh[i]), str(perlist_p[i])))
        wfile.close()

        if len(L) == 0:
            self.logger.warning("[%s]no data for powersun!", starttime.strftime('%Y%m%d%H'))
            return -1

        # 数据入库
        sql = 'insert ignore into t_r_powerplant_radiation(city_id,initial_time,forecast_time,total_radiation,scattered_radiation,straight_radiation,wind_speed,wind_direction,temperature,humidity,air_pressure)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
        result = self.db.write_data(sql, *L)

        if result is not None:
            if result > 0:
                self.logger.info('文件录入数据库成功')
            else:
                self.logger.warning('文件录入数据库失败')
        return 0

if __name__ == "__main__":
    #logpath = '/moji/meteo/cluster/program/moge/power'
    logpath='/Users/yetao.lu'
    logging.basicConfig()
    logger = logging.getLogger("apscheduler.executors.default")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
        '%a, %d %b %Y %H:%M:%S', )
    logfile = os.path.join(logpath,'power.log')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    hdlr = logging.handlers.TimedRotatingFileHandler(logfile, when='D',interval=1,backupCount=40)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    #db=pymysql.connect('172.16.8.28', 'admin', 'moji_China_123','moge', 3307)
    
    power=Powersun_base(logger,None)
    startime001=datetime.datetime.strptime('2018-11-26 00:00:00','%Y-%m-%d %H:%M:%S')
    #power.weatherfeatureFromEC(startime001,114.0352, 40.4638, 'TZHCS', '/moji/meteo/cluster/program/moge/power','/opt/meteo/cluster/data/ecmwf/orig/2018/2018-10-15')
    #power.powersun_check(startime001,114.0352, 40.4638,'TZHCS','/opt/meteo/cluster/data/ecmwf/temp/2018/2018-11-26')
    #ecrootpath='/opt/meteo/cluster/data/ecmwf/temp/2018/2018-11-26'
    ecrootpath='/Users/yetao.lu/Downloads/sunec'
    print(ecrootpath)
    #power.weatherfeatureFromEC_new(startime001,114.0352, 40.4638, 'TZHCS', '/moji/meteo/cluster/program/moge/power','/opt/meteo/cluster/data/ecmwf/orig/2018/2018-10-15')
    power.weatherfeatureFromEC_new(startime001,93.9203, 42.2439, 'XJHMSK', '/Users/yetao.lu/Downloads/sunec','/Users/yetao.lu/Downloads/1126')
# 日志模块

# ###
# ecrootpath='/opt/meteo/cluster/data/ecmwf/orig'
# powersun001=Powersun_base(logger,db)
# starttime=datetime.datetime.now()
# yearstr = starttime.year
# monthstr = starttime.month
# daystr = starttime.day
# hourstr = starttime.hour
# hourstr = starttime.hour
# initialtimestring=''
# if hourstr < 17:
#     pdatetime = datetime.datetime(yearstr, monthstr, daystr, 12, 0, 0) + datetime.timedelta(days=-1)
#     year_t=pdatetime.year
#     pdate_t=datetime.datetime.strftime(pdatetime,'%Y-%m-%d')
#     initial_txt=datetime.datetime.strftime(pdatetime,'%Y-%m-%d %H:%M:%S')
#     odatetime=pdatetime+datetime.timedelta(hours=8)
#     initialtimestring=datetime.datetime.strftime(odatetime,'%Y%m%d%H')
# else:
#     pdatetime = datetime.datetime(yearstr, monthstr, daystr)
# txtpath='/Users/yetao.lu/Downloads/tmp/SSRA'
#
# powersun001.weatherfeatureFromEC(pdatetime,114.0352, 40.4638, 'TZHCS', txtpath)
#
# #程序部分
# tmppath='/opt/meteo/cluster/data/ecmwf/tmp'
# #ecfile='/Users/yetao.lu/Downloads/_mars-atls04-70e05f9f8ba4e9d19932f1c45a7be8d8-eQOWUm.grib'
# ecfile='/opt/meteo/cluster/data/ecmwf/orig/2018/2018-09-29/D1D09290000092903001'
# starttime=datetime.datetime.now()
# #testfile='/Users/yetao.lu/D1D09250000092503001'
# powerstationname001='TZHCS'
# #txtpath='/Users/yetao.lu/2017/'
# txtpath='/opt/meteo/cluster/data/ecmwf/powerplant/'
# yearstr = starttime.year
# monthstr = starttime.month
# daystr = starttime.day
# hourstr = starttime.hour
# hourstr = starttime.hour
# initialtimestring=''
# if hourstr < 17:
#     pdatetime = datetime.datetime(yearstr, monthstr, daystr, 12, 0, 0) + datetime.timedelta(days=-1)
#     year_t=pdatetime.year
#     pdate_t=datetime.datetime.strftime(pdatetime,'%Y-%m-%d')
#     initial_txt=datetime.datetime.strftime(pdatetime,'%Y-%m-%d %H:%M:%S')
#     odatetime=pdatetime+datetime.timedelta(hours=8)
#     initialtimestring=datetime.datetime.strftime(odatetime,'%Y%m%d%H')
# else:
#     pdatetime = datetime.datetime(yearstr, monthstr, daystr)
# weatherfeatureFromEC(pdatetime,114.0352, 40.4638, 'TZHCS', txtpath)
# weatherfeatureFromEC(pdatetime,93.9203, 42.2439, 'XJHMSK', txtpath)
# weatherfeatureFromEC(pdatetime,101.8403, 24.7708, 'YNCXDZ', txtpath)
# 定时任务部分
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
#     print (e.message)
