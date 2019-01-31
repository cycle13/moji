#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/12/27
description: python3.6
"""
import os, datetime,subprocess,logging,sys


class ecrainsnow(object):
    def __init__(self):
        print ''
    
    # 判断文件是否存在,是返回true，否则false
    def ecrainsnow_premise(self, ecrootpath, starttime):
        yearint = starttime.year
        datestring = datetime.datetime.strftime(starttime, '%Y-%m-%d')
        ecfilepath = ecrootpath + '/' + str(yearint) + '/' + datestring
        flag = True
        for i in range(36):
            endtime = starttime + datetime.timedelta(hours=i + 1)
            fromstr = datetime.datetime.strftime(starttime, '%m%d%H')
            tostr = datetime.datetime.strftime(endtime, '%m%d%H')
            filename = 'D2S' + fromstr + '00' + tostr + '001'
            filefullpath = os.path.join(ecfilepath, filename)
            flag = flag or os.path.exists(filefullpath)
        return flag
    
    def ecrainsnow_main(self, ecrootpath, livepath, rulefile, starttime):
        yearint = starttime.year
        datestring = datetime.datetime.strftime(starttime, '%Y-%m-%d')
        ecfilepath = ecrootpath + '/' + str(yearint) + '/' + datestring
        for root, dirs, files in os.walk(ecfilepath):
            for file in files:
                if file[:3] == 'D2S' and file[-3:] == '001' and (
                        file[7:9] == '00' or file[7:9] == '12'):
                    print file
                    filename = os.path.join(root, file)
                    initial = str(yearint) + file[3:9]
                    forecast = str(yearint) + file[11:17]
                    initial_time = datetime.datetime.strptime(initial,
                                                              '%Y%m%d%H')
                    forecast_time = datetime.datetime.strptime(forecast,
                                                               '%Y%m%d%H')
                    #判断起报时间和预报时间，跨年+1
                    if initial_time>forecast_time:
                        forecast_time=forecast_time+datetime.timedelta(year=1)
                    d = (forecast_time - initial_time).days
                    f = (forecast_time - initial_time).seconds / 3600
                    hours = (d * 24 + (
                                forecast_time - initial_time).seconds / 3600)
                    if hours < 10:
                        hourstring = '00' + str(hours)
                    elif hours >= 10 and hours <= 36:
                        hourstring = '0' + str(hours)
                    else:
                        continue
                    if not os.path.exists(livepath + '/' + str(yearint)):
                        os.mkdir(livepath + '/' + str(yearint))
                    if not os.path.exists(
                            livepath + '/' + str(yearint) + '/' + datestring):
                        os.mkdir(
                            livepath + '/' + str(yearint) + '/' + datestring)
                    outfile = livepath + '/' + str(
                        yearint) + '/' + datestring + '/' + initial + '.' + hourstring
                    print(outfile, file)
                    os.system(
                        'grib_filter ' + rulefile + ' ' + filename + ' -o ' + outfile)
                    if os.path.exists(outfile):
                        #p=subprocess.call('/opt/meteo/cluster/program/moge/ec_rainsnow/ec_snow ' + outfile,shell=True)
                        p=subprocess.call(['./ec_snow',outfile],cwd='/opt/meteo/cluster/program/moge/ec_rainsnow')
                        
if __name__ == "__main__":
    # 加日志
    logger = logging.getLogger('learing.logger')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    logfile = '/home/cluser/gpu_winter.log'
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值
    
    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    
    starttime = datetime.datetime.strptime('2018-12-06 12:00:00',
                                           '%Y-%m-%d %H:%M:%S')
    ecrootpath = '/home/cluser/mosdata'
    livepath = '/opt/meteo/cluster/data/ecmwf/temp/live'
    rulefile = '/opt/meteo/cluster/program/moge/ec_rainsnow/rules_file'
    for i in range(24):
        starttime001=starttime+datetime.timedelta(days=i)
        ecrainsnow001 = ecrainsnow()
        ecrainsnow001.ecrainsnow_main(ecrootpath, livepath, rulefile, starttime001)