#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/12/27
description: python3.6
"""
import os, datetime,subprocess


class ecrainsnow(object):
    def __init__(self,logger):
        self.logger=logger
    
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
                        p=subprocess.call(
                            '/opt/meteo/cluster/program/moge/ec_rainsnow/ec_snow ' + outfile,shell=True)
                        if p<>0:
                            self.logger.info(outfile+'处理异常')
                            


if __name__ == "__main__":
    starttime = datetime.datetime.strptime('2018-12-27 12:00:00',
                                           '%Y-%m-%d %H:%M:%S')
    ecrootpath = '/opt/meteo/cluster/data/ecmwf/orig'
    livepath = '/opt/meteo/cluster/data/ecmwf/temp/live'
    rulefile = '/opt/meteo/cluster/program/moge/ec_rainsnow/rules_file'
    ecrainsnow = ecrainsnow()
    flag = ecrainsnow.ecrainsnow_premise(ecrootpath, starttime)
    if flag == True:
        ecrainsnow.ecrainsnow_main(ecrootpath, livepath, rulefile, starttime)