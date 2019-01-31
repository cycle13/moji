#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/14
description:
"""
import datetime,os,logging
from mosapp import app
from celery.utils.log import get_task_logger
from celery import chain
from celery import group
from bz2fileTogrib import unzipfileinchain
from ecxgboostlive_dem_temp import simplemodelProdict
from temp_accuracy_dem import calculateAccuracy
from deleteECdata import deleteECMWF
logger = get_task_logger(__name__)
# mos统计流程梳理
@app.task
def runmodel(oldfilepath,dirpath):
    starttime=datetime.datetime.now()
    #这里还是开始时间
    starttimestring=datetime.datetime.strftime(starttime,'%Y-%m-%d %H:%M:%S')
    #starttime=datetime.datetime.strptime('2018-09-15 20:00:00','%Y-%m-%d %H:%M:%S')
    yearstr = starttime.year
    monthstr=starttime.month
    daystr=starttime.day
    hourstr = starttime.hour
    if hourstr < 17:
        #把实时时间转成00时的时间传给计算准确率的函数
        ppdatetime=datetime.datetime(yearstr,monthstr,daystr,0,0,0)
        starttimestring=datetime.datetime.strftime(ppdatetime,'%Y-%m-%d %H:%M:%S')
        dict = {}
        nowdate = starttime + datetime.timedelta(days=-1)
        datepath = datetime.datetime.strftime(nowdate, '%Y-%m-%d')
        filepath = oldfilepath + '/' + str(yearstr) + '/' + datepath
        midpath = dirpath + '/' + str(yearstr)
        if not os.path.exists(midpath):
            os.mkdir(midpath)
        tofilepath = dirpath + '/' + str(yearstr) + '/' + datepath
        if not os.path.exists(tofilepath):
            os.mkdir(tofilepath)
        for root, dirs, files in os.walk(filepath):
            for file in files:
                if file[:3] == 'D1D' and file[7:9] == '12' and file[-7:-4]=='001' and file[-4:] == '.bz2':
                    filename = os.path.join(root, file)
                    logging.info(filename)
                    dict[filename] = tofilepath
        
        #chain为串行，group并行
        result = group(simplemodelProdict.s(key,value,yearstr) for key, value in dict.items())()
        #logging.info(result)
    else:
        #传入的日期的时间
        ppdatetime = datetime.datetime(yearstr, monthstr, daystr, 12, 0, 0)
        starttimestring = datetime.datetime.strftime(ppdatetime,
                                                     '%Y-%m-%d %H:%M:%S')
        dict00 = {}
        datepath = datetime.datetime.strftime(starttime, '%Y-%m-%d')
        filepath = oldfilepath + '/' + str(yearstr) + '/' + datepath
        midpath = dirpath + '/' + str(yearstr)
        if not os.path.exists(midpath):
            os.mkdir(midpath)
        tofilepath = dirpath + '/' + str(yearstr) + '/' + datepath
        if not os.path.exists(tofilepath):
            os.mkdir(tofilepath)
        for root, dirs, files in os.walk(filepath):
            for file in files:
                if file[:3] == 'D1D' and file[7:9] == '00' and file[-7:-4]=='001' and file[-4:] == '.bz2':
                    filename = os.path.join(root, file)
                    logging.info(filename)
                    dict00[filename] = tofilepath
        #注意后面有个括号。
        result = group(simplemodelProdict.s(key,value,yearstr) for key, value in dict00.items())()
        #logging.info(result)
#然后计算准确率
    calculateAccuracy(starttimestring)
    deleteECMWF(dirpath)
# if __name__ == "__main__":
#     starttime = datetime.datetime.now()
#     oldfilepath='/opt/meteo/cluster/data/ecmwf/orig'
#     dirpath='/moji/ecdata'
    #runmodel(oldfilepath,dirpath)
