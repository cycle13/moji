#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/14
description:
"""
import datetime,time
from apscheduler.schedulers.background import BackgroundScheduler
from tasks import add003
from bigdataDownloadEC import downloadECdatafrombigdata_threading
from bigdataDownloadEC import downloadECdatafrombigdata
from bz2fileTogrib import unzipfile_threading
from bz2fileTogrib import unzipfile
from bz2fileTogrib import copyAndUnzipfile
from ecxgboostlive_dem_temp import modelProdict
from temp_accuracy_dem import calculateAccuracy
from roottask import app
from celery.utils.log import get_task_logger
from ecxgboostlive_temp import modelpredict001
from temp_accuracy import calculateAccuracy001
from deleteECdata import deleteECMWF
logger = get_task_logger(__name__)
# mos统计流程梳理
@app.task
def mosrun_history(starttime):
    #1. 首先是下载EC数据，
    # yearint=datetime.datetime.now().year
    # hours=datetime.datetime.now().hour
    yearint=starttime.year
    hours=starttime.hour
    if hours>17:
        datestr=datetime.datetime.strftime(starttime,'%Y-%m-%d')
        pdatetimestring00 = datestr+" 00:00:00"
        print "2018-08-21 00:00:00"
    else:
        nowdate=starttime+datetime.timedelta(days=-1)
        datestr=datetime.datetime.strftime(nowdate,'%Y-%m-%d')
        pdatetimestring00 = datestr+' 12:00:00'
    initial_datetime = datetime.datetime.strptime(pdatetimestring00,'%Y-%m-%d %H:%M:%S')
    logger.info(initial_datetime)
    #传入时间为00点或者是12点
    downloadECdatafrombigdata(initial_datetime)
    #2.解压EC数据
    dirpath='/moji/ecdata'
    print "2018-08-21 00:00:00-----------------------------------------------------------"
    ecpath=unzipfile(initial_datetime,dirpath)
    print ecpath+'==================================================================='
    #3.DEM模型预测
    # path = '/moji/ecdata'
    outpath = '/home/wlan_dev/result'
    csvfile = '/home/wlan_dev/stations.csv'
    demcsv = '/mnt/data/dem.csv'
    modelProdict(ecpath, outpath, csvfile, demcsv,initial_datetime)
    #4.准确率计算
    calculateAccuracy(initial_datetime)
    # 模型预测和准确率计算
    modelpredict001(ecpath,outpath,csvfile,starttime)
    calculateAccuracy001(initial_datetime)
    print ecpath
    #5.删除EC数据
    if hours<12:
        deleteECMWF(ecpath)
def mosrun_live(starttime):
    #1. 首先是下载EC数据，
    # yearint=datetime.datetime.now().year
    # hours=datetime.datetime.now().hour
    yearint=starttime.year
    hours=starttime.hour
    if hours>=17:
        datestr=datetime.datetime.strftime(starttime,'%Y-%m-%d')
        pdatetimestring00 = datestr+" 00:00:00"
        print "2018-08-21 00:00:00"
    else:
        nowdate=starttime+datetime.timedelta(days=-1)
        datestr=datetime.datetime.strftime(nowdate,'%Y-%m-%d')
        pdatetimestring00 = datestr+' 12:00:00'
    initial_datetime = datetime.datetime.strptime(pdatetimestring00,'%Y-%m-%d %H:%M:%S')
    #2.拷贝解压EC数据,线上的数据是拷贝的
    oldfilepath='/opt/meteo/cluster/data/ecmwf/orig'
    dirpath='/moji/ecdata'
    print "2018-08-21 00:00:00-----------------------------------------------------------"
    ecpath=copyAndUnzipfile(initial_datetime,oldfilepath,dirpath)
    logger.info(ecpath)
    #3.DEM模型预测
    # path = '/moji/ecdata'
    outpath = '/home/wlan_dev/result'
    csvfile = '/home/wlan_dev/stations.csv'
    demcsv = '/mnt/data/dem.csv'
    modelProdict(ecpath, outpath, csvfile, demcsv,starttime)
    #4.准确率计算
    calculateAccuracy(initial_datetime)
    # 模型预测和准确率计算
    modelpredict001(ecpath,outpath,csvfile,starttime)
    calculateAccuracy001(initial_datetime)
    print ecpath
    #5.删除EC数据
    # if hours<12:
    #     deleteECMWF(ecpath)
if __name__ == "__main__":
    initialtime="2018-06-01 05:00:00"
    initial_time001=datetime.datetime.strptime(initialtime,'%Y-%m-%d %H:%M:%S')
    #测试
    #add003.delay(2,2)
    for i in range(32):
        initial_time=initial_time001+datetime.timedelta(days=i)
        mosrun_history(initial_time)
    
        starttime002=initial_time+datetime.timedelta(hours=18)
        mosrun_history(starttime002)
    #线上运行
    #starttime = datetime.datetime.now()
    #mosrun_live(starttime)
