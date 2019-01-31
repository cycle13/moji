#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/14
description:
"""
import datetime,os,logging,sys
from bigdataDownloadEC import downloadECdatafrombigdata
from bz2fileTogrib import unzipfile
from bz2fileTogrib import copyAndUnzipfile
from temp_accuracy_dem import calculateAccuracy
from temp_accuracy_dem2 import calculateAccuracy2
from deleteECdata import deleteECMWF
from ecxgboost_predicttemp_gpu import readymodel
from moslogger import Logger
# mos统计流程梳理
def mosrun_history(starttime):
    #1. 首先是下载EC数据，
    # yearint=datetime.datetime.now().year
    # hours=datetime.datetime.now().hour
    yearint=starttime.year
    hours=starttime.hour
    #给定时间世界时大于17计算本天的，否则计算昨天的12点的
    if hours>17:
        datestr=datetime.datetime.strftime(starttime,'%Y-%m-%d')
        pdatetimestring00 = datestr+" 00:00:00"
    else:
        nowdate=starttime+datetime.timedelta(days=-1)
        datestr=datetime.datetime.strftime(nowdate,'%Y-%m-%d')
        pdatetimestring00 = datestr+' 12:00:00'
    initial_datetime = datetime.datetime.strptime(pdatetimestring00,'%Y-%m-%d %H:%M:%S')
    #传入时间为00点或者是12点
    downloadECdatafrombigdata(initial_datetime)
    #2.解压EC数据
    dirpath='/home/wlan_dev/mosdata'
    print "-----------------------------------------------------------"
    ecpath=unzipfile(initial_datetime,dirpath)
    print ecpath+'==================================================================='
    #3.DEM模型预测
    # path = '/moji/ecdata'
    outpath = '/home/wlan_dev/mos/mosresult'
    csvfile = '/mnt/data/mosfile/stations.csv'
    demcsv = '/mnt/data/mosfile/dem0.1.csv'
    #modelProdict(ecpath, csvfile, demcsv,initial_datetime)
    ecfilename= os.path.split(ecpath)[1]
    readymodel(initial_datetime,dirpath)
    #4.准确率计算
    calculateAccuracy(initial_datetime)
    calculateAccuracy2(initial_datetime)
    # 模型预测和准确率计算
    # modelpredict001(ecpath,outpath,csvfile,starttime)
    # calculateAccuracy001(initial_datetime)
    print ecpath
    #5.删除EC数据
    # if hours<12:
    #     deleteECMWF(ecpath)
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
    ecpath=copyAndUnzipfile(initial_datetime,oldfilepath,dirpath)
    #3.DEM模型预测
    # path = '/moji/ecdata'
    outpath = '/home/wlan_dev/result'
    csvfile = '/home/wlan_dev/stations.csv'
    demcsv = '/mnt/data/dem.csv'
    # modelProdict(ecpath, csvfile, demcsv,starttime)
    #4.准确率计算
    calculateAccuracy(initial_datetime)
    #5.删除EC数据
    if hours<12:
        deleteECMWF(ecpath)
if __name__ == "__main__":
    global logger
    logger=Logger('log')
    initialtime="2018-10-01 05:00:00"
    initial_time001=datetime.datetime.strptime(initialtime,'%Y-%m-%d %H:%M:%S')
    #测试:10月30天的记录
    for i in range(10):
        initial_time=initial_time001+datetime.timedelta(days=i)
        mosrun_history(initial_time)
        
        starttime002=initial_time+datetime.timedelta(hours=18)
        mosrun_history(starttime002)
    #线上运行
    #starttime = datetime.datetime.now()
    #mosrun_live(starttime)
    # 创建后台执行的schedulers
    # scheduler = BackgroundScheduler()
    # scheduler.add_job(mosrun_live, 'cron', minute='55',hour='5,17',args=(starttime,))
    # scheduler.start()
    # try:
    #     while True:
    #         time.sleep(2)
    # except Exception as e:
    #     print e.message
