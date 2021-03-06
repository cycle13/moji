#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/3
description:大数据平台下周EC历史数据，调用SH脚本，
20180824这里有个坑：注意：多进程(multiprocessing) 无法在 celery 等后台进程中使用，
因为 celery 等后台进程不再允许生成子进程,这时候就只能使用多线程或者协程了。
20180825采用多线程测试EC数据下载，这里没有用协程是因为协程是用单线程来模式并发应该没有多线程好使
"""

from mosapp import app
import os,datetime,subprocess,multiprocessing
from celery.utils.log import get_task_logger
from concurrent import futures
#传入时间为0点或者是12点的时间pdatetime要么是00点要么是12点
@app.task
def getECfilenameandpath(i,pdatetime,yearstr,monstr,daystr,hourstr,datestr):
    if i < 40:
        udatetime = pdatetime + datetime.timedelta(hours=3 * (i + 1))
    else:
        udatetime = pdatetime + datetime.timedelta(hours=3 * 40 + 6 * (i - 39))
    # 取文件
    umonth = udatetime.month
    if umonth < 10:
        umonthstr = '0' + str(umonth)
    else:
        umonthstr = str(umonth)
    udays = udatetime.day
    if udays < 10:
        udaystr = '0' + str(udays)
    else:
        udaystr = str(udays)
    uhour = udatetime.hour
    if uhour < 10:
        uhourstr = '0' + str(uhour)
    else:
        uhourstr = str(uhour)
    ecfile00 = 'D1D' + monstr + daystr + hourstr + '00' + umonthstr + udaystr + uhourstr + '001.bz2'
    ecfullpath = '/meteo/moge/data/ecmwf/' + yearstr + '/' + datestr + '/' + ecfile00
    print udatetime,ecfullpath
    topath = '/moji/ecdata/' + yearstr + '/' + datestr
    if not os.path.exists(topath):
        os.mkdir(topath)
    print '/home/wlan_dev/software/api/start_hdfs_access.sh ' + ecfullpath + ' /moji/ecdata/' + yearstr + '/' + datestr + ' 1'
    #logger.info('/home/wlan_dev/software/api/start_hdfs_access.sh ' + ecfullpath + ' /moji/ecdata/' + yearstr + '/' + datestr + ' 1')
    p=subprocess.call('/home/wlan_dev/software/api/start_hdfs_access.sh ' + ecfullpath + ' /moji/ecdata/' + yearstr + '/' + datestr + ' 1',shell=True)
def getECfilenameandpath_celery(i,pdatetime,yearstr,monstr,daystr,hourstr,datestr):
    if i < 40:
        udatetime = pdatetime + datetime.timedelta(hours=3 * (i + 1))
    else:
        udatetime = pdatetime + datetime.timedelta(hours=3 * 40 + 6 * (i - 39))
    # 取文件
    umonth = udatetime.month
    if umonth < 10:
        umonthstr = '0' + str(umonth)
    else:
        umonthstr = str(umonth)
    udays = udatetime.day
    if udays < 10:
        udaystr = '0' + str(udays)
    else:
        udaystr = str(udays)
    uhour = udatetime.hour
    if uhour < 10:
        uhourstr = '0' + str(uhour)
    else:
        uhourstr = str(uhour)
    ecfile00 = 'D1D' + monstr + daystr + hourstr + '00' + umonthstr + udaystr + uhourstr + '001.bz2'
    print udatetime
    ecfullpath = '/meteo/moge/data/ecmwf/' + yearstr + '/' + datestr + '/' + ecfile00
    print ecfullpath
    topath = '/home/wlan_dev/tmp/' + yearstr + '/' + datestr
    if not os.path.exists(topath):
        os.mkdir(topath)
    os.system(
        '/home/wlan_dev/software/api/start_hdfs_access.sh ' + ecfullpath + ' /moji/ecdata/' + yearstr + '/' + datestr + ' 1')
@app.task
#传入时间参数为0点或12点
def downloadECdatafrombigdata(pdatetime):
    year=pdatetime.year
    yearstr=str(year)
    datestr=datetime.datetime.strftime(pdatetime,'%Y-%m-%d')
    month=pdatetime.month
    if month<10:
        monstr='0'+str(month)
    else:
        monstr=str(month)
    days=pdatetime.day
    if days<10:
        daystr='0'+str(days)
    else:
        daystr=str(days)
    hours=pdatetime.hour
    if hours==0:
        hourstr='00'
    else:
        hourstr='12'
    #原来任务是定时，现在修改为多进程，虽然subprocess也是多进程，因为原来不需要关心下载是否完成，现在需要知道完成，并进行下一步解压
    #因此在外面再包一层多进程，来实现。进程里面的进程需要等待
    pool=multiprocessing.Pool(processes=20)
    for i in range(60):
        print i
        pool.apply_async(getECfilenameandpath,args=(i,pdatetime,yearstr,monstr,daystr,hourstr,datestr))
    pool.close()
    pool.join()
@app.task
def downloadECdatafrombigdata_processing(pdatetime,i):
    year=pdatetime.year
    yearstr=str(year)
    datestr=datetime.datetime.strftime(pdatetime,'%Y-%m-%d')
    month=pdatetime.month
    if month<10:
        monstr='0'+str(month)
    else:
        monstr=str(month)
    days=pdatetime.day
    if days<10:
        daystr='0'+str(days)
    else:
        daystr=str(days)
    hours=pdatetime.hour
    if hours==0:
        hourstr='00'
    else:
        hourstr='12'
    #下载文件
    getECfilenameandpath(i,pdatetime,yearstr,monstr,daystr,hourstr,datestr)


    
