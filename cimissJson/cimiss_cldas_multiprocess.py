#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/17
description:从气象局信息中心的接口中下载CLDAS数据1公里的，APSCHEDULER
"""
import urllib2, os, json, datetime, time, logging, sys,multiprocessing

from apscheduler.schedulers.background import BackgroundScheduler

def downloadcldas(odatetime,dict):
    filename = dict['FILE_NAME']
    logger.info(filename)
    yearstr = str(odatetime.year)
    monstr = str(odatetime.month)
    daystr = str(odatetime.day)
    hourstr = str(odatetime.hour)
    if odatetime.hour < 10:
        hourstr = '0' + str(odatetime.hour)
    else:
        hourstr = str(odatetime.hour)
    pdate = datetime.datetime.strftime(odatetime, '%Y-%m-%d')
    if not os.path.exists(outpath + '/' + yearstr):
        os.mkdir(outpath + '/' + yearstr)
    if not os.path.exists(
            outpath + '/' + yearstr + '/' + pdate):
        os.mkdir(outpath + '/' + yearstr + '/' + pdate)
    if not os.path.exists(
            outpath + '/' + yearstr + '/' + pdate + '/' + hourstr):
        os.mkdir(outpath + '/' + yearstr + '/' + pdate + '/' + hourstr)
    file01 = os.path.join(
        outpath + '/' + yearstr + '/' + pdate + '/' + hourstr,
        filename)
    print file01
    fileurl = dict['FILE_URL']
    fileurl = str.replace(str(fileurl),
                          'http://ftp.data.cma.cn:8000/dlcdc/',
                          'http://cdcfileprivate.oss-cn-beijing-internal.aliyuncs.com/')
    print fileurl
    fileread = urllib2.urlopen(fileurl)
    filewrite = open(file01, 'w')
    filewrite.write(fileread.read())
    filewrite.close()
def cldasfileDownload(outpath, logger):
    pdatetime = datetime.datetime.now()
    odatetime = pdatetime + datetime.timedelta(hours=-29)
    pdatestr = odatetime.strftime('%Y%m%d%H') + '0000'
    url = 'http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_HRCLDAS_RT_NC&time=' + pdatestr + '&elements=Bul_Center,FILE_NAME,Data_Area,Datetime,File_URL&dataFormat=json'
    print url
    request = urllib2.Request(url);
    f = urllib2.urlopen(request)
    link = f.read()
    file = json.loads(link)
    print file
    dataset = file['DS']
    pool=multiprocessing.Pool(processes=9)
    for dict in dataset:
        pool.apply_async(downloadcldas, args=(odatetime,dict))
    pool.close()
    pool.join()
if __name__ == "__main__":
    #outpath = '/Users/yetao.lu/Downloads/tmp'
    #logpath ='/Users/yetao.lu/Downloads/tmp'
    outpath='/home/wlan_dev/cldas'
    logpath='/home/wlan_dev/log'
    logger = logging.getLogger("apscheduler.executors.default")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
        '%a, %d %b %Y %H:%M:%S', )
    logfile = os.path.join(logpath, "cldas.log")
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    scheduler.add_job(cldasfileDownload, 'cron', minute='40,50',
                      args=(outpath, logger))
    scheduler.start()
    try:
        while True:
            time.sleep(2)
    except(KeyboardInterrupt, SystemExit):
        logger.info('System out')







