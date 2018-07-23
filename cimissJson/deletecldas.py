#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/6/26
description:
"""
import os,time,datetime,shutil ,logging,sys
from apscheduler.schedulers.background import BackgroundScheduler
def deletecldasdata(filepath,flagtime):
    for root,dirs,files in os.walk(filepath):
        for file in files:
            filefullname=os.path.join(root,file)
            filetime=time.ctime(os.path.getctime(filefullname))
            filetimesecond=os.path.getctime(filefullname)
            print filetime,type(filetime)
            ModifiedTime=time.localtime(filetimesecond)
            y = time.strftime('%Y', ModifiedTime)
            m = time.strftime('%m', ModifiedTime)
            d = time.strftime('%d', ModifiedTime)
            H = time.strftime('%H', ModifiedTime)
            M = time.strftime('%M', ModifiedTime)
            d2 = datetime.datetime(int(y), int(m), int(d), int(H), int(M))
            print d2
            if d2<flagtime:
                os.remove(filefullname)
        for dir in dirs:
            dirpath=os.path.join(root,dir)
            filetimesecond = os.path.getctime(dirpath)
            print filetime, type(filetime)
            ModifiedTime = time.localtime(filetimesecond)
            y = time.strftime('%Y', ModifiedTime)
            m = time.strftime('%m', ModifiedTime)
            d = time.strftime('%d', ModifiedTime)
            H = time.strftime('%H', ModifiedTime)
            M = time.strftime('%M', ModifiedTime)
            d2 = datetime.datetime(int(y), int(m), int(d), int(H), int(M))
            print d2
            if d2 < flagtime:
                #os.remove(filefullname)
                shutil.rmtree(dirpath)
if __name__ == "__main__":
    now=datetime.datetime.now()
    nowstring=datetime.datetime.strftime(now,'%Y%m%d%H')
    #添加日志
    logger = logging.getLogger('apscheduler.executors.default')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    logfile='/home/wlan_dev/log/delclads'+nowstring+'.log'
    #logfile='/Users/yetao.lu/Desktop/mos/temp/loging.log'
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
    now=datetime.datetime.now()
    flagtime=now+datetime.timedelta(days=-3)
    #filepath='/Users/yetao.lu/Downloads/eccodes-2.3.0-Source'
    filepath='/moji/meteo/cluster/data/CLDAS/2018/'
    deletecldasdata(filepath,flagtime)
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    scheduler.add_job(deletecldasdata,'cron',hour='0,12',args=(filepath,flagtime))
    try:
        scheduler.start()
        while True:
            time.sleep(2)
    except Exception as e:
        logger.info(e.message)