#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/7/27
description:
"""
import datetime,os,subprocess,time,logging,sys
from apscheduler.schedulers.background import BackgroundScheduler
def deleteECdate(path,deldate,yearstr):
    delpath=datetime.datetime.strftime(deldate,'%Y-%m-%d')
    if not os.path.exists(path+'/'+yearstr):
        logger.info('目录不存在')
    fullpath=path+'/'+yearstr+'/'+delpath
    if os.path.exists(fullpath):
        subprocess.call('sudo rm -rf '+fullpath)
    else:
        logger.info('文件夹不存在')
if __name__ == "__main__":
    # 加日志
    logger = logging.getLogger('learing.logger')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    #logfile = '/home/wlan_dev/delec.log'
    logfile='/Users/yetao.lu/Desktop/mos/temp/ec.log'
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
    starttime = datetime.datetime.now()
    path='/mnt/data/ecdata'
    now=datetime.datetime.now()
    deldate=now+datetime.timedelta(days=-1)
    yearstr=deldate.year
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    scheduler.add_job(deleteECdate, 'cron', hour='23', max_instances=1,args=(path,deldate,yearstr))
    try:
        scheduler.start()
        while True:
            time.sleep(2)
    except Exception as e:
        print e.message