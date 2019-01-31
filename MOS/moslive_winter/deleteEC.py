#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/6/26
description:
"""
import os,time,datetime,shutil ,logging,sys,subprocess
from apscheduler.schedulers.background import BackgroundScheduler
def deleteECdata(filepath,flagtime):
    now=datetime.datetime.now()
    yearstr=str(now.year)
    #print yearstr
    filepathyear=filepath+'/'+yearstr
    flagtime=now+datetime.timedelta(days=-1)
    for root,dirs,files in os.walk(filepathyear):
        for dir in dirs:
            dirpath=os.path.join(root,dir)
            if len(os.listdir(dirpath)) == 0:
                shutil.rmtree(dirpath)
                if os.path.exists(dirpath):
                    logger.info(dirpath + "文件夹删除成功")
                else:
                    logger.info(dirpath + '文件夹删除失败')
            if len(str(dir))>5:
                dirpath=os.path.join(root,dir)
                pathtime=datetime.datetime.strptime(dir,'%Y-%m-%d')
                print pathtime,flagtime
                if pathtime<flagtime:
                    for croot,cdirs,cfiles in os.walk(dirpath):
                        for cdir in cdirs:
                            cdirpath=os.path.join(croot,cdir)
                            #判断文件夹是否为空
                            if len(os.listdir(cdirpath)) == 0:
                                shutil.rmtree(cdirpath)
                                if os.path.exists(cdirpath):
                                    logger.info(cdirpath+"文件夹删除成功")
                                else:
                                    logger.info(cdirpath+'文件夹删除失败')
                            else:
                                for droot,ddirs,dfiles in os.walk(cdir):
                                    for dfile in dfiles:
                                        dfilename=os.path.join(droot,dfile)
                                        print dfilename
                                        subprocess.call('sudo rm -rf '+dfilename)
if __name__ == "__main__":
    now=datetime.datetime.now()
    nowstring=datetime.datetime.strftime(now,'%Y%m%d%H')
    #添加日志
    logger = logging.getLogger('apscheduler.executors.default')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志learning
    logfile='/moji/meteo/cluster/data/log/delclads'+nowstring+'.log'
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
    flagtime=now+datetime.timedelta(days=-1)
    #filepath='/Users/yetao.lu/scala'
    filepath='/moji/meteo/cluster/data/CLDAS'
    deleteECdata(filepath,flagtime)
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    #scheduler.add_job(deletecldasdata, 'cron', minute='*/2',args=(filepath, flagtime))
    scheduler.add_job(deleteECdata,'cron',hour='0,12',args=(filepath,flagtime))
    try:
        scheduler.start()
        while True:
            time.sleep(2)
    except Exception as e:
        logger.info(e.message)