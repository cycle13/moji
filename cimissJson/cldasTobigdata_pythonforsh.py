#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/23
description:把CLDAS数据压缩，然后利用Python调用SH脚本传到大数据平台上
也可以用subprocess
"""
import os,tarfile,datetime,time,logging,sys,shutil
from apscheduler.schedulers.background import BackgroundScheduler
def tarfileonebyone(filepath,odatetime):
    if os.path.exists(filepath):
        return None
    else:
        tar=tarfile.open(filepath,'w:gz')
        for root,dirs,files in os.walk(filepath):
            for file in files:
                filename=os.path.join(root,file)
                tar.add(filename)
        tar.close()
    return filepath
    
def uploadfile():
    pdatetime = datetime.datetime.now()
    odatetime = pdatetime + datetime.timedelta(days=-6)
    odate=datetime.datetime.strftime(odatetime,'%Y-%m-%d')
    path='/home/wlan_dev/henan/'
    yearstr=str(odatetime.year)
    filepath=path+'/'+yearstr+'/'+odate
    filefullname=os.path.join(filepath,odate+'.tar.gz')
    if not os.path.exists(filefullname):
        filefullname=tarfileonebyone(filefullname,odatetime)
    filename=os.path.split(filefullname)[1]
    fromfilepath=os.path.split(filefullname)[0]
    tofilefullname='/meteo/moge/data/cldas/'+yearstr+'/'+filename
    #这里要写全路径
    a=os.system('/home/wlan_dev/software/bigdata/api/start_hdfs_access.sh '+tofilefullname+' '+fromfilepath)
    if a<>0:
        time.sleep(600)
        os.system('/home/wlan_dev/software/bigdata/api/start_hdfs_access.sh '+tofilefullname+' '+fromfilepath)
    else:
        time.sleep(600)
        os.remove(filefullname)
    #删除文件夹
    shutil.rmtree(filepath)
if __name__ == "__main__":
    logger=logging.getLogger('apscheduler.executors.default')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志
    file_handler = logging.FileHandler('/home/wlan_dev/log/uploadbigdata.log')
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值

    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    scheduler.add_job(uploadfile, 'cron', minute='16,23')
    scheduler.start()
    try:
        while True:
            time.sleep(2)
    except(KeyboardInterrupt, SystemExit):
        print 'failed'
    
    
