#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/23
description:把CLDAS数据压缩，然后利用Python调用SH脚本传到大数据平台上
也可以用subprocess
"""
import os,tarfile,datetime,time,logging,sys,shutil,subprocess
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
    path='/moji/meteo/cluster/data/CLDAS'
    yearstr=str(odatetime.year)
    #本地目录
    filepath=path+'/'+yearstr+'/'+odate
    for root,dirs,files in os.walk(filepath):
        for file in files:
            #大数据平台的文件全路径
            filefullname=os.path.join(root,file)
            oldfilepath=os.path.split(filefullname)[0]

            tofilefullname = '/meteo/moge/data/cldas/' + yearstr + '/' +odate+'/'+file
            logger.info('/moji/meteo/cluster/program/bigdata/api/start_hdfs_access.sh ' + '/meteo/moge/data/cldas/' + yearstr)
            os.system('/moji/meteo/cluster/program/bigdata/api/start_hdfs_access.sh ' + '/meteo/moge/data/cldas/' + yearstr)
            os.system('/moji/meteo/cluster/program/bigdata/api/start_hdfs_access.sh '+'/meteo/moge/data/cldas/' + yearstr + '/' +odate)
            logger.info( '/moji/meteo/cluster/program/bigdata/api/start_hdfs_access.sh '+tofilefullname+' '+oldfilepath)
            result=os.system('/moji/meteo/cluster/program/bigdata/api/start_hdfs_access.sh '+tofilefullname+' '+oldfilepath)
            if result<>0:
                os.system('/moji/meteo/cluster/program/bigdata/api/start_hdfs_access.sh '+'/meteo/moge/data/cldas/' + yearstr + '/' +odate+'/'+file++' '+filepath)
            else:
                os.remove(filefullname)
    if len(os.listdir(filepath)) == 0:
        shutil.rmtree(filepath)
if __name__ == "__main__":
    logger=logging.getLogger('apscheduler.executors.default')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志
    file_handler = logging.FileHandler('/moji/meteo/cluster/data/log/uploadbigdata.log')
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值

    # 为logger添加的日志处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    uploadfile()
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    scheduler.add_job(uploadfile, 'cron', hour='2')
    scheduler.start()
    try:
        while True:
            time.sleep(2)
    except(KeyboardInterrupt, SystemExit):
        print 'failed'
    
    
