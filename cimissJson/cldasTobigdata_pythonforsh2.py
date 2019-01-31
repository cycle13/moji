#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/23
description:把CLDAS数据压缩，然后利用Python调用SH脚本传到大数据平台上
也可以用subprocess
"""
import os,tarfile,datetime,time,logging,sys,shutil,subprocess,zipfile
from apscheduler.schedulers.background import BackgroundScheduler
def zip_ya(startdir,file_news):
    z = zipfile.ZipFile(file_news,'w',zipfile.ZIP_DEFLATED,allowZip64=True) #参数一：文件夹名
    for dirpath, dirnames, filenames in os.walk(startdir):
        fpath = dirpath.replace(startdir,'') #这一句很重要，不replace的话，就从根目录开始复制
        fpath = fpath and fpath + os.sep or ''#这句话理解我也点郁闷，实现当前文件夹以及包含的所有文件的压缩
        for filename in filenames:
            z.write(os.path.join(dirpath, filename),fpath+filename)
    z.close()
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
    odatetime = pdatetime + datetime.timedelta(days=-5)
    odate=datetime.datetime.strftime(odatetime,'%Y-%m-%d')
    path='/home/wlan_dev/cldas'
    yearstr=str(odatetime.year)
    filepath=path+'/'+yearstr
    filedatepath=path+'/'+yearstr+'/'+odate
    tofilefullname='/meteo/moge/data/cldas'+'/'+yearstr+'/'+odate+'.zip'
    fromfilepath=path+'/'+yearstr+'/'
    zip_ya(filedatepath,filedatepath+'.zip')
    #这里要写全路径
    logger.info('/home/wlan_dev/software/bigdata/api/start_hdfs_access.sh '+tofilefullname+' '+fromfilepath)
    #b = subprocess.call('/home/wlan_dev/software/bigdata/api/start_hdfs_access.sh '+tofilefullname)
    a = subprocess.call('/home/wlan_dev/software/bigdata/api/start_hdfs_access.sh '+tofilefullname+' '+fromfilepath)
    if a <> 0:
        time.sleep(600)
        subprocess.call('/home/wlan_dev/software/bigdata/api/start_hdfs_access.sh '+tofilefullname+' '+fromfilepath)
    else:
        time.sleep(600)
        os.remove(filedatepath+'.zip')
    #删除文件夹
    if len(os.listdir(filedatepath)) == 0:
        shutil.rmtree(filedatepath)
    #删除文件夹
    if len(os.listdir(filepath)) == 0:
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
    scheduler.add_job(uploadfile, 'cron', hour='2')
    scheduler.start()
    try:
        while True:
            time.sleep(2)
    except Exception as ex:
        logger.info(ex.message)
    
    
