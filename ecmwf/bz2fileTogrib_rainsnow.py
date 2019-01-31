#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/3
description:
遇到daemonic processes are not allowed to have children
celery中不允许有multiprocessing多进程，坑死了
"""
from concurrent.futures import ProcessPoolExecutor
from concurrent import futures

import bz2,os,multiprocessing,subprocess,time,datetime,shutil,logging,sys
from apscheduler.schedulers.background import BackgroundScheduler

def bz2togrib(filepath,outpath):
    for root,dirs,files in os.walk(filepath):
        pool = multiprocessing.Pool(processes=15)
        for file in files:
            filename = os.path.join(root, file)
            if file[:3]=='D1D' and file[7:9]=='12' and file[-4:]=='.bz2':
                #newfile=filename[:-4]+'.grib'
                newfile=os.path.join(outpath,file[:-4]+'.grib')
                pool.apply_async(writefile,args=(newfile,filename))
        pool.close()
        pool.join()
#这种方法会存在解压不完整的情况，这是个坑。
def writefile(newfile,filename):
    if not os.path.exists(newfile):
        a = bz2.BZ2File(filename, 'rb')
        b = open(newfile, 'wb')
        b.write(a.read())
        a.close()
        b.close()
def copyfile(filepath,tofilepath,aa):
    for root, dirs, files in os.walk(filepath):
        for file in files:
            logging.info(file+'-'+file[:3]+'-'+file[5:7]+'-'+file[-4:])
            if file[:3] == 'D1D' and file[5:7] == aa and file[-4:]=='.bz2':
                filename = os.path.join(root, file)
                logging.info(filename)
                shutil.copy(filename, tofilepath)
                logging.info('----------------------')
def copyfile2(filepath,tofilepath):
    shutil.copy(filepath,tofilepath)
def runshell(filefullname):
    p = subprocess.call('bzip2 -d -k ' + filefullname, shell=True)

def unzipfile(tofilepath):
    for root, dirs, files in os.walk(tofilepath):
        pool=ProcessPoolExecutor(max_workers=20)
        for file in files:
            if file[-4:]=='.bz2':
                initial = '2018' + file[3:9]
                forecast = '2018' + file[11:17]
                print file,initial,forecast
                starttime = datetime.datetime.strptime(initial,
                                                          '%Y%m%d%H')
                endtime = datetime.datetime.strptime(forecast,
                                                           '%Y%m%d%H')
                #计算两个时间差几个小时
                d = (endtime - starttime).days
                f = (endtime - starttime).seconds / 3600
                hours = (d * 24 + (endtime - starttime).seconds / 3600)
                #增加判断条件
                if hours<=36 :
                    filefullname=os.path.join(root,file)
                    unzipfile=filefullname[:-4]
                    print unzipfile
                    if not os.path.exists(unzipfile):
                        # 需要增加判断条件，如果解压文件存在的话就不解压了，
                        # 但是本身下面的命令会检验文件是否存在
                        obj=pool.submit(runshell,filefullname)
        pool.shutdown(wait=True)


if __name__ == "__main__":
    logpath='/home/wlan_dev/log'
    # 日志模块
    logging.basicConfig()
    logger = logging.getLogger("apscheduler.executors.default")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
        '%a, %d %b %Y %H:%M:%S', )
    logfile = os.path.join(logpath,'mos_temp.log')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    oldfilepath='/opt/meteo/cluster/data/ecmwf/orig'
    dirpath='/moji/ecdata'
    ecfile='/home/wlan_dev/mosdata/2018'
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    unzipfile(ecfile)
    # scheduler.add_job(unzipfile, 'cron', hour='5,17', max_instances=1,args=(oldfilepath,dirpath))
    # try:
    #     scheduler.start()
    #     while True:
    #         time.sleep(2)
    # except Exception as e:
    #     print e.message
