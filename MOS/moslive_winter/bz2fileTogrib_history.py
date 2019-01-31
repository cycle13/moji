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
'''
鉴于对解压文件的不完整性，下面代码采用subprocess创建多进程调用bzip2命令来解压
20180825针对bug进行修改，用多线程试试
'''
def unzipfile_threading(oldfilepath,dirpath):
    #文件拷贝，把文件从
    now=datetime.datetime.now()
    yearstr=datetime.datetime.now().year
    hourstr=datetime.datetime.now().hour
    if hourstr<12:
        nowdate=now+datetime.timedelta(days=-1)
        datepath=datetime.datetime.strftime(nowdate,'%Y-%m-%d')
        filepath = oldfilepath + '/' + str(yearstr) + '/' + datepath
        midpath=dirpath + '/' + str(yearstr)
        if not os.path.exists(midpath):
            os.mkdir(midpath)
        tofilepath=dirpath + '/' + str(yearstr) + '/' + datepath
        if not os.path.exists(tofilepath):
            os.mkdir(tofilepath)
        #logger.info(filepath+'-'+tofilepath)
        
        executor=futures.ThreadPoolExecutor()
        
        for root, dirs, files in os.walk(filepath):
            for file in files:
                print file + '-' + file[:3] + '-' + file[5:7] + '-' + file[-4:]
                logging.info(file + '-' + file[:3] + '-' + file[5:7] + '-' + file[-4:])
                if file[:3] == 'D1D' and file[7:9] == '12' and file[-4:] == '.bz2':
                    filename = os.path.join(root, file)
                    logging.info(filename)
                    executor.submit(copyfile2, args=(filename, tofilepath))
    else:
        datepath = datetime.datetime.strftime(now, '%Y-%m-%d')
        filepath=oldfilepath+'/'+str(yearstr)+'/'+datepath
        midpath=dirpath + '/' + str(yearstr)
        if not os.path.exists(midpath):
            os.mkdir(midpath)
        tofilepath=dirpath + '/' + str(yearstr) + '/' + datepath
        if not os.path.exists(tofilepath):
            os.mkdir(tofilepath)
        executor=futures.ThreadPoolExecutor()
        for root, dirs, files in os.walk(filepath):
            for file in files:
                logging.info(file + '-' + file[:3] + '-' + file[5:7] + '-' + file[-4:])
                if file[:3] == 'D1D' and file[7:9] == '00' and file[-4:] == '.bz2':
                    filename = os.path.join(root, file)
                    logging.info(filename)
                    executor.submit(copyfile2,args=(filename,tofilepath))
            #logging.info('----------------------')
    #文件解压
    for root, dirs, files in os.walk(tofilepath):
        pool=ProcessPoolExecutor()
        for file in files:
            if file[-4:]=='.bz2':
                filefullname=os.path.join(root,file)
                obj=pool.submit(runshell,filefullname)
        pool.shutdown()
    return tofilepath

def unzipfile(starttime,dirpath):
    yearstr=starttime.year
    hourstr=starttime.hour
    if hourstr==0:
        datepath=datetime.datetime.strftime(starttime,'%Y-%m-%d')
        midpath=dirpath + '/' + str(yearstr)
        if not os.path.exists(midpath):
            os.mkdir(midpath)
        tofilepath=dirpath + '/' + str(yearstr) + '/' + datepath
        hourflag='00'
    else:
        datepath = datetime.datetime.strftime(starttime, '%Y-%m-%d')
        midpath=dirpath + '/' + str(yearstr)
        if not os.path.exists(midpath):
            os.mkdir(midpath)
        tofilepath=dirpath + '/' + str(yearstr) + '/' + datepath
        hourflag='12'
    #文件解压
    for root, dirs, files in os.walk(tofilepath):
        pool=ProcessPoolExecutor(max_workers=20)
        for file in files:
            #增加判断条件
            if file[-4:]=='.bz2' and file[7:9]==hourflag:
                filefullname=os.path.join(root,file)
                # 需要增加判断条件，如果解压文件存在的话就不解压了，
                # 但是本身下面的命令会检验文件是否存在
                obj=pool.submit(runshell,filefullname)
        pool.shutdown(wait=True)
    return tofilepath
def unzipfile(tofilepath):
    for root, dirs, files in os.walk(tofilepath):
        pool=ProcessPoolExecutor(max_workers=20)
        for file in files:
            #增加判断条件
            if file[-4:]=='.bz2':
                filefullname=os.path.join(root,file)
                unzipfile=filefullname[:-4]
                print unzipfile
                if not os.path.exists(unzipfile):
                    # 需要增加判断条件，如果解压文件存在的话就不解压了，
                    # 但是本身下面的命令会检验文件是否存在
                    obj=pool.submit(runshell,filefullname)
        pool.shutdown(wait=True)
'''
线上的数据是从纪高的文件夹里面拷贝过来的，
而要算历史的是从大数据平台上下载的
'''
def copyAndUnzipfile(starttime,oldfilepath,dirpath):
    #文件拷贝，把文件从纪高的文件夹里面拷贝
    yearstr=starttime.year
    hourstr=starttime.hour
    if hourstr<17:
        nowdate=starttime+datetime.timedelta(days=-1)
        datepath=datetime.datetime.strftime(nowdate,'%Y-%m-%d')
        filepath = oldfilepath + '/' + str(yearstr) + '/' + datepath
        midpath=dirpath + '/' + str(yearstr)
        if not os.path.exists(midpath):
            os.mkdir(midpath)
        tofilepath=dirpath + '/' + str(yearstr) + '/' + datepath
        if not os.path.exists(tofilepath):
            os.mkdir(tofilepath)
        #logger.info(filepath+'-'+tofilepath)
        pool = multiprocessing.Pool(processes=20)
        for root, dirs, files in os.walk(filepath):
            for file in files:
                print file + '-' + file[:3] + '-' + file[5:7] + '-' + file[-4:]
                logging.info(file + '-' + file[:3] + '-' + file[5:7] + '-' + file[-4:])
                if file[:3] == 'D1D' and file[7:9] == '12' and file[-4:] == '.bz2':
                    filename = os.path.join(root, file)
                    logging.info(filename)
                    pool.apply_async(copyfile2, args=(filename, tofilepath))
            pool.close()
            pool.join()
    else:
        datepath = datetime.datetime.strftime(starttime, '%Y-%m-%d')
        filepath=oldfilepath+'/'+str(yearstr)+'/'+datepath
        midpath=dirpath + '/' + str(yearstr)
        if not os.path.exists(midpath):
            os.mkdir(midpath)
        tofilepath=dirpath + '/' + str(yearstr) + '/' + datepath
        if not os.path.exists(tofilepath):
            os.mkdir(tofilepath)
        pool=multiprocessing.Pool(processes=20)
        for root, dirs, files in os.walk(filepath):
            for file in files:
                logging.info(file + '-' + file[:3] + '-' + file[5:7] + '-' + file[-4:])
                if file[:3] == 'D1D' and file[7:9] == '00' and file[-4:] == '.bz2':
                    filename = os.path.join(root, file)
                    logging.info(filename)
                    pool.apply_async(copyfile2,args=(filename,tofilepath))
            pool.close()
            pool.join()
            #logging.info('----------------------')
    #文件解压
    for root, dirs, files in os.walk(tofilepath):
        pool=ProcessPoolExecutor()
        for file in files:
            if file[-4:]=='.bz2':
                filefullname=os.path.join(root,file)
                obj=pool.submit(runshell,filefullname)
        pool.shutdown(wait=True)
    return tofilepath
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
