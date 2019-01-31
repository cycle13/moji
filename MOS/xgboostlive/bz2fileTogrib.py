#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/3
description:
"""
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
'''
鉴于对解压文件的不完整性，下面代码采用subprocess创建多进程调用bzip2命令来解压
'''
def unzipfile(oldfilepath,dirpath):
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
        logger.info(filepath+'-'+tofilepath)
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
        datepath = datetime.datetime.strftime(now, '%Y-%m-%d')
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
        for file in files:
            if file[-4:]=='.bz2':
                filefullname=os.path.join(root,file)
                p=subprocess.Popen('bzip2 -d -k '+filefullname,shell=True)
if __name__ == "__main__":
    logpath='/home/wlan_dev/result'
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
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    unzipfile(oldfilepath,dirpath)
    scheduler.add_job(unzipfile, 'cron', hour='5,17', max_instances=1,args=(oldfilepath,dirpath))
    try:
        scheduler.start()
        while True:
            time.sleep(2)
    except Exception as e:
        print e.message
