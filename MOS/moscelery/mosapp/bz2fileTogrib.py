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
from mosapp import app
import bz2,os,multiprocessing,subprocess,time,datetime,shutil,logging,sys
from apscheduler.schedulers.background import BackgroundScheduler
from celery.utils.log import get_task_logger
from celery import chain
from celery import group
from ecxgboostlive_dem_temp import simplemodelProdict
logger=get_task_logger(__name__)
'''
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
'''
def copyfile(filepath,tofilepath,aa):
    for root, dirs, files in os.walk(filepath):
        for file in files:
            logging.info(file+'-'+file[:3]+'-'+file[5:7]+'-'+file[-4:])
            if file[:3] == 'D1D' and file[5:7] == aa and file[-4:]=='.bz2':
                filename = os.path.join(root, file)
                logging.info(filename)
                shutil.copy(filename, tofilepath)
                logging.info('----------------------')
@app.task
def copyfile2(filepath,tofilepath):
    shutil.copy(filepath,tofilepath)
    return tofilepath
@app.task
def rununbzip2shell(filefullname):
    p = subprocess.call('bzip2 -d -k ' + filefullname, shell=True)
    return filefullname[:-4]

#鉴于对解压文件的不完整性，下面代码采用subprocess创建多进程调用bzip2命令来解压
#20180825针对bug进行修改，用多线程试试

@app.task
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
@app.task
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
        pool=ProcessPoolExecutor()
        print len(files)
        for file in files:
            #增加判断条件
            if file[-4:]=='.bz2' and file[7:9]==hourflag:
                filefullname=os.path.join(root,file)
                obj=pool.submit(runshell,filefullname)
        pool.shutdown(wait=True)
    return tofilepath
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
        curr_proc=multiprocessing.current_process()
        curr_proc.daemon=False
        pool = multiprocessing.Pool(processes=20)
        curr_proc.daemon=True
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
        curr_proc=multiprocessing.current_process()
        curr_proc.daemon=False
        pool = multiprocessing.Pool(processes=20)
        curr_proc.daemon=True
        for root, dirs, files in os.walk(filepath):
            for file in files:
                logging.info(file + '-' + file[:3] + '-' + file[5:7] + '-' + file[-4:])
                if file[:3] == 'D1D' and file[7:9] == '00' and file[-4:] == '.bz2':
                    filename = os.path.join(root, file)
                    logging.info(filename)
                    result=chain
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
#20180830修改：遍历文件一个一个拷贝解压
@app.task
def unzipfileinchain(filepath,tofilepath):
    shutil.copy(filepath,tofilepath)
    logging.info(filepath+'-----'+tofilepath)
    filefullname=tofilepath
    subprocess.call('bzip2 -d -k ' + filefullname, shell=True)
    unzipfilename=filefullname[:-4]
    logger.info(unzipfilename)
    return unzipfilename
def copyfilefrombigdataAndUnzipfile(starttime,oldfilepath,dirpath):
    yearstr=starttime.year
    hourstr=starttime.hour
    if hourstr<17:
        dict = {}
        nowdate=starttime+datetime.timedelta(days=-1)
        datepath=datetime.datetime.strftime(nowdate,'%Y-%m-%d')
        filepath = oldfilepath + '/' + str(yearstr) + '/' + datepath
        midpath=dirpath + '/' + str(yearstr)
        if not os.path.exists(midpath):
            os.mkdir(midpath)
        tofilepath=dirpath + '/' + str(yearstr) + '/' + datepath
        if not os.path.exists(tofilepath):
            os.mkdir(tofilepath)
        for root, dirs, files in os.walk(filepath):
            for file in files:
                print file + '-' + file[:3] + '-' + file[5:7] + '-' + file[-4:]
                logging.info(file + '-' + file[:3] + '-' + file[5:7] + '-' + file[-4:])
                if file[:3] == 'D1D' and file[7:9] == '12' and file[-4:] == '.bz2':
                    filename = os.path.join(root, file)
                    logging.info(filename)
                    dict[filename]=tofilepath
        result001=group(unzipfileinchain(key,value) for key,value in dict.items()).get()
    else:
        dict00={}
        datepath = datetime.datetime.strftime(starttime, '%Y-%m-%d')
        filepath=oldfilepath+'/'+str(yearstr)+'/'+datepath
        midpath=dirpath + '/' + str(yearstr)
        if not os.path.exists(midpath):
            os.mkdir(midpath)
        tofilepath=dirpath + '/' + str(yearstr) + '/' + datepath
        if not os.path.exists(tofilepath):
            os.mkdir(tofilepath)
        for root, dirs, files in os.walk(filepath):
            for file in files:
                logging.info(file + '-' + file[:3] + '-' + file[5:7] + '-' + file[-4:])
                if file[:3] == 'D1D' and file[7:9] == '00' and file[-4:] == '.bz2':
                    filename = os.path.join(root, file)
                    logging.info(filename)
                    dict00[filename]=tofilepath
        result002=group(chain(unzipfileinchain(key,value),simplemodelProdict(starttime,key)) for key,value in dict00.items()).get()
        

