#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/11
description:从气象局信息中心的接口中下载CLDAS数据1公里的，采用多进程下载
"""
import urllib2,os,json,multiprocessing,time,sys,logging,datetime
from apscheduler.schedulers.background import BackgroundScheduler
def Readfeaturefile(featrue,dict,filename,odatetime,outpath):
    yearstr=str(odatetime.year)
    monstr=str(odatetime.month)
    if odatetime.hour < 10:
        hourstr = '0' + str(odatetime.hour)
    else:
        hourstr = str(odatetime.hour)
    pdate = datetime.datetime.strftime(odatetime, '%Y-%m-%d')
    if not os.path.exists(outpath + '/'+yearstr):
        os.mkdir(outpath + '/'+yearstr)
    if not os.path.exists(outpath + '/' + yearstr + '/' + pdate):
        os.mkdir(outpath + '/' + yearstr + '/' + pdate)
    if not os.path.exists(outpath + '/' + yearstr + '/' + pdate+'/'+hourstr):
        os.mkdir(outpath + '/' + yearstr + '/' + pdate+'/'+hourstr)
    file01 = os.path.join(outpath + '/' + yearstr + '/' + pdate+'/'+hourstr, filename)
    if not os.path.exists(file01):
        fileurl = dict['FILE_URL']
        fileurl=str.replace(str(fileurl),'http://ftp.data.cma.cn:8000/dlcdc/','http://cdcfileprivate.oss-cn-beijing-internal.aliyuncs.com/')
        print 'link'
        print fileurl
        #fileurl='http://cdcfileprivate.oss-cn-beijing-internal.aliyuncs.com/space/HRCLDAS/sod.F.0042.0001.S001/Z_NAFP_C_BABJ_20180515031247_P_HRCLDAS_RT_CHN_0P01_HOR-SSRA-2018051503.nc.tmp?Expires=1527049241&OSSAccessKeyId=CcULE6lAfEbIFtKD&Signature=Nh8PlSuAtqLLu%2BLyv8btnyIN8uk%3D&DataCode=NAFP_HRCLDAS_RT_NC&UserId=mjtq_mjtqmete_user'
        fileread = urllib2.urlopen(fileurl)
        print fileurl,fileread
        filewrite=open(file01,'w')
        filewrite.write(fileread.read())
        filewrite.close()
def getcldasdata(outpath,logger):
    pdatetime=datetime.datetime.now()
    odatetime=pdatetime+datetime.timedelta(hours=-8)
    pdatestr=odatetime.strftime('%Y%m%d%H')+'0000'
    #url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_CLDAS2.0_RT_NC&time=20180430000000&dataFormat=html'
    url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_HRCLDAS_RT_NC&time='+pdatestr+'&elements=Bul_Center,FILE_NAME,Data_Area,Datetime,File_URL&dataFormat=json'
    #url='https://cdcfileprivate.oss-cn-beijing.aliyuncs.com/space/HRCLDAS/sod.F.0042.0001.S001/Z_NAFP_C_BABJ_20180427001412_P_HRCLDAS_RT_CHN_0P01_HOR-PRE-2018042700.nc?Expires=1526352926&OSSAccessKeyId=TMP.AQG4tvxtimBqxhjL_38RqU7uvvyLj9ME2U7dZ6JTzG72IZfRPvg-RlljwMH1ADAtAhUAhprPqx9Sbrb3hPKYMkuxGJNEV_4CFBp3B-lQL-3Gpp0mfmE8oUdKZp9F&Signature=Sa18RNnjVFhnz%2F1OXdqg84xcDJI%3D'
    #print url
    request=urllib2.Request(url);
    f=urllib2.urlopen(request)
    link=f.read()
    #print link
    file=json.loads(link)
    dataset=file['DS']
    #print dataset
    pool=multiprocessing.Pool(processes=9)
    for dict in dataset:
        filename=dict['FILE_NAME']
        featurelist=['SSRA','TMP','SHU','PRS','WIN','PRE','WIU','WIV','DPT']
        for i in featurelist:
            if i in filename:
                # start=datetime.datetime.now()
                # #Readfeaturefile(i, dict, filename, odatetime)
                # end=datetime.datetime.now()
                # print (end-start).seconds
                pool.apply_async(Readfeaturefile,args=(i, dict, filename, odatetime,outpath))
                logger.info(filename)
    pool.close()
    pool.join()
def getcldasdataNext(outpath,logger):
    pdatetime=datetime.datetime.now()
    odatetime=pdatetime+datetime.timedelta(hours=-12)
    pdatestr=odatetime.strftime('%Y%m%d%H')+'0000'
    #url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_CLDAS2.0_RT_NC&time=20180430000000&dataFormat=html'
    url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_HRCLDAS_RT_NC&time='+pdatestr+'&elements=Bul_Center,FILE_NAME,Data_Area,Datetime,File_URL&dataFormat=json'
    #print url
    request=urllib2.Request(url);
    f=urllib2.urlopen(request)
    link=f.read()
    #print link
    file=json.loads(link)
    dataset=file['DS']
    #print dataset
    pool=multiprocessing.Pool(processes=9)
    for dict in dataset:
        filename=dict['FILE_NAME']
        featurelist=['SSRA','TMP','SHU','PRS','WIN','PRE','WIU','WIV','DPT']
        for i in featurelist:
            if i in filename:
                # start=datetime.datetime.now()
                # #Readfeaturefile(i, dict, filename, odatetime)
                # end=datetime.datetime.now()
                # print (end-start).seconds
                pool.apply_async(Readfeaturefile,args=(i, dict, filename, odatetime,outpath))
                logger.info(filename)
    pool.close()
    pool.join()
def getcldasdataNext01(outpath,logger,utc01):
    pdatetime=datetime.datetime.now()
    odatetime=pdatetime+datetime.timedelta(hours=utc01)
    pdatestr=odatetime.strftime('%Y%m%d%H')+'0000'
    #url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_CLDAS2.0_RT_NC&time=20180430000000&dataFormat=html'
    url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_HRCLDAS_RT_NC&time='+pdatestr+'&elements=Bul_Center,FILE_NAME,Data_Area,Datetime,File_URL&dataFormat=json'
    #url='https://cdcfileprivate.oss-cn-beijing.aliyuncs.com/space/HRCLDAS/sod.F.0042.0001.S001/Z_NAFP_C_BABJ_20180427001412_P_HRCLDAS_RT_CHN_0P01_HOR-PRE-2018042700.nc?Expires=1526352926&OSSAccessKeyId=TMP.AQG4tvxtimBqxhjL_38RqU7uvvyLj9ME2U7dZ6JTzG72IZfRPvg-RlljwMH1ADAtAhUAhprPqx9Sbrb3hPKYMkuxGJNEV_4CFBp3B-lQL-3Gpp0mfmE8oUdKZp9F&Signature=Sa18RNnjVFhnz%2F1OXdqg84xcDJI%3D'
    print url
    request=urllib2.Request(url);
    f=urllib2.urlopen(request)
    link=f.read()
    #print link
    file=json.loads(link)
    dataset=file['DS']
    #print dataset
    pool=multiprocessing.Pool(processes=9)
    for dict in dataset:
        filename=dict['FILE_NAME']
        featurelist=['SSRA','TMP','SHU','PRS','WIN','PRE','WIU','WIV','DPT']
        for i in featurelist:
            if i in filename:
                # start=datetime.datetime.now()
                # #Readfeaturefile(i, dict, filename, odatetime)
                # end=datetime.datetime.now()
                # print (end-start).seconds
                pool.apply_async(Readfeaturefile,args=(i, dict, filename, odatetime,outpath))
                logger.info(filename)
    pool.close()
    pool.join()
if __name__ == "__main__":
    starttime=datetime.datetime.now()
    # outpath='/Users/yetao.lu/Downloads/tmp'
    # logpath='/Users/yetao.lu/Downloads/tmp'
    outpath='/moji/meteo/cluster/data/CLDAS'
    logpath='/moji/meteo/cluster/data/log'
    utc=-9
    utc01=-10
    utc02=-11
    utc03=-22
    logger = logging.getLogger("apscheduler.executors.default")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
        '%a, %d %b %Y %H:%M:%S', )
    logfile = os.path.join(logpath, "cldas.log")
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    getcldasdata(outpath,logger)
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    scheduler.add_job(getcldasdata, 'cron', minute='45,50,55',args=(outpath, logger))
    scheduler.add_job(getcldasdataNext01, 'cron', minute='45',
                      args=(outpath, logger, utc))
    scheduler.add_job(getcldasdataNext01,'cron',minute='45',
                      args=(outpath,logger,utc01))
    scheduler.add_job(getcldasdataNext01, 'cron', minute='45',
                      args=(outpath, logger, utc02))
    scheduler.add_job(getcldasdataNext01, 'cron', minute='45',
                      args=(outpath, logger, utc03))
    scheduler.start()
    try:
        while True:
            time.sleep(2)
    except(KeyboardInterrupt, SystemExit):
        logger.info('System out')
    endtime=datetime.datetime.now()
    logger.info((endtime-starttime).seconds)
