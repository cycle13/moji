#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/11
description:从气象局信息中心的接口中下载CLDAS数据1公里的，APSCHEDULER
"""
import urllib2,os,json,datetime,time,logging,sys

from apscheduler.schedulers.background import BackgroundScheduler
def cldasfileDownload(outpath,logger):
    pdatetime=datetime.datetime.now()
    odatetime=pdatetime+datetime.timedelta(hours=-11)
    pdatestr=odatetime.strftime('%Y%m%d%H')+'0000'
    url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_HRCLDAS_RT_NC&time='+pdatestr+'&elements=Bul_Center,FILE_NAME,Data_Area,Datetime,File_URL&dataFormat=json'
    request=urllib2.Request(url);
    f=urllib2.urlopen(request)
    link=f.read()
    file=json.loads(link)
    dataset=file['DS']
    for dict in dataset:
        filename=dict['FILE_NAME']
        logger.info(filename)
        yearstr = str(odatetime.year)
        monstr = str(odatetime.month)
        daystr = str(odatetime.day)
        hourstr=str(odatetime.hour)
        if not os.path.exists(outpath  + '/' + yearstr):
            os.mkdir(outpath + '/' + yearstr)
        if not os.path.exists(
                outpath  + '/' + yearstr + '/' + monstr):
            os.mkdir(outpath  + '/' + yearstr + '/' + monstr)
        if not os.path.exists(
                outpath  + '/' + yearstr + '/' + monstr + '/' + daystr):
            os.mkdir(
                outpath  + '/' + yearstr + '/' + monstr + '/' + daystr + '/')
        if not os.path.exists(
                outpath  + '/' + yearstr + '/' + monstr + '/' + daystr+'/'+hourstr):
            os.mkdir(
                outpath  + '/' + yearstr + '/' + monstr + '/' + daystr +'/'+hourstr)
        file01 = os.path.join(outpath  + '/' + yearstr + '/' + monstr + '/' + daystr + '/'+hourstr,filename)
        print file01
        fileurl = dict['FILE_URL']
        fileurl=str.replace(str(fileurl),'http://ftp.data.cma.cn:8000/dlcdc/','http://cdcfileprivate.oss-cn-beijing-internal.aliyuncs.com/')
        print fileurl
        fileread = urllib2.urlopen(fileurl)
        filewrite=open(file01,'w')
        filewrite.write(fileread.read())
        filewrite.close()
if __name__ == "__main__":
    outpath = '/Users/yetao.lu/Downloads/tmp'
    logger = logging.getLogger("endlesscode")
    formatter = logging.Formatter(
        '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
        '%a, %d %b %Y %H:%M:%S', )
    logfile=os.path.join(outpath,"test.log")
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    #创建后台执行的schedulers
    scheduler=BackgroundScheduler()
    #添加调度任务
    #调度方法为timeTask,触发器选择定时，
    scheduler.add_job(cldasfileDownload,'cron',minute='40,50,55',args=(outpath,logger))
    scheduler.start()
    try:
        while True:
            time.sleep(2)
    except(KeyboardInterrupt,SystemExit):
        logger.info('System out')
    






