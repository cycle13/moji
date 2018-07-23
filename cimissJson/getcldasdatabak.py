#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/11
description:从气象局信息中心的接口中下载CLDAS数据1公里的，采用多进程下载
"""
import urllib2,os,json,multiprocessing,datetime,sys,logging
def Readfeaturefile(featrue,dict,filename,odatetime):
    yearstr=str(odatetime.year)
    monstr=str(odatetime.month)
    daystr=str(odatetime.day)
    if not os.path.exists(outpath + '/'+featrue):
        os.mkdir(outpath + '/'+featrue)
    if not os.path.exists(outpath + '/'+featrue+'/'+yearstr):
        os.mkdir(outpath + '/'+featrue+'/'+yearstr)
    if not os.path.exists(outpath + '/'+featrue+'/'+yearstr+'/'+monstr):
        os.mkdir(outpath + '/'+featrue+'/'+yearstr+'/'+monstr)
    if not os.path.exists(outpath + '/'+featrue+'/'+yearstr+'/'+monstr+'/'+daystr):
        os.mkdir(outpath + '/'+featrue+'/'+yearstr+'/'+monstr+'/'+daystr+'/')
    file01 = os.path.join(outpath + '/'+featrue+'/'+yearstr+'/'+monstr+'/'+daystr+'/', filename)
    print file01
    if not os.path.exists(file01):
        fileurl = dict['FILE_URL']
        fileurl=str.replace(str(fileurl),'http://ftp.data.cma.cn:8000/dlcdc/','http://cdcfileprivate.oss-cn-beijing-internal.aliyuncs.com/')
        print fileurl
        fileread = urllib2.urlopen(fileurl)
        print fileurl,fileread
        filewrite=open(file01,'w')
        filewrite.write(fileread.read())
        filewrite.close()
def getcldasdata(i):
    #sys.stdout=open('/home/wlan_dev/cldas.log','w')
    pdatetime=datetime.datetime.now()
    odatetime=pdatetime+datetime.timedelta(hours=i)
    pdatestr=odatetime.strftime('%Y%m%d%H')+'0000'
    logfile=os.path.join(logpath,pdatestr+'.log')
    logger_handler=logging.FileHandler(logfile)
    logger=logging.getLogger('test')
    logger.addHandler(logger_handler)
    logger.setLevel(logging.INFO)
    #url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_CLDAS2.0_RT_NC&time=20180430000000&dataFormat=html'
    url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getNafpFileByTime&dataCode=NAFP_HRCLDAS_RT_NC&time='+pdatestr+'&elements=Bul_Center,FILE_NAME,Data_Area,Datetime,File_URL&dataFormat=json'
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
                #Readfeaturefile(i, dict, filename, odatetime)
                pool.apply_async(Readfeaturefile,args=(i, dict, filename, odatetime,))
                logger.info(filename)
    pool.close()
    pool.join()
if __name__ == "__main__":
    outpath='/Users/yetao.lu/Downloads/tmp'
    logpath='/Users/yetao.lu/Downloads/tmp'
    # logpath='/home/wlan_dev/log'
    # outpath='/mnt/cldas'
    # for i in range(450):
    #     getcldasdata(i-460)
    getcldasdata()
