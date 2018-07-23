#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/17
description:
"""
import os,numpy,urllib2,json,datetime,logging,sys,time
from netCDF4 import Dataset
from invdisttree import Invdisttree
from apscheduler.schedulers.background import BackgroundScheduler
'''
#根据国家观测站极大风速插值成阵风格点，并取河南的数据
'''
def interpolate_KDTree(x,y,v,grid):
    print x.shape,y.shape
    known=zip(x,y)
    print type(known)
    known=numpy.array(known)
    print type(known),type(v),known.shape,v.shape
    lats=numpy.arange(15,60,0.01)
    lons=numpy.arange(70,140,0.01)
    print lats.shape,lons.shape
    ask=[]
    for i in range(len(lats)):
        for j in range(len(lons)):
            ask.append((lats[i],lons[j]))
    ask=numpy.array(ask)
    # print '-------------'
    # print type(ask),ask.shape
    leafsize=10
    Nnear=8
    eps=0.005
    p=2
    invdisttree = Invdisttree( known, v, leafsize=leafsize, stat=1 )
    interpol = invdisttree( ask, nnear=Nnear, eps=eps, p=p )
    grid=interpol.reshape(4500,7000)
    return grid
#方法1：这种方法也可以
def iwd(x,y,v,grid,power):
    for i in xrange(grid.shape[0]):
        for j in xrange(grid.shape[1]):
            distance = numpy.sqrt((x-i)**2+(y-j)**2)
            if (distance**power).min()==0:
                grid[i,j] = v[(distance**power).argmin()]
            else:
                total = numpy.sum(1/(distance**power))
                grid[i,j] = numpy.sum(v/(distance**power)/total)
    return grid
def zhenfengCalculate(odatetime):
    edatetime=odatetime+datetime.timedelta(hours=1)
    start=datetime.datetime.strftime(odatetime,'%Y%m%d%H')
    end=datetime.datetime.strftime(edatetime,'%Y%m%d%H')
    url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&dataFormat=json&interfaceId=getSurfEleByTimeRange&timeRange=['+start+'0000,'+end+'0000)&elements=Station_id_c,Lat,Lon,WIN_S_Inst_Max,D_datetime&dataCode=SURF_CHN_MUL_HOR'
    logger.info(url)
    fileread = urllib2.urlopen(url)
    jsonstring=fileread.read()
    print type(jsonstring)
    jsonfile=json.loads(jsonstring)
    dataset=jsonfile['DS']
    x=[]
    y=[]
    v=[]
    for row in dataset:
        if row['WIN_S_Inst_Max']<>'999999':
            x.append(float(row['Lat']))
            y.append(float(row['Lon']))
            v.append(float(row['WIN_S_Inst_Max']))
    x=numpy.array(x)
    y=numpy.array(y)
    v=numpy.array(v)
    grid=numpy.zeros((4500,7000),dtype='float32')
    #方法1
    #grid=iwd(x,y,v,grid,2)
    #方法2
    grid=interpolate_KDTree(x,y,v,grid)
    return grid

def ReadCLDASncFile(ncfilepath,outpath,odatetime,logger):
    for root,dirs,files in os.walk(ncfilepath):
        for file in files:
            if 'WIN' in file:
                wsfilename=file
                logger.info(file)
            elif 'TMP' in file:
                tempfilename=file
                logger.info(file)
            elif 'SHU' in file:
                rhfilename=file
                logger.info(file)
            elif 'PRE' in file:
                prefilename=file
                logger.info(file)
    #取阵风要素，用国家观测站极大风速
    wsfile=os.path.join(ncfilepath,wsfilename)
    tempfile=os.path.join(ncfilepath,tempfilename)
    rhfile=os.path.join(ncfilepath,rhfilename)
    prefile=os.path.join(ncfilepath,prefilename)
    wsnc=Dataset(wsfile,'r')
    tempnc=Dataset(tempfile,'r')
    rhnc=Dataset(rhfile,'r')
    prenc=Dataset(prefile,'r')
    #print wsnc,tempnc,rhnc,prenc
    start=datetime.datetime.strftime(odatetime,'%Y%m%d%H')
    outfile=os.path.join(outpath,'henan_live_tprwg_'+start+'.nc')
    dataset=Dataset(outfile,'w',format='NETCDF4_CLASSIC')
    dataset.createDimension('lat',600)
    dataset.createDimension('lon',700)
    latitudes=dataset.createVariable('lats',numpy.float,('lat'))
    longitudes=dataset.createVariable('lons',numpy.float,('lon'))
    #describe:属性转成GEO2
    latitudes.units='degrees_north'
    longitudes.units='degrees_east'
    ws=dataset.createVariable('ws',numpy.float,('lat','lon'))
    temp=dataset.createVariable('temp',numpy.float,('lat','lon'))
    rh=dataset.createVariable('rh',numpy.float,('lat','lon'))
    pre=dataset.createVariable('pre',numpy.float,('lat','lon'))
    gust=dataset.createVariable('gust',numpy.float,('lat','lon'))
    latindex_start=int((31-15)/0.01)
    latindex_end=int((37-15)/0.01)
    lonindex_start=int((110-70)/0.01)
    lonindex_end=int((117-70)/0.01)
    #print latindex_start,latindex_end,lonindex_end,lonindex_start
    lats=wsnc.variables['LAT']
    lons=wsnc.variables['LON']
    ws_hn=wsnc.variables['WIND']
    rh_hn=rhnc.variables['QAIR']
    temp_hn=tempnc.variables['TAIR']
    pre_hn=prenc.variables['PREC']
    #阵风计算
    grid=zhenfengCalculate(odatetime)
    if grid==[]:
        logger.info('grid is null')
    gust_hn=grid[latindex_start:latindex_end,lonindex_start:lonindex_end]
    print ws_hn,rh_hn,temp_hn,pre_hn.shape
    ws_hn=ws_hn[latindex_start:latindex_end,lonindex_start:lonindex_end]
    rh_hn = rh_hn[latindex_start:latindex_end,lonindex_start:lonindex_end]
    temp_hn = temp_hn[latindex_start:latindex_end,lonindex_start:lonindex_end]
    pre_hn = pre_hn[latindex_start:latindex_end,lonindex_start:lonindex_end]
    print ws_hn,rh_hn,temp_hn,pre_hn,lats,lons
    latitudes[:]=lats[latindex_start:latindex_end]
    longitudes[:]=lons[lonindex_start:lonindex_end]
    ws[:]=ws_hn
    rh[:]=rh_hn
    temp[:]=temp_hn
    pre[:]=pre_hn
    gust[:]=gust_hn
    dataset.close()
    
if __name__ == "__main__":
    pdatetime=datetime.datetime.now()
    odatetime=pdatetime+datetime.timedelta(hours=-8)
    yearstring=str(odatetime.year)
    if odatetime.hour < 10:
        hourstr = '0' + str(odatetime.hour)
    else:
        hourstr = str(odatetime.hour)
    pdatestring=datetime.datetime.strftime(odatetime,'%Y-%m-%d')
    #timestring=datetime.datetime.strftime(odatetime,'%Y%m%d%H')
    #ncfilepath='/Users/yetao.lu/信息中心API/cldas'
    ncfilepath='/home/wlan_dev/cldas/'+yearstring+'/'+pdatestring+'/'+hourstr
    outpath='/home/wlan_dev/henan'
    logpath='/home/wlan_dev/log'
    #日志模块
    logger = logging.getLogger("apscheduler.executors.default")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
        '%a, %d %b %Y %H:%M:%S', )
    logfile=os.path.join(logpath,'log'+pdatestring+hourstr+'.log')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    lighting()
    # 创建后台执行的schedulers
    scheduler = BackgroundScheduler()
    # 添加调度任务
    # 调度方法为timeTask,触发器选择定时，
    scheduler.add_job(ReadCLDASncFile, 'cron', minute='43,53,58',
                      args=(ncfilepath,outpath, odatetime,logger))
    scheduler.start()
    try:
        while True:
            time.sleep(2)
    except(KeyboardInterrupt, SystemExit):
        logger.info('System out')

