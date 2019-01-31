#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/17
description:增加单位版本。
"""
import os, numpy, urllib2, json, datetime, logging, sys, time, shutil,math,subprocess
import logging.handlers
from netCDF4 import Dataset
from invdisttree import Invdisttree
from apscheduler.schedulers.background import BackgroundScheduler

'''
#根据国家观测站极大风速插值成阵风格点，并取河南的数据
#原来的范围是取得全国的，四个焦点坐标是15,60,70,140
#河南范围取四个焦点坐标：31，37，110，117
#插值范围取稍微大一点：30，38，109，118
#2018-08-22将气温单位换成摄氏度，将比湿修改为相对湿度
'''
#闪电的算法处理比较简单，即使观测站落在那个格点上，那个格点赋值。
def lightingfromcimiss(odatetime, logger):
    try:
        lightingdataset = numpy.zeros((800, 900), dtype='float32')
        edatetime = odatetime + datetime.timedelta(hours=1)
        start = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
        end = datetime.datetime.strftime(edatetime, '%Y%m%d%H')
        # url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getUparEleByTimeRange&dataCode=UPAR_ADTD_CAMS&elements=RECORD_ID,DATA_ID,IYMDHM,D_DATETIME,Datetime,Lat,Lon,Alti,Year,Mon,Day,Hour,Min,Second,MSecond,Lit_Current,MARS_3,Pois_Err,Pois_Type,Lit_Prov,Lit_City,Lit_Cnty&timeRange=[20180516150000,20180516160000)&dataFormat=json'
        url = 'http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getUparEleByTimeRange&dataCode=UPAR_ADTD_CAMS&elements=RECORD_ID,DATA_ID,IYMDHM,D_DATETIME,Datetime,Lat,Lon,Alti,Year,Mon,Day,Hour,Min,Second,MSecond,Lit_Current,MARS_3,Pois_Err,Pois_Type,Lit_Prov,Lit_City,Lit_Cnty&timeRange=[' + start + '0000,' + end + '0000)&dataFormat=json'
        logger.info(url)
        jsonstring = urllib2.urlopen(url)
        #print jsonstring
        jsondata = json.loads(jsonstring.read())
        #print jsondata
        returncode = jsondata['returnCode']
        dataset = jsondata['DS']
        for row in dataset:
            lat = float(row['Lat'])
            lon = float(row['Lon'])
            if lat < 30 or lat > 38 or lon < 109 or lon > 118:
                continue
            lightvalue = float(row['Lit_Current'])
            latindex = int((lat - 30) / 0.01)
            lonindex = int((lon - 109) / 0.01)
            lightingdataset[latindex, lonindex] = lightvalue
    except Exception as e:
        logger.info(e.message)
    return lightingdataset
def interpolate_KDTree(x, y, v, grid, logger):
    logger.info(datetime.datetime.now())
    logger.info(x.shape)
    known = zip(x, y)
    # print type(known)
    known = numpy.array(known)
    # print type(known), type(v), known.shape, v.shape
    lats = numpy.arange(30, 38, 0.01)
    lons = numpy.arange(109, 118, 0.01)
    # print lats.shape, lons.shape
    ask = []
    for i in range(len(lats)):
        for j in range(len(lons)):
            ask.append((lats[i], lons[j]))
    ask = numpy.array(ask)
    # print '-------------'
    # print type(ask),ask.shape
    leafsize = 10
    Nnear = 8
    eps = 0.005
    p = 2
    invdisttree = Invdisttree(known, v, leafsize=leafsize, stat=1)
    interpol = invdisttree(ask, nnear=Nnear, eps=eps, p=p)
    grid = interpol.reshape(800, 900)
    logger.info(datetime.datetime.now())
    del interpol
    return grid
# 方法1：这种方法也可以
def iwd(x, y, v, grid, power):
    for i in xrange(grid.shape[0]):
        for j in xrange(grid.shape[1]):
            distance = numpy.sqrt((x - i) ** 2 + (y - j) ** 2)
            if (distance ** power).min() == 0:
                grid[i, j] = v[(distance ** power).argmin()]
            else:
                total = numpy.sum(1 / (distance ** power))
                grid[i, j] = numpy.sum(v / (distance ** power) / total)
    return grid

def zhenfengCalculate(odatetime, logger):
    try:
        grid = numpy.zeros((800, 900), dtype='float32')
        edatetime = odatetime + datetime.timedelta(hours=1)
        start = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
        end = datetime.datetime.strftime(edatetime, '%Y%m%d%H')
        # url = 'http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&dataFormat=json&interfaceId=getSurfEleByTimeRange&timeRange=[20180516000000,20180521000000)&elements=Station_id_c,Lat,Lon,WIN_S_Inst_Max,D_datetime&dataCode=SURF_CHN_MUL_HOR'
        url = 'http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&dataFormat=json&interfaceId=getSurfEleByTimeRange&timeRange=[' + start + '0000,' + end + '0000)&elements=Station_id_c,Lat,Lon,WIN_S_Inst_Max,D_datetime&dataCode=SURF_CHN_MUL_HOR'
        logger.info(url)
        fileread = urllib2.urlopen(url)
        jsonstring = fileread.read()
        #logger.info(type(jsonstring))
        jsonfile = json.loads(jsonstring)
        #print jsonfile
        # returncode=jsonfile['returnCode']
        dataset = jsonfile['DS']
        #logger.info(dataset)
        x = []
        y = []
        v = []
        for row in dataset:
            if row['WIN_S_Inst_Max'] <> '999999':
                x.append(float(row['Lat']))
                y.append(float(row['Lon']))
                v.append(float(row['WIN_S_Inst_Max']))
        x = numpy.array(x)
        y = numpy.array(y)
        v = numpy.array(v)
        # 方法1
        # grid=iwd(x,y,v,grid,2)
        # 方法2
        grid = interpolate_KDTree(x, y, v, grid, logger)
    except Exception as e:
        logger.info('gust failed' + e.message)
    return grid
def winddirectCalculate(odatetime, logger):
    try:
        grid = numpy.zeros((800, 900), dtype='float32')
        edatetime = odatetime + datetime.timedelta(hours=1)
        start = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
        end = datetime.datetime.strftime(edatetime, '%Y%m%d%H')
        # url = 'http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&dataFormat=json&interfaceId=getSurfEleByTimeRange&timeRange=[20180516000000,20180521000000)&elements=Station_id_c,Lat,Lon,WIN_S_Inst_Max,D_datetime&dataCode=SURF_CHN_MUL_HOR'
        url = 'http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&dataFormat=json&interfaceId=getSurfEleByTimeRange&timeRange=[' + start + '0000,' + end + '0000)&elements=Station_id_c,Lat,Lon,WIN_D_Avg_10mi,D_datetime&dataCode=SURF_CHN_MUL_HOR'
        logger.info(url)
        fileread = urllib2.urlopen(url)
        jsonstring = fileread.read()
        #logger.info(type(jsonstring))
        jsonfile = json.loads(jsonstring)
        #print jsonfile
        # returncode=jsonfile['returnCode']
        dataset = jsonfile['DS']
        #logger.info(dataset)
        x = []
        y = []
        v = []
        for row in dataset:
            if row['WIN_S_Inst_Max'] <> '999999':
                x.append(float(row['Lat']))
                y.append(float(row['Lon']))
                v.append(float(row['WIN_S_Inst_Max']))
        x = numpy.array(x)
        y = numpy.array(y)
        v = numpy.array(v)
        # 方法1
        # grid=iwd(x,y,v,grid,2)
        # 方法2
        grid = interpolate_KDTree(x, y, v, grid, logger)
    except Exception as e:
        logger.info('gust failed' + e.message)
    return grid
def ReadCLDASncFile(ncpath, outpath, logpath):
    pdatetime = datetime.datetime.now()
    pdatetimess = datetime.datetime.strftime(pdatetime, '%Y%m%d%H%M%S')
    odatetime = pdatetime + datetime.timedelta(hours=-8)
    yearstring = str(odatetime.year)
    if odatetime.hour < 10:
        hourstr = '0' + str(odatetime.hour)
    else:
        hourstr = str(odatetime.hour)
    pdatestring = datetime.datetime.strftime(odatetime, '%Y-%m-%d')
    ncfilepath = ncpath + yearstring + '/' + pdatestring + '/' + hourstr
    for root, dirs, files in os.walk(ncfilepath):
        for file in files:
            if 'WIN' in file:
                wsfilename = file
                logger.info(file)
            elif 'TMP' in file:
                tempfilename = file
                logger.info(file)
            elif 'SHU' in file:
                rhfilename = file
                logger.info(file)
            elif 'PRE' in file:
                prefilename = file
                logger.info(file)
    wsfile = os.path.join(ncfilepath, wsfilename)
    tempfile = os.path.join(ncfilepath, tempfilename)
    rhfile = os.path.join(ncfilepath, rhfilename)
    prefile = os.path.join(ncfilepath, prefilename)
    wsnc = Dataset(wsfile, 'r')
    tempnc = Dataset(tempfile, 'r')
    rhnc = Dataset(rhfile, 'r')
    prenc = Dataset(prefile, 'r')
    # print wsnc,tempnc,rhnc,prenc
    #print odatetime
    start = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    outfile = os.path.join(outpath,
                           'HNHSR_Meteo_ELE_LIVE_' + start + '_000.nc')
    if os.path.exists(outfile):
        if os.path.getsize(outfile) < 20 * 1024 * 1024:
            os.remove(outfile)
        else:
            return None
    dataset = Dataset(outfile, 'w', format='NETCDF4_CLASSIC')
    dataset.createDimension('lat', 600)
    dataset.createDimension('lon', 700)
    latitudes = dataset.createVariable('lats', numpy.float, ('lat'))
    longitudes = dataset.createVariable('lons', numpy.float, ('lon'))
    # describe:属性转成GEO2
    latitudes.units = 'degrees_north'
    longitudes.units = 'degrees_east'
    ws = dataset.createVariable('ws', numpy.float, ('lat', 'lon'))
    temp = dataset.createVariable('temp', numpy.float, ('lat', 'lon'))
    rh = dataset.createVariable('rh', numpy.float, ('lat', 'lon'))
    pre = dataset.createVariable('pre', numpy.float, ('lat', 'lon'))
    gust = dataset.createVariable('gust', numpy.float, ('lat', 'lon'))
    # 横风和致灾强降水
    crosswind = dataset.createVariable('crosswind', numpy.float,
                                       ('lat', 'lon'))
    heavyrain = dataset.createVariable('heavyrain', numpy.float,
                                       ('lat', 'lon'))
    lightning = dataset.createVariable('lightning', numpy.float,
                                       ('lat', 'lon'))
    crosswind.units='m/s'
    heavyrain.units='mm'
    lightning.units='10KA'
    latindex_start = int((31 - 15) / 0.01)
    latindex_end = int((37 - 15) / 0.01)
    lonindex_start = int((110 - 70) / 0.01)
    lonindex_end = int((117 - 70) / 0.01)
    # print latindex_start,latindex_end,lonindex_end,lonindex_start
    lats = wsnc.variables['LAT']
    lons = wsnc.variables['LON']
    ws_hn = wsnc.variables['WIND']
    rh_hn = rhnc.variables['QAIR']
    temp_hn = tempnc.variables['TAIR']
    pre_hn = prenc.variables['PREC']
    # 阵风计算:取阵风要素，用国家观测站极大风速插值计算
    grid = zhenfengCalculate(odatetime, logger)
    logger.info('gust finish')
    # 雷电处理
    lightingrid = lightingfromcimiss(odatetime, logger)
    logger.info('lightning finish')
    lighting_hn = lightingrid[latindex_start:latindex_end,
    lonindex_start:lonindex_end]
    gust_hn = grid[latindex_start:latindex_end, lonindex_start:lonindex_end]
    # print ws_hn, rh_hn, temp_hn, pre_hn.shape
    ws_hn = ws_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    rh_hn = rh_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    temp_hn = temp_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    pre_hn = pre_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    # print ws_hn, rh_hn, temp_hn, pre_hn, lats, lons
    latitudes[:] = lats[latindex_start:latindex_end]
    longitudes[:] = lons[lonindex_start:lonindex_end]
    # 横风和强降水赋值
    crosswind_hn = ws_hn
    heavyrain_hn = pre_hn
    crosswind_hn[crosswind_hn < 10.8] = 0
    heavyrain_hn[heavyrain_hn < 20] = 0
    ws[:] = ws_hn
    rh[:] = rh_hn
    temp[:] = temp_hn
    pre[:] = pre_hn
    gust[:] = gust_hn
    crosswind[:] = crosswind_hn
    heavyrain[:] = heavyrain_hn
    lightning[:] = lighting_hn
    dataset.close()
    logger.info('nc writer finish')
    stream_handler.close()
    logger.removeHandler(stream_handler)
    logging.shutdown()
def ReadCLDASncFileWriteNC(ncpath, outpath, logpath, udt):
    pdatetime = datetime.datetime.now()
    pdatetimess = datetime.datetime.strftime(pdatetime, '%Y%m%d%H%M%S')
    odatetime = pdatetime + datetime.timedelta(hours=udt)
    yearstring = str(odatetime.year)
    if odatetime.hour < 10:
        hourstr = '0' + str(odatetime.hour)
    else:
        hourstr = str(odatetime.hour)
    pdatestring = datetime.datetime.strftime(odatetime, '%Y-%m-%d')
    ncfilepath = ncpath + yearstring + '/' + pdatestring + '/' + hourstr


    for root, dirs, files in os.walk(ncfilepath):
        for file in files:
            if 'WIN' in file:
                wsfilename = file
                logger.info(file)
            elif 'TMP' in file:
                tempfilename = file
                logger.info(file)
            elif 'SHU' in file:
                rhfilename = file
                logger.info(file)
            elif 'PRE' in file:
                prefilename = file
                logger.info(file)
    wsfile = os.path.join(ncfilepath, wsfilename)
    tempfile = os.path.join(ncfilepath, tempfilename)
    rhfile = os.path.join(ncfilepath, rhfilename)
    prefile = os.path.join(ncfilepath, prefilename)
    wsnc = Dataset(wsfile, 'r')
    tempnc = Dataset(tempfile, 'r')
    rhnc = Dataset(rhfile, 'r')
    prenc = Dataset(prefile, 'r')
    # print wsnc,tempnc,rhnc,prenc
    #print odatetime
    start = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    outfile = os.path.join(outpath,
                           'HNHSR_Meteo_ELE_LIVE_' + start + '_000.nc')
    if os.path.exists(outfile):
        if os.path.getsize(outfile) < 20 * 1024 * 1024:
            os.remove(outfile)
        else:
            return None
    dataset = Dataset(outfile, 'w', format='NETCDF4_CLASSIC')
    dataset.createDimension('lat', 600)
    dataset.createDimension('lon', 700)
    latitudes = dataset.createVariable('lats', numpy.float, ('lat'))
    longitudes = dataset.createVariable('lons', numpy.float, ('lon'))
    # describe:属性转成GEO2
    latitudes.units = 'degrees_north'
    longitudes.units = 'degrees_east'
    ws = dataset.createVariable('ws', numpy.float, ('lat', 'lon'))
    temp = dataset.createVariable('temp', numpy.float, ('lat', 'lon'))
    rh = dataset.createVariable('rh', numpy.float, ('lat', 'lon'))
    pre = dataset.createVariable('pre', numpy.float, ('lat', 'lon'))
    gust = dataset.createVariable('gust', numpy.float, ('lat', 'lon'))
    #变量的单位
    ws.units='m/s'
    temp.units='K'
    rh.units='%'
    pre.units='mm'
    gust.units='m/s'
    # 横风和致灾强降水
    crosswind = dataset.createVariable('crosswind', numpy.float,
                                       ('lat', 'lon'))
    heavyrain = dataset.createVariable('heavyrain', numpy.float,
                                       ('lat', 'lon'))
    lightning = dataset.createVariable('lightning', numpy.float,
                                       ('lat', 'lon'))
    latindex_start = int((31 - 15) / 0.01)
    latindex_end = int((37 - 15) / 0.01)
    lonindex_start = int((110 - 70) / 0.01)
    lonindex_end = int((117 - 70) / 0.01)
    # print latindex_start,latindex_end,lonindex_end,lonindex_start
    lats = wsnc.variables['LAT']
    lons = wsnc.variables['LON']
    ws_hn = wsnc.variables['WIND']
    rh_hn = rhnc.variables['QAIR']
    temp_hn = tempnc.variables['TAIR']
    pre_hn = prenc.variables['PREC']
    # 阵风计算:取阵风要素，用国家观测站极大风速插值计算
    grid = zhenfengCalculate(odatetime, logger)
    logger.info('gust finish')
    # 雷电处理
    lightingrid = lightingfromcimiss(odatetime, logger)
    logger.info('lightning finish')
    lighting_hn = lightingrid[latindex_start:latindex_end,
    lonindex_start:lonindex_end]
    gust_hn = grid[latindex_start:latindex_end, lonindex_start:lonindex_end]
    # print ws_hn, rh_hn, temp_hn, pre_hn.shape
    ws_hn = ws_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    rh_hn = rh_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    temp_hn = temp_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    pre_hn = pre_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    # print ws_hn, rh_hn, temp_hn, pre_hn, lats, lons
    latitudes[:] = lats[latindex_start:latindex_end]
    longitudes[:] = lons[lonindex_start:lonindex_end]
    # 横风和强降水赋值
    crosswind_hn = ws_hn
    heavyrain_hn = pre_hn
    crosswind_hn[crosswind_hn < 10.8] = 0
    heavyrain_hn[heavyrain_hn < 20] = 0
    ws[:] = ws_hn
    rh[:] = rh_hn
    temp[:] = temp_hn
    pre[:] = pre_hn
    gust[:] = gust_hn
    crosswind[:] = crosswind_hn
    heavyrain[:] = heavyrain_hn
    lightning[:] = lighting_hn
    dataset.close()
    logger.info('nc writer finish')
    stream_handler.close()
    logger.removeHandler(stream_handler)
    logging.shutdown()
#要求文件不大于20M，下面两个函数就是将上面的分开，
#实况：气温、降水、风速、风向、湿度、阵风
#相对湿度计算需要气压
def convertSHUtoRH(t,p,q):
    t=numpy.array(t)
    p=numpy.array(p)
    q=numpy.array(q)
    
    rh=numpy.zeros((600,700),dtype=float)
    i=-1
    for tt,pp,qq,RH in zip(t,p,q,rh):
        i=i+1
        j=-1
        for t1,p1,q1,h1 in zip(tt,pp,qq,RH):
            j=j+1
            a=17.67*t1/(t1+243.4)

            E=6.112*(math.pow(2.718281828459,a))
            
            e=q1*p1*0.01/(0.622+0.378*q1)

            RH=e*100/E
            #print i,j,a,e,E,RH
            rh[i,j]=RH
    return rh
def readcldasforlive(ncpath, outpath, udt):
    pdatetime = datetime.datetime.now()
    pdatetimess = datetime.datetime.strftime(pdatetime, '%Y%m%d%H%M%S')
    odatetime = pdatetime + datetime.timedelta(hours=udt)
    yearstring = str(odatetime.year)
    if odatetime.hour < 10:
        hourstr = '0' + str(odatetime.hour)
    else:
        hourstr = str(odatetime.hour)
    pdatestring = datetime.datetime.strftime(odatetime, '%Y-%m-%d')
    ncfilepath = ncpath +'/'+ yearstring + '/' + pdatestring + '/' + hourstr
    print ncfilepath
    # # 日志模块
    # logging.basicConfig()
    # logger = logging.getLogger("henan_ele")
    # logger.setLevel(logging.INFO)
    # formatter = logging.Formatter(
    #     '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
    #     '%a, %d %b %Y %H:%M:%S', )
    # logfile = os.path.join(logpath,
    #                        'ele' + pdatestring + hourstr + '_' + pdatetimess + '.log')
    # file_handler = logging.FileHandler(logfile)
    # file_handler.setFormatter(formatter)
    # stream_handler = logging.StreamHandler(sys.stderr)
    # logger.addHandler(file_handler)
    # logger.addHandler(stream_handler)
    #获取CLDAS数据
    for root, dirs, files in os.walk(ncfilepath):
        for file in files:
            if 'WIN' in file:
                wsfilename = file
                #print wsfilename
                #logger.info(file)
            elif 'TMP' in file:
                tempfilename = file
                #logger.info(file)
            elif 'SHU' in file:
                rhfilename = file
                #logger.info(file)
            elif 'PRE' in file:
                prefilename = file
                #logger.info(file)
            elif 'WIV' in file:
                wivfilename=file
            elif 'WIU' in file:
                wiufilename=file
            elif 'PRS' in file:
                prsfilename=file
    wsfile = os.path.join(ncfilepath, wsfilename)
    tempfile = os.path.join(ncfilepath, tempfilename)
    rhfile = os.path.join(ncfilepath, rhfilename)
    prefile = os.path.join(ncfilepath, prefilename)
    wiufile=os.path.join(ncfilepath,wiufilename)
    wivfile=os.path.join(ncfilepath,wivfilename)
    prsfile=os.path.join(ncfilepath,prsfilename)
    #读取CLDAS数据
    wsnc = Dataset(wsfile, 'r')
    tempnc = Dataset(tempfile, 'r')
    rhnc = Dataset(rhfile, 'r')
    prenc = Dataset(prefile, 'r')
    wvnc=Dataset(wivfile,'r')
    wunc=Dataset(wiufile,'r')
    prsnc=Dataset(prsfile,'r')
    start = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    if not os.path.exists(outpath+'/'+yearstring):
        os.mkdir(outpath+'/'+yearstring)
    if not os.path.exists(outpath+'/'+yearstring + '/' + pdatestring):
        os.makedirs(outpath+'/'+yearstring + '/' + pdatestring)
    outfile001 = os.path.join(outpath+'/'+yearstring + '/' + pdatestring,'HNHSR_Meteo_ELE_LIVE_' + start + '_000.nc')
    #判断文件是否存在，存在则删除
    logger.info(outfile001)
    # if os.path.exists(outfile001):
    #     p=subprocess.Popen('rm -rf '+outfile001,shell=True)
    #     p.wait()
    #写NC文件
    dataset = Dataset(outfile001, 'w', format='NETCDF4_CLASSIC')
    dataset.createDimension('lat', 600)
    dataset.createDimension('lon', 700)
    latitudes = dataset.createVariable('lats', numpy.float, ('lat'))
    longitudes = dataset.createVariable('lons', numpy.float, ('lon'))
    # describe:属性转成GEO2
    latitudes.units = 'degrees_north'
    longitudes.units = 'degrees_east'
    #其中风速、风向、气温、相对湿度和降水是从CLDAS数据中截取
    ws = dataset.createVariable('windspeed', numpy.float, ('lat', 'lon'))
    #风向改为整型，减小数据存储
    wd=dataset.createVariable('winddirection',numpy.int32,('lat','lon'))
    temp = dataset.createVariable('temperature', numpy.float, ('lat', 'lon'))
    rh = dataset.createVariable('humidity', numpy.float, ('lat', 'lon'))
    pre = dataset.createVariable('precipitation', numpy.float, ('lat', 'lon'))
    gust = dataset.createVariable('gustofwind', numpy.float, ('lat', 'lon'))
    #这是CLDAS数据的索引获取，15、70是CLDAS数据的起始经纬度
    latindex_start = int((31 - 15) / 0.01)
    latindex_end = int((37 - 15) / 0.01)
    lonindex_start = int((110 - 70) / 0.01)
    lonindex_end = int((117 - 70) / 0.01)
    lats = wsnc.variables['LAT']
    lons = wsnc.variables['LON']
    print lats.shape,lons.shape
    ws_hn = wsnc.variables['WIND']
    rh_hn = rhnc.variables['QAIR']
    temp_hn = tempnc.variables['TAIR']
    pre_hn = prenc.variables['PREC']
    wv_hn=wvnc.variables['VWIN']
    wu_hn=wunc.variables['UWIN']
    prs_hn=prsnc.variables['PAIR']
    #logger.info(odatetime)
    # 阵风计算:取阵风要素，用国家观测站极大风速插值计算，插值的格点比需求数据稍微大点，
    # 30、109是阵风插值格点的起始经纬度。
    glatindex_start = int((31 - 30) / 0.01)
    glatindex_end = int((37 - 30) / 0.01)
    glonindex_start = int((110 - 109) / 0.01)
    glonindex_end = int((117 - 109) / 0.01)
    grid = zhenfengCalculate(odatetime, logger)
    logger.info('gust finish')
    gust_hn = grid[glatindex_start:glatindex_end, glonindex_start:glonindex_end]
    ws_hn = ws_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    rh_hn = rh_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    temp_hn = temp_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]-273.15
    pre_hn = pre_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    wv_hn=wv_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    wu_hn=wu_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    prs_hn=prs_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    wv_hn=numpy.array(wv_hn)
    wu_hn=numpy.array(wu_hn)
    #print wv_hn,wu_hn
    #logger.info(wv_hn.shape)
    #logger.info(wu_hn.shape)
    wd_hn=[]
    for x,y in zip(wv_hn,wu_hn):
        wd_hncon = []
        for v,u in zip(x,y):
            if u>0 and v>0:
                fx=270-math.atan(v/u)*180/math.pi
            elif u<0 and v>0:
                fx=90 -math.atan(v/u)*180/math.pi
            elif u<0 and v<0:
                fx=90-math.atan(v/u)*180/math.pi
            elif u>0 and v<0:
                fx=270-math.atan(v/u)*180/math.pi
            elif u==0 and v>0:
                fx=180
            elif u==0 and v<0:
                fx=0
            elif u>0 and v==0:
                fx=90
            elif u==0 and v==0:
                fx=-9999
            wd_hncon.append(fx)
        wd_hn.append(wd_hncon)
    wd_hn=numpy.array(wd_hn)
    latitudes[:] = lats[latindex_start:latindex_end]
    longitudes[:] = lons[lonindex_start:lonindex_end]
    #将比湿转为相对湿度
    rharray=convertSHUtoRH(temp_hn,prs_hn,rh_hn)
    print rharray
    ws[:] = ws_hn
    rh[:] = rharray
    temp[:] = temp_hn
    pre[:] = pre_hn
    gust[:] = gust_hn
    wd[:]=wd_hn
    dataset.close()
    logger.info('nc writer finish')
    wsnc.close()
    prenc.close()
    tempnc.close()
    rhnc.close()
    wvnc.close()
    wunc.close()
    prsnc.close()
    del wsnc
    del prenc
    del tempnc
    del rhnc
    del grid
    del dataset
    endtime=datetime.datetime.now()
    logger.info((endtime-pdatetime).seconds/3600)
    subprocess.call('/moji/meteo/cluster/program/moge/hnhsr/ncprocess4hnhsr/shell/start_extract.sh '+outfile001,shell=True)
    # cmd_line = ['./start_extract.sh',outfile001]
    # subprocess.call(cmd_line,cwd='/moji/meteo/cluster/program/moge/hnhsr/ncprocess4hnhsr/shell')
    
#灾害：强风、强降水、闪电
def readcimissfordisaster(ncpath, outpath, udt):
    pdatetime = datetime.datetime.now()
    pdatetimess = datetime.datetime.strftime(pdatetime, '%Y%m%d%H%M%S')
    odatetime = pdatetime + datetime.timedelta(hours=udt)
    yearstring = str(odatetime.year)
    if odatetime.hour < 10:
        hourstr = '0' + str(odatetime.hour)
    else:
        hourstr = str(odatetime.hour)
    pdatestring = datetime.datetime.strftime(odatetime, '%Y-%m-%d')
    ncfilepath = ncpath +'/'+ yearstring + '/' + pdatestring + '/' + hourstr
    # # 日志模块
    # logging.basicConfig()
    # logger = logging.getLogger("apscheduler.executors.default")
    # logger.setLevel(logging.INFO)
    # formatter = logging.Formatter(
    #     '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
    #     '%a, %d %b %Y %H:%M:%S', )
    # logfile = os.path.join(logpath,
    #                        'dis' + pdatestring + hourstr + '_' + pdatetimess + '.log')
    # file_handler = logging.FileHandler(logfile)
    # file_handler.setFormatter(formatter)
    # stream_handler = logging.StreamHandler(sys.stderr)
    # logger.addHandler(file_handler)
    # logger.addHandler(stream_handler)
    for root, dirs, files in os.walk(ncfilepath):
        for file in files:
            if 'WIN' in file:
                wsfilename = file
                logger.info(file)
            elif 'PRE' in file:
                prefilename = file
                logger.info(file)
    wsfile = os.path.join(ncfilepath, wsfilename)
    prefile = os.path.join(ncfilepath, prefilename)
    wsnc = Dataset(wsfile, 'r')
    prenc = Dataset(prefile, 'r')
    # print wsnc,tempnc,rhnc,prenc
    print
    if not os.path.exists(outpath+'/'+yearstring):
        os.mkdir(outpath+'/'+yearstring)
    if not os.path.exists(outpath+'/'+yearstring + '/' + pdatestring):
        os.makedirs(outpath+'/'+yearstring + '/' + pdatestring)
    start = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    outfile = os.path.join(outpath+'/'+yearstring + '/' + pdatestring,'HNHSR_Meteo_DIS_LIVE_' + start + '_000.nc')
    logger.info(outfile)
    if os.path.exists(outfile):
        if os.path.getsize(outfile) < 1 * 1024 * 1024:
            try:
                p=subprocess.Popen('rm -rf '+outfile,shell=True)
                p.wait()
            except Exception,ex:
                logger.info(ex.message)
        else:
            return None
    dataset = Dataset(outfile, 'w', format='NETCDF4_CLASSIC')
    dataset.createDimension('lat', 600)
    dataset.createDimension('lon', 700)
    latitudes = dataset.createVariable('lats', numpy.float, ('lat'))
    longitudes = dataset.createVariable('lons', numpy.float, ('lon'))
    # describe:属性转成GEO2
    latitudes.units = 'degrees_north'
    longitudes.units = 'degrees_east'
    # 横风和致灾强降水
    crosswind = dataset.createVariable('crosswind', numpy.float,
                                       ('lat', 'lon'))
    heavyrain = dataset.createVariable('heavyrain', numpy.float,
                                       ('lat', 'lon'))
    lightning = dataset.createVariable('lightning', numpy.float,
                                       ('lat', 'lon'))
    latindex_start = int((31 - 15) / 0.01)
    latindex_end = int((37 - 15) / 0.01)
    lonindex_start = int((110 - 70) / 0.01)
    lonindex_end = int((117 - 70) / 0.01)
    # print latindex_start,latindex_end,lonindex_end,lonindex_start
    lats = wsnc.variables['LAT']
    lons = wsnc.variables['LON']
    ws_hn = wsnc.variables['WIND']
    pre_hn = prenc.variables['PREC']
    # 雷电处理
    lightingrid = lightingfromcimiss(odatetime, logger)
    logger.info('lightning finish')
    lighting_hn = lightingrid[latindex_start:latindex_end,
    lonindex_start:lonindex_end]
    # print ws_hn, rh_hn, temp_hn, pre_hn.shape
    ws_hn = ws_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    pre_hn = pre_hn[latindex_start:latindex_end, lonindex_start:lonindex_end]
    # print ws_hn, rh_hn, temp_hn, pre_hn, lats, lons
    latitudes[:] = lats[latindex_start:latindex_end]
    longitudes[:] = lons[lonindex_start:lonindex_end]
    # 横风和强降水赋值
    crosswind_hn = ws_hn
    heavyrain_hn = pre_hn
    crosswind_hn[crosswind_hn < 10.8] = 0
    heavyrain_hn[heavyrain_hn < 20] = 0
    crosswind[:] = crosswind_hn
    heavyrain[:] = heavyrain_hn
    lightning[:] = lighting_hn
    dataset.close()
    logger.info('dis nc writer finish')
    del wsnc
    del prenc
    endtime=datetime.datetime.now()
    logger.info((endtime-pdatetime).seconds/3600)
    subprocess.call('/moji/meteo/cluster/program/moge/hnhsr/ncprocess4hnhsr/shell/start_extract.sh '+outfile,shell=True)
#备份数据源:用cldas625公里的数据做备份来生成产品
def readcldas625forlivebak(ncpath, outpath):
    #先判断该时次产品是否生成
    pdatetime = datetime.datetime.now()
    pdatetimess = datetime.datetime.strftime(pdatetime, '%Y%m%d%H%M%S')
    odatetime = pdatetime + datetime.timedelta(hours=-11)
    yearstring = str(odatetime.year)
    hourstring=str(odatetime.year)
    print yearstring
    ###
    if odatetime.hour < 10:
        hourstr = '0' + str(odatetime.hour)
    else:
        hourstr = str(odatetime.hour)
    pdatestring = datetime.datetime.strftime(odatetime, '%Y-%m-%d')
    namedatestring=datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    # # 日志模块
    # logging.basicConfig()
    # logger = logging.getLogger("henan_ele")
    # logger.setLevel(logging.INFO)
    # formatter = logging.Formatter(
    #     '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
    #     '%a, %d %b %Y %H:%M:%S', )
    # logfile = os.path.join(logpath,
    #                        'elebak' + pdatestring + hourstr + '_' + pdatetimess + '.log')
    # file_handler = logging.FileHandler(logfile)
    # file_handler.setFormatter(formatter)
    # stream_handler = logging.StreamHandler(sys.stderr)
    # logger.addHandler(file_handler)
    # logger.addHandler(stream_handler)
    #cldas625路径
    ncfilepath = ncpath +'/'+ yearstring + '/' + pdatestring + '/' + hourstr
    #生成结果路径
    productpath = '/moji/meteo/cluster/data/henan/' + yearstring + '/' + pdatestring
    print ncfilepath
    #结果文件是
    elefile='HNHSR_Meteo_ELE_LIVE_'+namedatestring+'_000.nc'
    #disfile='HNHSR_Meteo_DIS_LIVE_'+namedatestring+'_000.nc'
    elefullpath=os.path.join(productpath,elefile)
    #disfullpath=os.path.join(productpath,disfile)
    print '222222'
    #判断文件是否存在，不存在启动备份生产
    if not os.path.exists(elefullpath):
        #获取CLDAS625数据,cldas625没有风向要素
        for root, dirs, files in os.walk(ncfilepath):
            for file in files:
                if 'WIN' in file:
                    wsfilename = file
                elif 'TMP' in file:
                    tempfilename = file
                    logger.info(file)
                elif 'SHU' in file:
                    rhfilename = file
                    logger.info(file)
                elif 'PRE' in file:
                    prefilename = file
                elif 'PRS' in file:
                    prsfilename=file
        wsfile = os.path.join(ncfilepath, wsfilename)
        tempfile = os.path.join(ncfilepath, tempfilename)
        rhfile = os.path.join(ncfilepath, rhfilename)
        prefile = os.path.join(ncfilepath, prefilename)
        prsfile=os.path.join(ncfilepath,prsfilename)

        #读取CLDAS625数据
        wsnc = Dataset(wsfile, 'r')
        tempnc = Dataset(tempfile, 'r')
        rhnc = Dataset(rhfile, 'r')
        prenc = Dataset(prefile, 'r')
        prsnc=Dataset(prsfile,'r')
        start = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
        if not os.path.exists(outpath+'/'+yearstring):
            os.mkdir(outpath+'/'+yearstring)
        if not os.path.exists(outpath+'/'+yearstring + '/' + pdatestring):
            os.makedirs(outpath+'/'+yearstring + '/' + pdatestring)
        outfile001 = os.path.join(outpath+'/'+yearstring + '/' + pdatestring,'HNHSR_Meteo_ELE_LIVE_' + start + '_000.nc')
        #判断文件是否存在，存在则删除
        logger.info(outfile001)
        if os.path.exists(outfile001):
            try:
                p=subprocess.Popen('rm -rf '+outfile001,shell=True)
                p.wait()
            except Exception,ex:
                logger.info(ex.message)
        #写NC文件
        dataset = Dataset(outfile001, 'w', format='NETCDF4_CLASSIC')
        dataset.createDimension('lat', 600)
        dataset.createDimension('lon', 700)
        latitudes = dataset.createVariable('lats', numpy.float, ('lat'))
        longitudes = dataset.createVariable('lons', numpy.float, ('lon'))
        # describe:属性转成GEO2
        latitudes.units = 'degrees_north'
        longitudes.units = 'degrees_east'
        #其中风速风向气温相对湿度和降水是从CLDAS数据中截取
        ws = dataset.createVariable('windspeed', numpy.float, ('lat', 'lon'))
        #风向改为整型，减小数据存储
        wd=dataset.createVariable('winddirection',numpy.int32,('lat','lon'))
        temp = dataset.createVariable('temperature', numpy.float, ('lat', 'lon'))
        rh = dataset.createVariable('humidity', numpy.float, ('lat', 'lon'))
        pre = dataset.createVariable('precipitation', numpy.float, ('lat', 'lon'))
        gust = dataset.createVariable('gustofwind', numpy.float, ('lat', 'lon'))
        #赋值，从CLDAS625中获取格点的值：用1公里的格点直接从625数据中取值
        #625公里数据起始经度60，起始纬度0，分辨率都是0.0625，为1600*1040个网格
        #河南的起始经纬度为31\37和110\117，分辨率为0.01，从1公里的数据上对应到6.25公里数据
        lats = wsnc.variables['LAT']
        lons = wsnc.variables['LON']
        print lats.shape,lons.shape
        ws_value = wsnc.variables['WIND']
        rh_value = rhnc.variables['QAIR']
        temp_value = tempnc.variables['TAIR']
        pre_value = prenc.variables['PRCP']
        prs_value = prsnc.variables['PAIR']
        #创建空矩阵
        ws_gridarray=numpy.zeros((600,700),dtype=float)
        temp_gridarray=numpy.zeros((600,700),dtype=float)
        pre_gridarray=numpy.zeros((600,700),dtype=float)
        shu_gridarray=numpy.zeros((600,700),dtype=float)
        prs_gridarray=numpy.zeros((600,700),dtype=float)
        lat_gridarray=[]
        lon_gridarray=[]
        logger.info('aaa')
        #对空矩阵赋值，将1公里的网格折算成6.25公里的网格
        for i in range(600):
            print i
            x=31+(0.01*i)
            lat_gridarray.append(x)
            for j in range(700):
                y=110+(j*0.01)
                #只加1遍记录,将经度数列存起来
                if i==1:
                    lon_gridarray.append(y)
                xindex=int(x/0.0625)
                yindex=int((y-60)/0.0625)
                #print i,x,j,y,xindex,yindex
                ws_gridarray[i,j]=ws_value[xindex,yindex]
                temp_gridarray[i,j]=temp_value[xindex,yindex]-273.15
                pre_gridarray[i,j]=pre_value[xindex,yindex]
                shu_gridarray[i,j]=rh_value[xindex,yindex]
                prs_gridarray[i,j]=prs_value[xindex,yindex]
        #风速、降水和气温都可以直接取
        print len(lat_gridarray),len(lon_gridarray)
        lat_gridarray=numpy.array(lat_gridarray).reshape(600,)
        lon_gridarray=numpy.array(lon_gridarray).reshape(700,)
        latitudes[:]=lat_gridarray
        longitudes[:]=lon_gridarray
        ws[:]=ws_gridarray
        temp[:]=temp_gridarray
        pre[:]=pre_gridarray
        logger.info('bbb')
        #将比湿转换为相对湿度
        rharray = convertSHUtoRH(temp_gridarray, prs_gridarray, shu_gridarray)
        rh[:]=rharray
        # 阵风计算:取阵风要素，用国家观测站极大风速插值计算，插值的格点比需求数据稍微大点，
        # 30、109是阵风插值格点的起始经纬度。
        glatindex_start = int((31 - 30) / 0.01)
        glatindex_end = int((37 - 30) / 0.01)
        glonindex_start = int((110 - 109) / 0.01)
        glonindex_end = int((117 - 109) / 0.01)
        grid = zhenfengCalculate(odatetime, logger)
        gust[:]=grid[glatindex_start:glatindex_end, glonindex_start:glonindex_end]
        logger.info('ccc')
        #风向计算，因为没有uv分量也没有风向，只能插值了
        wdgrid=winddirectCalculate(odatetime,logger)
        print wdgrid.shape
        #风向插值是800*900的矩阵，截取600*700
        wdgrid=wdgrid[glatindex_start:glatindex_end, glonindex_start:glonindex_end]
        wd[:]=wdgrid
        dataset.close()
        subprocess.call(
            '/moji/meteo/cluster/program/moge/hnhsr/ncprocess4hnhsr/shell/start_extract.sh ' + outfile001,
            shell=True)
        wsnc.close()
        prenc.close()
        tempnc.close()
        prsnc.close()
        rhnc.close()
def readcimissfordisasterbak(ncpath, outpath):
    pdatetime = datetime.datetime.now()
    pdatetimess = datetime.datetime.strftime(pdatetime, '%Y%m%d%H%M%S')
    odatetime = pdatetime + datetime.timedelta(hours=-11)
    yearstring = str(odatetime.year)
    if odatetime.hour < 10:
        hourstr = '0' + str(odatetime.hour)
    else:
        hourstr = str(odatetime.hour)
    pdatestring = datetime.datetime.strftime(odatetime, '%Y-%m-%d')
    ncfilepath = ncpath +'/'+ yearstring + '/' + pdatestring + '/' + hourstr
    # # 日志模块
    # logging.basicConfig()
    # logger = logging.getLogger("apscheduler.executors.default")
    # logger.setLevel(logging.INFO)
    # formatter = logging.Formatter(
    #     '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
    #     '%a, %d %b %Y %H:%M:%S', )
    # logfile = os.path.join(logpath,
    #                        'dis' + pdatestring + hourstr + '_' + pdatetimess + '.log')
    # file_handler = logging.FileHandler(logfile)
    # file_handler.setFormatter(formatter)
    # stream_handler = logging.StreamHandler(sys.stderr)
    # logger.addHandler(file_handler)
    # logger.addHandler(stream_handler)
    for root, dirs, files in os.walk(ncfilepath):
        for file in files:
            if 'WIN' in file:
                wsfilename = file
                logger.info(file)
            elif 'PRE' in file:
                prefilename = file
                logger.info(file)
    wsfile = os.path.join(ncfilepath, wsfilename)
    prefile = os.path.join(ncfilepath, prefilename)
    wsnc = Dataset(wsfile, 'r')
    prenc = Dataset(prefile, 'r')
    # print wsnc,tempnc,rhnc,prenc
    print
    if not os.path.exists(outpath+'/'+yearstring):
        os.mkdir(outpath+'/'+yearstring)
    if not os.path.exists(outpath+'/'+yearstring + '/' + pdatestring):
        os.makedirs(outpath+'/'+yearstring + '/' + pdatestring)
    start = datetime.datetime.strftime(odatetime, '%Y%m%d%H')
    outfile = os.path.join(outpath+'/'+yearstring + '/' + pdatestring,'HNHSR_Meteo_DIS_LIVE_' + start + '_000.nc')
    logger.info(outfile)
    if not os.path.exists(outfile):
        dataset = Dataset(outfile, 'w', format='NETCDF4_CLASSIC')
        dataset.createDimension('lat', 600)
        dataset.createDimension('lon', 700)
        latitudes = dataset.createVariable('lats', numpy.float, ('lat'))
        longitudes = dataset.createVariable('lons', numpy.float, ('lon'))
        # describe:属性转成GEO2
        latitudes.units = 'degrees_north'
        longitudes.units = 'degrees_east'
        # 横风和致灾强降水
        crosswind = dataset.createVariable('crosswind', numpy.float,
                                           ('lat', 'lon'))
        heavyrain = dataset.createVariable('heavyrain', numpy.float,
                                           ('lat', 'lon'))
        lightning = dataset.createVariable('lightning', numpy.float,
                                           ('lat', 'lon'))
        latindex_start = int((31 - 15) / 0.01)
        latindex_end = int((37 - 15) / 0.01)
        lonindex_start = int((110 - 70) / 0.01)
        lonindex_end = int((117 - 70) / 0.01)
        #横风和致灾强降水
        lats = wsnc.variables['LAT']
        lons = wsnc.variables['LON']
        ws_hn = wsnc.variables['WIND']
        pre_hn = prenc.variables['PRCP']
        # 雷电处理
        lightingrid = lightingfromcimiss(odatetime, logger)
        logger.info('lightning finish')
        lighting_hn = lightingrid[latindex_start:latindex_end,
        lonindex_start:lonindex_end]

        latitudes[:] = lats[latindex_start:latindex_end]
        longitudes[:] = lons[lonindex_start:lonindex_end]
        # 横风和强降水赋值
        crosswind_gridarray = numpy.zeros((600,700), dtype=float)
        heavyrain_gridarray = numpy.zeros((600,700), dtype=float)
        lat_gridarray = []
        lon_gridarray = []
        # 对空矩阵赋值，将1公里的网格折算成6.25公里的网格
        for i in range(600):
            print i
            x = 31 + (0.01 * i)
            lat_gridarray.append(x)
            for j in range(700):
                y = 110 + (j * 0.01)
                if i==1:
                    lon_gridarray.append(y)
                xindex = int(x / 0.0625)
                yindex = int((y - 60) / 0.0625)
                crosswind_gridarray[i, j] = ws_hn[xindex, yindex]
                heavyrain_gridarray[i, j] = pre_hn[xindex, yindex]
        crosswind_hn = crosswind_gridarray
        heavyrain_hn = heavyrain_gridarray
        crosswind_hn[crosswind_hn < 10.8] = 0
        heavyrain_hn[heavyrain_hn < 20] = 0
        crosswind[:] = crosswind_hn
        heavyrain[:] = heavyrain_hn
        lightning[:] = lighting_hn
        dataset.close()
        logger.info('dis nc writer finish')
        del wsnc
        del prenc
        endtime=datetime.datetime.now()
        logger.info((endtime-pdatetime).seconds/3600)
        subprocess.call(
            '/moji/meteo/cluster/program/moge/hnhsr/ncprocess4hnhsr/shell/start_extract.sh ' + outfile,
            shell=True)
if __name__ == "__main__":
    # 日志模块
    logpath = '/moji/meteo/cluster/data/log'
    #logpath = '/Users/yetao.lu/Downloads/tmp/SSRA'
    logging.basicConfig()
    logger = logging.getLogger("apscheduler.executors.default")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(name)-12s %(asctime)s %(levelname)-8s %(message)s',
        '%a, %d %b %Y %H:%M:%S', )
    logfile = os.path.join(logpath,'ele.log')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stderr)
    hdlr = logging.handlers.TimedRotatingFileHandler(logfile, when='D',
                                                     interval=1,
                                                     backupCount=40)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    ncpath = '/moji/meteo/cluster/data/CLDAS'
    ncpath625 = '/moji/meteo/cluster/data/CLDAS625'
    outpath = '/moji/meteo/cluster/data/henan'
    # ncpath = '/Users/yetao.lu/Downloads/tmp'
    # outpath = '/Users/yetao.lu/Downloads/tmp/SSRA'
    utc=-8
    utc01 = -9
    utc02 = -10
    utc03 = -13
    utc04 = -22
    for i in range(-60,-11,1):
        readcldasforlive(ncpath, outpath, i)
    #readcldas625forlivebak(ncpath625,outpath)
    #readcimissfordisasterbak(ncpath625,outpath)


