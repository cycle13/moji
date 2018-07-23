#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/22
description: 闪电、强降水、横风实况监测要素
"""
import urllib2,numpy,os,netCDF4,datetime
def ReadCLDASncFile(ncfilepath,outpath,odatetime,logger):
    for root,dirs,files in os.walk(ncfilepath):
        for file in files:
            if 'WIN' in file:
                wsfilename=file
                logger.info(file)
            elif 'PRE' in file:
                prefilename=file
                logger.info(file)
    #取阵风要素，用国家观测站极大风速
    wsfile=os.path.join(ncfilepath,wsfilename)
    prefile=os.path.join(ncfilepath,prefilename)
    wsnc=netCDF4(wsfile,'r')
    prenc=netCDF4(prefile,'r')
    #print wsnc,tempnc,rhnc,prenc
    start=datetime.datetime.strftime(odatetime,'%Y%m%d%H')
    outfile=os.path.join(outpath,'henan_live_tprwg_'+start+'.nc')
    dataset=netCDF4(outfile,'w',format='NETCDF4_CLASSIC')
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
    pre_hn=prenc.variables['PREC']
    
    gust_hn=grid[latindex_start:latindex_end,lonindex_start:lonindex_end]
    #print ws_hn,rh_hn,temp_hn,pre_hn.shape
    ws_hn=ws_hn[latindex_start:latindex_end,lonindex_start:lonindex_end]
    pre_hn = pre_hn[latindex_start:latindex_end,lonindex_start:lonindex_end]
    #print ws_hn,rh_hn,temp_hn,pre_hn,lats,lons
    latitudes[:]=lats[latindex_start:latindex_end]
    longitudes[:]=lons[lonindex_start:lonindex_end]
    ws[:]=ws_hn
    pre[:]=pre_hn
    gust[:]=gust_hn
    dataset.close()
def lighting():
    url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getUparEleByTimeRange&dataCode=UPAR_ADTD_CAMS&elements=RECORD_ID,DATA_ID,IYMDHM,D_DATETIME,Datetime,Lat,Lon,Alti,Year,Mon,Day,Hour,Min,Second,MSecond,Lit_Current,MARS_3,Pois_Err,Pois_Type,Lit_Prov,Lit_City,Lit_Cnty&timeRange=[20180516150000,20180516160000)&dataFormat=json'
    jsonstring=urllib2.urlopen(url)
    jsondata=jsonstring.read()
    print jsondata
    
if __name__ == "__main__":
    print ''
    lighting()