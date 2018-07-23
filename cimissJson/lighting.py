#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/16
description:
"""
import urllib2
def lighting():
    #url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getUparEleByTimeRange&dataCode=UPAR_ADTD_CAMS&elements=RECORD_ID,DATA_ID,IYMDHM,D_DATETIME,Datetime,Lat,Lon,Alti,Year,Mon,Day,Hour,Min,Second,MSecond,Lit_Current,MARS_3,Pois_Err,Pois_Type,Lit_Prov,Lit_City,Lit_Cnty&timeRange=[20180514000000,20180517010000]&dataFormat=json'
    url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getUparEleByTimeRange&dataCode=UPAR_ADTD_CAMS&elements=RECORD_ID,DATA_ID,IYMDHM,D_DATETIME,Datetime,Lat,Lon,Alti,Year,Mon,Day,Hour,Min,Second,MSecond,Lit_Current,MARS_3,Pois_Err,Pois_Type,Lit_Prov,Lit_City,Lit_Cnty&timeRange=[20180610000000,20180614010000]&dataFormat=json'
    print url
    request=urllib2.Request(url);
    f=urllib2.urlopen(request)
    link=f.read()
    print link
if __name__ == "__main__":
    print ''
    lighting()
