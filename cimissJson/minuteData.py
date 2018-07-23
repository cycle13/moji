#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/18
description:
"""
import urllib2
def minutedatafromcimiss():
    url='http://api.data.cma.cn:8090/api?userId=mjtq_mjtqmete_user&pwd=123456&interfaceId=getSurfEleByTimeRange&dataCode=SURF_CHN_MAIN_MIN_YWSF&elements=RECORD_ID,D_DATETIME,Datetime,DATA_ID,IYMDHM,RYMDHM,UPDATE_TIME,STATION_Id_C,Station_Id_d,Lat,Lon,Alti,PRS_Sensor_Alti,Station_Type,Station_levl,Admin_Code_CHN,Year,Mon,Day,Hour,Min,PRS,TEM,RHU,WIN_D_Avg_1mi,WIN_S_Avg_1mi,LGST,GST,GST_5cm,GST_10cm,GST_15cm,GST_20cm,GST_40Cm&timeRange=[20180518000000,20180518040000]&dataFormat=json'
    request=urllib2.Request(url);
    f=urllib2.urlopen(request)
    link=f.read()
    print link
if __name__ == "__main__":
    print ''
    minutedatafromcimiss()
