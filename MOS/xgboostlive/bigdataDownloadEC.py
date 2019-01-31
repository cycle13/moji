#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/3
description:大数据平台下周EC历史数据，调用SH脚本，
"""
import os,sys,logging,multiprocessing,datetime,subprocess
def downloadECdatafrombigdata(pdatetimestring):
    pdatetime=datetime.datetime.strptime(pdatetimestring,'%Y-%m-%d %H:%M:%S')
    year=pdatetime.year
    yearstr=str(year)
    datestr=datetime.datetime.strftime(pdatetime,'%Y-%m-%d')
    month=pdatetime.month
    if month<10:
        monstr='0'+str(month)
    else:
        monstr=str(month)
    days=pdatetime.day
    if days<10:
        daystr='0'+str(days)
    else:
        daystr=str(days)
    hours=pdatetime.hour
    if hours==0:
        hourstr='00'
    else:
        hourstr='12'
    for i in range(60):
        if i<40:
            udatetime=pdatetime+datetime.timedelta(hours=3*(i+1))
        else:
            udatetime=pdatetime+datetime.timedelta(hours=3*40+6*(i-39))
        #取文件
        umonth = udatetime.month
        if umonth < 10:
            umonthstr = '0' + str(umonth)
        else:
            umonthstr = str(umonth)
        udays = udatetime.day
        if udays < 10:
            udaystr = '0' + str(udays)
        else:
            udaystr = str(udays)
        uhour = udatetime.hour
        if uhour < 10:
            uhourstr = '0' + str(uhour)
        else:
            uhourstr = str(uhour)
        ecfile00 = 'D1D' + monstr + daystr + hourstr+'00' + umonthstr + udaystr + uhourstr + '001.bz2'
        print udatetime
        ecfullpath='/meteo/moge/data/ecmwf/'+yearstr+'/'+datestr+'/'+ecfile00
        print ecfullpath
        topath='/home/wlan_dev/tmp/'+yearstr+'/'+datestr
        if not os.path.exists(topath):
            os.mkdir(topath)
        p=subprocess.Popen('/home/wlan_dev/software/bigdata/api/start_hdfs_access.sh '+ecfullpath+' /home/wlan_dev/tmp/'+yearstr+'/'+datestr+' 1',shell=True)
        # p=subprocess.Popen(
        #     'java -jar /home/wlan_dev/software/bigdata/api/bigdata/hdfshelper.jar -f  ' + ecfullpath + '-g /home/wlan_dev/tmp/' + yearstr + '/' + datestr ,
        #     shell=True)
if __name__ == "__main__":
    pdatetimestring00 = '2018-07-16 00:00:00'
    downloadECdatafrombigdata(pdatetimestring00)
    pdatetimestring12 = '2018-07-16 12:00:00'
    downloadECdatafrombigdata(pdatetimestring12)
    
    
