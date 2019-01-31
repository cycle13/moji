#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/9/25
description:
"""
import datetime,subprocess,os
def writeconfigfile(marsfile,pdatestring,gribfullpath):
    marswf=open(marsfile,'w')
    marswf.write('retrieve,')
    marswf.write('\n')
    marswf.write('    class      = od,')
    marswf.write('\n')
    marswf.write('    type       = fc,')
    marswf.write('\n')
    marswf.write('    stream     = oper,')
    marswf.write('\n')
    marswf.write('    expver     = 1,')
    marswf.write('\n')
    marswf.write('    levtype    = pl,')
    marswf.write('\n')
    marswf.write('    levelist   = 500/700/850/925/950/1000,')
    marswf.write('\n')
    marswf.write('    grid       = 0.1/0.1,')
    marswf.write('\n')
    marswf.write('    area       = 60/60/10/160,')
    marswf.write('\n')
    marswf.write('    param      = 129.128/130.128/131.128/132.128/133.128/135.128/157.128,')
    marswf.write('\n')
    marswf.write('    date       = '+pdatestring+',')
    marswf.write('\n')
    marswf.write('    time       = 12,')
    marswf.write('\n')
    marswf.write('    step       = 0/3/6/9/12/15/18/21/24/27/30/33/36/39/42/45/48/51/54/57/60/63/66/69/72/75/78/81/84/87/90/93/96/99/102/105/108/111/114/117/120/123/126/129/132/135/138/141/144/150/156/162/168/174/180/186/192/198/204/210/216/222/228/234/240,')
    marswf.write('\n')
    marswf.write('    target     = "'+gribfullpath+'"')
    marswf.close()
if __name__ == "__main__":
    starttimestring='2017-01-01 12:00:00'
    starttime=datetime.datetime.strptime(starttimestring,'%Y-%m-%d %H:%M:%S')
    ecrootpath='/mnt/data/MOS'
    marsroot='/moji/meteo/cluster/program/mars/MOJI_demand/MOSpara'
    for i in range(3):
        endtime=starttime+datetime.timedelta(days=i)
        pdatetimestring=datetime.datetime.strftime(endtime,'%Y%m%d')
        yearstring=str(endtime.year)
        monthint=endtime.month
        if monthint<10:
            monthstring='0'+str(monthint)
        else:
            monthstring=str(monthint)
        marsfile=marsroot+'/'+'pl_request_'+pdatetimestring+'_12.mars'
        gribfullpath=ecrootpath+'/'+yearstring+'/'+monthstring+'/pl_'+pdatetimestring+'_12.grib'
        #print marsfile,gribfullpath
        writeconfigfile(marsfile,pdatetimestring,gribfullpath)
        if not os.path.exists(gribfullpath):
            subprocess.call('mars '+marsfile,shell=True)