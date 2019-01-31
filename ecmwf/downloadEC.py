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
    marswf.write('    levtype    = sfc,')
    marswf.write('\n')
    marswf.write('    grid       = 0.1/0.1,')
    marswf.write('\n')
    marswf.write('    area       = 60/60/10/160,')
    marswf.write('\n')
    marswf.write('    param      = 34.128/129.128/134.128/141.128/143.128/151.128/164.128/165.128/166.128/167.128/168.128/186.128/187.128/188.128/228.128/235.128/260015,')
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
    starttimestring='2017-10-16 12:00:00'
    starttime=datetime.datetime.strptime(starttimestring,'%Y-%m-%d %H:%M:%S')
    ecrootpath='/mnt/data/MOS'
    marsroot='/moji/meteo/cluster/program/mars/MOJI_demand/MOSpara'
    for i in range(2):
        endtime=starttime+datetime.timedelta(days=i)
        pdatetimestring=datetime.datetime.strftime(endtime,'%Y%m%d')
        yearstring=str(endtime.year)
        monthint=endtime.month
        if monthint<10:
            monthstring='0'+str(monthint)
        else:
            monthstring=str(monthint)
        marsfile=marsroot+'/'+'request_'+pdatetimestring+'_12.mars'
        gribfullpath=ecrootpath+'/'+yearstring+'/'+monthstring+'/sfc_'+pdatetimestring+'_12.grib'
        #print marsfile,gribfullpath
        writeconfigfile(marsfile,pdatetimestring,gribfullpath)
        if not os.path.exists(gribfullpath):
            print 'nohup mars '+marsfile+' >a.log 2>&1 &'
            p=subprocess.Popen('nohup mars '+marsfile+' >a.log 2>&1 &',shell=True)
            p.wait()
        
