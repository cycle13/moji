#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/13
description:
"""
import datetime,subprocess,os
def writeconfigfile(marsfile,gribfullpath,dateparams,timestring):
    marswf=open(marsfile,'w')
    marswf.write('retrieve,')
    marswf.write('\n')
    marswf.write('class=od,')
    marswf.write('\n')
    marswf.write('date='+dateparams+',')
    marswf.write('\n')
    marswf.write('expver=1,')
    marswf.write('\n')
    marswf.write('levtype=sfc,')
    marswf.write('\n')
    marswf.write('param=21.228/47.128/134.128/165.128/166.128/167.128/168.128/169.128,')
    marswf.write('\n')
    marswf.write('step=0/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15/16/17/18/19/20/21/22/23/24/25/26/27/28/29/30/31/32/33/34/35/36/37/38/39/40/41/42/43/44/45/46/47/48/49/50/51/52/53/54/55/56/57/58/59/60/61/62/63/64/65/66/67/68/69/70/71/72/73/74/75/76/77/78/79/80/81/82/83/84/85/86/87/88/89/90/93/96/99/102/105/108/111/114/117/120/123/126/129/132/135/138/141/144/150/156/162/168/174/180/186/192/198/204/210/216/222/228/234/240,')
    marswf.write('\n')
    marswf.write('stream=oper,')
    marswf.write('\n')
    marswf.write('time='+timestring+',')
    marswf.write('\n')
    marswf.write('grid= 0.1/0.1,')
    marswf.write('\n')
    marswf.write('area= 60/60/10/160,')
    marswf.write('\n')
    marswf.write('type=fc,')
    marswf.write('\n')
    marswf.write('target="'+gribfullpath+'"')
    marswf.close()
if __name__ == "__main__":
    starttimestring='2018-10-14 12:00:00'
    starttime=datetime.datetime.strptime(starttimestring,'%Y-%m-%d %H:%M:%S')
    ecrootpath='/opt/meteo/cluster/data/sample'
    marsroot='/moji/meteo/cluster/program/mars/MOJI_demand/MOSpara'
    for i in range(38):
        endtime=starttime+datetime.timedelta(hours=12*i)
        pdatetimestring=datetime.datetime.strftime(endtime,'%Y%m%d_%H')
        dateparams=datetime.datetime.strftime(endtime,'%Y-%m-%d')
        if endtime.hour==0:
            timestring='00:00:00'
        else:
            timestring='12:00:00'
        yearstring=str(endtime.year)
        monthint=endtime.month
        if monthint<10:
            monthstring='0'+str(monthint)
        else:
            monthstring=str(monthint)
        marsfile=marsroot+'/'+'power_sfc_request_'+pdatetimestring+'.mars'
        gribfullpath=ecrootpath+'/'+yearstring+'/'+monthstring+'/sfc_'+pdatetimestring+'.grib'
        print marsfile,gribfullpath
        writeconfigfile(marsfile,gribfullpath,dateparams,timestring)
        if not os.path.exists(gribfullpath):
            p=os.system('mars '+marsfile)
            if p==0:
                os.remove(marsfile)
