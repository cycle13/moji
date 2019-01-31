#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/13
description:
"""
import datetime,subprocess,os,multiprocessing
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
    marswf.write('param=134.128/141.128/144.128/165.128/166.128/167.128/168.128/188.128/260015,')
    marswf.write('\n')
    marswf.write('step=0/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15/16/17/18/19/20/21/22/23/24/25/26/27/28/29/30/31/32/33/34/35/36,')
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
def downECfile(starttime,marsroot,ecrootpath,i):
    endtime = starttime + datetime.timedelta(hours=12 * i)
    pdatetimestring = datetime.datetime.strftime(endtime, '%Y%m%d_%H')
    dateparams = datetime.datetime.strftime(endtime, '%Y-%m-%d')
    if endtime.hour == 0:
        timestring = '00:00:00'
    else:
        timestring = '12:00:00'
    yearstring = str(endtime.year)
    monthint = endtime.month
    if monthint < 10:
        monthstring = '0' + str(monthint)
    else:
        monthstring = str(monthint)
    marsfile = marsroot + '/' + 'snow_sfc_request_' + pdatetimestring + '.mars'
    gribfullpath = ecrootpath + '/' + yearstring + '/' + monthstring + '/snow_sfc_' + pdatetimestring + '.grib'
    writeconfigfile(marsfile, gribfullpath, dateparams, timestring)
    if not os.path.exists(ecrootpath + '/' + yearstring):
        os.makedirs(ecrootpath + '/' + yearstring)
    elif not os.path.exists(ecrootpath + '/' + yearstring + '/' + monthstring):
        os.makedirs(ecrootpath + '/' + yearstring + '/' + monthstring)
    elif not os.path.exists(gribfullpath):
        p = subprocess.Popen('mars ' + marsfile, shell=True)
    else:
        os.remove(marsfile)
if __name__ == "__main__":
    starttimestring='2016-12-05 00:00:00'
    starttime=datetime.datetime.strptime(starttimestring,'%Y-%m-%d %H:%M:%S')
    ecrootpath='/home/wlan_dev/ecdata'
    marsroot='/home/wlan_dev/mars'
    pool=multiprocessing.Pool(processes=16)
    for i in range(50):
        pool.apply_async(downECfile,args=(starttime,marsroot,ecrootpath,i))
    pool.close()
    pool.join()