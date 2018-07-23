#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/2/6
description:
"""
import os,datetime
tempcsv='/Users/yetao.lu/Desktop/mos/data/tempCSV'
stationdict={}
for srootpath,sdirs,stationfile in os.walk(tempcsv):
    for i in range(len(stationfile)):
        if stationfile[i][-4:]=='.csv':
            stationfilepath=os.path.join(srootpath,stationfile[i])
            print stationfilepath
            fileR=open(stationfilepath,'r')
            while True:
                line=fileR.readline()
                linearray=line.split(',')
                if len(linearray)>4:
                    sdictId=linearray[0]+linearray[1]
                    pdatetime=datetime.datetime.strptime(linearray[1],'%Y-%m-%d %H:%M:%S')
                    timestring=datetime.datetime.strftime(pdatetime,'%Y%m%d%H%M%S')
                    sdictId = linearray[0] + '_'+timestring
                    print sdictId
                    stationdict[sdictId]=linearray[4]
                if not line:
                    break
print len(stationdict)