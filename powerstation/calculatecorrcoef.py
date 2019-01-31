#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/21
description:
"""
import scipy.stats as stats
def calculat_xiangguanxish(csvfile):
    csvread=open(csvfile,'r')
    list_xibanya=[]
    list_live=[]
    list_moji=[]

    firstline=csvread.readline()
    while True:
        line=csvread.readline()
        linearray=line.split(',')
        if len(linearray)>6:
            list_xibanya.append(float(linearray[4]))
            list_live.append(float(linearray[5]))
            list_moji.append(float(linearray[6]))
        if not line:
            break
    #计算相关系数
    a=stats.pearsonr(list_xibanya,list_live)
    b=stats.pearsonr(list_moji,list_live)
    print a,b
if __name__ == "__main__":
    csvfile001='/Users/yetao.lu/Documents/电力/光伏发电需求/02.csv'
    csvfile002='/Users/yetao.lu/Documents/电力/光伏发电需求/02b.csv'
    calculat_xiangguanxish(csvfile001)
    calculat_xiangguanxish(csvfile002)
    