#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/15
description:duimostongjideyichangjieguojinxing
"""

if __name__ == "__main__":
    testcsv='/Users/yetao.lu/Desktop/mos/result_temp/test51.csv'
    eccsv='/Users/yetao.lu/Desktop/mos/result_temp/origin51.csv'
    testfile=open(testcsv,'r')
    testread=testfile.read()
    testarray=testread.split(',')
    print len(testarray)
    ecfile=open(eccsv,'r')
    ecread=ecfile.read()
    ecarray=ecread.split(',')
    print len(ecarray)
    n1=0
    n2=0
    n3=0
    n4=0
    n5=0
    for i in range(len(testarray)-1):
        print testarray[i],ecarray[i]
        a=abs(float(testarray[i])-float(ecarray[i]))
        if a<3:
            n1=n1+1
        elif a>=3 and a<=5:
            n2=n2+1
        elif a>5 and a<=7:
            n3=n3+1
        elif a>7 and a<10:
            n4=n4+1
        else:
            n5=n5+1
    print str(n1)+','+str(n2)+','+str(n3)+','+str(n4)+','+str(n5)
    