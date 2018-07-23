#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/7
description:
"""
import math,numpy,os,multiprocessing
from decimal import Decimal
def functionabc(file,file1):
    a=1
    b=2
    c=a+b
    print file,c,file1

if __name__ == "__main__":
    a=numpy.array([[1,2,3,4,5],[1,2,3,4,5]])
    b=a[:,0:3]
    c=a[:,3:]
    print b
    print c
    

    
    
    
    
