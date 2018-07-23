#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/2/8
description:
"""
import numpy
def max_min_normalization(x, max, min):
    return (x - min) / (max - min)
x=([1.1,2,3],[4.1,5,6],[7.1,1,9],[3,4,4])
pmax=numpy.max(x,axis=0)
pmin=numpy.min(x,axis=0)
print pmax,pmin
p=(x-pmin)/(pmax-pmin)
print p

