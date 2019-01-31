#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/22
description:
"""
import numpy
if __name__ == "__main__":
    print ''
    a=[[1,2,3],[4,5,6]]
    b=[[5,5,5],[7,7,7]]
    a=numpy.array(a)
    b=numpy.array(b)
    for (x,y) in zip(a,b):
        print x,y
