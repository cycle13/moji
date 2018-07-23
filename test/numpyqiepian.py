#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/16
description:
"""
import numpy
if __name__ == "__main__":
    print ''
    a = [[0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5],
        [0, 1, 2, 3, 4, 5]]
    a=numpy.array(a)
    #print a[:,3:]
    b=a[:,0:2]
    c=a[:,4:]
    e=a[:2]
    print e
    #print b ,c,'--------',b.shape,c.shape
    d=numpy.hstack((b,c))
    #print d
    print d.shape