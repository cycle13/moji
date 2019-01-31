#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/21
description:
"""
import numpy as np
import pylab as pl
from scipy import interpolate
import matplotlib.pyplot as plt

x = np.linspace(0, 2*np.pi+np.pi/4, 10)
y = np.sin(x)
print x,len(x)
print y,len(y)
x_new = np.linspace(0, 2*np.pi+np.pi/4, 100)
print x_new,len(x_new)
f_linear = interpolate.interp1d(x, y)
print f_linear
tck = interpolate.splrep(x, y)
print tck
y_bspline = interpolate.splev(x_new, tck)
print y_bspline,len(y_bspline)

plt.xlabel(u'安培/A')
plt.ylabel(u'伏特/V')

plt.plot(x, y, "o",  label=u"原始数据")
plt.plot(x_new, f_linear(x_new), label=u"线性插值")
plt.plot(x_new, y_bspline, label=u"B-spline插值")

pl.legend()
pl.show()
