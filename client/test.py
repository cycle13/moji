#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/2/2
description:
"""
import Nio
nc='/Users/yetao.lu/Documents/mosdata/D1D01020000010203001.grib'
gribfile=Nio.open_file(nc,'r')
names=gribfile.variables.keys()
ds=gribfile.dimensions.keys()
aff=gribfile.attributes
var=gribfile.variables['10V_GDS0_SFC']
pp=var.attributes.keys()
timess=getattr(var,'forecast_time')
times1=getattr(var,'initial_time')
times2=getattr(var,'forecast_time_units')
print names
print ds
print aff
print pp
print timess,times1,times2
