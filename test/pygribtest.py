#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/7/9
description:
"""
import  pygrib
if __name__ == "__main__":
    inputfile='/Users/yetao.lu/Desktop/mos/mosdata/d/D1D04280000042803001.grib'
    grbs = pygrib.open(inputfile)
    grb = grbs.read(1)[0]

    for grb in grbs:
        print grb.analDate
        print grb.validDate

        print grb
        #print grb.values