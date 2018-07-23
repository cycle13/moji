#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/17
description:
"""
import shapefile
w = shapefile.Writer(shapefile.POINT)
w.point(90.3, 30)
w.point(92, 40)
w.point(-122.4, 30)
w.point(-90, 35.1)
w.field('FIRST_FLD')
w.field('SECOND_FLD','C','40')
w.field('th')
w.record('First','Point','1')
w.record('Second','Point','2')
w.record('Third','Point','3')
w.record('Fourth','Point','4')
w.save('/Users/yetao.lu/Downloads/shared/rain/test1.shp')