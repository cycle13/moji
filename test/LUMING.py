#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/2/9
description:
"""
import PIL,cv2,numpy,gdal,scipy
from PIL import Image
file='/Users/yetao.lu/Downloads/Z_AGME_C_BABJ_20171020000000_P_CAGMSS_CGRM_LARI-DVS_CHN_L88_PD_000_24.img'
import os, sys



# from osgeo import gdal
#
# Image  = gdal.Open(file)
# Band   = Image.GetRasterBand(1) # 1 based, for this example only the first
# NoData = Band.GetNoDataValue()  # this might be important later
# print Band
# nBands = Image.RasterCount      # how many bands, to help you loop
# nRows  = Image.RasterYSize      # how many rows
# nCols  = Image.RasterXSize      # how many columns
# dType  = Band.DataType          # the datatype for this band
# RowRange = range(nRows)
# for ThisRow in RowRange:
#     # read a single line from this band
#     ThisLine = Band.ReadRaster(0,ThisRow,nCols,1,nCols,1,dType)
#
#     if ThisRow % 100 == 0: # report every 100 lines
#         print "Scanning %d of %d" % (ThisRow,nRows)
#
#     for Val in ThisLine: # some simple test on the values
#         if Val == 65535:
#             print 'Bad value'
#
# Image = None # close the raster