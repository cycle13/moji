#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/5/3
description:
"""
import bz2,os,threadpool
def bz2togrib(filepath,outpath):
    pool = threadpool.ThreadPool(40)
    for root,dirs,files in os.walk(filepath):
        for file in files:
            print file
            filename = os.path.join(root, file)
            print file[:3]
            if file[:3]=='D1D' and file[7:9]=='12':
                #newfile=filename[:-4]+'.grib'
                newfile=os.path.join(outpath,file[:-4]+'.grib')
                print newfile
                if not os.path.exists(newfile):
                    a = bz2.BZ2File(filename, 'rb')
                    b = open(newfile, 'wb')
                    b.write(a.read())
                    a.close()
                    b.close()
if __name__ == "__main__":
    #filepath='/mnt/data/test'
    filepath='/Users/yetao.lu/Desktop/mos/mosdata/grib2015'
    #outpath='/mnt/data/grib'
    outpath='/Users/yetao.lu/Desktop/mos/mosdata/grib2015'
    bz2togrib(filepath,outpath)

