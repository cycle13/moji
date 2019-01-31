#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/8/6
description:
"""
import os,subprocess
def bzip2file(tofilepath):
    for root, dirs, files in os.walk(tofilepath):
        for file in files:
            if file[-4:]=='.bz2':
                filefullname=os.path.join(root,file)
                p=subprocess.Popen('bzip2 -d -k '+filefullname,shell=True)
if __name__ == "__main__":
    tofilepath='/home/wlan_dev/tmp/2018/2018-07-16'
    bzip2file(tofilepath)
