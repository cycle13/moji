#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/21
description:
"""
import multiprocessing,os
from logging import getLogger, INFO
from cloghandler import ConcurrentRotatingFileHandler

def acb(a,b,ii):
    log.info('a dfaafd')
    a=1
    b=2
    c=a+b+ii
    log.info(c)
if __name__ == "__main__":
    log = getLogger()
    # Use an absolute path to prevent file rotation trouble.
    logfile = os.path.abspath("/Users/yetao.lu/Documents/tmp/mylog.log")
    # Rotate log after reaching 512K, keep 5 old copies.
    rotateHandler = ConcurrentRotatingFileHandler(logfile, "a", 512 * 1024, 5)
    log.addHandler(rotateHandler)
    log.setLevel(INFO)
    
    log.info("Here is a very exciting log message, just for you")
    print ''
    pool=multiprocessing.Pool()
    for i in range(0,8):
        a=pool.apply_async(acb,args=(1,2,i))
    