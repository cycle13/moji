#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/19
description:
"""
import multiprocesstest,time
def worker(interval):
    n = 5
    while n > 0:
        print("The time is {0}".format(time.ctime()))
        time.sleep(interval)
        n -= 1
def f():
    print '777'
if __name__=="__main__":
    #target表示调用的target
    pool=multiprocesstest.Pool(multiprocesstest.cpu_count() - 1)
    for i in range(0,9):
        result=pool.apply_async(f)
    pool.close()
    pool.join()
    if result.successful():
        print '00'
