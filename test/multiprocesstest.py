#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/6
description:
"""
import multiprocessing
import time

def worker(interval):
    n = 5
    while n > 0:
        print("The time is {0}".format(time.ctime()))
        time.sleep(interval)
        n -= 1

if __name__ == "__main__":
    for i in range(6):
        processlist=[]
        p = multiprocessing.Process(target = worker, args = (3,))
        p.start()
        print "p.pid:", p.pid
        print "p.name:", p.name
        print "p.is_alive:", p.is_alive()
        processlist.append(p)
    for p in processlist:
        p.join()