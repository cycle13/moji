#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/3/6
description:
"""
import  multiprocessing
def a(i):
    print i
if __name__ == "__main__":
    print ''
    prcesslist=[]
    for k in range(10):
        p=multiprocessing.Process(target=a,args=(k,))
        p.start()
    prcesslist.append(p)
    for p in prcesslist:
        p.join()
