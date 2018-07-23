#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/2/9
description:
"""
import numpy as np
from sklearn.cross_validation import train_test_split
a, b = np.arange(10).reshape((5, 2)), range(5)
#print a
c=([[100 ,1],[2, 3],[4 ,5],[6 ,7],[8 ,9]])

list(b)
#print b
d=([100, 1, 2, 3, 4])
a_train, a_test, b_train, b_test = train_test_split(a, b, test_size=0.33, random_state=42)
c_train, c_test, d_train, d_test = train_test_split(c, d, test_size=0.33, random_state=42)
e_train,e_test=train_test_split(a,test_size=0.33, random_state=42)
print'---------'
print a_train,c_train,e_train
print'---------'
print b_train,d_train
print'---------'
print a_test,c_test,e_test
print'---------'
print b_test,d_test