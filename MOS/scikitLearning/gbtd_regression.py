#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/29
description:
"""
import numpy as np
from sklearn.metrics import mean_squared_error
from sklearn.datasets import make_friedman1
from sklearn.ensemble import GradientBoostingRegressor

X, y = make_friedman1(n_samples=1200, random_state=0, noise=1.0)

X_train, X_test = X[:200], X[200:]
y_train, y_test = y[:200], y[200:]
#print X_train

est = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1,
     max_depth=1, random_state=0, loss='ls').fit(X_train, y_train)
#print est
mean_squared_error(y_test, est.predict(X_test))
print mean_squared_error(y_test, est.predict(X_test))
result=est.predict(X_test)
#print est.predict(X_test)
#print mean_squared_error(y_test, est.predict(X_test))
est.set_params(n_estimators=350,warm_start=True)
est.fit(X_train,y_train)
mean_squared_error(y_test,est.predict(X_test))
print mean_squared_error(y_test,est.predict(X_test))