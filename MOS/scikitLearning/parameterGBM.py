#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/1/26
description:
"""
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn import model_selection,metrics
#from sklearn import cross_validation, metrics
from sklearn.grid_search import GridSearchCV
import matplotlib.pylab as plt

train = pd.read_csv('/Users/yetao.lu/Downloads/train_modified.csv')
#print train
target='Disbursed' # Disbursed的值就是二元分类的输出
IDcol = 'ID'
#将Disbursed字段的值分类统计数目，0值多少个，1的值多少个
train['Disbursed'].value_counts()
#挑选不是Disbursed和ID的列
x_columns = [x for x in train.columns if x not in [target, IDcol]]
#X为因子矩阵
X = train[x_columns]
#y是结果矩阵
y = train['Disbursed']
#gbm0为
gbm0 = GradientBoostingClassifier(random_state=10)
gbm0.fit(X,y)
#根据X值预测
y_pred = gbm0.predict(X)
#每个X样本为1的概率
y_predprob = gbm0.predict_proba(X)[:,1]
#打印分类准确的百分比。
print "Accuracy : %.4g" % metrics.accuracy_score(y.values, y_pred)
#直接根据真实值（必须是二值）、预测值（可以是0/1,也可以是proba值）计算出auc值，中间过程的roc计算省略
print "AUC Score (Train): %f" % metrics.roc_auc_score(y, y_predprob)
#range(start,stop ,step)
param_test1 = {'n_estimators':range(20,81,10)}
gsearch1 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, min_samples_split=300,
                                  min_samples_leaf=20,max_depth=8,max_features='sqrt', subsample=0.8,random_state=10),
                       param_grid = param_test1, scoring='roc_auc',iid=False,cv=5)
gsearch1.fit(X,y)
gsearch1.grid_scores_, gsearch1.best_params_, gsearch1.best_score_

param_test2 = {'max_depth':range(3,14,2), 'min_samples_split':range(100,801,200)}
gsearch2 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=60, min_samples_leaf=20,
      max_features='sqrt', subsample=0.8, random_state=10),
   param_grid = param_test2, scoring='roc_auc',iid=False, cv=5)
gsearch2.fit(X,y)
print gsearch2.grid_scores_, gsearch2.best_params_, gsearch2.best_score_

param_test3 = {'min_samples_split':range(800,1900,200), 'min_samples_leaf':range(60,101,10)}
gsearch3 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=60,max_depth=7,
                                     max_features='sqrt', subsample=0.8, random_state=10),
                       param_grid = param_test3, scoring='roc_auc',iid=False, cv=5)
gsearch3.fit(X,y)
print gsearch3.grid_scores_, gsearch3.best_params_, gsearch3.best_score_

gbm1 = GradientBoostingClassifier(learning_rate=0.1, n_estimators=60,max_depth=7, min_samples_leaf =60,
               min_samples_split =1200, max_features='sqrt', subsample=0.8, random_state=10)
gbm1.fit(X,y)
y_pred = gbm1.predict(X)
y_predprob = gbm1.predict_proba(X)[:,1]
print "Accuracy : %.4g" % metrics.accuracy_score(y.values, y_pred)
print "AUC Score (Train): %f" % metrics.roc_auc_score(y, y_predprob)

param_test4 = {'max_features':range(7,20,2)}
gsearch4 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=60,max_depth=7, min_samples_leaf =60,
               min_samples_split =1200, subsample=0.8, random_state=10),
                       param_grid = param_test4, scoring='roc_auc',iid=False, cv=5)
gsearch4.fit(X,y)
gsearch4.grid_scores_, gsearch4.best_params_, gsearch4.best_score_

param_test5 = {'subsample':[0.6,0.7,0.75,0.8,0.85,0.9]}
gsearch5 = GridSearchCV(estimator = GradientBoostingClassifier(learning_rate=0.1, n_estimators=60,max_depth=7, min_samples_leaf =60,
               min_samples_split =1200, max_features=9, random_state=10),
                       param_grid = param_test5, scoring='roc_auc',iid=False, cv=5)
gsearch5.fit(X,y)
gsearch5.grid_scores_, gsearch5.best_params_, gsearch5.best_score_

gbm2 = GradientBoostingClassifier(learning_rate=0.05, n_estimators=120,max_depth=7, min_samples_leaf =60,
               min_samples_split =1200, max_features=9, subsample=0.7, random_state=10)
gbm2.fit(X,y)
y_pred = gbm2.predict(X)
y_predprob = gbm2.predict_proba(X)[:,1]
print "Accuracy : %.4g" % metrics.accuracy_score(y.values, y_pred)
print "AUC Score (Train): %f" % metrics.roc_auc_score(y, y_predprob)

gbm3 = GradientBoostingClassifier(learning_rate=0.01, n_estimators=600,max_depth=7, min_samples_leaf =60,
               min_samples_split =1200, max_features=9, subsample=0.7, random_state=10)
gbm3.fit(X,y)
y_pred = gbm3.predict(X)
y_predprob = gbm3.predict_proba(X)[:,1]
print "Accuracy : %.4g" % metrics.accuracy_score(y.values, y_pred)
print "AUC Score (Train): %f" % metrics.roc_auc_score(y, y_predprob)

gbm4 = GradientBoostingClassifier(learning_rate=0.005, n_estimators=1200,max_depth=7, min_samples_leaf =60,
               min_samples_split =1200, max_features=9, subsample=0.7, random_state=10)
gbm4.fit(X,y)
y_pred = gbm4.predict(X)
y_predprob = gbm4.predict_proba(X)[:,1]
print "Accuracy : %.4g" % metrics.accuracy_score(y.values, y_pred)
print "AUC Score (Train): %f" % metrics.roc_auc_score(y, y_predprob)