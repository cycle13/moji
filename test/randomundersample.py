#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
author:     yetao.lu
date:       2018/11/1
description:
"""

if __name__ == "__main__":
    from collections import Counter
    from sklearn.datasets import make_classification
    from imblearn.under_sampling import RandomUnderSampler  # doctest: +NORMALIZE_WHITESPACE
    X, y = make_classification(n_classes=2, class_sep=2,weights = [0.1, 0.9], n_informative = 3, n_redundant = 1, flip_y = 0,n_features = 20, n_clusters_per_class = 1, n_samples = 10, random_state = 10)
    print('Original dataset shape %s' % Counter(y))
    print X.shape,y.shape
    print X,y
    rus = RandomUnderSampler(random_state=42)
    X_res, y_res = rus.fit_sample(X, y)
    print X_res.shape,y_res.shape
    print X_res,y_res
    print('Resampled dataset shape %s' % Counter(y_res))
