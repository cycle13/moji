#encoding=utf-8

import pandas as pd
import xgboost as xgb
import time
import random
from sklearn.model_selection import StratifiedKFold
import numpy as np

#For sampling rows from input file
random_seed = 9
subset = 0.3# The original subset number is 0.4 . To run on my pc  ,we drcrease it into 0.3

root_dir="H:/github_samples/data/datasets/"
n_rows = 1183747;#这个是要跳过的行数。跳过的行数越大，我们所使用的数据集就越小，同时所需内存也越小。

train_rows = int(n_rows * subset)
random.seed(random_seed)
skip = sorted(random.sample(xrange(1,n_rows + 1),n_rows-train_rows))
data = pd.read_csv(root_dir+"train_numeric.csv", index_col=0, dtype=np.float32, skiprows=skip)
y = data['Response'].values
# y=np.load("y.npy")
print(y.shape)

# np.save("y",y)
del data['Response']
X = data.values
# X=np.load("X.npy")
print(X.shape)
# np.save("X",X)

param = {}
param['objective'] = 'binary:logistic'
param['eval_metric'] = 'auc'
param['max_depth'] = 5
param['eta'] = 0.3
param['silent'] = 0
# param['nthread']=7 # cpu 线程数
param['updater'] = 'grow_gpu_hist'
# param['updater'] = 'grow_gpu'
#param['updater'] = 'grow_colmaker'

num_round = 20

# cv = StratifiedKFold(y, n_folds=5)
cv = StratifiedKFold(n_splits=5)

print (cv.get_n_splits(X,y))

for i, (train, test) in enumerate(cv.split(X,y)):
    print (len(train),len(test))
    dtrain = xgb.DMatrix(X[train], label=y[train])
    tmp = time.time()
    bst = xgb.train(param, dtrain, num_round)
    boost_time = time.time() - tmp
    res = bst.eval(xgb.DMatrix(X[test], label=y[test]))
    print("Fold {}: {}, Boost Time {}".format(i, res, str(boost_time)))
    del bst