from sklearn import preprocessing
import numpy as np

x = np.array([[1., -1., 2.],
                 [2., 0., 0.],
                 [0., 1., -1.]])
x_scaled = preprocessing.scale(x)

print x_scaled.mean(axis=0)
print x_scaled.std(axis=0)
print '------------------------'
print x_scaled

scaler = preprocessing.StandardScaler().fit(x)
scaler.mean_
scaler.var_
print scaler.mean_
print scaler.var_
y=scaler.transform(x)
print y
