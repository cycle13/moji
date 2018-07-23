import numpy as np


X = np.array([[1., 0.], [2., 1.], [0., 0.]])
y = np.array([0, 1, 2])

from scipy.sparse import coo_matrix
X_sparse = coo_matrix(X)
print X_sparse

print '----------'

from sklearn.utils import resample
X, X_sparse, y = resample(X, X_sparse, y, random_state=0)
print X

print X_sparse


print X_sparse.toarray()


print y

a=resample(y, n_samples=2, random_state=0)
print a
