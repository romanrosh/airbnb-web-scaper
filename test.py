import numpy as np


# Write a naive linear search version of the algorithm.
# Time your solution, and test it on the basic distribution:

def dist(q, p):
    res = 0
    for i in range(len(q) - 1):
        res += q[i] ** 2 + p[i] ** 2
    return np.sqrt(res)


def RangeQuery(DB, Q, eps):
    Neighbors = np.empty([1, 3])
    for P in DB:  # Scan all points in the database
        if dist(Q, P) <= eps:  # Compute distance and check epsilon
            Neighbors = np.vstack([Neighbors, P])
    return Neighbors[1:,]


def DBSCAN(DB, eps, minPts):
    C = 0  # Cluster counter
    i = 0
    while i < len(DB):
        if DB[i][2] == 0:  # Previously processed in inner loop
            N = RangeQuery(DB, DB[i], eps)  # Find neighbors
        if len(N) < minPts:  # Density check
            DB[i][2] = -1  # Label as Noise
        C += 1  # next cluster label
        DB[i][2] = C  # Label initial point
        if i < len(N):
            N = np.delete(N, i, 0)
        S = N
        j = 0
        while j < len(S):  # Process every seed point
            if S[j][2] == -1:
                S[j][2] = C  # Change Noise to border point
            if S[j][2] != 0:
                j += 1
                continue  # Previously processed
            S[j][2] = C  # Label neighbor
            if DB[j][2] == 0:
                DB[j][2] = C
            N = RangeQuery(DB, S[j], eps)  # Find neighbors
            if len(N) >= minPts and len(S) < len(DB):  # Density check
                S = np.vstack([S, N])  # Add new neighbors to seed set
            j += 1
            if len(S)>len(DB):
                break
        i += 1
    return DB


import numpy
import matplotlib.pyplot as pyplot

# kmeans parameters:
# number of clusters:
k = 2
# variance
var = 1
# generating distributions:
# first real mean, x axis
mean1x = 0.5
# first mean, y axis
mean1y = -0.5
# same for second means
mean2x = -3
mean2y = -3
num_points = 10

a = numpy.random.multivariate_normal([mean1x, mean1y], [[var, 0], [0, var]], size=num_points)
b = numpy.random.multivariate_normal([mean2x, mean2y], [[var, 0], [0, var]], size=num_points)

# pyplot.plot(a[:, 0], a[:, 1], 'ro')
# pyplot.plot(b[:, 0], b[:, 1], 'bo')
# pyplot.show()

c = np.append(a, b, axis=0)
arr = (len(c), 1)
label = np.zeros(arr)
label = label.astype(int)
c = np.append(c, label, axis=1)
res = DBSCAN(c, 3, 5)

print(res)

pyplot.scatter(res[:, 0], res[:, 1], c=res[:, 2])
# pyplot.show()