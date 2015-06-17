__author__ = 'MrQuantum'
from scipy import stats
import numpy as np
import pdb
import json

def KDE(points,xmin,xmax,ymin,ymax):

    xCor = []
    yCor = []
    corSet = set();
    #according to resolution to build the map
    X, Y = np.mgrid[xmin:xmax:100j, ymin:ymax:100j]
    positions = np.vstack([X.ravel(), Y.ravel()])
    for point in points:
        xCor.append(point[0])
        yCor.append(point[1])
        corSet.add(point[0] * 1000 + point[1])
    #print("there are %d point",(len(xCor)))
    #print("there are %d point",(len(corSet)))
    values = np.vstack([xCor,yCor])
    kernel = stats.gaussian_kde(values)
    Z = np.reshape(kernel(positions).T, X.shape)
    return Z
    
if __name__ == '__main__':
    loc = [[5,6],[7,8],[8,9],[8,8],[34,34],[34,35],[35,34],[99,99],[0,0]]
    rst = KDE(loc,0,102,0,102)
    #truncate the result to limited digits.