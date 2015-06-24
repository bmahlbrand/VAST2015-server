__author__ = 'MrQuantum'
import csv
import re
import operator
import math
import random
import sys
import copy
import json
import math
import numpy as np
import scipy as sp
import matplotlib.pyplot as plt
from sklearn import cluster
import csv





def getMatIndex(date):
    reader = csv.reader(open("FriMovDataLoc.csv"))
    peopleToIndex = {}
    locToIndex = {}
    for text in reader:
        locToIndex[text[0]] = int(text[1])
    #######################need modified##############################
    if date == "sunday":
        reader1 = csv.reader(open("SunMovDataPeoUsingBillyData.csv"))
    elif  date == "saturday":
        reader1 = csv.reader(open("SatMovDataPeo.csv"))
    elif date == "friday":
        reader1 = csv.reader(open("FriMovDataPeo.csv"))
    for text in reader1:
        peopleToIndex[text[0]] = int(text[1])
    return peopleToIndex, locToIndex



def buildMatBilly(peopleToIndex,locToIndex,data):
    mat =  [[0 for col in range(len(locToIndex.keys()))] for row in range(len(peopleToIndex.keys()))]
    head = True
    for text in data:
        if head:
            head = False
            continue
        loc = text[1] + ',' + text[2]
        id = text[0]
        try:
            mat[peopleToIndex[id]][locToIndex[loc]] += int(text[3])
        except:
            continue
    mat, peopleToIndex = filterMat(mat,peopleToIndex)
    for i in range(len(mat)):
        rowSum = sum(mat[i])
        if rowSum == 0:
            continue
        for j in range(len(mat[0])):
            mat[i][j] = float(float(mat[i][j]) / rowSum)
    return mat,peopleToIndex



def filterMat(mat,peopleToIndex):
    ret = []
    index = 0
    peopleToIndexNew = {}
    for key in peopleToIndex.keys():
        if sum(mat[peopleToIndex[key]]) == 0:
            continue
        else:
            ret.append(copy.deepcopy(mat[peopleToIndex[key]]))
            peopleToIndexNew[key] = index
            index += 1
    return ret, peopleToIndexNew


def generateJasonRes(label,peopleToIndex,mat,locToIndex):
    ret = []
    locRet = {}
    for loc in locToIndex:
        locRet[locToIndex[loc]] = loc
    ret.append(copy.deepcopy(locRet))
    for id in peopleToIndex.keys():
        index = peopleToIndex[id]
        temp = {'id':id,'cluster':label[index],'fraction':mat[index]}
        ret.append(copy.deepcopy(temp))
    return ret





def kmeans(date,data):
    peopleToIndex, locToIndex = getMatIndex(date)
    mat, peopleToIndex = buildMatBilly(peopleToIndex, locToIndex,date,data)

    scores = []
    clusterNum = 0
    for i in range(1,12,1):
        clusterNum = i
        k_means = cluster.KMeans(n_clusters = clusterNum)
        k_means.fit(mat)
        label = k_means.labels_
        scores.append(k_means.inertia_)
        if i >= 4:
            if float(abs(scores[i - 2] - scores[i - 3]))/float(abs(scores[i - 3] - scores[i - 4])) <= 0.8:
                clusterNum = i
                break
    k_means = cluster.KMeans(n_clusters = clusterNum)
    k_means.fit(mat)
    label = k_means.labels_
    centers = k_means.cluster_centers_
    for i in range(len(centers)):
        centerSum = sum(centers[i])
        for j in range(len(centers[i])):
            centers[i][j] = centers[i][j] / centerSum
    ret = generateJasonRes(label,peopleToIndex,mat,locToIndex)
    return ret


