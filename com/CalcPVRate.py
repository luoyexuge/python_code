#encoding:utf-8
"""
Created on Mon May 25 18:48:33 2015

@author: david
"""

import collections
import numpy as np


def sort(a):
    for k in range(len(a)):
        (a[k][0],a[k][1]) = (a[k][1],a[k][0])
    a.sort()
    for k in range(len(a)):
        (a[k][0],a[k][1]) = (a[k][1],a[k][0])

#计算winrate
def calcWinRate(filepath):
    winrate_list = []    
    winrate_dict=collections.OrderedDict()  
    interval = 0.1
    i = 0
    
    fp = open(filepath,'r+')    
    arr = [l.strip().split(',') for l in fp.readlines()]
    
    while( i < 20):
        if i >= 2:
            interval = 1
        respsum, showsum = 0,0
        for j in range(len(arr)):
            if float(arr[j][0]) >= i and float(arr[j][0]) < i + interval:
                respsum += int(arr[j][1])
                showsum += int(arr[j][2])
        if respsum > 0:
            rate = (showsum + 0.0)/respsum
        else:
            rate = 0
        winrate_dict[str(i) + '~' + str(i + interval)] = rate
        winrate_list.append(rate)
        i = i + interval
    
    return winrate_list    
    
#求平均CTR时，加权平均的最小CTR
#跑这个之前要做排序，把最大的放前面
#第一列是ctr的值，第二列是ctr对应的pv，例如[0.0,192323252]
def calcCtrRate(filepath):
    ctrrate_list = []
    ctrrate_dict=collections.OrderedDict()     
    sum=0.0
    expectCtr = 0.0001
    
    fp = open(filepath, 'r+')    
    arr = [l.strip().split(',') for l in fp.readlines()]
    
    #统计结果中的ctr乘以了1000倍，需要变为正常的数值，并计算每个ctr对应流量的占比
    for i in range(len(arr)):
        arr[i][0] = float(arr[i][0])/1000
        sum += float(arr[i][1])    
    for i in range(len(arr)):   
        arr[i][1] = float(arr[i][1])/sum
        
    arr.sort()
    
    #计算每个ctr的可投放流量比
    while ( expectCtr <= 0.02 ):
        pvsum = calcPV(expectCtr, arr)
        ctrrate_dict[str(expectCtr*100)+'%']=str(round(pvsum*100,6))+'%'
        ctrrate_list.append(pvsum)
        expectCtr += 0.0001
      
    return ctrrate_list
    
#计算加权平均  
def calcPV(predictCtr, arr):
    sum, pvsum = 0,0
    
    i = len(arr)-1
    while( i >= 0 ):
        sum += float(arr[i][0]) * float(arr[i][1])
        pvsum += float(arr[i][1])
        if pvsum > 0 and sum > 0:
            per =  sum/pvsum
            if per<predictCtr:
                return pvsum
        i -= 1
    return 0 

def calcPVRate(ctrrate_list, winrate_list):
    winrate_np = np.array(winrate_list)
    winrate_np.shape = (len(winrate_list), 1)
    ctrrate_np = np.array(ctrrate_list)
    ctrrate_np.shape = (len(ctrrate_list), 1 )
    
    pvrate_np = np.dot(winrate_np, np.transpose(ctrrate_np))
    
    return np.transpose(pvrate_np)
import  pandas  as pd

if __name__ == '__main__':
    #通过价格和竞价响应、曝光数，计算竞价成功率
    winrate_list_cpm=calcWinRate(u'd:\\wilson.zhou\\Desktop\\可投放流量币计算逻辑\\data0413\\winratecpc040')
    
    #通过ctr和pv的统计，计算出平均ctr的可用流量比
    ctrrate_list = calcCtrRate(u'd:\\wilson.zhou\\Desktop\\可投放流量币计算逻辑\\data0413\\ctr040')
    
    pvrate_matrix = calcPVRate(ctrrate_list, winrate_list_cpm)   
    pd.DataFrame(pvrate_matrix).to_csv(u'd:\\wilson.zhou\\Desktop\\可投放流量币计算逻辑\\data0413\\sss.csv')
    
    
    
    
    
    
    
     
    