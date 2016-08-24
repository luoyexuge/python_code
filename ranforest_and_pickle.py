# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 11:45:00 2016

@author: 周美旭
"""
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
import pickle

def high(data):
    return len(data[0,:])
    
datafile1 = 'd:\\wilson.zhou\\Desktop\\1.csv'
train = pd.read_csv(datafile1)
train = train.as_matrix()

x_train = train[:,1:high(train)-1]
y_train = train[:,high(train)-1].astype(int)

model= RandomForestClassifier(n_estimators=500)
model.fit(x_train, y_train)

pickle.dump(model,open('ranfor.model','wb'))