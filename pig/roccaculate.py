#!/usr/bin/python
# encoding:utf-8
'''
Created on 2016年7月5日

@author: wilson.zhou
'''
import pandas  as pd
import numpy   as np
from  sklearn.metrics import auc  
from  sklearn.metrics  import roc_curve
import matplotlib.pyplot as plt
import scipy as sp


def logloss(act, pred):
    epsilon = 1e-15
    pred = sp.maximum(epsilon, pred)
    pred = sp.minimum(1-epsilon, pred)
    ll = sum(act*sp.log(pred) + sp.subtract(1,act)*sp.log(sp.subtract(1,pred)))
    ll = ll * -1.0/len(act)
    return ll


# df=pd.read_table(r"d:\wilson.zhou\Desktop\AUC\auc_more_feature_back.txt", sep=',',names=["label","predict"])
# df=df[df["label"]!=999]
# df.sort("predict", ascending=True, inplace=True)
# label = np.array(df["label"].tolist())
# pred = np.array(df["predict"].tolist())
# ll=logloss(label, pred)
# print("ll:"+str(ll))
# fpr, tpr, thresholds = roc_curve(label, pred)
# result = auc(fpr,tpr)
# # ax=plt.subplot(1,1,1)
# res=plt.plot( fpr,tpr,color="red",  linewidth=2.5, label="AUC:"+str(round(result,4))+","+"LL:"+str(round(ll,4)))



df_less=pd.read_table(r"d:\wilson.zhou\Desktop\AUC\auc_less_feature_back.txt", sep=',',names=["label","predict"])
df_less=df_less[df_less["label"]!=999]
df_less.sort("predict", ascending=True, inplace=True)
label_less = np.array(df_less["label"].tolist())
pred_less = np.array(df_less["predict"].tolist())
ll_less=logloss(label_less, pred_less)
print("ll_less"+str(ll_less))
fpr_less, tpr_less, thresholds_less = roc_curve(label_less, pred_less)
result_less = auc(fpr_less,tpr_less)
res=plt.plot( fpr_less,tpr_less,color="b",  linewidth=2.5, label="AUC:"+str(round(result_less,4))+","+"LL:"+str(round(ll_less,4)))
print("result_less:"+str(result_less))


# handles, labels = ax.get_legend_handles_labels()
# ax.legend(handles[::-1], labels[::-1],loc="upper right")    
plt.legend((res),('Average'),loc="upper right")
plt.title(u"广点通旧ctr模型roc图",fontproperties='SimHei')
plt.xlabel(u"fpr值",fontproperties='SimHei')
plt.ylabel(u"tpr值",fontproperties='SimHei')

print(plt.show())

# print("result:"+str(result))

