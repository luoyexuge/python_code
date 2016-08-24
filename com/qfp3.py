# -*- coding: utf-8 -*-
"""
Created on Wed Aug 17 13:50:02 2016

@author: j
"""

import pandas as pd
import numpy as np
import re
import os
import pprint, pickle
from functools import reduce
import sys


####时间长度##################################################################
#ff=pd.read_csv ('/home/j/re/weibo4/恒生电子.csv')
#ff=ff[(ff['daykey']>='2014-01-06')&(ff['daykey']<='2016-08-13')]
#
#
##################从pkl中取出######这些股票财务数据齐全####################
#
#
#target='/home/j/re/fdata/forDL/plist.pkl'
#
#scores = {} # scores is an empty dict already
#
#if os.path.getsize(target) > 0:      
#    with open(target, "rb") as file:
#        unpickler = pickle.Unpickler(file)
#        scores = unpickler.load()
#        
##################从pkl中取出###########################
#
#
#dx=pd.read_hff('/home/j/re/fdata/forDL/xqpriceH.h5')
#dx=dx[dx['time']<'20160816']
# dx=pd.date_range(start='20140106', end='20160816', periods=None, freq='D')
# 
# pydate_array = dx.to_pydatetime()
# date_only_array = np.vectorize(lambda s: s.strftime('%Y%m%d'))(pydate_array )
# tl = pd.Series(date_only_array)
# tl.sort_values(inplace=True,ascending=False)
# tl=np.array(tl)
# dw=pd.DataFrame(data=tl,columns=['time'])
# dw['time']=dw['time'].astype(str)
# dw=dw.sort_values('time',ascending=False)

#xgpl=dx['code'].unique()
#
#jiao=list(set(xgpl).intersection(set(scores)))
#
#output = open('/home/j/re/fdata/forDL/jlist.pkl', 'wb')
#pickle.dump(jiao, output)
#output.close()

##############################获得股票交集#######################
# target='/home/j/re/fdata/forDL/plist0.pkl'
# 
# scores = {} # scores is an empty dict already
# 
# if os.path.getsize(target) > 0:      
#     with open(target, "rb") as file:
#         unpickler = pickle.Unpickler(file)
#         scores = unpickler.load()


#mx=dx.groupby('code')

#####################

df=pd.read_hdf('d:\\wilson.zhou\\Desktop\\bonus.h5')
print df.head()

df=df[['code','exrightdate','tranaddskraio','bonusskratio','cdividend']]

df['exrightdate']=df['exrightdate'].astype(str)

df=df[df['tranaddskraio']!='divibegdate']
#df=df[df['divibegdate']!='人民币']

#df['tranaddskraio'].isnull()
#
#df['tranaddskraio']=df['tranaddskraio'].astype('float')
#df['bonusskratio']=df['bonusskratio'].astype('float')
#df['cdividend']=df['cdividend'].astype('float')


#df[['tranaddskraio','bonusskratio','cdividend']]=df[['tranaddskraio','bonusskratio','cdividend']].astype('float')
mf=df.groupby('code')

#####################################################

################取时间#################
# dp=pd.read_hdf('/home/j/re/fdata/forDL/xqpriceH.h5')
# mp=dp.groupby('code')
################取时间#################

datab=np.empty((0,8))
for g in scores:
    try:
        ff=mf.get_group(g)#除权数据
    except:
        info=sys.exc_info()
#        print(info[0],":",info[1])
#    ff=ff.fillna(0)   
#    ff['exrightdate']=pd.to_datetime(ff['exrightdate'],format='%Y%m%d')
#    ff['recorddate']=pd.to_datetime(ff['recorddate'],format='%Y%m%d')
#    ff[['tranaddskraio','bonusskratio','cdividend']]=ff[['tranaddskraio','bonusskratio','cdividend']].astype('float')

    
        
    ff['exrightdate']=ff['exrightdate'].astype(str)
    ff=ff[ff['exrightdate']>='20140106']
    ff[['tranaddskraio','bonusskratio','cdividend']]=ff[['tranaddskraio','bonusskratio','cdividend']].fillna(0) 

    ff=ff[ff['bonusskratio']==u'人民币']

    ff['tranaddskraio']=ff['tranaddskraio'].astype('float')
    ff['bonusskratio']=ff['bonusskratio'].astype('float')
    ff['cdividend']=ff['cdividend'].astype('float')
#    tl=np.array(ww['time'])
    dt1=[]
    for e in ff['exrightdate']:
        #print(len(np.where(tl[:]==e)[0]))
        if len(np.where(tl[:]==e)[0])>0:
            dt1.append(int(np.where(tl[:]==e)[0]))  #发布日期
    dt1.append(0)
    dt1.append(len(tl))
    dt1.sort()
    
    
    data=np.empty((0,4))
    pp=mp.get_group(g)#价格数据
    dates = pd.to_datetime(pp['time'], format = '%Y%m%d')
    pp['time']=dates.apply(lambda x: x.strftime('%Y%m%d'))
   
    pp=pp.sort_values('time',ascending=False)
#    
#   
    ww=pd.merge(dw,pp,how='left', on='time')
    ww[['volume','turnrate']]=ww[['volume','turnrate']].fillna(0)
    ww[['close','high','low','open','volume','turnrate']]=ww[['close','high','low','open','volume','turnrate']].astype('float')
#    ww=ww[['time','code','high','low','open','close','volume','turnrate','percent']]
    ww['code']=g
    ww=ww.fillna(method='bfill') 




    
    if len(ff)==0:
        dc1=ww[['close','high','low','open','volume','turnrate','code','time']]
        datab=dc1.as_matrix()
        
    
    
   

    elif len(ff['exrightdate'])>0:
        dzz=[]
        dsg=[]
        dhl=[]
        h=0
        dzz.append(1)
        dsg.append(1)
        dhl.append(0)
    for i in range(len(ff)):
   
#        if ff['tranaddskraio'][i]==ff['tranaddskraio'][i]:
#            zz=10/(10+ff['tranaddskraio'][i])
#            dzz.append(zz)
#        elif ff['tranaddskraio'][i]!=ff['tranaddskraio'][i]:
#            zz=1
#            dzz.append(zz)
#            #print(i,t)
#        if ff['bonusskratio'][i]==ff['bonusskratio'][i]:
#            sg=10/(10+ff['bonusskratio'][i])
#            dsg.append(sg)
#        
#        elif ff['bonusskratio'][i]!=ff['bonusskratio'][i]:
#            sg=1
#            dsg.append(sg)
#            #print(i,s)
#        if ff['cdividend'][i]==ff['cdividend'][i]:
#            h+=ff['cdividend'][i]
#            dhl.append(h)
#        elif ff['cdividend'][i]!=ff['cdividend'][i]:
#            dhl.append(0)
            
        if ff['tranaddskraio'][i]!=0:
            zz=10/(10+ff['tranaddskraio'][i])
            dzz.append(zz)
        elif ff['tranaddskraio'][i]==0:
            zz=1
            dzz.append(zz)
            #print(i,t)
        if ff['bonusskratio'][i]!=0:
            sg=10/(10+ff['bonusskratio'][i])
            dsg.append(sg)
        
        elif ff['bonusskratio'][i]==0:
            sg=1
            dsg.append(sg)
            #print(i,s)
        if ff['cdividend'][i]!=0:
            h+=int(ff['cdividend'][i])
            dhl.append(h)
        elif ff['cdividend'][i]==0:
            dhl.append(0)
      
            #print(i,h)
    dz1=[reduce(lambda x, y: x * y, dzz[ :i ] ) for i in range(1, len(dzz) + 1)]
    ds1=[reduce(lambda x, y: x * y, dsg[ :i ] ) for i in range(1, len(dsg) + 1)]   

        
    #print(dt1)
    for s in range(len(dt1)-1):
        if s==0:
            dc1=ww.ix[dt1[s]:dt1[s+1]][['close','high','low','open']]
            matrix1=dc1.as_matrix()
        if len(dzz)>=s+1:
            matrix1=matrix1*dz1[s]
        if len(dsg)>=s+1:
            matrix1=matrix1*ds1[s]
        if len(dhl)>=s+1:
            matrix1=matrix1-dhl[s]/10
        
        if s>0:
            dc1=ww.ix[dt1[s]+1:dt1[s+1]][['close','high','low','open']]
            matrix1=dc1.as_matrix()
        if len(dzz)>=s+1:
            matrix1=matrix1*dz1[s]
        if len(dsg)>=s+1:
            matrix1=matrix1*ds1[s]
        if len(dhl)>=s+1:
            matrix1=matrix1-dhl[s]/10
        if s>=(len(dt1)-2):
            dc1=ww.ix[dt1[s]+1:][['close','high','low','open']]
            matrix1=dc1.as_matrix()
            if len(dzz)>=s+1:
                matrix1=matrix1*dz1[s]
            if len(dsg)>=s+1:
                matrix1=matrix1*ds1[s]
            if len(dhl)>=s+1:
                matrix1=matrix1-dhl[s]/10
        
        
    


            
            
        data=np.vstack((data,matrix1))
        #print(ww.ix[dt1[s]][['time','close']],ww.ix[dt1[s+1]-1][['time','close']])
        print(data.shape,g)
    if len(dt1)>2:
        
#            print(data.shape,g)
        dc3=ww[['volume','turnrate','code']]
        dc3['time']=ww['time']
        matrix3=dc3.as_matrix()
        data=np.hstack((data,matrix3))
        datab=np.vstack((datab,data))
        #print(datab.shape)
    
        
        
 
co=['close','high','low','open','volume','turnrate','code','time']  
dzs=pd.DataFrame(datab,columns=co)           
#dzs.HDFStore('/home/j/re/fdata/forDL/caiwu0816.h5')    
dzs.to_hdf('/home/j/re/fdata/forDL/qianfuquan.h5','a')        

