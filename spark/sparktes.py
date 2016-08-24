#encoding:utf-8
'''
Created on 2016年3月24日

@author: wilson.zhou
'''
from pyspark import  SparkContext
sc = SparkContext( 'local', 'pyspark')
data=sc.textFile("d:\\wilson.zhou\\Desktop\\test_csv.csv").map(lambda x:str(x)).map(lambda x:x.split(",")).map(lambda x:(x[0],x[1],x[2]))
numPu=data.count()
print(data.map(lambda  x:x[0]).map(lambda x:float(x)).sum()/numPu)

sc.stop()