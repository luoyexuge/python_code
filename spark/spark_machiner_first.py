#encoding:utf-8
'''
Created on 2016年3月25日

@author: wilson.zhou
'''
from pyspark import  SparkContext
sc = SparkContext('local', 'pyspark')
user_data=sc.textFile(u"D:\\SPARKCONFALL\\Spark机器学习数据\\ml-100k\\u.user")
print user_data.first()
user_fieds=user_data.map(lambda line:line.split("|"))
num_users=user_fieds.map(lambda x:x[0]).count()
print num_users
print user_fieds.map(lambda x:x[3]).distinct().count()
 
 
 
movie_data=sc.textFile(u"D:\\SPARKCONFALL\\Spark机器学习数据\\ml-100k\\u.item")
print  movie_data.first()
print movie_data.count()
movie_fields=movie_data.map(lambda line:line.split("|"))
def  convert_year(x):
    try:
        return int(x[-4:])
    except:
        return 1900
years=movie_fields.map(lambda fields:fields[2]).map(lambda x:convert_year(x))
years.filter(lambda x:x!=1900)













