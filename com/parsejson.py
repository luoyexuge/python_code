#encoding:utf-8
'''
Created on 2016年8月18日

@author: wilson.zhou
'''
import json
f=open("d:\\wilson.zhou\\Desktop\\image16569.txt","r+")
f1=open("d:\\wilson.zhou\\Desktop\\image165691111.txt","w+")
li=[]
for i in f.readlines():
    li.append(json.loads(i.split("\t")[2])["query_hash"]["opxvid"]+","+json.loads(i.split("\t")[2])["uuid"]+"\n")
f1.writelines(li)
f1.close()
f.close()