#!/usr/bin/python
#encoding:utf-8
'''
Created on 2016年1月5日

@author: wilson.zhou
'''
import  os
import sys
def  sqoop(directory):
    with open(directory) as f:
        for i in f:
            i=i.strip()
            try:
                os.system(i)
            except Exception,e:
                continue
            print("sucess")
            

if __name__=='__main__':
    sys1='/root/data_transfer/sqoop_sh/'
    sqoop(sys1+sys.argv[1])
       
        