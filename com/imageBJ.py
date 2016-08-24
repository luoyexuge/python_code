#encoding:utf-8
'''
Created on 2015��12��24��

@author: wilson.zhou
'''
import json
# import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')

import datetime
import json
import pymysql
import pandas  as pd
import re
import  urllib
import urllib2
conn=pymysql.connect(host="10.10.10.152",user="usr_sync",passwd="^YGH*aJdH2TS134tgb",db="xmo_summaries_sync",use_unicode=True, charset="utf8")
cur=conn.cursor()
def  gaincurrenttime(day):
    time=(datetime.datetime.now()+datetime.timedelta(days=day)).strftime("%Y%m%d")
    return time
def query(ip):
    data={}
    key = "T1ICLICK203902268"
    data['key']=key
    data['ip']=ip
    data['fm']=15
    post_data = urllib.urlencode(data)
    url = "http://10.21.20.221:8080/osapi/query"
    click= urllib2.urlopen(url, post_data).read()
    return click

handtime=gaincurrenttime(0)
def  handerLog_imageimp(result):
        result_list_imageimp=[]
        result_list_imageimp_displayimage_id=[]
        result_list_imageimp_placement_id=[]
        line_num=0
        line_error=0
        with  open(result)  as f:
            for line in f:
                line_num+=1
                try:
                    line=json.loads(line.strip().split('\t')[2])
                except:
                    continue
                try:
                    if re.match(r'[0-9]{1,}',str(line['query_hash']['opxcreativeid'])):
                        result_list_imageimp.append(line['query_hash']['opxcreativeid'])
                except Exception,e:
                    print e
                    continue
                try:
                    result_list_imageimp_displayimage_id.append(line['query_hash']['opxcreativeassetid'])
                except:
                    result_list_imageimp_displayimage_id.append(0)
                try:
                    result_list_imageimp_placement_id.append(line['query_hash']['opxplacementid'])
                except:
                    result_list_imageimp_placement_id.append(0)  
        f.close()
        line_error=line_num-len(result_list_imageimp)
        return result_list_imageimp,line_num,line_error,result_list_imageimp_displayimage_id,result_list_imageimp_placement_id
result_list_imageimp,line_num,line_error,result_list_imageimp_displayimage_id,result_list_imageimp_placement_id=handerLog_imageimp("d:\\wilson.zhou\\Desktop\\image.BJ2.2015123106_7.log")

print len(result_list_imageimp)
print len(result_list_imageimp_placement_id)
print len(result_list_imageimp_displayimage_id)
print line_error
print line_num