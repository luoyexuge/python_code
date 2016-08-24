#encoding:utf-8
'''
Created on 2015年12月11日

@author: wilson.zhou

'''
import datetime
import json
import pymysql
import glob
import pandas as pd
#以下是数据库的操作
# def  conDB():
#     conn=pymysql.connect(host="",user="",passwd="",db="",charset="utf8")
#     cur=conn.cursor()
#     return conn,cur
# def exeUpate(conn,cur,sql):
#     sta=cur.execute(sql)
#     conn.commit()
#     return s
# def exDelete(conn,cur,IDs):
#     sta=0
#     for each  in IDs.split(''):
#         sta+=cur.execute("delete  from student where Id=%d"%(int(each)))
#     conn.commit()
#     return sta
# def exQuery(cur,sql):
#     cur.execute(sql)
#     return cur
# def  conClose(conn,cur):
#     cur.close()
#     conn.close()
#搜索日期路径
yesterday=(datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y%m%d")  #获取前一天的数据的日期
path="d:\\wilson.zhou\\Desktop\\rtb.BJ2."+yesterday
# result=glob.glob(path+"*.log") 
# def HanderLog(result):
#     start=datetime.datetime.now()
#     for i in result:
#         linenum=0
#         lineerror=0
#         lis=[]
#         with open(i) as f:
#             for line in f:
#                 linenum+=1
#                 try:
#                     line=line.strip().split('\t')[2]
#                     s=json.loads(line)
#                 
#                 except:
#                     continue
#                 lis.append(s['rtb_hash']['opxseid'])
#                  
#         f.close()
#         lineerror=linenum-len(lis)
#         print linenum,lineerror
#     total=(datetime.datetime.now()-start).total_seconds()
#     print("总共花费了{0}秒".format(total))  
# if __name__=='__main__':
#     if len(result)>0:
#         HanderLog(result)
#     else:
#         print(u"没有日志数据")
def  handerLog_addgroup(result):
        result_list_addgrop=[]
        result_list_ip=[]
        line_num=0
        line_error=0
        with  open(result)  as f:
            for line in f:
                line_num+=1
                try:
                    line=json.loads(line.strip().split('\t')[2])
                except Exception,e:
                    print e
                    continue
                try:
                    
                    result_list_ip.append(line['query_hash']['opxip'])
                    result_list_addgrop.append(line['query_hash']['opxseid'])
                except Exception,e:
                    print e
                    continue
        f.close()
        line_error=line_num-len(result_list_addgrop)
        return result_list_addgrop,result_list_ip,line_num,line_error
result_list_addgrop,result_list_ip,line_num,line_error=handerLog_addgroup('d:\\wilson.zhou\\Desktop\\adgroup.HK.2015122213_4.log')
# print result_list_addgrop

temp=zip(result_list_ip,result_list_addgrop)

import  urllib
import urllib2
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


def  ttt(result_list_ip):
    j=0
    result=""
    if len(result_list_ip)<=5000:
        result=query(','.join(result_list_ip))
    else :
        while j<len(result_list_ip):
            if j+5000>len(result_list_ip):
                temp=query(','.join(result_list_ip[j:len(result_list_ip)]))
            else:
                temp=query(','.join(result_list_ip[j:j+5000]))
            j+=5000
            result=result+'_'+temp
    return result[1:]

xxx=ttt(result_list_ip).split('_')

print(xxx)
print(len(xxx))
print  [ j.split(',')[1] for j in xxx ]


df=pd.DataFrame({'score':[ j.split(',')[1] for j in xxx ],'ip':result_list_ip})
print df

# # print result_list_ip
# print line_num
# print line_error

