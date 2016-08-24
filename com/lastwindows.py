#encoding:utf-8
'''
Created on 2015年12月14日

@author: wilson.zhou
'''
import datetime
import json
import pymysql
import glob
import collections
import Queue
import  threading
import  time
import pandas  as pd
import re
import redis
import  urllib
import urllib2
pool=redis.ConnectionPool(host="10.10.10.155",port=6379,db=1)
r=redis.Redis(connection_pool=pool)
def  gaincurrenttime(day):
    time=(datetime.datetime.now()+datetime.timedelta(days=day)).strftime("%Y%m%d")
    return time
global  handtime
handtime=gaincurrenttime(-1)
global logname
global lognum
global logerror
logname=[]
lognum=[]
logerror=[]
def query(ip):
    data={}
    key = "T1ICLICK203902268"
    data['key']=key
    data['ip']=ip
    data['fm']=15
    post_data = urllib.urlencode(data)
    url = "http://10.21.20.221:8080/osapi/query"
    click= urllib2.urlopen(url, post_data).read()
    return click.split(',')[1]
def  queryIp(ip):
    nhtip='nhtip'+ip 
    try:
        click=r.get(nhtip).split(',')[1]
    except:
        try:
            click=int(query(ip))
        except:
            click=0
    return click

queue=Queue.Queue()
date=datetime.datetime.now().strftime("%Y%m%d")
conn=pymysql.connect(host="10.10.10.77",user="usr_dba",passwd="4rfv%TGB^YHN",db="xmo_summaries",use_unicode=True, charset="utf8")
cur=conn.cursor()
cur.execute("delete  from LOGNAME_load")
class ThreadNum(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.queue=queue
    
    def  handerLog_addgroup(self,result):
        result_list_addgrop=[]
        result_list_addgroup_true=[]
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
                    if queryIp(line['query_hash']['opxip'])>=50:
                        result_list_addgroup_true.append(line['query_hash']['opxseid'])
                except:
                    continue
                try:
                    result_list_addgrop.append(line['query_hash']['opxseid'])
                except:
                    continue
        f.close()
        line_error=line_num-len(result_list_addgrop)
        return result_list_addgrop,result_list_addgroup_true,line_num,line_error
    def  handerLog_RTB(self,result):
        result_list_rtb=[]
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
                    result_list_rtb.append(line['rtb_hash']['opxseid'])
                except:
                    continue
        line_error=line_num-len(result_list_rtb)
        f.close()
        return result_list_rtb,line_num,line_error
    def  handerLog_imagecli(self,result):
        result_list_imagecli=[]
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
                    result_list_imagecli.append(line['query_hash']['opxcreativeid'])
                except:
                    continue
        f.close()
        line_error=line_num-len(result_list_imagecli)
        return result_list_imagecli,line_num,line_error
    def  handerLog_imageimp(self,result):
        result_list_imageimp=[]
        line_num=0
        line_error=0
        with  open(result)  as f:
            line_num+=1
            for line in f:
                try:
                    line=json.loads(line.strip().split('\t')[2])
                except:
                    continue
                try:
                    result_list_imageimp.append(line['query_hash']['opxseid'])
                except:
                    continue
        f.close()
        line_error=line_num-len(result_list_imageimp)
        return result_list_imageimp,line_num,line_error
    
    def handerLog(self,result):
        if re.match(r'/mapr/bjidc\.hadoop\.iclick/staging/tracking/incoming/rtb\.BJ.*?',result):            
            result_list_rtb=self.handerLog_RTB(result)
            
            df=pd.DataFrame({'ClickLogName':collections.Counter(result_list_rtb).keys(),'count':collections.Counter(result_list_rtb).values()})
            df['ClickDate']=datetime.datetime.now().strftime("%Y%m%d")
            df.to_sql('blog_user',conn, flavor="mysql", if_exists='append', index=False)
            conn.commit()
            
        elif re.match(r'/mapr/bjidc\.hadoop\.iclick/staging/tracking/incoming/adgroup\.BJ.*?',result):         
            result_list_addgrop,result_list_addgroup_true=self.handerLog_addgroup(result)
        elif re.match(r'/mapr/bjidc\.hadoop\.iclick/staging/tracking/incoming/image\.BJ1.*?',result):
            result_list_imagecli=self.handerLog_imagecli(result)
        elif re.match(r'/mapr/bjidc\.hadoop\.iclick/staging/tracking/incoming/image\.BJ2.*?',result):
            result_list_imageimp=self.handerLog_imageimp(result)       
            df=pd.DataFrame({'ClickLogName':collections.Counter(result_list_imageimp).keys(),'count':collections.Counter(result_list_imageimp).values()})
            df['ClickDate']=datetime.datetime.now().strftime("%Y%m%d")
            df['Clickupdate']=datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")
            df.to_sql('blog_user',conn, flavor="mysql", if_exists='append', index=False)
            conn.commit()
        else:
            pass   
 
    def run(self):
        while True:
            num=self.queue.get()
            logname.append(num)
            if re.match(r'rtb\.BJ.*?',num):            
                result_list_rtb,line_num,line_error=self.handerLog_RTB("d:\\wilson.zhou\\Desktop\\"+num)
                df=pd.DataFrame({'searchengine_id':collections.Counter(result_list_rtb).keys(),'impressions':collections.Counter(result_list_rtb).values()})
                df['k_date']=handtime
                
                df.to_sql('rtb',conn, flavor="mysql", if_exists='append', index=False)
                conn.commit()
                
                
            elif re.match(r'adgroup\.BJ.*?',num):         
                result_list_addgrop,result_list_addgroup_true,line_num,line_error=self.handerLog_addgroup("d:\\wilson.zhou\\Desktop\\"+num)
                
                df=pd.DataFrame({'searchengine_id':collections.Counter(result_list_addgrop).keys(),'clicks':collections.Counter(result_list_addgrop).values()})
                df1=pd.DataFrame({'searchengine_id':collections.Counter(result_list_addgroup_true).keys(),'click_true':collections.Counter(result_list_addgroup_true).values()})
                df2=df.merge(df1,left_on='searchengine_id',right_on='searchengine_id',how='left')
                df2['click_true']=df2['click_true'].fillna('null')
                print df2
                df2['k_date']=handtime
                df2.to_sql('addgroup',conn, flavor="mysql", if_exists='append', index=False)
                conn.commit()
            elif re.match(r'image\.BJ1.*?',num):
                result_list_imagecli,line_num,line_error=self.handerLog_imagecli("d:\\wilson.zhou\\Desktop\\"+num)
                df=pd.DataFrame({'creative_id':collections.Counter(result_list_imagecli).keys(),'clicks':collections.Counter(result_list_imagecli).values()})
                df['date']=handtime
                df.to_sql('imageBJ1',conn, flavor="mysql", if_exists='append', index=False)
                conn.commit()
            elif re.match(r'image\.BJ2.*?',num):
                result_list_imageimp,line_num,line_error=self.handerLog_imagecli("d:\\wilson.zhou\\Desktop\\"+num)
                df=pd.DataFrame({'creative_id':collections.Counter(result_list_imageimp).keys(),'impressions':collections.Counter(result_list_imageimp).values()})
                df['date']=handtime
                df.to_sql('imageBJ2',conn, flavor="mysql", if_exists='append', index=False)
                conn.commit()
            lognum.append(line_num)
            logerror.append(line_error)
            print("success")

            self.queue.task_done()
start=time.time()
#获取当天的时间日期，day参数可以获取前后几天的日期
def main():
    for i  in range(10):
        t=ThreadNum(queue)
        t.setDaemon(True)
        t.start()
    
    path_imageBJ2="d:\\wilson.zhou\\Desktop\\image.BJ2."+handtime
    path_rtbBJ2="d:\\wilson.zhou\\Desktop\\rtb.BJ2."+handtime
    path_imageBJ1="d:\\wilson.zhou\\Desktop\\image.BJ1."+handtime
    path_adgroup1="d:\\wilson.zhou\\Desktop\\adgroup.BJ1."+handtime
    df=pd.read_sql("select  logname from LOGNAME where k_date=%s"%(handtime),conn)
    temp=df['logname'].tolist()
    
    result_imageBJ2=glob.glob(path_imageBJ2+"*.log")
    result_rtbBJ2=glob.glob(path_rtbBJ2+"*.log")
    result_imageBJ1=glob.glob(path_imageBJ1+"*.log")
    result_adgroup1=glob.glob(path_adgroup1+"*.log")
    
    
    result_imageBJ2=[j for j in result_imageBJ2 if j.split('\\')[-1]  not in  temp]
    result_rtbBJ2=[j for j in result_rtbBJ2  if j.split('\\')[-1] not in  temp]
    result_imageBJ1=[j for j in result_imageBJ1  if j.split('\\')[-1] not in  temp]
    result_adgroup1=[j for j in result_adgroup1 if j.split('\\')[-1] not in  temp]
    last=[j.split('\\')[-1] for j in  result_imageBJ2]
    
    t1=[j.split('\\')[-1] for j in result_rtbBJ2]
    t2=[j.split('\\')[-1] for j in  result_imageBJ1]
    t3=[j.split('\\')[-1] for j in  result_adgroup1]
    last.extend(t1)
    last.extend(t2)
    last.extend(t3)
#     df1=pd.DataFrame({'logname':last})
#     df1['k_date']=handtime
#     df1.to_sql('LOGNAME_load',conn, flavor="mysql", if_exists='append', index=False)
    conn.commit()
    print last
    if len(last)==0:
        pass
    elif len(last)>50:
        for num in last[0:49]:
            queue.put(num)
            queue.join()
    else:
        for num in last:
            queue.put(num)
            queue.join()
        
if __name__=='__main__':
    main()
    df1=pd.DataFrame({'logname':logname,'lognum':lognum,'logerror':logerror})
    df1['k_date']=handtime
    print df1
    df1.to_sql('LOGNAME_load',conn, flavor="mysql", if_exists='append', index=False)           
    print"Elapsed Time: %s" % (time.time() - start)  
    
    
