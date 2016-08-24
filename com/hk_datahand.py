#!/usr/bin/python
#encoding:utf-8
'''
Created on 2015年12月22日

@author: wilson.zhou
'''
import datetime
import json
import pymysql
import glob
import Queue
import  threading
import  time
import os
import pandas  as pd
import re
import  urllib
import urllib2
def  gaincurrenttime(day):
    time=(datetime.datetime.now()+datetime.timedelta(days=day)).strftime("%Y%m%d")
    return time
global  handtime
global logname
global lognum
global logerror
logname=[]
lognum=[]
logerror=[]
handtime=gaincurrenttime(0)

queue=Queue.Queue()
# conn=pymysql.connect(host="10.1.1.28",user="usr_dba",passwd="4rfv%TGB^YHN",db="xmo_summaries",use_unicode=True, charset="utf8")
conn=pymysql.connect(host="10.1.1.130",user="usr_sync",passwd="^YGH*aJdH2TS134tgb",db="xmo_summaries_sync",use_unicode=True, charset="utf8")
cur=conn.cursor()
class ThreadNum(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.queue=queue
    
#     def  handerLog_addgroup(self,result):
#         result_list_addgrop=[]
#         result_list_addgroup_ip=[]
#         line_num=0
#         line_error=0
#         with  open(result)  as f:
#             for line in f:
#                 line_num+=1
#                 try:
#                     line=json.loads(line.strip().split('\t')[2])
#                 except:
#                     continue
#                 try:
#                     if re.match('[0-9]{1,}',str(line['query_hash']['opxseid'])):
#                         result_list_addgroup_ip.append(line['query_hash']['opxip'])
#                         result_list_addgrop.append(line['query_hash']['opxseid'])
#                 except:
#                     continue
#         f.close()
#         line_error=line_num-len(result_list_addgrop)
#         return result_list_addgrop,result_list_addgroup_ip,line_num,line_error
    def query(self,ip):
        data={}
        key = "T1ICLICK203902268"
        data['key']=key
        data['ip']=ip
        data['fm']=15
        post_data = urllib.urlencode(data)
        url = "http://10.21.20.221:8080/osapi/query"
        click= urllib2.urlopen(url, post_data).read()
        return click
    def  handerLog_addgroup(self,result):
        result_list_addgrop=[]
#         result_list_addgroup_fraud=[]
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
                    if re.match('[0-9]{1,}',str(line['query_hash']['opxseid'])):
#                         result_list_addgroup_fraud.append(line['query_hash']['fraud'])
                        result_list_addgrop.append(line['query_hash']['opxseid'])
                        
                except:
                    continue
        f.close()
        line_error=line_num-len(result_list_addgrop)
        return result_list_addgrop,line_num,line_error
    def  check_ip(self,result_list_addgroup_ip):
        j=0
        result=""
        if len(result_list_addgroup_ip)<=5000:
            try:
                result=self.query(",".join(result_list_addgroup_ip))
            except:
                try:
                    result=self.query(",".join(result_list_addgroup_ip))
                except:
                    pass
            return result.split('_')
        else:
            while j<len(result_list_addgroup_ip):
                if j+5000>=len(result_list_addgroup_ip):
                    try:
                        temp=self.query(",".join(result_list_addgroup_ip[j:len(result_list_addgroup_ip)]))
                    except:
                        try:
                            temp=self.query(",".join(result_list_addgroup_ip[j:len(result_list_addgroup_ip)]))
                        except:
                            pass
                else:
                    try:
                        temp=self.query(",".join(result_list_addgroup_ip[j:j+5000]))
                    except:
                        try:
                            temp=self.query(",".join(result_list_addgroup_ip[j:j+5000]))
                        except:
                            pass
                        
                j=j+5000
                result=result+"_"+temp
            return result[1:].split('_')
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
                    if re.match(r'[0-9]{1,}', str(line['rtb_hash']['opxseid'])):
                        result_list_rtb.append(line['rtb_hash']['opxseid'])
                except:
                    continue
        line_error=line_num-len(result_list_rtb)
        f.close()
        return result_list_rtb,line_num,line_error

    def  handerLog_imageimpcli(self,result):
        result_imagecli=[]
        result_imageimp=[]
        line_num=0
        line_error=0
        result_imageimp_displayimage_id=[]
        result_imageimp_placement_id=[] 
        result_imagecli_displayimage_id=[]
        result_imagecli_placement_id=[]
        result_imagecli_opxip=[]
        result
        with open(result) as f: 
            for line in f:
                line_num+=1
                try:
                    line=json.loads(line.strip().split('\t')[2])
                    if  line['query_hash']['opxtype']=='I':
                        
                        try:
                            if  re.match(r'[0-9]{1,}',str(line['query_hash']['opxcreativeid'])):
                                result_imageimp.append(line['query_hash']['opxcreativeid'])
                                try:
                                    result_imageimp_displayimage_id.append(line['query_hash']['opxcreativeassetid'])
                                except:
                                    result_imageimp_displayimage_id.append(0)
                                try:
                                    result_imageimp_placement_id.append(line['query_hash']['opxplacementid'])
                                except:
                                    result_imageimp_placement_id.append(0)
                        except Exception,e:
                            print e
                            continue
                    elif line['query_hash']['opxtype']=='C':
                            
                            try:
                                if  re.match(r'[0-9]{1,}',str(line['query_hash']['opxcreativeid'])):
                                    result_imagecli.append(line['query_hash']['opxcreativeid'])
                                    try:
                                        result_imagecli_displayimage_id.append(line['query_hash']['opxcreativeassetid'])
                                    except:
                                        result_imagecli_displayimage_id.append(0)
                                    try:
                                        result_imagecli_placement_id.append(line['query_hash']['opxplacementid'])
                                    except:
                                        result_imagecli_placement_id.append(0)
                                    try:
                                        result_imagecli_opxip.append(line['query_hash']['opxip'])
                                    except:
                                        result_imagecli_opxip.append(0)
                            except Exception,e:
                                print e
                                continue
                except Exception,e:
                    print e
                    continue
        f.close()
        line_error=line_num-len(result_imageimp)-len(result_imagecli)        
        return result_imageimp,result_imagecli,line_num,line_error, result_imageimp_displayimage_id,result_imageimp_placement_id,result_imagecli_displayimage_id,result_imagecli_placement_id,result_imagecli_opxip
    

    def run(self):
     
        while True: 
            try:  
                num=self.queue.get()  
                if re.match(r'rtb\.HK\..*?',num):            
                    result_list_rtb,line_num,line_error=self.handerLog_RTB("/hkidc.hadoop.iclick/staging/tracking/incoming/"+num)
                    df=pd.DataFrame({'searchengine_id':dict((c, result_list_rtb.count(c)) for c in set(result_list_rtb)).keys(),'impressions':dict((c, result_list_rtb.count(c)) for c in set(result_list_rtb)).values()})
                    df['k_date']=handtime
                    df.to_sql('rtb_log_load_data_hk',conn, flavor="mysql", if_exists='append', index=False)
                    conn.commit()               
#             elif re.match(r'adgroup\.HK\..*?',num): 
#                 result_list_addgrop,result_list_addgroup_ip,line_num,line_error=self.handerLog_addgroup("/hkidc.hadoop.iclick/staging/tracking/incoming/"+num)      
#                 temp_list=zip(result_list_addgrop,result_list_addgroup_ip)
#                 result_list_addgrop=list(zip( *[(i,j) for i,j in temp_list if re.match("[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}",j)])[0])
#                 result_list_addgroup_ip=list(zip( *[(i,j) for i,j in temp_list if re.match("[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}",j)])[1])     
#                 score=self.check_ip(result_list_addgroup_ip)
#                 if score!=['']:
#                     df_temp=pd.DataFrame({'addgroup':result_list_addgrop,'check':[ j.split(',')[1] for j in score]})
#                     df_temp['check']=df_temp['check'].map(lambda x: 101 if x=='' else int(x))
#                     df_temp=df_temp[df_temp['check']>=50]
#                     result_list_addgroup_true=df_temp['addgroup'].tolist()
#                     df=pd.DataFrame({'searchengine_id':dict((c, result_list_addgrop.count(c)) for c in set(result_list_addgrop)).keys(),'clicks':dict((c, result_list_addgrop.count(c)) for c in set(result_list_addgrop)).values()})                
#                     df1=pd.DataFrame({'searchengine_id':dict((c, result_list_addgroup_true.count(c)) for c in set(result_list_addgroup_true)).keys(),'click_true':dict((c, result_list_addgroup_true.count(c)) for c in set(result_list_addgroup_true)).values()})
#                     df2=df.merge(df1,left_on='searchengine_id',right_on='searchengine_id',how='left')
#                     df2['click_true']=df2['click_true'].fillna(0)
#                     df2['k_date']=handtime
#                 else:
#                     print("HTTP Error 500: Internal Server Error") 
#                     df2=pd.DataFrame({'searchengine_id':dict((c, result_list_addgrop.count(c)) for c in set(result_list_addgrop)).keys(),'clicks':dict((c, result_list_addgrop.count(c)) for c in set(result_list_addgrop)).values()})                
#                     df2['click_true']=0
#                     df2['k_date']=handtime
#                 df2.to_sql('adgroup_log_load_data_hk',conn, flavor="mysql", if_exists='append', index=False)
#                 conn.commit()
                elif re.match(r'adgroup\.HK\..*?',num): 
                    result_list_addgrop,line_num,line_error=self.handerLog_addgroup("/hkidc.hadoop.iclick/staging/tracking/incoming/"+num)
                    if len(result_list_addgrop)>0:
#                         addgroup_filter=zip(result_list_addgrop,result_list_addgroup_fraud)
#                         result_list_addgrop_real=list(zip( *[(i,j) for i,j in addgroup_filter if str(j)=="0"])[0])
                        df_click_total=pd.DataFrame({'searchengine_id':dict((c, result_list_addgrop.count(c)) for c in set(result_list_addgrop)).keys(),'clicks':dict((c, result_list_addgrop.count(c)) for c in set(result_list_addgrop)).values()}) 
#                         df_click_true_total=pd.DataFrame({'searchengine_id':dict((c, result_list_addgrop_real.count(c)) for c in set(result_list_addgrop_real)).keys(),'click_true':dict((c, result_list_addgrop_real.count(c)) for c in set(result_list_addgrop_real)).values()})
#                         df_total=df_click_total.merge(df_click_true_total,left_on='searchengine_id',right_on='searchengine_id',how='left')
                        df_click_total['click_true']=df_click_total['clicks']
                        df_click_total['k_date']=handtime
                        df_click_total.to_sql('adgroup_log_load_data_hk',conn, flavor="mysql", if_exists='append', index=False)
                        conn.commit()
                    else:
                        pass
                elif re.match(r'image\.HK\..*?',num):
                    result_imageimp,result_imagecli,line_num,line_error, result_imageimp_displayimage_id,result_imageimp_placement_id,result_imagecli_displayimage_id,result_imagecli_placement_id,result_imagecli_opxip=self.handerLog_imageimpcli("/hkidc.hadoop.iclick/staging/tracking/incoming/"+num)
                    if len(result_imageimp)>0:
                        df=pd.DataFrame({'creative_id':result_imageimp,'displayimage_id':result_imageimp_displayimage_id,'placement_id':result_imageimp_placement_id})
                        df['impressions']=0
                        df['impressions']=df.groupby(['creative_id','displayimage_id','placement_id'])['impressions'].transform('count')
                        df.drop_duplicates(['creative_id','displayimage_id','placement_id'], inplace=True)
                        df['date']=handtime
                        df.to_sql('image_imp_log_load_data_hk',conn, flavor="mysql", if_exists='append', index=False)
                        conn.commit()
                    else:
                        pass
                    if len(result_imagecli)>0:                   
                        df_imagecli_toal=pd.DataFrame({'creative_id':result_imagecli,'displayimage_id':result_imagecli_displayimage_id,'placement_id':result_imagecli_placement_id})
                        df_imagecli_toal['clicks']=0
                        df_imagecli_toal['clicks']=df_imagecli_toal.groupby(['creative_id','displayimage_id','placement_id'])['clicks'].transform('count')
                        df_imagecli_toal.drop_duplicates(['creative_id','displayimage_id','placement_id'], inplace=True)            
                        total_true_image=zip(result_imagecli,result_imagecli_displayimage_id,result_imagecli_placement_id,result_imagecli_opxip)
                        zip_true_image=zip(*[(i,j,m,n) for i,j,m,n in total_true_image  if re.match("[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}",str(n))])
                        result_imagecli_true=list(zip_true_image[0])
                        result_imagecli_displayimage_id_true=list(zip_true_image[1])
                        result_imagecli_placement_id_true=list(zip_true_image[2])
                        result_imagecli_opxip_true=list(zip_true_image[3])
                        score=self.check_ip(result_imagecli_opxip_true)
                        if score!=['']:
                            check=[ j.split(',')[1] for j in score]
                            df_imagecli_true=pd.DataFrame({'creative_id':result_imagecli_true,'displayimage_id':result_imagecli_displayimage_id_true,'placement_id':result_imagecli_placement_id_true,'check':check})
                            df_imagecli_true['check']=df_imagecli_true['check'].map(lambda  x:101 if x=='' else int(x))
                            df_imagecli_true=df_imagecli_true[df_imagecli_true['check']>50]
                            df_imagecli_true.drop('check',axis=1,inplace=True)
                            df_imagecli_true['clicks_original']=0
                            df_imagecli_true['clicks_original']=df_imagecli_true.groupby(['creative_id','displayimage_id','placement_id'])['clicks_original'].transform('count')
                            df_imagecli_true.drop_duplicates(['creative_id','displayimage_id','placement_id'], inplace=True)
                        else:
                            print("HTTP Error 500: Internal Server Error")
                            df_imagecli_true=pd.DataFrame({'creative_id':result_imagecli_true,'displayimage_id':result_imagecli_displayimage_id_true,'placement_id':result_imagecli_placement_id_true})
                            df_imagecli_true.drop_duplicates(['creative_id','displayimage_id','placement_id'], inplace=True)
                            df_imagecli_true['clicks_original']=0
                        df=df_imagecli_toal.merge(df_imagecli_true,left_on=['creative_id','displayimage_id','placement_id'],right_on=['creative_id','displayimage_id','placement_id'],how='left')   
                        df['clicks_original']=df['clicks_original'].fillna(0)
                        df['date']=handtime
                        df.to_sql('image_cli_log_load_data_hk',conn, flavor="mysql", if_exists='append', index=False)
                        conn.commit()
                    else:
                        pass
                logname.append(num)
                lognum.append(line_num)
                logerror.append(line_error)
                print(num +" "+"has completed,Now time is :"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            except  Exception,e:
                print e
                print (num+" "+"has some errors  when handing")
                os.system("""python  /home/xmo/sendmail.py "Now the time is {0}"  "/home/xmo/hk_datahand.py has some errors when hand {1} , please check it."  """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),num))
                
                
            finally:
                self.queue.task_done()
start=time.time()
#获取当天的时间日期，day参数可以获取前后几天的日期
def main():
    for i  in range(10):
        t=ThreadNum(queue)
        t.setDaemon(True)
        t.start()
    
  
    path_rtbhk="/hkidc.hadoop.iclick/staging/tracking/incoming/rtb.HK."+handtime
    path_adgrouphk="/hkidc.hadoop.iclick/staging/tracking/incoming/adgroup.HK."+handtime
    path_imagehk="/hkidc.hadoop.iclick/staging/tracking/incoming/image.HK."+handtime
    df=pd.read_sql("select  logname from logname_hk where k_date=%s"%(handtime),conn)
  
    temp=df['logname'].tolist()

    path_rtbhk=glob.glob(path_rtbhk+"*.log")

    path_adgrouphk=glob.glob(path_adgrouphk+"*.log")
    path_imagehk=glob.glob(path_imagehk+"*.log")   
    last=[j.split('/')[-1]  for j in path_rtbhk if j.split('/')[-1] not in  temp]
    t3=[j.split('/')[-1]  for j in path_adgrouphk if j.split('/')[-1] not in  temp]
    t4=[j.split('/')[-1]  for j in path_imagehk if j.split('/')[-1]  not in temp]
    last.extend(t3)
    last.extend(t4)  
    if len(last)==0:
        print("No file needs to be processed,Now time is :"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        pass
    elif  len(last)>50:
        print("To deal with files:");print(last[0:50])
        for num in last[0:50]:
            queue.put(num)
            queue.join()
       
    else:
        print("To deal with files:");print(last)
        for num in last:    
            queue.put(num)
            queue.join()
           
        
            
if __name__=='__main__':
    main()
    df1=pd.DataFrame({'logname':logname,'lognum':lognum,'logerror':logerror})
    df1['k_date']=handtime
    df1.to_sql('logname_load_hk',conn, flavor="mysql", if_exists='append', index=False)
    conn.commit()        
    print"Elapsed Time: %s" %(time.time() - start)  
    print("sucess,Now time is:"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
