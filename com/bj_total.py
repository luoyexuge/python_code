#!/usr/bin/python
#encoding:utf-8
'''
Created on 2015年12月14日

@author: wilson.zhou
'''
import pymysql
import pandas  as pd
import datetime
import os
print("Start processing file，Now time is :"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
# conn=pymysql.connect(host="10.10.10.77",user="usr_dba",passwd="4rfv%TGB^YHN",db="xmo_summaries",use_unicode=True, charset="utf8")
# cur=conn.cursor()
SysError=1
def  gaincurrenttime1(day):
    time=(datetime.datetime.now()+datetime.timedelta(days=day)).strftime("%Y%m%d")
    return time
global  handtime1
handtime1=gaincurrenttime1(0)
logtable="""insert into  logname_bj(k_date,logname,lognum,logerror) SELECT k_date,logname,lognum,logerror from  logname_load_bj  b where b.k_date ={0};""".format(handtime1)


banner_imp="""insert into search_market_conversions_summary_bj(searchengine_id,k_date,impressions) 
select searchengine_id,k_date,impressions from (select  searchengine_id, k_date,sum(impressions) as impressions from rtb_log_load_data_bj where k_date={0} group by searchengine_id,k_date)  b 
on duplicate key update search_market_conversions_summary_bj.impressions=ifnull(search_market_conversions_summary_bj.impressions,0)+ifnull(b.impressions,0);""".format(handtime1) 


banner_click="""insert into search_market_conversions_summary_bj(searchengine_id,k_date,clicks ,click_true ) 
select searchengine_id,k_date,clicks ,click_true from (select  searchengine_id, k_date,sum(clicks) as clicks ,sum(click_true) as click_true  from adgroup_log_load_data_bj   
 where k_date={0} group by searchengine_id,k_date) b on duplicate key update search_market_conversions_summary_bj.clicks=ifnull(search_market_conversions_summary_bj.clicks,0)+ifnull(b.clicks,0), 
search_market_conversions_summary_bj.click_true=ifnull(search_market_conversions_summary_bj.click_true,0)+ifnull(b.click_true,0);""".format(handtime1)


otv_imp=""" insert imageviews_report_summary_core_bj(creative_id,date,impressions,displayimage_id,placement_id) select creative_id,date,impressions ,displayimage_id,placement_id from (select  creative_id, date,displayimage_id,placement_id ,sum( impressions) as impressions from image_imp_log_load_data_bj 
 where date={0} group by creative_id,date,displayimage_id,placement_id) b on duplicate key update imageviews_report_summary_core_bj.impressions=ifnull(imageviews_report_summary_core_bj.impressions,0)+ifnull(b.impressions,0);""".format(handtime1)


otv_click="""insert imageviews_report_summary_core_bj(creative_id,date,clicks,displayimage_id,placement_id ,clicks_original) select creative_id,date,clicks ,displayimage_id,placement_id ,clicks_original from (select  creative_id, date,displayimage_id,placement_id ,sum(clicks) as clicks  ,sum(clicks_original) as 
clicks_original from image_cli_log_load_data_bj where date={0} group by creative_id,date,displayimage_id,placement_id ) b  
on duplicate key update imageviews_report_summary_core_bj.clicks=ifnull(imageviews_report_summary_core_bj.clicks,0)+ifnull(b.clicks,0),
imageviews_report_summary_core_bj.clicks_original=ifnull(imageviews_report_summary_core_bj.clicks_original,0)+ifnull(b.clicks_original,0);""".format(handtime1)


if  os.path.exists('/home/xmo/bj_datahand.py.lck'):
    print('the  program  is running')
    os.system("""python  /home/xmo/sendmail.py  " BJ python program is running,Now the time is {0}"  "Please try again later" """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    pass

else:
    os.system("ln -s /home/xmo/bj_datahand.py  /home/xmo/bj_datahand.py.lck")
    try:
#         conn=pymysql.connect(host="10.10.10.77",user="usr_dba",passwd="4rfv%TGB^YHN",db="xmo_summaries",use_unicode=True, charset="utf8")
        conn=pymysql.connect(host="10.10.10.152",user="usr_sync",passwd="^YGH*aJdH2TS134tgb",db="xmo_summaries_sync",use_unicode=True, charset="utf8")
        cur=conn.cursor()
        cur.execute("delete from logname_load_bj where k_date={0};".format(handtime1))
        conn.commit()
        cur.execute("delete from adgroup_log_load_data_bj where k_date={0};".format(handtime1))
        conn.commit()
        cur.execute("delete from image_cli_log_load_data_bj  where date={0};".format(handtime1))
        conn.commit()
        cur.execute("delete from image_imp_log_load_data_bj where date={0};".format(handtime1))
        conn.commit()
        cur.execute("delete from rtb_log_load_data_bj  where k_date={0};".format(handtime1))
        conn.commit()
        execfile('/home/xmo/bj_datahand.py')

    except  Exception, e:
        print e
        SysError=SysError+1
        os.system("unlink  /home/xmo/bj_datahand.py.lck")
    finally:
        if SysError==1:
            
    
#     banner数据插入更新
            try:
                cur.execute(banner_click)
                
                cur.execute(banner_imp)
#                 conn.commit()
#             except Exception,e:
#                 print e
#     otv数据插入更新
#             try:
                cur.execute(otv_imp)
                conn.commit()
#             except Exception,e:
#                 print e
#             try:
                cur.execute(otv_click)
#                 conn.commit()
#             except:
#                 print e
#             try:
                cur.execute(logtable)
#                 conn.commit()
#             except Exception,e:
#                 print e
#       banner数据备份
#             try:
                cur.execute("""insert into rtb_log_load_data_history_bj(searchengine_id,k_date,impressions,updatetime) select searchengine_id,k_date,impressions,DATE_FORMAT(now(),"%Y%m%d %H:%i:%s") as updatetime from(select  searchengine_id, k_date,sum(impressions) as impressions from rtb_log_load_data_bj   
 where k_date={0} group by searchengine_id,k_date)  b """.format(handtime1))
#                 conn.commit()
#             except Exception,e:
#                 print e
#             try:
                cur.execute("""INSERT into adgroup_log_load_data_history_bj(searchengine_id,k_date,clicks ,click_true,updatetime) select searchengine_id,k_date,clicks ,click_true,DATE_FORMAT(now(),"%Y%m%d %H:%i:%s") as updatetime  from  
 (select  searchengine_id, k_date,sum(clicks) as clicks ,sum(click_true) as click_true from adgroup_log_load_data_bj 
 where k_date={0} group by searchengine_id,k_date) b;""".format(handtime1))
#                 conn.commit()
#             except:
#                 print e

#       otv数据备份
#             try:
                cur.execute("""insert into image_cli_log_load_data_history_bj(creative_id,date,clicks ,displayimage_id,placement_id ,clicks_original,updatetime)  select  creative_id,date,clicks ,displayimage_id,placement_id ,clicks_original,DATE_FORMAT(now(),"%Y%m%d %H:%i:%s") as updatetime from (select  creative_id, date,displayimage_id,placement_id ,sum(clicks) as clicks ,sum(clicks_original) as clicks_original
                  from image_cli_log_load_data_bj where date={0} group by creative_id,date,displayimage_id,placement_id ) b """.format(handtime1))
#                 conn.commit()
#             except Exception,e:
#                 print e
#             try:
                cur.execute("""insert into image_imp_log_load_data_history_bj(creative_id,date,impressions,displayimage_id,placement_id ,updatetime) select creative_id,date,impressions,displayimage_id,placement_id ,DATE_FORMAT(now(),"%Y%m%d %H:%i:%s") as updatetime  from (select  creative_id, date,sum( impressions) as impressions ,displayimage_id,placement_id  from image_imp_log_load_data_bj
 where date={0} group by creative_id,date,displayimage_id,placement_id ) b""".format(handtime1))
                conn.commit()
            except Exception,e:
                print e
                conn.rollback()
    try:
        os.system("unlink  /home/xmo/bj_datahand.py.lck")
    except:
        pass
    if SysError==1:
       
        print("There is no error,Now time is :"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        try:
            os.system("""python  /home/xmo/sendmail.py "BJ PYTHON  ERROR ,Now the time is {0}"  "BJ scheduler has an error in  python program, please check it."  """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        except:
            print("mail has some errors")
        print("Please check for errors,Now time is :"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    conn.close()
