#!/usr/bin/python
#encoding:utf-8
'''
Created on 2015年12年30日
@author: wilson.zhou
'''

import  pymysql
import datetime
import os

def  gaincurrenttime(day):
    time=(datetime.datetime.now()+datetime.timedelta(days=day)).strftime("%Y%m%d")
    return time
global  handtime
global handtime_fivedays_ago
handtime=gaincurrenttime(0)
handtime_fivedays_ago=gaincurrenttime(-5)

sysrror=1

delete_sql="""delete  from  xmo_summaries.imageviews_report_summary_core_realtime  where date<={0} and  date>={1};""".format(handtime,handtime_fivedays_ago)
 
insert_sql="""insert into xmo_summaries.imageviews_report_summary_core_realtime( date,date_i,campaign_id,creative_id,displayimage_id,placement_id,impressions
,clicks,impressions_uv,clicks_uv,clicks_original)
select date,date_i,campaign_id,creative_id,displayimage_id,placement_id,impressions
,clicks,impressions_uv,clicks_uv,clicks_original  from (select  date,DATE_FORMAT(date,"%Y%m%d") as date_i,campaign_id,creative_id,displayimage_id,placement_id
, sum(impressions) as impressions ,sum(clicks) as clicks ,impressions_uv,clicks_uv,
sum(clicks_original) as clicks_original  from (SELECT a.date,a.date_i,b.campaign_id,a.creative_id,a.displayimage_id,a.placement_id,
        a.impressions,a.clicks,a.impressions_uv,a.clicks_uv,a.clicks_original FROM imageviews_report_summary_core_bj a LEFT JOIN xmo.creatives b ON a.creative_id = b.id
 where a.date<={0} and  a.date>={1}  and a.creative_id>0
UNION select a.date,a.date_i,b.campaign_id,a.creative_id,a.displayimage_id,a.placement_id,a.impressions,
        a.clicks,a.impressions_uv,a.clicks_uv,a.clicks_original from imageviews_report_summary_core_hk a LEFT JOIN xmo.creatives b ON a.creative_id = b.id
 where a.date<={0} and  a.date>={1}  and a.creative_id>0 ) f group  by date,creative_id,displayimage_id,placement_id) d
 on duplicate key update  xmo_summaries.imageviews_report_summary_core_realtime.impressions=ifnull(d.impressions,0),
xmo_summaries.imageviews_report_summary_core_realtime.clicks=ifnull(d.clicks,0),
xmo_summaries.imageviews_report_summary_core_realtime.clicks_original=ifnull(d.clicks_original,0);""".format(handtime,handtime_fivedays_ago)


if  os.path.exists('/home/xmo/otv_report.py.lck'):
        print("the programe is running")
        os.system("""python  /home/xmo/sendmail.py  " otv_report program is running,Now the time is {0}"  "Please try again later" """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        pass
else:
    os.system("ln -s /home/xmo/otv_report.py  /home/xmo/otv_report.py.lck")
    try:
        conn=pymysql.connect(host="10.1.1.130",user="usr_sync",passwd="^YGH*aJdH2TS134tgb",db="xmo_summaries_sync",use_unicode=True, charset="utf8")
        
        cur=conn.cursor()
        cur.execute(delete_sql)
        cur.execute(insert_sql)
        conn.commit()
        
    except  Exception,e:
        conn.rollback()
        print e
        sysrror=sysrror+1
#       os.system("unlink  /home/xmo/otv_report.py.lck")  
    finally:
        
        if sysrror>1:
            print("otv_reportt programe has some errors ,please check it")
            try:
                os.system("""python  /home/xmo/sendmail.py "otv_report program has some ERRORS ,Now the time is {0}"  "otv_report programe has an error in  python program, please check it."  """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            except:
                print("mail has some errors")
        else:
            print("otv_report programe sucess:"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
conn.close()            
os.system("unlink  /home/xmo/otv_report.py.lck")

