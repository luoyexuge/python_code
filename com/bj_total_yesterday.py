#encoding:utf-8
'''
Created on 2015��12��14��

@author: wilson.zhou
'''
import pymysql
import pandas  as pd
import datetime
import os
# conn=pymysql.connect(host="10.10.10.77",user="usr_dba",passwd="4rfv%TGB^YHN",db="xmo_summaries",use_unicode=True, charset="utf8")
# cur=conn.cursor()
SysError=1
def  gaincurrenttime1(day):
    time=(datetime.datetime.now()+datetime.timedelta(days=day)).strftime("%Y%m%d")
    return time
global  handtime1
handtime1=gaincurrenttime1(-1)
logtable="""insert into  xmo_summaries.LOGNAME   SELECT * from  xmo_summaries.LOGNAME_load  b where b.k_date ={0};""".format(handtime1)


banner_imp="""insert into search_market_conversions_summary_bj(searchengine_id,k_date,impressions) 
select searchengine_id,k_date,impressions from (select  searchengine_id, k_date,sum(impressions) as impressions from rtb where k_date={0} group by searchengine_id,k_date)  b 
on duplicate key update search_market_conversions_summary_bj.impressions=search_market_conversions_summary_bj.impressions+b.impressions;""".format(handtime1) 


banner_click="""insert into search_market_conversions_summary_bj(searchengine_id,k_date,clicks ,click_true ) 
select searchengine_id,k_date,clicks ,click_true from (select  searchengine_id, k_date,sum(clicks) as clicks ,sum(click_true) as click_true  from addgroup   
 where k_date={0} group by searchengine_id,k_date) b on duplicate key update search_market_conversions_summary_bj.clicks=search_market_conversions_summary_bj.clicks+b.clicks, 
search_market_conversions_summary_bj.click_true=search_market_conversions_summary_bj.click_true+b.click_true;""".format(handtime1)


otv_imp=""" insert imageviews_report_summary_core_bj(creative_id,date,impressions) select creative_id,date,impressions  from (select  creative_id, date,sum( impressions) as impressions  from imageBJ2 
 where date={0} group by creative_id,date) b on duplicate key update imageviews_report_summary_core_bj.impressions=imageviews_report_summary_core_bj.impressions+b.impressions;""".format(handtime1)


otv_click="""insert imageviews_report_summary_core_bj(creative_id,date,clicks) select creative_id,date,clicks  from (select  creative_id, date,sum(clicks) as clicks   from imageBJ1 
 where date={0} group by creative_id,date) b  on duplicate key update imageviews_report_summary_core_bj.clicks=ifnull(imageviews_report_summary_core_bj.clicks,0)+ifnull(b.clicks,0);""".format(handtime1)


if  os.path.exists('/home/xmo/yesterday/lastlinux_yesterday.py.lck'):
    print('the  program  is running')
    pass

else:
    os.system("ln -s /home/xmo/yesterday/lastlinux_yesterday.py  /home/xmo/yesterday/lastlinux_yesterday.py.lck")
    try:
        conn=pymysql.connect(host="10.10.10.77",user="usr_dba",passwd="4rfv%TGB^YHN",db="xmo_summaries",use_unicode=True, charset="utf8")
        cur=conn.cursor()
        cur.execute("""delete from LOGNAME_load  where k_date={0}""".format(handtime1))
        conn.commit()
        cur.execute("""delete from addgroup  where k_date={0}""".format(handtime1))
        conn.commit()
        cur.execute("""delete from imageBJ1 where date={0}""".format(handtime1))
        conn.commit()
        cur.execute("""delete from imageBJ2 where date={0}""".format(handtime1))
        conn.commit()
        cur.execute("""delete from rtb where k_date={0} """.format(handtime1))
        conn.commit()
        execfile('/home/xmo/yesterday/lastlinux_yesterday.py')
    
    except  Exception, e:
        print e
        SysError=SysError+1
        os.system("unlink  /home/xmo/yesterday/lastlinux_yesterday.py.lck")
    finally:
        if SysError==1:
            try:
                cur.execute(logtable)
                conn.commit()
            except Exception,e:
                print e
    
#     banner��ݲ������
            try:
                cur.execute(banner_click)
                conn.commit()
            except Exception,e:
                print e
            try:  
                cur.execute(banner_imp)
                conn.commit()
            except Exception,e:
                print e
#     otv��ݲ������
            try:
                cur.execute(otv_imp)
                conn.commit()
            except Exception,e:
                print e
            try:
                cur.execute(otv_click)
                conn.commit()
            except:
                print e
#       banner��ݱ���
            try:
                cur.execute("""insert into rtb_back(searchengine_id,k_date,impressions,updatetime) select searchengine_id,k_date,impressions,DATE_FORMAT(now(),"%Y%m%d %H:%i:%s") as updatetime from(select  searchengine_id, k_date,sum(impressions) as impressions from rtb   
 where k_date={0} group by searchengine_id,k_date)  b """.format(handtime1))
                conn.commit()
            except Exception,e:
                print e
            try:
                cur.execute("""INSERT into addgroup_back(searchengine_id,k_date,clicks ,click_true,updatetime) select searchengine_id,k_date,clicks ,click_true,DATE_FORMAT(now(),"%Y%m%d %H:%i:%s") as updatetime  from  
 (select  searchengine_id, k_date,sum(clicks) as clicks ,sum(click_true) as click_true from addgroup   
 where k_date={0} group by searchengine_id,k_date) b;""".format(handtime1))
                conn.commit()
            except:
                print e

#       otv��ݱ���
            try:
                cur.execute("""insert into imageBJ1_back(creative_id,date,clicks ,updatetime)  select  creative_id,date,clicks ,DATE_FORMAT(now(),"%Y%m%d %H:%i:%s") as updatetime from (select  creative_id, date,sum(clicks) as clicks   from imageBJ1 
 where date={0} group by creative_id,date) b """.format(handtime1))
                conn.commit()
            except Exception,e:
                print e
            try:
                cur.execute("""insert into imageBJ2_back(creative_id,date,impressions,updatetime) select creative_id,date,impressions,DATE_FORMAT(now(),"%Y%m%d %H:%i:%s") as updatetime  from (select  creative_id, date,sum( impressions) as   impressions  from imageBJ2 
 where date={0} group by creative_id,date) b""".format(handtime1))
                conn.commit()
            except:
                print e
    try:
        os.system("unlink  /home/xmo/yesterday/lastlinux_yesterday.py.lck")
    except:
        pass
    print('i want to sucess')
    print SysError
    conn.close()