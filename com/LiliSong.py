#!/usr/bin/python
#encoding:utf-8
'''
Created on 2016年3月9日

@author: wilson.zhou
'''
import smtplib
import email.MIMEMultipart# import MIMEMultipart
import email.MIMEText# import MIMEText
import email.MIMEBase# import MIMEBase
import os.path



# 需要的字段：两边客户id，两边客户名称，booking的品牌，客户状态，
# 客户渠道来源，客户联系人，
# 两边业务id（booking的orderid，xmo的campaignid、orderid）


# -- 两边数据核对的上的 
# 
# select  total.* ,clients.clientname as xmo_clientname,clients.clientcontact as xmo_clientcontact from (
#   select c.booking_client_id ,c.booking_name, c.linkman_name,c.state ,c.brand,d.id  as  campaign_id  ,d.client_id as xmo_client_id,c.channel,
# c.code  as booking_orderid,  d.order_code  as  xmo_orderid
#  from   
# (select a.id as booking_client_id ,a.name as booking_name, a.linkman_name, a.channel,a.state,a.brand,b.code from  sales_booking_production.clients  a 
# 
# INNER JOIN   sales_booking_production.orders b
# on  a.id=b.client_id   )
#  c INNER JOIN xmo.campaigns  d    on c.`code`=d.order_code  where  d.order_code!='')
# total  inner join  xmo.clients clients  on  total.xmo_client_id=clients.id;
# 
# 
# -- booking数据有  xmo没有的
# 
# 
# select a.id as booking_client_id ,a.name as booking_name, a.linkman_name, a.channel,a.state,a.brand,b.code from  sales_booking_production.clients  a 
# INNER JOIN   sales_booking_production.orders b
# on  a.id=b.client_id   where b.code   not in (select order_code  from xmo.campaigns   where  order_code!='')
# 
# 
# 
# 
# 
# --  xmo有  booking没有的
# 
# 
# select  DISTINCT xmo_client_id,xmo_clientname, xmo_clientcontact from 
# 
#  (select  d.client_id as xmo_client_id,d.order_code  as  xmo_orderid ,
# clients.clientname as xmo_clientname,clients.clientcontact as xmo_clientcontact from xmo.campaigns d inner join  
# xmo.clients clients  on  d.client_id=clients.id  where    clients.clientname!='Benchmark Test'  and
# d.order_code  not in (select  code from  sales_booking_production.orders where  code!='')  ) total
#  ;


# -- 两边数据核对的上的 
# 
/* select  total.* ,clients.clientname as xmo_clientname,clients.clientcontact as xmo_clientcontact from (
  
select c.booking_client_id ,c.booking_name, c.linkman_name,c.state ,c.brand,d.id  as  campaign_id  ,d.client_id as xmo_client_id,c.channel,
c.code  as booking_orderid,  d.order_code  as  xmo_orderid
 from   
(select a.id as booking_client_id ,a.name as booking_name, a.linkman_name, a.channel,a.state,a.brand,b.code from  sales_booking_production.clients  a 
INNER JOIN   sales_booking_production.orders b 
on  a.id=b.client_id  where a.state="approved"   and substr(b.created_at,1,10)>'2015-01-01'
 )
 c INNER JOIN xmo.campaigns  d    on c.`code`=d.order_code  where  d.order_code!='')
total  inner join  xmo.clients clients  on  total.xmo_client_id=clients.id;



# -- booking数据有  xmo没有的
# 
# 
 select a.id as booking_client_id ,a.name as booking_name, a.linkman_name, a.channel,a.state,a.brand,b.code from  sales_booking_production.clients  a 
INNER JOIN   sales_booking_production.orders b
on  a.id=b.client_id   where b.code   not in (select order_code  from xmo.campaigns   where  order_code!='')  and a.state="approved"   and substr(b.created_at,1,10)>'2015-01-01';
 

  -- 
select  DISTINCT xmo_client_id,xmo_clientname, xmo_clientcontact from 
(select  d.client_id as xmo_client_id,d.order_code  as  xmo_orderid ,
clients.clientname as xmo_clientname,clients.clientcontact as xmo_clientcontact from xmo.campaigns d inner join  
 xmo.clients clients  on  d.client_id=clients.id  where    clients.clientname!='Benchmark Test'  and
 d.order_code  not in (select  code from  sales_booking_production.orders where  code!='')
and  substr(d.created_at,1,10)>'2015-01-01'
  ) total
  ;*/





import  pymysql
import pandas  as pd

conn=pymysql.connect(host="10.1.1.130",user="usr_sync",passwd="^YGH*aJdH2TS134tgb",db="xmo_summaries_sync",use_unicode=True, charset="utf8")
# cur=conn.cursor()

lilisql="""select  total.* ,clients.clientname as xmo_clientname,clients.clientcontact as xmo_clientcontact from (
  select c.booking_client_id ,c.booking_name, c.linkman_name,c.state ,c.brand,d.id  as  campaign_id  ,d.client_id as xmo_client_id,c.channel,
c.code  as booking_orderid,  d.order_code  as  xmo_orderid
 from   
(select a.id as booking_client_id ,a.name as booking_name, a.linkman_name, a.channel,a.state,a.brand,b.code from  sales_booking_production.clients  a 

INNER JOIN   sales_booking_production.orders b
on  a.id=b.client_id   )
 c INNER JOIN xmo.campaigns  d    on c.`code`=d.order_code  where  d.order_code!='')
total  inner join  xmo.clients clients  on  total.xmo_client_id=clients.id;"""

df=pd.read_sql(lilisql,conn)
df.to_excel("d:\\test.xlsx",index=False)
conn.close()

def  sendmailwithattach(file_name,attach):
    From ="wilson.zhou@i-click.com"
    To = "lily.song@i-click.com;dba@i-click.com"
    port=587
    file_name =file_name #附件名
    
#     server = smtplib.SMTP("smtp.i-click.com",587)
#     

    server = smtplib.SMTP()
    server.connect("smtp.i-click.com",port)
    server.login("wilson.zhou@i-click.com","wilsonzhou1119") #仅smtp服务器需要验证时
    
    # 构造MIMEMultipart对象做为根容器
    main_msg = email.MIMEMultipart.MIMEMultipart()
    # 构造MIMEText对象做为邮件显示内容并附加到根容器
    text_msg = email.MIMEText.MIMEText("Attachments are the results you want.",_charset="utf-8")
    main_msg.attach(text_msg)
    
    # 构造MIMEBase对象做为文件附件内容并附加到根容器
    contype = 'application/octet-stream'
    maintype, subtype = contype.split('/', 1)
    
    ## 读入文件内容并格式化 [方式2]－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－
    data = open(file_name, 'rb')
    file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg.set_payload(data.read())
    data.close( )
    email.Encoders.encode_base64(file_msg)#把附件编码
    ## 设置附件头
    basename = os.path.basename(file_name)
    file_msg.add_header('Content-Disposition','attachment', filename = basename)#修改邮件头
    main_msg.attach(file_msg)
    
    # 设置根容器属性
    main_msg['From'] = From
    main_msg['To'] = To
    main_msg['Subject'] =attach
    main_msg['Date'] = email.Utils.formatdate()
    # 得到格式化后的完整文本
    fullText = main_msg.as_string( )
    
    # 用smtp发送邮件
    try:
        server.sendmail(From, To, fullText)
    finally:
        server.quit()
if  __name__=='__main__':
    print(u"数据库读完,正在开始发邮件，请稍等：")
    sendmailwithattach("d:\\test.xlsx","please check  the attach ,please feel free to contact wilson if you have any questions")
    print(u"邮件发完")
    