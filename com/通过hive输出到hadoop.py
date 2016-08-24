#!/usr/bin/python

import time,os

day = time.strftime('%Y-%m-%d',time.localtime(time.time() -24*60*60))[2::]

#ROW FORMAT DELIMITED
#FIELDS TERMINATED BY ',' 
sql = '''"insert overwrite directory '/shortdata/rtb_banner/{0}/'
SELECT 
 a.ds,
 a.h,
 a.w,
 parse_url(a.url,'HOST'),
 a.city,
 count(a.bid) pv,
 count(distinct(a.c)) uv,
 count(c.bid) impression ,
 count(b.bid) click,
 a.p,
 sum(d.price)
FROM 
 (select a.ds, b.bid,b.city,b.p,b.url,b.c,c.h,c.w 
  from bs_rtbreq a 
  lateral view json_tuple(a.str, 'bid','city','p','url','c','slots','video') b as bid,city,p,url,c,slots,video 
  lateral view json_tuple(substring(b.slots,2,length(b.slots)-2),'h','w') c as h ,w 
  where b.video='false' and a.ds='{1}'
 ) a
 LEFT JOIN 
 (select a.ds, b.* 
 from bs_showup a 
 lateral view json_tuple(a.str, 'bid','browser','camp','date','device','ip','os') b as bid,bs,camp,date,device,ip,os
 where a.ds='{2}'
 ) c
ON a.bid = c.bid
LEFT JOIN 
 (select a.ds, b.* 
  from bs_click a 
  lateral view json_tuple(a.str, 'bid','browser','camp','date','device','ip','os') b as bid,bs,camp,date,device,ip,os
  where a.ds='{3}'
 ) b
 ON c.bid = b.bid
 left join
 (
 select a.ds, b.* 
 from bs_rtbwinner a 
 lateral view json_tuple(a.str, 'bid','price') b as bid,price
 where a.ds='{4}'
 ) d
 on a.bid = d.bid 
 GROUP BY a.p,a.ds,a.city,parse_url(a.url,'HOST'),a.h,a.w"'''.format(day,day,day,day,day)

#print(sql)

cmd = "hive -e "+sql

print(cmd)

os.system(cmd)
