#!/usr/bin/python

import time,os
USER="wilson.zhou"
PWD="YCt452uz"
URL="jdbc:postgresql://10.1.1.230:5432/xmo_dw"

day = time.strftime('%Y-%m-%d',time.localtime(time.time() -24*60*60))[2::]

sql = '''"insert overwrite directory '/shortdata/media_planner/{0}/'
SELECT req.adx,req.city,req.d,req.url,req.adformat,req.size,count(req.bid),count(show.bid),count(click.bid),sum(win.price)
FROM( 
    SELECT concat_ws('','20',a.ds) d,b.adx,b.bid,b.city,
           parse_url(b.url,'HOST') url,
           CASE b.video
               WHEN 'false' THEN 'Banner'
               ELSE 'Rich Media'
           END adformat,
           concat_ws('x',c.w,c.h) SIZE
   FROM bs_rtbreq a 
   LATERAL VIEW json_tuple(a.str, 'adx','bid','city','url','slots','video') b AS adx, bid,city,url,slots,video 
   LATERAL VIEW json_tuple(substring(b.slots,2,length(b.slots)-2),'h','w') c AS h ,w
   WHERE a.ds='{0}' AND parse_url(b.url,'HOST') rlike '{1}'
   UNION ALL 
    SELECT concat_ws('','20',ds) d,
                    get_json_object(str,'$.adx') AS adx,
                    get_json_object(str,'$.bid') AS bid,
                    get_json_object(str,'$.city') AS city,
                    parse_url(get_json_object(str,'$.url'),'HOST') AS url,
                    CASE get_json_object(str,'$.video')
                        WHEN 'false' THEN 'Banner'
                        ELSE 'Rich Media'
                    END adformat,
                    concat_ws('x',get_json_object(str,'$.slots[0].w'),get_json_object(str,'$.slots[0].h')) SIZE
    FROM bs_rtbreq_tanx
    WHERE parse_url(get_json_object(str,'$.url'),'HOST') rlike '{1}' AND ds='{0}' ) req
LEFT JOIN
  ( SELECT a.ds,b.bid,b.price
    FROM bs_rtbwinner a 
    LATERAL VIEW json_tuple(a.str, 'bid','price') b AS bid,price
    WHERE a.ds='{0}'
   ) win ON req.bid = win.bid
LEFT JOIN
  ( SELECT a.ds,b.bid
   FROM bs_showup a 
   LATERAL VIEW json_tuple(a.str,'bid') b AS bid
   WHERE a.ds='{0}' 
   ) SHOW ON win.bid = SHOW.bid
LEFT JOIN
  ( SELECT a.ds,b.bid
   FROM bs_click a 
   LATERAL VIEW json_tuple(a.str, 'bid') b AS bid
   WHERE a.ds='{0}' 
   ) click ON SHOW.bid = click.bid
GROUP BY req.adx,req.city,req.d,req.url,req.adformat,req.SIZE
"'''.format(day,'^[a-zA-Z0-9\-\.\_]+\.[a-zA-Z]{2,3}(/\S*)?\.?$','','','','','','')


cmd = "hive -e "+sql

print(cmd)

os.system(cmd)

print("hive execute end")
print("sqoop execute start...")


try:
    sline = "sqoop export --connect {0} --username {1} --password {2} --export-dir /shortdata/media_planner/{3}  --table media_planner --fields-terminated-by '\001'  --input-null-non-string '\\\N' --input-null-string '\\\N'  --columns adx,city,ds,url,adformat,size,pv,impression,click,expense".format(URL,USER,PWD,day)

    print("sqoop cmd="+sline)
    os.system(sline)
except:
    print("error line")

print("all end")
