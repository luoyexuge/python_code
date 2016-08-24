#!/usr/bin/python

import time,os
USER="wilson.zhou"
PWD="YCt452uz"
URL="jdbc:postgresql://10.1.1.230:5432/xmo_dw"

day = time.strftime('%Y-%m-%d',time.localtime(time.time() -24*60*60))[2::]

tanx = '''insert overwrite directory '/shortdata/media_planner/{0}_tanx/'
select show.adx,ip2city(show.ip),show.d,show.url,show.size,count(show.bid),count(click.bid),sum(win.price)
FROM
(SELECT a.ds,b.bid,b.price
 FROM bs_rtbwinner a
 LATERAL VIEW json_tuple(a.str, 'bid','price') b AS bid,price
 WHERE a.ds='{0}' and split(b.bid,'_')[0]='tanx'
) win
left join
(
 select concat_ws('','20',a.ds) d,b.bid,split(b.bid,'_')[0] as adx,b.ip,parse_url(b.url,'HOST') url,concat_ws('x',c.w,c.h) SIZE
 from bs_showup a
 lateral view json_tuple(a.str, 'bid','ip','url','slotInfo') b as bid,ip,url,slotInfo
 lateral view json_tuple(b.slotInfo,'h','w') c as h ,w
 where a.ds='{0}' and split(b.bid,'_')[0]='tanx' and parse_url(b.url,'HOST') rlike '{3}'
) show
on win.bid = show.bid
left join
( 
 SELECT a.ds,b.bid
 FROM bs_click a
 LATERAL VIEW json_tuple(a.str, 'bid') b AS bid
 WHERE a.ds='{0}' and split(b.bid,'_')[0]='tanx'
)as click
ON SHOW.bid = click.bid
group by adx,ip,d,url,size"'''.format(day,day,day,'^[a-zA-Z0-9\-\.\_]+\.[a-zA-Z]{2,3}(/\S*)?\.?$')

tanx_cmd = "hive -e \"add jar /opt/pig_home/Pig_script/media/iclick-hive-udf-0.0.1-SNAPSHOT.jar; create temporary function ip2city as 'com.iclick.hive.udf.getCityByIp'; "+tanx

print(tanx_cmd)

os.system(tanx_cmd)

try:
    tanxline = "sqoop export --connect {0} --username {1} --password {2} --export-dir /shortdata/media_planner/{3}_tanx  --table media_planner_tanx --fields-terminated-by '\001'  --input-null-non-string '\\\N' --input-null-string '\\\N'  --columns adx,city,ds,url,size,impression,click,expense".format(URL,USER,PWD,day)

    print("sqoop cmd="+tanxline)
    os.system(tanxline)
except:
    print("insert database failure")

