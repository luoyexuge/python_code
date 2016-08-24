# coding:utf-8
'''
Created :2016-01-27
@author: mask
@note:1.http://www.dcharm.com/?p=13
'''
import pandas as pd
from  datetime import  datetime
import pymysql
#from .compat import u

import  urllib2
import re

# con=pymysql.connect(host="127.0.0.1",port=3306,user="root",passwd="1234",db="test",use_unicode=True, charset="utf8")
for i in range(1,20):
    urlxx="http://www.lgfdcw.com/cs/index.php?userid=&infotype=&dq=&fwtype=&hx=&price01=&price02=&pricetype=&fabuday=&addr=&PageNo="+str(3)
    response = urllib2.urlopen(urlxx)
    html = str(response.read()).decode('gbk')
    blank=re.findall("""target\="_blank"><strong>(.*?)<\/strong><\/a><\/td>""",html)
    dq=re.findall("""href=\?dq=0>(.*?)</a></td>""",html)
    fwtype=re.findall("""href=\?fwtype=0>(.*?)</a></td>""",html)
    hx=re.findall("""href=\?hx=.*?>(.*?)</a></td""",html)
    m2= re.findall("""<td>([0-9]{1,}).*?</td>""",html)
    price=re.findall("""<td><font color="#FF0000">\r\n              ([0-9]{1,})*.?""",html)
    date=re.findall("""<td>\[([0-9]{1,}-[0-9]{1,})\]</td>""",html)

    data = {'v_blank':blank,'v_dq':dq,'v_fwtype':fwtype,'v_hx':hx,'v_m2':m2,'v_price':price,'v_date':date}
    frame =pd.DataFrame(data)
    frame1=frame[frame.v_blank!=""]
    print i
    print frame1
#     frame1.to_sql("frame1", con, flavor="mysql", if_exists='append', index=False)
#     con.commit()
# con.close()
print("success")
