#encoding:utf-8
'''
Created on 2016年8月19日

@author: wilson.zhou
'''
import urllib2
import  re
import  requests
import sys

headers= {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',  
'cookie': '0033WrSXqPxfM725Ws9jqgMF55529P9D9WWgBq7je56FPUGZJ13Xe7MW5JpX5o2p5NHD95QEeK5fS0Bc1KnfWs4Dqcj-i--ciK.Ni-82i--ciK.Ni-829c-t'} 
count=0
page=12
while page>11:
    req=urllib2.Request("http://weibo.com/lumengling?topnav=1&wvr=6&topsug=1&is_all="+str(page),headers=headers)

    r=urllib2.urlopen(req)
    data=r.read().decode("gbk")
    print data
    p=re.compile((r'src=\"http\:\/\/ww(.)\.sinaimg\.cn\/.*?/(.*?)\.jpg\"'))
    uuids=p.findall(data)
    urls=[]
    print(uuids)
    for uuid in uuids:
        url="http://ww"+uuid[0]+'.sinaimg.cn/mw690/' + uuid[1] +'.jpg'
        urls.append(url)
    urls.reverse()
    for url in urls:
        response=requests.get(url)
        if response.status_code == 200: 
            count+=1
            f=open("d:\\lumengling\\"+str(count)+".jpg","wb")
            f.write(response.content)
            f.close()
    page-=1
    
    
    
    
    
    