# -*- coding: utf-8 -*-
from imp import reload
import random
import urllib
import urllib2
import re
import sys
import time
import xlrd as xlrd
import xlwt
__author__ = 'Jerry'
f = open("d:/med.txt","w")
ROWNUMBEGIN = 0
mch_area = []
mch_pagecnt = []
search_url = []

default_headers =  {'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.101 Safari/537.36'}
#     {
#     'Connection': 'Keep-Alive',
#     'Accept': 'text/html, application/xhtml+xml, */*',
#     'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
#     'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; WindowsNT 6.1; WOW64; Trident/6.0; QQBrowser/7.7.24962.400) '
# }


#获取网页内容
def getHtml(url, headers = default_headers):

    req = urllib2.Request(url, headers = headers)

    page = urllib2.urlopen(req,timeout=180)
    html = str(page.read())
  
    #print html
    return html


#获取店铺信息
def get_info(myPage):

    #店铺名称
    mch_name_raw = re.findall('<li><a target="_blank" title="(.*?)" href="/a/20151124/019129.htm">',myPage,re.S)
    len1 = len(mch_name_raw)
    print len1
    for j in range(1,len(mch_name_raw),1):
        txt = str(mch_name_raw[j])+'\r\n'
        print txt
        f.write(txt)



print sys.getfilesystemencoding()
if __name__ == '__main__':
     url ='http://finance.qq.com/'
     html = getHtml(url)    
     print html
     get_info(html)
     #f = open("d:/tests.txt","w")

     # for i in range(1, len(search_url), 1):
     #    #print(search_url)
     #    f.write(search_url[i])


