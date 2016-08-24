#encoding:utf-8
'''
Created on 2015年12月2日

@author: wilson.zhou
'''

import urllib
import sys,os


print(os.getcwd())  #获得当前路径
print(sys.argv[0])  #获得当前文件所在的路径
print(sys.path[0])  #获得当前文件路径

def  cur_file_dir():
    path=sys.path[0]
    if os.path.isdir(path):
        return path
    elif os.path.isfile(path):
        return os.path.dirname(path)
        
print(cur_file_dir())


#url编码的问题
url = 'http://test.com/s?wd=哈哈'

url=url.decode('gbk','repalce')

print urllib.quote(url.encode('utf-8','replace'))
#http%3A//test.com/s%3Fwd%3D%E9%8D%9D%E5%A0%9D%E6%90%B1

#url解码的问题
test='http%3A//test.com/s%3Fwd%3D%E9%8D%9D%E5%A0%9D%E6%90%B1'

print urllib.unquote(test)
# http://test.com/s?wd=鍝堝搱   会出现梵文的情况

print urllib.unquote(test).decode('utf-8', 'replace').encode('gbk', 'replace')
#http://test.com/s?wd=哈哈

