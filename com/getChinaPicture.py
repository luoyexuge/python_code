#encoding:utf-8
'''
Created on 2016年7月14日

@author: wilson.zhou
'''
"""
下载中国商标局的商标图片

Created on Wed Jul 13 14:24:38 2016

@author: admin
"""
import urllib
import xlrd
import time

def getTrademark(url,absolutelyFileNmae):
    
    try:
        urllib.urlretrieve(url,absolutelyFileNmae)
        return 0,0
    except Exception, e:
        return url,absolutelyFileNmae
    
    
#打开 excel 文件读取数据
wb=xlrd.open_workbook("d:\\wilson.zhou\\Desktop\\Trademark.xlsx")
#获取一个工作表，通过索引顺序获取
st=wb.sheets()[0]
#获取整行的数值，
#rowValue=st.row_values(2)
#stNrow 为表的行数
stNrow=st.nrows

notDownload=[]
notSave=[]
#遍历表第三行开始到最后一行，trademarkNumber为商标注册申请号
#classNum为分类号，tradeFile为商标图片存储位置

for i in range(2,40):
    trademarkNumber=str(int(st.cell(i,0).value))
    classNum=str(int(st.cell(i,1).value))
    trademarkFileName="D:\\jps\\"+ trademarkNumber+".jpg"
    trademarkUrl="http://sbcx.saic.gov.cn:9080/tmois/wszhcx_getImageInputSteremSF.xhtml?regNum="+trademarkNumber+"&intcls="+classNum+"&size=1"
    classNum+"&size=1"
    print(trademarkUrl)
    print(trademarkFileName)
    (a,b)=getTrademark(trademarkUrl,trademarkFileName)

    notDownload.append(a)
    notSave.append(b)
for j in range(0,len(notDownload)):
    if notDownload[j]!=0:
        urllib.urlretrieve(notDownload[j], notSave[j])
        print("--------")
        
print(u"结束") 
        
        
