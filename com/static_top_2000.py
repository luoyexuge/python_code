#encoding:utf-8
'''
Created on 2016��1��19��

@author: wilson.zhou
'''
import os
import pandas  as pd
import numpy as np

# df=pd.read_table("d:\wilson.zhou\Desktop\\result.txt",names=["url","size","pv"])
# df=df[(df['url']!='http:') & (df['url']!=np.NaN)]
# df1=pd.read_excel(u"d:\\wilson.zhou\\Desktop\\tmp\\统计表.xlsx",sheetname="top imp2016.1.15-2016.1.19")
# 
# df=df.merge(df1[["url","imp","clic"]],left_on="url",right_on="url",how="left")
# print df.head()
#  
#  
# if os.path.exists('d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx'):
#     try:
#         os.remove('d:\\wilson.zhou\\Desktop\tmp\\tmp.xlsx')
#         df.to_excel("d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx",index=False)
#     except:
#         print("文件没插入进去，请重新查看")
# else:
#     df.to_excel("d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx",index=False) 



# df_dict =pd.read_excel(u'd:\\wilson.zhou\\Desktop\\tmp\\info.xlsx',sheetname="Sheet2")  
# id_dict = dict(zip(df_dict['adx_code'], df_dict['adx_name'])) 
# print df_dict.head()
#  
# df=pd.read_table("d:\wilson.zhou\Desktop\\result.txt",names=["adx","ds","url","pv"])
# df["ds"]=df["ds"].map(lambda x:"20"+x)
# print df.head()
# df=df[df["adx"].isin(["mediamax","bes","megamedia","vam","zol","accuren","admax"])]
#  
#  
# df["adx"]=df["adx"].map(id_dict)
# df.sort(columns=["adx",'ds','pv'],ascending=[1,1,0],inplace=True)
# print df.head(2)
# if os.path.exists('d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx'):
#     try:
#         os.system("del d:\\wilson.zhou\\Desktop\tmp\\tmp.xlsx")
#         df.to_excel("d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx",index=False)
#     except:
#         print("文件没插入进去，请重新查看")
# else:
#     df.to_excel("d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx",index=False) 




# df_dict =pd.read_excel(u'd:\\wilson.zhou\\Desktop\\tmp\\info.xlsx',sheetname="Sheet1") 
# df_dict1 =pd.read_excel(u'd:\\wilson.zhou\\Desktop\\tmp\\info.xlsx',sheetname="Sheet2")   
# print df_dict1.head
# id_dict = dict(zip(df_dict['code'], df_dict['city'])) 
# id_dict1 = dict(zip(df_dict1['adx_code'], df_dict1['adx_name']))
# print df_dict.head()
#  
# df=pd.read_table("d:\wilson.zhou\Desktop\\result_joey.txt",names=["adx","city","os","pv","uv"])
# print df.head()
# df["city"]=df["city"].map(id_dict)
# df["adx"]=df["adx"].map(id_dict1)
# print df.head()
# result_pivot=pd.pivot_table(df, index=['city'],columns=["adx","os"],values=['uv'],  aggfunc=np.sum)  
# print(type(result_pivot))  
# result_pivot[u'总计']=result_pivot.sum(axis=1)  
# result_sum=pd.DataFrame(result_pivot.sum()).T  
# result_pivot_sum=result_pivot.append(result_sum)  
# result_pivot_sum=result_pivot_sum.rename(index={0:u'总计'})  
# if os.path.exists('d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx'):
#     try:
#         os.system("del d:\\wilson.zhou\\Desktop\tmp\\tmp.xlsx")
#         result_pivot_sum.to_excel("d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx")
#     except:
#         print("文件没插入进去，请重新查看")
# else:
#     result_pivot_sum.to_excel("d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx") 



df_dict1 =pd.read_excel(u'd:\\wilson.zhou\\Desktop\\tmp\\info.xlsx',sheetname="Sheet2")  
id_dict1 = dict(zip(df_dict1['adx_code'], df_dict1['adx_name']))
df=pd.read_table("d:\wilson.zhou\Desktop\\tt_3.txt",names=["adx","gender","age","uv"])
df["adx"]=df["adx"].map(id_dict1)

df["uv%"]=df['uv'].map(lambda  x: (x*1.0/df['uv'].sum()))
result_sum=pd.DataFrame(df.sum()).T 
df=df.append(result_sum)  
df=df.rename(index={0:u'总计'})  
print df

if os.path.exists('d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx'):
    try:
        os.system("del d:\\wilson.zhou\\Desktop\tmp\\tmp.xlsx")
        df.to_excel("d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx")
    except:
        print("文件没插入进去，请重新查看")
else:
    df.to_excel("d:\\wilson.zhou\\Desktop\\tmp\\tmp.xlsx") 

