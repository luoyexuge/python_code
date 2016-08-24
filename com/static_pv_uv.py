#encoding:utf-8
'''
Created on 2016年1月8日

@author: wilson.zhou
'''
import os
import pandas  as pd
df_total_1=pd.read_excel("d:\\wilson.zhou\\Desktop\\Fwd OTV traffic quality study\\OTV placements_20160106.xlsx",sheetname="Sheet2", \
       index_col = None, header = 0 )
# df_total=df_total.reindex(columns=['placement_id','pv','uv'])
df_total_1=df_total_1.sort("placement_id")
df_total_3=pd.read_excel("d:\\wilson.zhou\\Desktop\\Fwd OTV traffic quality study\\OTV placements_20160106.xlsx",sheetname="Sheet3", \
       index_col = None, header = 0 )
df_total_3=df_total_3.sort("placement_id")
df_total=pd.read_excel("d:\\wilson.zhou\\Desktop\\Fwd OTV traffic quality study\\OTV placements_20160106.xlsx",sheetname="Sheet4", \
       index_col = None, header = 0 )

df_total=df_total.sort("placement_id")

df_total=df_total.merge(df_total_1,left_on="placement_id",right_on="placement_id",how="left")
df_total=df_total.merge(df_total_3,left_on="placement_id",right_on="placement_id",how="left")
df_toal=df_total.fillna(0)
print df_total

if os.path.exists('d:\\wilson.zhou\\Desktop\\Fwd OTV traffic quality study\\last.xlsx'):
    try:
        os.remove('d:\\wilson.zhou\\Desktop\\Fwd OTV traffic quality study\\last.xlsx')
        df_total.to_excel("d:\\wilson.zhou\\Desktop\\Fwd OTV traffic quality study\\last.xlsx",index=False)
    except:
        print("文件没插入进去，请重新查看")
    
else:
    df_total.to_excel("d:\\wilson.zhou\\Desktop\\Fwd OTV traffic quality study\\last.xlsx",index=False)
# print df_total_1.head()
# print df_total_3.head()
