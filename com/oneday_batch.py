#!/usr/bin/python

import time
import sys,os

yesterday = time.strftime('%Y-%m-%d',time.localtime(time.time() -24*60*60))
short_day = yesterday[2::]

input_path = '/rawdata/logcompress/rtbReq/ds='+short_day

out_dir_day= time.strftime('%Y%m%d',time.localtime(time.time() -24*60*60))
save_path = '/shortdata/iclick_data/'+out_dir_day

print(save_path)
cmd = "pig  -p LOG_PATH=%s -p SAVE_PATH=%s /opt/pig_home/Pig_script/GetResTv.pig" %(input_path,save_path)
print(cmd)
os.system(cmd)
