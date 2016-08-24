#!/bin/env python
#-*-coding:utf-8-*-
'''
Created on 2016年1月14日

@author: wilson.zhou
'''
import os
string_cmd="""/home/pentaho/pentaho/pdi-ce-4.3.0-stable/kitchen.sh -file /home/pentaho/etlscript/basher_imp_cli/main.kjb -param:JOB_ROOT=/home/pentaho/etlscript/basher_imp_cli -param:INTERVAL_DAY=3 -logfile /home/pentaho/etlscript/basher_imp_cli/bashre_imp_cli.log.`date +%Y%m%d%H%M%S`"""
  
if  os.path.exists('/home/pentaho/etlscript/basher_imp_cli/main.kjb.lck'):
    print('the  /home/pentaho/etlscript/basher_imp_cli/main.kjb is running,please  try again')

else:
    os.system("ln -s /home/pentaho/etlscript/basher_imp_cli/main.kjb  /home/pentaho/etlscript/basher_imp_cli/main.kjb.lck")
    try:
        os.system(string_cmd)
        os.system("unlink  /home/pentaho/etlscript/basher_imp_cli/main.kjb.lck")
    except:
        os.system("unlink  /home/pentaho/etlscript/basher_imp_cli/main.kjb.lck")
        
        