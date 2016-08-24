#!/usr/bin/python
#encoding:utf-8
'''
Created on 2016年2月29日

@author: wilson.zhou
'''
import  os
import  datetime
import glob
import json
import time
import urllib

start=time.time()
global path
path="/home/hdbatch/wilson"
def  gaincurrenttime(day):
    time=(datetime.datetime.now()+datetime.timedelta(days=day)).strftime("%Y%m%d%H")
    return time
nowtime=gaincurrenttime(0)
twodaysagotime=gaincurrenttime(-2)

def  globfile(dir):
    dir=dir+"/"+"*.json"
    file=glob.glob(dir)
    result=[ i for i in file  if str(i.split("/")[-1].split(".")[0])<=str(twodaysagotime)]
    return  result
def   getBlackList(dir):
    dir=dir+"/"+nowtime+".json"
    try: 
        os.system("""curl  "http://10.40.8.141:65000/?method=blacklist"  -o  {0}""".format(dir))
        print("Download blacklist sucess")
    except  Exception,e:
        os.system("""python  {2}/sendmail.py  "black list programe of downs has some  error:Now the time is {0}"   "the error is :{1}, please check it." """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),e,path))
        print(e)
		
def removefile(dir):
    removefiles=globfile(dir)
    for  i in removefiles:
        try:
            if os.path.exists(i):
                os.remove(i)
        except:
            print("Download blacklist error")
            continue 
        
        
def  getdataFromurl(url):
    dat=urllib.urlopen(url)
    data=dat.read()
    return  data
def  getLastJson(dir):
    dir=dir+"/"+"*.json"
    file=glob.glob(dir)
    file.sort()
    result=open(file[-1])
    file=result.readlines()
    return file

def   inserGP():
    
    os.system("""psql -h "pdi_xmo_dw" -U "etladmin" "xmo_dw" -f /home/hdbatch/wilson/Gp_Blactlist.sql""")
    

if  __name__=="__main__":
    removefile(path)
    getBlackList(path)
    
  
    result=json.loads(getLastJson(path)[0])
    if len(result["antispam_cookie"])>0:
        antispam_cookie=[ str(j)+'\n' for j in result["antispam_cookie"]  if len(str(j))<=50]
        diff_cookie=list(set(result["antispam_cookie"]).difference(set([j for j in result["antispam_cookie"] if len(str(j))<=50])))
        if len(diff_cookie)>0:
            os.system("""python  {2}/sendmail.py  "black list of cookie Illegal,Now the time is {0}"   "the cookie is {1} , please check it."  """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),diff_cookie,path))
        else:
            pass
        if not os.path.exists("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_cookie.txt"):
            try:
                os.system("touch /mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_cookie.txt")
                f_antispam_cookie=open("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_cookie.txt","w")
                f_antispam_cookie.writelines(antispam_cookie)
                f_antispam_cookie.close()
            except:
                print("there has some error when  touch file")
        else:
            f_antispam_cookie=open("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_cookie.txt","w")
            f_antispam_cookie.writelines(antispam_cookie)
            f_antispam_cookie.close()
    else:
        pass
    if len(result["antispam_ip"])>0:  
        antispam_ip=[str(j)+'\n' for j in result["antispam_ip"] if len(str(j))<=16]
        diff_ip=list(set(result["antispam_ip"]).difference(set([j for j in result["antispam_ip"] if len(str(j))<=16])))
        if len(diff_ip)>0:
            os.system("""python  {2}/sendmail.py  "black list of ip Illegal,Now the time is {0}"   "the ip is {1} , please check it."  """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),diff_ip,path))
        else:
            pass
        if not os.path.exists("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_ip.txt"):
            try:
                os.system("touch /mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_ip.txt")
                f_antispam_ip=open("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_ip.txt","w")
                f_antispam_ip.writelines(antispam_ip)
                f_antispam_ip.close()
            except:
                print("there has some error when  touch file")
        else:
            f_antispam_ip=open("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_ip.txt","w")
            f_antispam_ip.writelines(antispam_ip)
            f_antispam_ip.close()
    else:
        pass
    if len(result["antispam_tagid"])>0:
        antispam_tagid=[str(j)+'\n' for j in result["antispam_tagid"] if len(str(j))<=500]
        diff_tagid=list(set(result["antispam_tagid"]).difference(set([j for j in result["antispam_tagid"] if len(str(j))<=500])))
        if len(diff_tagid)>0:
            os.system("""python  {2}/sendmail.py  "black list of tagid Illegal,Now the time is {0}"   "the tagid is {1} , please check it."  """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),diff_tagid,path))
        else:
            pass
        if not os.path.exists("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_tagid.txt"):
            try:
                os.system("touch /mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_tagid.txt")
                f_antispam_tagid=open("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_tagid.txt","w")
                f_antispam_tagid.writelines(antispam_tagid)
                f_antispam_tagid.close()
            except:
                print("there has some error when  touch file")
        else:
            f_antispam_tagid=open("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_tagid.txt","w")
            f_antispam_tagid.writelines(antispam_tagid)
            f_antispam_tagid.close()
    else:
        pass
    if len(result["antispam_domain"])>0:
        antispam_domain=[str(j)+'\n' for j in result["antispam_domain"] if  len(str(j))<=100]
        diff_domain=list(set(result["antispam_domain"]).difference(set([j for j in result["antispam_domain"] if len(str(j))<=100])))
        if len(diff_domain)>0:
            os.system("""python  {2}/sendmail.py  "black list of domain Illegal,Now the time is {0}"   "the domain is {1} , please check it."  """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),diff_domain,path))
        else:
            pass
        
        if not os.path.exists("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_domain.txt"):
            try:
                os.system("touch /mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_domain.txt")
                f_antispam_domain=open("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_domain.txt","w")
                f_antispam_domain.writelines(antispam_domain)
                f_antispam_domain.close()
            except:
                print("there has some error when  touch file")
        else:
            f_antispam_domain=open("/mapr/hkidc.hadoop.iclick/staging/bshare_blacklist/antispam_domain.txt","w")
            f_antispam_domain.writelines(antispam_domain)
            f_antispam_domain.close()
    else:
        pass
    try:
        inserGP()
    except Exception,e:
        print("sucess,Now time is:"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print e
        os.system("""python  {2}/sendmail.py  "black list programe of insert GP has some error,Now the time is {0}"   "blacklist into GP has some errors:{1} , please check it."  """.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),e,path))
        
    print"Elapsed Time: %s" %(time.time() - start)  
    print("sucess,Now time is:"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

  
    
    