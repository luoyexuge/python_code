# encoding:utf-8
'''
Created on 2016年6月23日

@author: wilson.zhou
'''
import os 
import datetime 
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import sys
day=int(sys.argv[1])

mail_host = 'smtp.i-click.com'
mail_port = 587
mail_pass = 'wilsonzhou1119'
mail_send_from = 'dba@i-click.com'
mail_user = 'wilson.zhou@i-click.com'
mail_pass = 'wilsonzhou1119'

def send_mail(to_list, sub, content):
    '''
    to_list:发给谁
    sub:主题
    content:内容
    send_mail("aaa@126.com","sub","content")
    '''
    me = mail_send_from
    msg = MIMEText(content, _subtype='html', _charset='utf8')
    msg['Subject'] = Header(sub, 'utf8')
    msg['From'] = Header(me, 'utf8')
    msg['To'] = ";".join(to_list)
    try:
        smtp = smtplib.SMTP()
        smtp.connect(mail_host, mail_port)
        smtp.login(mail_user, mail_pass)
        smtp.sendmail(me, to_list, msg.as_string())
        smtp.close()
        return True
    except Exception, e:
        print str(e)
        return False

def  gettime(day):
    time = (datetime.datetime.now()+datetime.timedelta(days=day)).strftime("%y-%m-%d")
    return "ds=" + time
date_str = gettime(day)
print(date_str)

pig_cmd= """pig -Dpig.additional.jars=buzzads-bidding-jobs-0.1-SNAPSHOT.jar:elephant-bird-hadoop-compat-4.1.jar:elephant-bird-core-4.1.jar:elephant-bird-pig-4.1.jar -p RTB_CLICK=/rawdata/logcompress/rtbClick/{0} -p RTB_SHOWUP=/rawdata/logcompress/rtbShowup/{0} -p RTB_REQ=/rawdata/logcompress/rtbReq/{0}  -p  RTB_RESP=/rawdata/logcompress/rtbResp/{0} -p AD_CLICK_LOG=/tmp/wilson/collectadclicklog/{0}   CollectAdClickLog_20160622.pig""".format(date_str)

print  pig_cmd
def  oscmd(cmd):
    result = os.system(cmd)
    if result != 0:
        send_mail(["wilson.zhou@i-click.com"], "collecttadiclick has error", "collecttadiclick has error")
    else:
        print("sucess")
        
if  __name__ == '__main__':
    oscmd(pig_cmd)

