# encoding:utf-8
'''
Created on 2016年6月28日

@author: wilson.zhou
'''
import os 
import sys
import logging
import datetime 
import smtplib
from email.mime.text import MIMEText
from email.header import Header

mail_host = 'smtp.i-click.com'
mail_port = 587
mail_pass = 'wilsonzhou1119'
mail_send_from = 'dba@i-click.com'
mail_user = 'wilson.zhou@i-click.com'
mail_pass = 'wilsonzhou1119'

program=os.path.basename(sys.argv[0])
logger=logging.getLogger(program)
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)
day=sys.argv[1]
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
    return time
date_str = gettime(-1)
logger.info(date_str)
pig_cmd = """pig -Dpig.additional.jars=buzzads-bidding-jobs-0.1-SNAPSHOT.jar -p CLICK_LOG=/tmp/wilson/collectadclicklog -p TIME={0} -p MODEL_KEEP_DAYS=7 -p OUTPUT_PARENT=/tmp/wilson/ad_click_stat  AnalyzeClickLog_20160628.pig """.format(date_str)

logger.info(pig_cmd)
def  oscmd(cmd):
    result = os.system(cmd)
    if result != 0:
        send_mail(["wilson.zhou@i-click.com"], "collecttadiclick has error", "collecttadiclick has error")
    else:
        print("sucess")
        

if  __name__ == '__main__':
    oscmd(pig_cmd)
