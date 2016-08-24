#!/usr/bin/python
#-*-coding:utf-8-*-

import string
import sys 
reload(sys) 
sys.setdefaultencoding('utf8')
import ConfigParser
import smtplib
from email.mime.text import MIMEText
from email.message import Message
from email.header import Header


mail_host = 'smtp.i-click.com'
mail_port = 587
mail_user = 'wilson.zhou@i-click.com'
mail_pass = 'wilsonzhou1119'
mail_send_from ='dba@i-click.com'


def send_mail(to_list,sub,content):
    '''
    to_list:发给谁
    sub:主题
    content:内容
    send_mail("aaa@126.com","sub","content")
    '''
    #me=mail_user+"<</span>"+mail_user+"@"+mail_postfix+">"
    me=mail_send_from
    msg = MIMEText(content, _subtype='html', _charset='utf8')
    msg['Subject'] = Header(sub,'utf8')
    msg['From'] = Header(me,'utf8')
    msg['To'] = ";".join(to_list)
    try:
        smtp = smtplib.SMTP()
        smtp.connect(mail_host,mail_port)
        smtp.login(mail_user,mail_pass)
        smtp.sendmail(me,to_list, msg.as_string())
        smtp.close()
        return True
    except Exception, e:
        print str(e)
        return False
#content="the scheduler has an error, please check it."
#sub="ERROR"
if  __name__=='__main__':
    send_mail(["dba@i-click.com"], sys.argv[1] ,sys.argv[2])
