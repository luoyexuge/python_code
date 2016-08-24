#encoding:utf-8
'''
Created on 2015年12月14日

@author: wilson.zhou
'''
import  threading
import datetime
class  ThreadClass(threading.Thread):
    num=10
    def run(self):
        print 'threadname is %s ,the time is %s\n'%(self.getName(),datetime.datetime.now())
        while self.num>0:
            print 'i的值是%d'%(self.num)
            self.num-=1
    
for  i in range(20):   #同时启动20个线程,不能达到资源共享的目的
    t=ThreadClass()
    t.start()











    