#encoding:utf-8
'''
Created on 2015年12月11日

@author: wilson.zhou
'''
from threading import Thread  
import time  
class race(Thread):  
    def __init__(self,threadname,interval):  
        Thread.__init__(self,name=threadname)  
        self.interval = interval  
        self.isrunning = True  
          
      
    def run(self):       #重写threading.Thread中的run()  
        while self.isrunning:  
            print 'thread %s is running,time:%s\n' %(self.getName(),time.ctime()) #获得线程的名称和当前时间  
            time.sleep(self.interval)  
    def stop(self):  
        self.isrunning = False  
  
def test():  
    thread1 = race('A',1)  
    thread2 = race('B',2)  
    thread1.start()  
    thread2.start()  
    time.sleep(5)  
    thread1.stop()  
    thread2.stop()  
      
if __name__ !='__main__':  
    test()  
class  race1(Thread):
    def __init__(self,threadname,interval):
        Thread.__init__(self,name=threadname)
        self.inteval=interval
        self.isruning=True
    def run(self):
        while self.isruning:
            print 'thread %s is running,time:%s\n' %(self.getName(),time.ctime())
            time.sleep(self.inteval)
    def stop(self):
        self.isruning=False
def test1():
    thread1=race1('A',0)
    thread2=race1('B',0)
    thread3=race1('C',0)
    thread1.start()
    thread2.start()
    thread3.start()
    time.sleep(5)
    thread1.stop()
    thread2.stop()
    thread3.stop()
test1()
























 
    
