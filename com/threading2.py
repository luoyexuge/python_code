#encoding:utf-8
'''
Created on 2015年12月14日

@author: wilson.zhou
'''
import datetime
import json
import pymysql
import glob
import collections
import Queue
import  threading
import  time
queue=Queue.Queue()
import Queue
import threading
import time
 
queue = Queue.Queue()
 
class ThreadNum(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
 
    def run(self):
        while True:
            num = self.queue.get()
            print"i'm num %s"%(num)
            time.sleep(1)
            self.queue.task_done()
            print self.queue.qsize()
 
start = time.time()
def main():
    for i in range(10):
        t = ThreadNum(queue)
        t.setDaemon(True)
        t.start()
    for num in range(10):
        queue.put(num)
        queue.join()
 
main()
print"Elapsed Time: %s" % (time.time() - start)