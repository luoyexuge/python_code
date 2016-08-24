#encoding:utf-8
'''
Created on 2015年12月11日

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
#以下是数据库的操作
def  conDB():
    conn=pymysql.connect(host="",user="",passwd="",db="",charset="utf8")
    cur=conn.cursor()
    return conn,cur
def exeUpate(conn,cur,sql):
    sta=cur.execute(sql)
    conn.commit()
    return sta
def exDelete(conn,cur,IDs):
    sta=0
    for each  in IDs.split(''):
        sta+=cur.execute("delete  from student where Id=%d"%(int(each)))
    conn.commit()
    return sta
def exQuery(cur,sql):
    cur.execute(sql)
    return cur
def  conClose(conn,cur):
    cur.close()
    conn.close()
#搜索日期路径
now=datetime.datetime.now().strftime("%Y%m%d")  #今天的数据的日期

def HanderLog(result):
    start=datetime.datetime.now()
    result_list=[]
    for i in result:
        with open(i) as f:
            for line in f:
                try:
                    line=line.strip().split('\t')[2]
                    s=json.loads(line)
                except:
                    continue
                result_list.append(s['rtb_hash']['opxseid'])
                print 
        f.close()
    total=(datetime.datetime.now()-start).total_seconds()
    print(u"总共花费了{0}秒".format(total))
    return result_list  
# if __name__=='__main__':
#     if len(result)>0:
#        print  collections.Counter(HanderLog(result))
#     else:
#         print(u"没有日志数据")
# total=(datetime.datetime.now()-start).total_seconds()

class ThreadNum(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.queue=queue
    def handerLog(self,result):
        result_list=[]
        with open(result) as f:
            for line in f:
                try:
                    line=line.strip().split('\t')[2]
                    s=json.loads(line)
                except:
                    continue
                result_list.append(s['rtb_hash']['opxseid'])
               
        f.close()
        print(collections.Counter(result_list))
    def run(self):
        flag=True
        while flag:
            #消费者端，从队列中获取num
            num=self.queue.get()
            self.handerLog(num)
             #在完成这项工作之后，使用 queue.task_done() 函数向任务已经完成的队列发送一个信号
            
            self.queue.task_done()
            
        
        
start=time.time()
def main():
    #产生一个 threads pool, 并把消息传递给thread函数进行处理，这里开启10个并发
    for i  in range(10):
        t=ThreadNum(queue)
        t.setDaemon(True)
        t.start()
    
#往队列中填错数据  
    now=(datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y%m%d")
    path="d:\\wilson.zhou\\Desktop\\rtblogtest\\rtb.BJ2."+now
    result=glob.glob(path+"*.log") 
    for num in result:
        queue.put(num)
        queue.join()
if __name__=='__main__':
    main()        
    print"Elapsed Time: %s" % (time.time() - start)  

