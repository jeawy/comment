# -*-coding:utf-8 -*-
"""
测试thread
"""
import threading
import time
import random


class MyThread(object):
    
    def job(self, i):
        print (time.ctime()+'job '+str(i)+ 'start running\n')
        time.sleep(random.randint(3, 7))
        print (time.ctime()+'job '+str(i)+ 'stop running\n')
 
    def run(self):
        ts = []
        for i in range(1, 4):
            t = threading.Thread(target=self.job, args=(i,))
            ts.append(t)
        for t in ts:
            t.start()
        for t in ts:
            t.join()

if __name__ == "__main__":
    mythread = MyThread()
    mythread.run()
    print('end')