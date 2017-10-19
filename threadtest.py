# -*-coding:utf-8 -*-
"""
测试thread
"""
import threading
import time
import random
import pdb

lock = threading.Lock()

class MyThread(object):
    count = 0
    def job(self, i):
        lock.acquire()
        print (time.ctime()+'job '+str(i)+ 'start running\n')
        time.sleep(random.randint(3, 7))
        print (time.ctime()+'job '+str(i)+ 'stop running\n')
        
        self.count += 1
        print(self.count)
        lock.release()
        

 
    def run(self):
        ts = []
        for i in range(1, 17):
            t = threading.Thread(target=self.job, args=(i,))
            ts.append(t)

        for t in ts:
            t.start()
        for t in ts:
            t.join()
    
    @classmethod
    def clstest(cls):
        pdb.set_trace()
        print(cls.__dict__)

if __name__ == "__main__":
    MyThread.clstest()
    print('end')