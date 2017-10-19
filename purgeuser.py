# -*- coding:utf-8 -*-

from Fetchnewuser import PergeUser
import time
import threading

def run():
    p = PergeUser()
    start = time.ctime() 
    # 1 取clean=0
    users = p.get_unclean_user( 10000)
    """
    length = len(users)

    step = int(length/4)
    ts = [] 
    for i in range(0, 4): # 4个线程
        if i == 3:
            t = threading.Thread(target=job, args=( users[i*step:],)) 
        else:
            t = threading.Thread(target=job, args=( users[i*step:(i+1)*step],))
        
        ts.append(t)
    
    for t in ts: # 启动所有线程
        t.start()
    for t in ts:# 等待所有线程完成
        t.join()
    """
    job(p, users)

    p.close()
    print ('All done' ) 
    print('start:', start)
    print('end  :', time.ctime())
    
     
def job(p,  users): 
    count = 0
   
    newusers = []
    print(len(users))
    for user in users:
        
        userid = user[0]
        newuserid = user[2]
        # 2 查comment表，并update到新
        comments = p.get_comments(userid, newuserid)
        print(len(comments))
      
        for comment in comments:
            appid = comment[1]
            # 3 在评论分表t{}中查找用户的评论，并更新其新用户id
            print (appid)
            p.get_t_comments(appid, userid, newuserid)
       
        # 4 将用户插入新用户表中
        p.insert_user(newuserid, user[1]) 
        # 5 delete
        p.delete_user(userid)
        count += 1
        if count % 50 == 0:
            print(count)
    
 
if __name__ == "__main__":  
    run()
 
   
 