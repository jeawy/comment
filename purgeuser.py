# -*- coding:utf-8 -*-

from Fetchnewuser import PergeUser
import time

if __name__ == "__main__": 
    p = PergeUser()
    start = time.ctime() 
    # 1 取clean=0
    users = p.get_unclean_user(50000)
    count = 0
    
    for user in users:
        userid = user[0]
        newuserid = user[2]
        # 2 查comment表，并update到新
        comments = p.get_comments(userid, newuserid)
     
        for comment in comments:
            appid = comment[1]
            # 3 在评论分表t{}中查找用户的评论，并更新其新用户id
            p.get_t_comments(appid, userid, newuserid)
        # 4 将用户插入新用户表中
        p.insert_user(newuserid, user[1])
        # 5 delete
        p.delete_user(userid)
        count += 1
        print (count)
    
    p.close()
    print ('All done' ) 
    print('start:', start)
    print('end  :', time.ctime())
 