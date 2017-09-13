# -*- coding:utf-8 -*-
"""
以多线程的方式，每天抓取各个分类下新上榜的APP的id以及title更新到数据库中，以便其他程序根据id去
抓取这个app的评论
"""
import re
import pdb 
import time
import json
from applespider import Spider
import logging
import threading

def logger():
    logging.basicConfig(level=logging.INFO, datefmt='%a, %d %b %Y %H:%M:%S',
    filename='fetch.log', filemode='a+')

def job_get_app(spider, categories):
    """获取指定分类下的热门app"""
    for category in categories:
        url = category[2] 
        # 开始抓取每个分类的热门app
        spider.getapps(url, category[0])
        logging.info('fetched {}'.format(category[0]))
        spider.update_category_fetched(category[0])

if __name__ == "__main__":
    logger()
    spider = Spider()    
    # 获取还没有抓取过app的分类 
    categories = spider.get_unfetched_category()
    
    length = len(categories)
    start = 0
    threads = []
    THREADNUM = 10

   
    while start < length:
        if length - start < THREADNUM: 
            tmp_categories = categories[start:]
        else:
            tmp_categories = categories[start:start+THREADNUM]
        
        start += THREADNUM
        t = threading.Thread(target=job_get_app, args=(spider, tmp_categories ))
        threads.append(t)
    
    for t in threads: 
        t.start()
    
    for t in threads:
        t.join()
    
    print('finished')

        
        
        


    