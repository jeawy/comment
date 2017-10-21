# -*- coding:utf-8 -*-
import re
import pdb 
from FetchComment import FetchComment
import time
import json 
import logging


def readappinfo():
    filename = 'detail1.json'
    rj = Readjson() 
    rj.create_app_tb()
    
    rj.readappinfo(filename)
def logger():
    logging.basicConfig(level=logging.INFO, datefmt='%a, %d %b %Y %H:%M:%S',
    filename='log\\fetch.log', filemode='a+')

if __name__ == "__main__":
    logger()
    fetch = FetchComment()    
    # 获取还没有抓取过app的分类 
    categories = fetch.get_unfetched_category()
   
    for category in categories:
        url = category[2]
        
        # 开始抓取每个分类的热门app
        spider.getapps(url, category[0])
        logging.info('fetched {}'.format(category[0]))
        spider.update_category_fetched(category[0])
        
    
    """ 
    spider.get_category_internet()
    print(time.ctime())
    for item in spider.categoryitem:
        logging.info('start {}')
    spider.update_appinfo() 
    print(time.ctime())
    """
   
    