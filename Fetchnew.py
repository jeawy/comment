# -*- coding:utf-8 -*-

from FetchComment import fetch_new_without_thr
from applespider import SpiderRun
if __name__ == "__main__": 
    spider = SpiderRun()
    spider.get_daily_hot_apps()
    spider.re_fetch_all_category_apps()
    spider.close()
    fetch_new_without_thr(sql="fetched = 0")
    #fetch_new_without_thr(sql="category = 6014") # 抓取游戏分类下的评论
    #fetch_new_without_thr(sql="category = 6015")
    #fetchedone(1207640832)
    #f = FetchJob()
    #f.find_new_ones() 
    #fetch_new_without_thr(sql='fetched = 2')
    #f.close()
    #readlog()
    #fetchall(  num=50)
    
