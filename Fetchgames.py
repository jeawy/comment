# -*- coding:utf-8 -*-

from FetchComment import fetch_new_without_thr
    
if __name__ == "__main__": 
    fetch_new_without_thr(sql="category = 6014") # 抓取游戏分类下的评论
    #fetch_new_without_thr(sql="category = 6005") # 抓取社交分类下的评论
 