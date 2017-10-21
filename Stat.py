# -*- coding:utf-8 -*-

import requests
import xml.etree.ElementTree as ET
from lxml import etree
import pdb
import MySQLdb
import re
import time
import threading
import random

from datetime import datetime, timedelta
 
import logging
from applespider import Spider
from FetchComment import FetchComment



class Stat(FetchComment):
    """
    数据统计
    """
    # 在(http://www.xicidaili.com/wt/)上面收集的ip用于测试
    # 没有使用字典的原因是 因为字典中的键是唯一的 http 和https 只能存在一个 所以不建议使用字典
    pro = ['221.214.214.144:53281', '113.77.240.236:9797', '221.7.49.209:53281', '61.135.217.7']  
    #pro = ['139.129.166.68:3128', '59.59.146.69:53281', '180.168.179.193:8080', '122.72.32.88', '183.190.74.185']  
    #pro =['111.155.116.203:8123', '123.121.68.220:9000']
    
    COUNT = 20
    def __init__(self ): 
        logging.basicConfig(level=logging.INFO, datefmt='%a, %d %b %Y %H:%M:%S', 
                     filename='log\\stat.log', filemode='a+')
         
        super(Stat, self).__init__()
     
    def create_top_counter_tb(self):
        """创建表：top_comment_app ,用来记录每天评论最热的app，以及他们的评论数量"""
        sql ="""create table if not exists top_comment_app(
            id int(32) not null auto_increment,
            appid int(32),
            title varchar(1024),
            date date,
            counter int,
            fetched boolean default 0,
            primary key (id),
            
            unique key (appid, date)
        ) engine=myisam auto_increment =1 CHARSET=utf8mb4;"""
        self.cursor.execute(sql)
        self.db.commit()
    
    def create_fakeapp_tb(self):
        """
        创建表：fakeapp ,用来记录可能存在刷评论的app
        hot 刷评指数hot，默认为24
        每小时抓取刷评池中app的评论，如果没有抓到则把刷评指数hot-1，如果超过100评论，则hot+1，不超过100，则不增加也不减少
        """
        sql ="""create table if not exists fakeapp(
            appid int(32),
            title varchar(1024),
            date date,
            hot int(32) default 24,
            fetched boolean default 0,
            primary key (appid)  
        ) engine=myisam  CHARSET=utf8mb4;"""
        self.cursor.execute(sql)
        self.db.commit()
    

     
class StatJob(Stat):
    """
    这个类主要用来运行一些job，如：抓取
    """
    def __init__(self):
        super(StatJob, self).__init__()
    
    def cal_count(self, all_mark=False, day=-1):
        """计算每天app评论数量"""
        lastday = datetime.today().date() + timedelta(days=day)
        #lastday = datetime.today().date() + timedelta(days=-1)
        lastday = lastday.strftime('%Y-%m-%d')
        results = []
        insertsql = """insert into top_comment_app (appid,title, date, counter) values (%s, %s ,%s ,%s)
                                                     on duplicate key update counter =values(counter)"""
        appids = self.get_all_appin()
        num = 0
        for app in appids:
            num += 1
            sql = """select count(*), Date(updated) as updated from t{0} 
                     where date(updated) = '{1}' group by date(updated)""".format(app[0], lastday)
            self.cursor.execute(sql)
            count = self.cursor.fetchone()
            if count:
                print(app[0],num,app[1])
                result = (app[0],app[1], lastday, count[0])
                updatesql = """update appinfo set fetched = 1 where id = {0}""".format(app[0])
                
                try:
                    self.cursor.execute(insertsql,result )
                    self.cursor.execute(updatesql )
                    self.db.commit()
                except MySQLdb.Error as e:
                     logging.error(e)
       
     
if __name__ == "__main__":
    s = StatJob()
    s.cal_count(day = -1)
    s.cal_count(day = -2)  
    s.cal_count(day = -3)
    s.cal_count(day = -4) 
    s.close()
    
