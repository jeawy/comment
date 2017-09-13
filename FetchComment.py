# -*- coding:utf-8 -*-

import requests
import xml.etree.ElementTree as ET
from lxml import etree
import pdb
import MySQLdb
import re
import time


class FetchComment(object):
    """"""
    COUNT = 20
    def __init__(self, appid):
        #db = MySQLdb.connect(host="127.0.0.1",user="root",passwd="sqlroot",db="Comment",charset="utf8mb4") 
        db = MySQLdb.connect(host="192.168.1.103",user="root",passwd="comment",db="comment",charset="utf8mb4") 
        #db = MySQLdb.connect("localhost", 'root', 'sqlroot', 'Comment', 'utf8')
        self.db = db
        self.cursor = self.db.cursor()
        self.appid = appid #414478124 

        userid_patter_str = '\d{5,}' #提取用户id的正则表达式
        self.userid_patter = re.compile(userid_patter_str)
        self.usernum = self.COUNT # 每20条写一次数据库
        self.commentnum = self.COUNT # 每20条写一次数据库
        

    def close(self):
        self.db.close()

    def create_tb(self):
        """如果table不存在则创建，为每个app创建一个表，共9万多个表，因为有9万多个APP"""
        createsql = """CREATE TABLE if not exists t{}(
            id int(32) NOT NULL auto_increment,
            userid int,
            name varchar(1024),
            updated datetime,
            title varchar(4096) not null, 
            rating int,
            version varchar(128),
            votesum int,
            votecount int,
            contenthtml text,
            content text,  
            PRIMARY KEY(id) 
        )ENGINE=InnoDB auto_increment=1 DEFAULT CHARSET=utf8mb4;""".format(self.appid)
        print(time.ctime())
        self.cursor.execute(createsql)
        self.db.commit()
        print(time.ctime())
    
    def create_comment_tb(self):
        """创建一张comment表，这张表中存储了全部的comment数据"""
        createsql = """CREATE TABLE if not exists comment(
            id int(32) NOT NULL auto_increment,
            appid int,
            userid int, 
            name varchar(1024),
            updated datetime,
            title varchar(4096) not null, 
            rating int,
            version varchar(128),
            votesum int,
            votecount int,
            contenthtml text,
            content text,  
            PRIMARY KEY(id) 
        )ENGINE=InnoDB auto_increment=1 DEFAULT CHARSET=utf8mb4;""" 
        print(time.ctime())
        self.cursor.execute(createsql)
        self.db.commit()
        print(time.ctime())

    def create_appleuser_tb(self):
        """如果评论用户表table不存在则创建，为每个app创建一个表，共9万多个表，因为有9万多个APP"""
        createsql = """CREATE TABLE if not exists appleuser( id int(32) ,  name varchar(1024),  PRIMARY KEY(id))ENGINE=InnoDB ;""" 
        
        self.cursor.execute(createsql)
        self.db.commit()

    def insert_appleuser_tb(self, appleuserid, username):
        """向苹果用户表中插入用户数据，如果用户id已存在，则不插入"""
       
        insertsql = 'insert into appleuser( id,  name) values ({0}, "{1}")'.format(appleuserid, username) 
        try:
            self.cursor.execute(insertsql)
        except MySQLdb.Error as e:
            pass
            #self.db.rollback() 
            #print ('user:{0} exist already.'.format(str(e)))
        
        if self.usernum <=0:
            self.db.commit()
            self.usernum = self.COUNT
        else:
            self.usernum -= 1
    
    def insert_comment_tb(self,  **kwargs):
        """向每个app的评论是单独分开的，本函数是为某个app的comment表中插入数据"""
       
        insertsql = """insert into t%s( userid, name, updated, title,   rating, version, votesum, votecount,contenthtml, content) 
                       values (%s, %s,%s, "%s",  %s,%s,%s,%s,%s, %s)"""
                       

        try:
            self.cursor.execute(insertsql, (self.appid, kwargs['userid'], kwargs['name'],kwargs['updated'],
                                           kwargs['title'], kwargs['rating'], kwargs['version'], kwargs['voteSum'],
                                           kwargs['voteCount'], kwargs['contenthtml'], kwargs['content']))
        except MySQLdb.Error as e:
            self.db.rollback()
            pdb.set_trace()
            print ('insert:{} exist failed.'.format(str(e)))
        
        if self.commentnum <=0:
            self.db.commit()
            self.commentnum = self.COUNT
        else:
            self.commentnum -= 1
    

    def add_comment_couter(self, counter, appid):
        """
           更新评论数量:
           appinfo表中记录评论数量的字段counter，更新原则是+=counter
        """
        selectsql = 'select counter from appinfo where id ='+ str(appid) +';' 
        self.cursor.execute(selectsql)
        app = self.cursor.fetchone()
        org_counter = app[0] # 获取原始comment数量
        with open('appinfofetched.log', 'a+') as f:
            f.write(time.ctime() + ':  ' + str(appid) + ' org counter: ' +str(org_counter)+ ' add counter' 
            + str(counter) + '\n')

        new_counter =  org_counter + counter
        updatesql = 'update appinfo set counter='+str(new_counter) + ' where id ='+ str(appid) +';'
        self.cursor.execute(updatesql)
        self.db.commit()


    def init_read(self):
        """首次抓取，抓取最多的记录"""
        
        url = 'https://itunes.apple.com/rss/customerreviews/page={0}/id={1}/sortby=mostrecent/xml?l=en&&cc=cn'
        ns = {
            'replace_w3org' : '{http://www.w3.org/2005/Atom}',
            'replace_apple': '{http://itunes.apple.com/rss}' 
        }
        count = 0
        for pagei in list(reversed(range(1, 11))):
            url = url.format(pagei, self.appid)
            results_xml = requests.get(url)

            results_text = results_xml.content.decode('utf-8', 'ignore')
            
            try:
                root = ET.fromstring(results_text )
            except ET.ParseError:
                continue # xml文件本身有问题

            userid = ''
            authorname = ''
            updated = ''
            title = ''
            content = ''
            rating = ''
            voteSum = ''
            voteCount = ''
            version = ''

            for entries in root.iter(ns['replace_w3org']+'entry'): 
                
                item = {} 
                
                for entry in  entries:  
                    tag = entry.tag.replace(ns['replace_apple'],'')
                    tag = tag.replace(ns['replace_w3org'],'')
                    
                    if tag == 'id' :
                        match = self.userid_patter.match(entry.text)
                        if match: 
                            userid = match.group() 
                            item['userid'] = userid # 用户id
                        else: 
                            break # 非用户评论
                    if tag =='author':  
                        for name in entry.iter(ns['replace_w3org']+'name'):
                            authorname = name.text
                            item['name'] = authorname # 用户名
                              
                    if tag =='updated':
                        updated = entry.text[:19].replace('T', ' ')
                        item['updated'] = updated # 用户评论日期

                    if tag =='title': # 用户评论标题
                        title = entry.text  
                        item['title'] = title.encode().decode('utf8', 'ignore') 
                    
                    if tag =='content':   # 评论内容
                        content = entry.text
                        if entry.attrib['type']=='html':
                            # 保存HTML版本的content用于阅读
                            item['contenthtml'] = content  
                        else:
                            # 保存纯文本版本的content用于分析
                            item['content'] = content 

                    if  tag=='rating':  # 评分
                        rating = entry.text
                        item['rating'] = rating 
                    
                    if  tag=='voteSum': 
                        voteSum = entry.text
                        item['voteSum'] = voteSum # 不知道是什么

                    if  tag=='voteCount': 
                        voteCount = entry.text
                        item['voteCount'] = voteCount # 不知道是什么

                    if  tag=='version': 
                        version = entry.text
                        item['version'] = version # 应用版本
                      
                # 将apple用户+新提取的评论插入数据库
                if userid and authorname:
                    count += 1 # count 为实际抓取的数量
                    print(count, item['title'], item['updated']) 
                    self.insert_appleuser_tb(userid, authorname)
                    self.insert_comment_tb(**item)
        # 数据已全部抓取完毕，更新appinof表的counter字段
        self.add_comment_couter(count, self.appid)

    def read_everyday(self):
        """
            每天都抓取，碰到日期小于昨天的记录就停止抓取
            # appinfo表中会记录最新一条评论的时间
        """ 
        url = 'https://itunes.apple.com/rss/customerreviews/page={0}/id={1}/sortby=mostrecent/xml?l=en&&cc=cn'
        replace_w3org = '{http://www.w3.org/2005/Atom}'
        replace_apple = '{http://itunes.apple.com/rss}' 
        count = 0
        for pagei in range(1, 10):
            url = url.format(pagei, self.appid)
            results_xml = requests.get(url)
            results_text = results_xml.content
            root = ET.fromstring(results_text)
          
            for entries in root.iter('{http://www.w3.org/2005/Atom}entry'): 
                for entry in  entries: 
                    tag = entry.tag.replace(replace_apple,'')
                    tag = tag.replace(replace_w3org,'')
                    count += 1
                    if tag =='author': 
                        print(count, tag, entry.find('name').text)
                    if tag =='im': 
                
                        pass
    
    def get_unfetched_appin(self): 
        """获取还未抓取的appid"""
        sql = """select id , title from appinfo where fetched = 0"""
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps
    
    def update_appin_fetched(self, appid): 
        """
        更新app，fetch=1， 表示已经抓取完了
        """
        sql = """update  appinfo set fetched = 1 where id = {}""".format(appid) 
        self.cursor.execute(sql)
        self.db.commit()
    
    def run(self, num):
        """"""
        appids = self.get_unfetched_appin()
        for appid in appids[:num]:
            self.appid = appid[0]
            self.create_tb() # 1 创建app的comment表
            self.init_read()
            self.update_appin_fetched(self.appid)
            print(self.appid, appid[1], 'comment fetched')

    def run_everyday(self, num):
        """
        运行本函数可以每天来抓取新的评论，并且不重新创建表
        """
        appids = self.get_unfetched_appin()
        for appid in appids[:num]:
            self.appid = appid[0]
            self.create_tb() # 1 创建app的comment表
            self.init_read()
            self.update_appin_fetched(self.appid)
            print(self.appid, appid[1], 'comment fetched')         
    

    def get_all_appin(self): 
        """获取所有appid"""
        sql = """select id  from appinfo"""
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps
    
    def merge(slef):
        """
        1 遍历每张表，1.1 把数据插入总comment表，并将总数count更新到appinfo表中
        
        """
        apps = self.get_all_appin()
        
        for app in apps:
            count = 0
            appid = app[0]
            sql = 'select * from t{}'.format(appid)
            self.cursor.execute(sql)
            comments = self.cursor.fetchall()
            for comment in comments:
                insertsql = 'insert into comment '+\
                            '(appid, userid, name, updated,'+\
                            'title, rating, version, votesum,'+\
                            'votecount, contenthtml, content) '+\
                            'values (%s, %s, %s ,%s, %s,   %s,'+\
                            ' %s ,%s, %s ,%s ,%s)'
                self.cursor.execute(insertsql, (appid, comment[0], comment[1], comment[2],
                                               comment[3], comment[4], comment[5], comment[6],
                                               comment[7], comment[8], comment[9] ))
                self.db.commit() #插入总comment表

            count = len(comments)
            # 更新appinfo表的counter字段
            self.add_comment_couter(count, appid)

class FetchJob(FetchComment):
    """
    这个类主要用来运行一些job，如：抓取
    """
    def __init__(self):
        pass
    
    def merge(self):
        pass

if __name__ == "__main__":
    f = FetchComment(281796108)
    f.run(7000)
    #f.create_comment_tb()
    #f.add_comment_couter(10, 281796108)
    f.close()
