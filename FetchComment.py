# -*- coding:utf-8 -*-

import requests
import xml.etree.ElementTree as ET
from lxml import etree
import pdb
import MySQLdb
import re
import time
import threading

import logging
from applespider import Spider

logging.basicConfig(level=logging.INFO, datefmt='%a, %d %b %Y %H:%M:%S', 
                     filename='fetchdata.log', filemode='a+')

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
        self.usernum = self.COUNT # 每20条写一次数据库　＃这个可以不用了，因为有ｅｘｃｕｔｅｍａｎｙ，执行的更快
        self.commentnum = self.COUNT # 每20条写一次数据库
        

    def close(self):
        self.db.close()

    def create_tb(self, appid):
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
            merged boolean default 0,
            PRIMARY KEY(id) 
        )ENGINE=InnoDB auto_increment=1 DEFAULT CHARSET=utf8mb4;""".format(appid)
        self.cursor.execute(createsql)
        self.db.commit()
         
    
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
            PRIMARY KEY(id) ,
            UNIQUE KEY uniq_comment(updated, appid, userid)
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

    def insertmany_appleuser_tb(self, users):
        """向苹果用户表中插入用户数据，如果用户id已存在，则不插入"""
       
        insertsql = 'insert into appleuser( id,  name) values (%s, %s) on duplicate key update name =values(name)' 
        try: 
            self.cursor.executemany(insertsql, users)
            self.db.commit()
        except MySQLdb.Error as e:
            pdb.set_trace()
            #self.db.rollback() 
            print ('user:{0} exist already.'.format(str(e)))
        
      
    def insert_comment_tb(self,  **kwargs):
        """向每个app的评论是单独分开的，本函数是为某个app的comment表中插入数据"""
       
        insertsql = """insert ignore into t%s( userid, name, updated, title, rating, version, votesum, votecount,contenthtml, content) 
                       values (%s, "%s",%s, "%s",  %s,%s,%s,%s,%s, %s)"""
        
        insertcommentsql = """insert ignore into comment(appid, userid, name, updated, title, rating, version, votesum, votecount, content) 
                       values (%s, %s, "%s",%s, "%s",  %s,%s,%s,%s,%s)"""
 
        try:
            if 'version' in kwargs:
                self.cursor.execute(insertsql, (self.appid, kwargs['userid'], kwargs['name'],kwargs['updated'],
                                           kwargs['title'], kwargs['rating'], kwargs['version'], kwargs['voteSum'],
                                           kwargs['voteCount'], kwargs['contenthtml'], kwargs['content']))
                
                # 插入总comment表
                self.cursor.execute(insertcommentsql, (self.appid, kwargs['userid'], kwargs['name'],kwargs['updated'],
                                           kwargs['title'], kwargs['rating'], kwargs['version'], kwargs['voteSum'],
                                           kwargs['voteCount'],  kwargs['content']))
            else: 
                self.cursor.execute(insertsql, (self.appid, kwargs['userid'], kwargs['name'],kwargs['updated'],
                                           kwargs['title'], kwargs['rating'], '-1', kwargs['voteSum'],
                                           kwargs['voteCount'], kwargs['contenthtml'], kwargs['content']))
                
                # 插入总comment表
                self.cursor.execute(insertcommentsql, (self.appid, kwargs['userid'], kwargs['name'],kwargs['updated'],
                                           kwargs['title'], kwargs['rating'], kwargs['version'], kwargs['voteSum'],
                                           kwargs['voteCount'],   kwargs['content']))
        except MySQLdb.Error as e:
            self.db.rollback() 
            print ('insert:{} exist failed.'.format(str(e)))
        except KeyError as e:
            self.db.rollback() 
            print ('insert:{} exist failed.'.format(str(e)))
        
        if self.commentnum <=0:
            self.db.commit()
            self.commentnum = self.COUNT
        else:
            self.commentnum -= 1
    
    def insertmany_comment_tb(self, appid, comments,comments_t):
        """向每个app的评论是单独分开的，本函数是为某个app的comment表中插入数据"""
       
         
        inserttotalsql = """insert  into comment(appid, userid, name, updated, title, 
                            rating, version, votesum, votecount, content ) 
                            values (%s, %s,%s,%s,%s, %s,%s,%s,%s,%s)"""

        insertsql = """insert ignore into t{}( userid, name, updated, title, rating, version,
                                               votesum, votecount,contenthtml, content) 
                       values (%s,%s,%s, %s,%s, %s,%s,%s,%s, %s)""".format(appid)

        try: 
            #start = time.ctime()
            self.cursor.executemany(inserttotalsql, comments) 
            self.cursor.executemany(insertsql, comments_t) 
            self.db.commit()
            #print('commit:',start)
            #print('commit:',time.ctime())
        except MySQLdb.Error as e:
            self.db.rollback() 
            print ('insert:{} exist failed.'.format(str(e)))
        except KeyError as e:
            self.db.rollback() 
            print ('insert:{} exist failed.'.format(str(e)))
         
    def add_comment_couter(self, counter, appid):
        """
           更新评论数量:
           appinfo表中记录评论数量的字段counter，更新原则是+=counter
        """ 
        updatesql = 'update appinfo set counter= counter + '+str(counter) + ' where id ='+ str(appid) +';'
        self.cursor.execute(updatesql)
        self.db.commit()


    def init_read(self, appid):
        """首次抓取，抓取最多的记录"""
        
        templateurl = 'https://itunes.apple.com/rss/customerreviews/page={0}/id={1}/sortby=mostrecent/xml?l=en&&cc=cn'
        ns = {
            'replace_w3org' : '{http://www.w3.org/2005/Atom}',
            'replace_apple': '{http://itunes.apple.com/rss}' 
        }
        count = 0
        items = [] # comment记录
        items_t = [] # t23893238239记录
        users = [] # 评论用户
        entry_count = 0
        comment_updated = '' #最新评论时间

        start = time.ctime()
        for pagei in list(reversed(range(1, 11))):
            url = templateurl.format(pagei, appid)
            
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
                itementry = {} 
                user = {} 
                 
                for entry in  entries:  
                    tag = entry.tag.replace(ns['replace_apple'],'')
                    tag = tag.replace(ns['replace_w3org'],'')
                    
                    if tag == 'id' :
                        match = self.userid_patter.match(entry.text)
                        if match: 
                            userid = match.group() 
                            itementry['userid'] = userid # 用户id
                            user['userid'] = userid
                        else:  
                            break # 非用户评论
                    if tag =='author':  
                        for name in entry.iter(ns['replace_w3org']+'name'):
                            authorname = name.text
                            itementry['name'] = authorname # 用户名
                            user['name'] = authorname
                              
                    if tag =='updated':
                        updated = entry.text[:19].replace('T', ' ')
                        itementry['updated'] = updated # 用户评论日期
                        comment_updated = updated

                    if tag =='title': # 用户评论标题
                        title = entry.text  
                        itementry['title'] = title.encode().decode('utf8', 'ignore') 
                    
                    if tag =='content':   # 评论内容
                        content = entry.text
                        if entry.attrib['type']=='html':
                            # 保存HTML版本的content用于阅读
                            itementry['contenthtml'] = content  
                        else:
                            # 保存纯文本版本的content用于分析
                            itementry['content'] = content 

                    if  tag=='rating':  # 评分
                        rating = entry.text
                        itementry['rating'] = rating 
                    
                    if  tag=='voteSum': 
                        voteSum = entry.text
                        itementry['voteSum'] = voteSum # 不知道是什么

                    if  tag=='voteCount': 
                        voteCount = entry.text
                        itementry['voteCount'] = voteCount # 不知道是什么

                    if  tag=='version': 
                        version = entry.text
                        itementry['version'] = version # 应用版本
                      
                # 将apple用户+新提取的评论插入数据库
                if userid and authorname:
                    count += 1 # count 为实际抓取的数量 
                    if 'version' not in itementry:
                        itementry['version'] = '-1'

                    items.append((appid, itementry['userid'], itementry['name'] ,itementry['updated'] ,
                    itementry['title'] ,itementry['rating'],itementry['version'] ,itementry['voteSum'] ,itementry['voteCount'] ,
                    itementry['content']   ))

                    items_t.append((itementry['userid'], itementry['name'] ,itementry['updated'] ,
                    itementry['title'] ,itementry['rating'],itementry['version'] ,itementry['voteSum'] ,itementry['voteCount'] ,
                    itementry['contenthtml'], itementry['content']   ))

                    users.append((user['userid'], user['name']))
                    #print(itementry['updated'],count, itementry['title'])
                     
                    
                    #数据为一条一条插入方式
                    #print(count, itementry['title'], itementry['updated']) 
                    #self.insert_appleuser_tb(userid, authorname)
                    #self.insert_comment_tb(**item)
        
        #print('fetched:',start)
        #print('fetched:',time.ctime())   
        # 数据已全部抓取完毕，更新appinof表的counter字段 
        if count > 0:   
            self.insertmany_appleuser_tb(users)
            self.insertmany_comment_tb(appid, items, items_t)
            self.add_comment_couter(count, appid) 
            self.update_appin_fetched(appid, comment_updated)
            print(appid, count)
        else:
            self.update_appin_fetched(appid)
            

    def get_appinfo_internet(self):
        """从api中获取appinfo的icon、content和artist数据"""
        
        templateurl = 'https://itunes.apple.com/rss/customerreviews/page=1/id={0}/sortby=mostrecent/xml?l=en'
        ns = {
            'replace_w3org' : '{http://www.w3.org/2005/Atom}',
            'replace_apple': '{http://itunes.apple.com/rss}' 
        } 
 
        url = templateurl.format(self.appid) 
        results_xml = requests.get(url) 
        results_text = results_xml.content.decode('utf-8', 'ignore') 
        
        try:
            root = ET.fromstring(results_text )
        except ET.ParseError :
            results_xml = requests.get(url.replace('&&cc=cn','')) 
            results_text = results_xml.content.decode('utf-8', 'ignore') 
            root = ET.fromstring(results_text )

        itementry = {}     
        for entries in root.iter(ns['replace_w3org']+'entry'):  
            for entry in  entries:  
                tag = entry.tag.replace(ns['replace_apple'],'')
                tag = tag.replace(ns['replace_w3org'],'')
            
                if tag =='content':  
                    content = entry.text
                    itementry['content'] = content 

                if  tag=='artist':  # artist
                    artist = entry.text
                    itementry['artist'] = artist 
              
                if  tag=='image' and entry.attrib['height']=='100': 
                    icon100 = entry.text
                    itementry['icon100'] = icon100  
            break 

        return itementry
        

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

    def get_unfetch_detail_appin(self): 
        """获取还未抓取的详细信息appid"""
        sql = """select id , title from appinfo where icon100 is null"""  
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps
    
    def update_appin_fetched(self, appid, comment_updated=''): 
        """
        更新app，fetch=1， 表示已经抓取完了
        """
        if len(comment_updated) > 0:
            sql = """update  appinfo set fetched = 1, comment_updated="{0}" where id = {1}""".format( comment_updated, appid) 
        else:
            sql = """update  appinfo set fetched = 1 where id = {0}""".format(appid) 
     
        self.cursor.execute(sql)
        self.db.commit()

    def update_appin_basic(self, appid, appinfo): 
        """
        更新app的artist，content，和icon信息
        """
        sql = """update  appinfo set description = %s,  seller=%s, icon100=%s where id = {0}""".format( appid)  
        self.cursor.execute(sql, (appinfo['content'], appinfo['artist'], appinfo['icon100']))
        self.db.commit()

    def run(self, num=0):
        """
        获取所有fetched=0的app的comment
        num参数大于0小于length时可用。
        """
        appids = self.get_unfetched_appin()
        if num > 0 and num < len(appids):
            appids = appids[:num]

        for appid in appids: 
            self.create_tb(appid[0]) # 1 创建app的comment表
            self.init_read(appid[0])
            
            print(appid[0], appid[1], 'comment fetched')
    
    def runapps(self, apps):
        """
        多线程方式
        """ 
        for appid in apps: 
            self.create_tb(appid[0]) # 1 创建app的comment表
            self.init_read(appid[0]) 
            print(appid[0], appid[1], 'comment fetched')

    def runfetch(self):
        """
        更新appinfo的icon、description等信息
        """
        appids = self.get_unfetch_detail_appin()
       
        for appid in appids:
            self.appid = appid[0] 
            infos = self.get_appinfo_internet()
            if infos:
                self.update_appin_basic(self.appid, infos) 
                print(self.appid, appid[1], 'updated')

    def run_everyday(self, num):
        """
        运行本函数可以每天来抓取新的评论，并且不重新创建表
        """
        appids = self.get_unfetched_appin()
        for appid in appids[:num]: 
            self.create_tb() # 1 创建app的comment表
            self.init_read()
            self.update_appin_fetched(appid[0])
            print(self.appid, appid[1], 'comment fetched')         
    

    def get_all_appin(self): 
        """获取所有appid"""
        sql = """select id  from appinfo"""
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps
     
class FetchJob(FetchComment):
    """
    这个类主要用来运行一些job，如：抓取
    """
    def __init__(self, appid):
        super(FetchJob, self).__init__(appid)

    def delall_comment_tb(self):
        apps = self.get_all_appin()
        for app in apps:
            try:
                sql = 'drop table t{}'.format(app[0])
                self.cursor.execute(sql)
                self.db.commit()
                print(app[0], 'deleted')
            except MySQLdb.Error as e:
                logging.error(e) 
 
    def merge(self):
        """
        把各个APP分表中的comment集中在一个comment表中。
        算法：
        1 通过appinfo表查找所有app的comment表
        2 遍历所有表，每一次用executemany来将分表插入总表， 并对appinfo表中的数据进行标记
        3
        """
        insertsql = "insert into comment (appid, userid, name,  updated, title, rating, version,  votesum, votecount, content) values ( %s, %s,%s,%s,%s, %s, %s,%s,%s,%s); "
        timestart = time.ctime()
        apps = self.get_unmerged_app() 
        count = 0
        for app in apps:
            # 开始merge
            # 
            comments = self.get_comments_app(app[0])
            length = len(comments)
            if length > 0:
                try:
                    self.cursor.executemany(insertsql, comments )
                    self.db.commit()
                    self.update_appinfo_merged(length, app[0])
                    count += length
                    print ('total:', count, app[0], 'merged')
                except Exception as e:
                    pdb.set_trace()
                    self.db.rollback()

        print('finished')
        print('Start from :', timestart)
        print('End        :', time.ctime())


    def get_comments_app(self, appid):
        """
        获取appid指定的t{appid},如t122632344表中所有comment，并以元祖的方式返回
        """ 
        timestart = time.ctime()
        sql = 'select  userid, name, updated,  title, rating, version, votesum,  votecount, content from t'+str(appid) + ';'
        self.cursor.execute(sql)
        comments = self.cursor.fetchall()
        comments_list = []
        for comment in comments: 
            tmp = list(comment)
            tmp.insert(0, appid)  
            comments_list.append(tuple(tmp))
        print('Start from :', timestart)
        print('End        :', time.ctime()) 
      
        return comments_list
        

    def get_unmerged_app(self): 
        """
        获取所有未merged的app
        """
        sql = """select id from appinfo where merged = 0"""
        self.cursor.execute(sql)
        apps = self.cursor.fetchall()
        return apps

    def update_appinfo_merged(self, counter, appid): 
        """
        更新app，merged=1， 表示已经merge完了
        """
        sql = """update  appinfo set merged = 1, counter={0} where id = {1}""".format(counter, appid) 
        self.cursor.execute(sql)
        self.db.commit()
    
def job(apps):
    f = FetchJob(1226396146)
    f.runapps(apps)
    f.close()
    print(str(threading.current_thread())+'Done')

if __name__ == "__main__":
    
    f = FetchJob(1226396146)
    #f.get_appinfo_internet()
    #f.run(40)
    apps = f.get_unfetched_appin()
    start = time.ctime()
    length = len(apps)
    step = int(length/4)
    print(step)
    ts = []
    spider = Spider()
    oldusercount = spider.count_all_appleusers()
    oldcommentcount = spider.count_all_comments()
    for i in range(0, 4): # 4个线程
        if i == 3:
            t = threading.Thread(target=job, args=(apps[i*step:],)) 
        else:
            t = threading.Thread(target=job, args=(apps[i*step:(i+1)*step],))
        
        ts.append(t)
    
    for t in ts: # 启动所有线程
        t.start()
    for t in ts:# 等待所有线程完成
        t.join()
    print ('fetched done' )
    newusercount = spider.count_all_appleusers()
    newcommentcount = spider.count_all_comments()
    spider.insert_stat_newfetched(newcomment=newcommentcount-oldcommentcount, 
                                  newuser=newusercount-oldusercount)
 
    print ('start:', start)
    print('end   :', time.ctime())
    f.close()
