# -*- coding:utf-8 -*-

import requests
import xml.etree.ElementTree as ET
from lxml import etree
from lxml import html 
import pdb
import MySQLdb
import re
import time
import threading
import random
import os
from datetime import datetime

import logging
from applespider import Spider
from update_app_info_title import FetchAppinfo

from decorators import timelog
from conn import ConnectDBA
fileos = os.path.dirname(os.path.realpath(__file__))
print(fileos)
logging.basicConfig(level=logging.INFO, datefmt='%a, %d %b %Y %H:%M:%S', 
                     filename= os.path.join(fileos,'newapps.log'), filemode='a+')

class FetchComment(ConnectDBA):
    """"""
    # 在(http://www.xicidaili.com/wt/)上面收集的ip用于测试
    # 没有使用字典的原因是 因为字典中的键是唯一的 http 和https 只能存在一个 所以不建议使用字典
    
    
    COUNT = 20
    def __init__(self ): 
        
        userid_patter_str = '\d{5,}' #提取用户id的正则表达式
        self.userid_patter = re.compile(userid_patter_str)
        self.usernum = self.COUNT # 每20条写一次数据库　＃这个可以不用了，因为有ｅｘｃｕｔｅｍａｎｙ，执行的更快
        self.commentnum = self.COUNT # 每20条写一次数据库
        super(FetchComment, self).__init__()
        

    

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
            PRIMARY KEY(id) ,
            UNIQUE (userid, updated)
        )ENGINE=myisam auto_increment=1 DEFAULT CHARSET=utf8mb4;""".format(appid)
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
            jieba tinyint default 0,
            PRIMARY KEY(id) ,
            UNIQUE KEY uniq_comment(updated, appid, userid)
        )ENGINE=myisam auto_increment=1 DEFAULT CHARSET=utf8mb4;""" 
        print(time.ctime())
        self.cursor.execute(createsql)
        self.db.commit()
        print(time.ctime())

    


    def create_appleuser_tb(self):
        """如果评论用户表table不存在则创建，为每个app创建一个表，共9万多个表，因为有9万多个APP"""
        createsql = """CREATE TABLE if not exists appleuser( 
                                              id int(32) ,  
                                              name varchar(1024),
                                              userid int(32),
                                              clean int default 0,
                                              PRIMARY KEY(userid))ENGINE=myisam ;""" 
        
        self.cursor.execute(createsql)
        self.db.commit()
    
    def create_appinstore_record_tb(self):
        """
        APP 在apple store的上下架记录
        mark = 0 记录下架信息
        mark = 1 记录上架信息
        """
        createsql = """CREATE TABLE if not exists appinstore_record( 
                                              id int(32) NOT NULL auto_increment,
                                              appid int, 
                                              updated datetime, 
                                              mark int,
                                              PRIMARY KEY(id))ENGINE=myisam auto_increment=1 DEFAULT CHARSET=utf8mb4;""" 
        
        self.cursor.execute(createsql)
        self.db.commit()

    def create_newappleuser_tb(self):
        """新appleuserchuser表，之前那个appleuser表的id弄错了，旧表会逐渐移到新表中"""
        createsql = """CREATE TABLE if not exists newappleuser( 
                                              id int(32) ,  
                                              name varchar(1024), 
                                              PRIMARY KEY(id))ENGINE=myisam ;""" 
        
        self.cursor.execute(createsql)
        self.db.commit()
    
    def create_tables(self):
        """
        系统初始化的时候调用该函数创建数据库表
        """

        # 创建app上架下架记录表
        self.create_appinstore_record_tb()

    def revert_appinfo_fetched(self): 
        """将所有app设置成为未抓取状态，以便每天定时重新抓取"""
        sql = """update appinfo set fetched=0"""
        self.cursor.execute(sql)
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
        
    def insert_fakeapp(self, appid, apptitle):
        """
        放入appid和apptitle信息到刷评app池中
        """
        sql = """insert into fakeapp (appid, title, date) values(%s, %s, %s) on duplicate key update hot = hot + 1, title =values(title) """
        now = datetime.today().date()
        try:
            self.cursor.execute(sql, (appid, apptitle, now))
            self.db.commit()
        except MySQLdb.Error as e:
            pass 

    def insertmany_appleuser_tb(self, users):
        """向苹果用户表中插入用户数据，如果用户id已存在，则不插入"""
       
        insertsql = 'insert into newappleuser( id,  name) values (%s, %s) on duplicate key update name =values(name)' 
        try: 
            self.cursor.executemany(insertsql, users)
            self.db.commit()
        except MySQLdb.Error as e: 
            #self.db.rollback() 
            print ('user:{0} exist already.'.format(str(e)))
        
      
    def insert_comment_tb(self,appid,  **kwargs):
        """向每个app的评论是单独分开的，本函数是为某个app的comment表中插入数据"""
       
        insertsql = """insert ignore into t%s( userid, name, updated, title, rating, version, votesum, votecount,contenthtml, content) 
                       values (%s, "%s",%s, "%s",  %s,%s,%s,%s,%s, %s)"""
        
        insertcommentsql = """insert ignore into comment(appid, userid, name, updated, title, rating, version, votesum, votecount, content) 
                       values (%s, %s, "%s",%s, "%s",  %s,%s,%s,%s,%s)"""
 
        try:
            if 'version' in kwargs:
                self.cursor.execute(insertsql, (appid, kwargs['userid'], kwargs['name'],kwargs['updated'],
                                           kwargs['title'], kwargs['rating'], kwargs['version'], kwargs['voteSum'],
                                           kwargs['voteCount'], kwargs['contenthtml'], kwargs['content']))
                
                # 插入总comment表
                self.cursor.execute(insertcommentsql, (appid, kwargs['userid'], kwargs['name'],kwargs['updated'],
                                           kwargs['title'], kwargs['rating'], kwargs['version'], kwargs['voteSum'],
                                           kwargs['voteCount'],  kwargs['content']))
            else: 
                self.cursor.execute(insertsql, (appid, kwargs['userid'], kwargs['name'],kwargs['updated'],
                                           kwargs['title'], kwargs['rating'], '-1', kwargs['voteSum'],
                                           kwargs['voteCount'], kwargs['contenthtml'], kwargs['content']))
                
                # 插入总comment表
                self.cursor.execute(insertcommentsql, (appid, kwargs['userid'], kwargs['name'],kwargs['updated'],
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

    def insert_comment_tb_tmp(self, appid,  **kwargs):
        """向每个app的评论是单独分开的，本函数是为某个app的comment表中插入数据"""
        
        insertcommentsql = """insert  into comment(appid, userid, name, updated, title, rating, version, votesum, votecount, content) 
                       values (%s, %s, "%s",%s, "%s",  %s,%s,%s,%s,%s)"""
 
        try: 
            self.cursor.execute(insertcommentsql, (appid, kwargs['userid'], kwargs['name'],kwargs['updated'],
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
       
         
        inserttotalsql = """insert  ignore into comment(appid, userid, name, updated, title, 
                            rating, version, votesum, votecount, content ) 
                            values (%s, %s,%s,%s,%s, %s,%s,%s,%s,%s)"""

        insertsql = """insert ignore into t{}( userid, name, updated, title, rating, version,
                                               votesum, votecount,contenthtml, content) 
                       values (%s,%s,%s, %s,%s, %s,%s,%s,%s, %s)""".format(appid)

        try:  
            self.cursor.executemany(inserttotalsql, comments) 
        
            # 暂时先不插入分表了
            #self.cursor.executemany(insertsql, comments_t) 
            self.db.commit()  
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
        
    def get_app_comment_updated(self, appid):
        """
        获取appid指定的comment_updated日期
        """ 
 
        sql = 'select  id, comment_updated, title from appinfo   where id='+str(appid) + ';'
        self.cursor.execute(sql)
        appinfo = self.cursor.fetchone() 
        return appinfo
 
      
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
 
    def get_unfetched_appin(self): 
        """获取还未抓取的appid"""
        sql = """select id , comment_updated, title from appinfo where fetched = 0"""  
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps
    
    def get_appin_bysql(self, sql): 
        """获取还未抓取的appid"""
        sql = """select id , comment_updated, title from appinfo where """+sql  
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps
    
    def get_top_appin_bysql(self, sql): 
        """从top_comment_app获取还未抓取的top appid"""
        sql = """select appid, date, title from top_comment_app where """+sql  
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps
    
    def get_fakeapp_bysql(self, sql): 
        """从fakeapp获取还未抓取的top appid"""
        sql = """select appid, title from fakeapp  """+sql  
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps

    def get_unfetch_detail_appin(self): 
        """获取还未抓取的详细信息appid"""
        sql = """select id , title from appinfo where icon100 is null"""  
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps
    
    def update_appin_fetched(self, appid, url, comment_updated='' , newapptitle = False): 
        """
        更新app，fetch=1， 表示已经抓取完了
        """ 
        if newapptitle:
            if len(comment_updated) > 0:
                sql = """update  appinfo set fetched = 1, url = '{2}', title = '{3}', comment_updated="{0}" where id = {1}""".format( comment_updated, appid, url, newapptitle) 
            else:
                sql = """update  appinfo set fetched = 1, url = '{1}' , title = '{2}' where id = {0}""".format(appid, url, newapptitle)
        else:
            if len(comment_updated) > 0:
                sql = """update  appinfo set fetched = 1, url = '{2}', comment_updated="{0}" where id = {1}""".format( comment_updated, appid, url) 
            else:
                sql = """update  appinfo set fetched = 1, url = '{1}' where id = {0}""".format(appid, url) 
     
        self.cursor.execute(sql)
        self.db.commit()
    

    def update_appin_basic(self, appid, appinfo): 
        """
        更新app的artist，content，和icon信息
        """
        sql = """update  appinfo set description = %s,  seller=%s, icon100=%s where id = {0}""".format( appid)  
        self.cursor.execute(sql, (appinfo['content'], appinfo['artist'], appinfo['icon100']))
        self.db.commit()

           
    def get_all_appin(self): 
        """获取所有appid"""
        sql = """select id, title  from appinfo"""
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps

    def get_new_appin(self): 
        """获取所有appid"""
        sql = """select id, title  from appinfo where counter = 0 and icon100 is null and fetched = 0"""
        self.cursor.execute(sql)
        apps = self.cursor.fetchall() 
        return apps
    
    def recounter_allapps(self):
        """重新计算每个app的counter"""
        apps = self.get_all_appin()
        count = 0
        for app in apps: 
            count += 1
            try:
                sqlcounter = "select count(*) from t"+str(app[0])+";"
                self.cursor.execute(sqlcounter)
                counter = self.cursor.fetchone()
                updatesql = 'update  appinfo set counter = {0}, fetched = 1 where id = {1};'.format(counter[0], app[0])
                self.cursor.execute(updatesql)
                self.db.commit()
            except MySQLdb.Error as e:
                logging.error(e)
            print (count, app[0], counter[0], 'DONE')
     
class FetchJob(FetchComment):
    """
    这个类主要用来运行一些job，如：抓取
    """
    def __init__(self):
        super(FetchJob, self).__init__()

    def delall_comment_tb(self):
        return 
        apps = self.get_all_appin()
        for app in apps:
            try:
                sql = 'drop table t{}'.format(app[0])
                self.cursor.execute(sql)
                self.db.commit()
                print(app[0], 'deleted')
            except MySQLdb.Error as e:
                logging.error(e) 
            

    def get_appinfo_internet(self, appid ):
        """从api中获取appinfo的icon、content和artist数据"""
        
        templateurl = 'https://itunes.apple.com/rss/customerreviews/page=1/id={0}/sortby=mostrecent/xml?l=cn&&cc=cn'
        ns = {
            'replace_w3org' : '{http://www.w3.org/2005/Atom}',
            'replace_apple': '{http://itunes.apple.com/rss}' 
        } 
 
        url = templateurl.format(appid) 
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

    def run(self, num=0):
        """
        获取所有fetched=0的app的comment
        num参数大于0小于length时可用。
        """
        appids = self.get_unfetched_appin()
        if num > 0 and num < len(appids):
            appids = appids[:num]

        for appid in appids: 
            
            if appid[1]: 
                self.init_read(appid[0], apptitle=appid[2],lastcomment_updated=appid[1])
            else: 
                self.init_read(appid[0])
            
            print(appid[0], appid[1], appid[2], 'comment fetched')
    
    def runapps(self, apps, fake=False):
        """
        多线程方式
        fake 刷评检测标示
        """ 
        spider = Spider()  
        for appid in apps:  
            oldusercount = spider.count_all_appleusers()
            oldcommentcount = spider.count_all_comments()
            if appid[1]: 
                counter = self.init_read(appid[0], apptitle=appid[2], lastcomment_updated=appid[1], fake=fake)
            else: 
                counter = self.init_read(appid[0], apptitle=appid[2] )
            if counter > 0:
                newusercount = spider.count_all_appleusers()
                newcommentcount = spider.count_all_comments()
                spider.insert_stat_newfetched(newcomment=newcommentcount-oldcommentcount, 
                                            newuser=newusercount-oldusercount)
            print(appid[0], appid[2], 'comment fetched')
        spider.close()

    def fetch_appinfo(self):
        """
        更新appinfo的icon、description等信息
        """
        appids = self.get_unfetch_detail_appin()
       
        for appid in appids:
            appid = appid[0] 
            infos = self.get_appinfo_internet()
            if infos:
                self.update_appin_basic(appid, infos) 
                print(appid, appid[1], 'updated')

    
    def update_appinstore_record(self, appid, mark):
        """
        更新app 在apple store上的状态：上架还是下架
        """
        today = datetime.today()  
        get_last_record_sql = "select appid, mark from appinstore_record where appid={0} order by id desc limit 1".format(appid)
        insert_app_record_sql = "insert into appinstore_record (appid, mark, updated) values ({0}, {1}, '{2}')"
        self.cursor.execute(get_last_record_sql)
        apps = self.cursor.fetchall() 
        
        numbers = len(apps)
        if numbers == 0: 
            insert_app_record_sql = insert_app_record_sql.format(appid, mark, today)
            self.cursor.execute(insert_app_record_sql)
            self.db.commit()
        elif numbers == 1: 
            if apps[0][1] != mark:
                insert_app_record_sql = insert_app_record_sql.format(appid, mark, today)
                self.cursor.execute(insert_app_record_sql)
                self.db.commit()

        return apps

    def get_app_title(self, appid):
        """
        在 app的介绍页中获取app的最新title
        """
        appurl = "https://itunes.apple.com/cn/app//id{0}?mt=8&ign-mpt=uo%3D2".format(appid)
        apppage = requests.get(appurl, proxies={'http':random.choice(self.pro)}) 
        if apppage.status_code == 403:
            return ''
        
        tree = html.fromstring(apppage.content)
        results = tree.xpath('//h1[contains(@class, "product-header__title app-header__title")]')
      
        titles = [title for title in results] 
        if titles:
            title = titles[0].text.strip()
            print(str(appid) + " : " + title)
            return title
        else:
            return ''

    def init_read(self, appid, apptitle='', lastcomment_updated=None, fake=False):
        """
        抓取某个app的评论
        lastcomment_updated 为app上次抓取到最新评论的时间,如果是首次抓取时，或者之前没有抓取到评论时，该值为None
        fake 刷评检测标志，如果fake为真时，则开启刷评检测
        """
        # 最有帮助的评论链接
        mosthelpful = 'https://itunes.apple.com/rss/customerreviews/page=1/id=350962117/sortby=mosthelpful/xml?l=cn&&cc=cn'
        # 最新评论的评论链接
        templateurl = 'https://itunes.apple.com/rss/customerreviews/page={0}/id={1}/sortby=mostrecent/xml?l=cn&&cc=cn'
        ns = {
            'replace_w3org' : '{http://www.w3.org/2005/Atom}',
            'replace_apple': '{http://itunes.apple.com/rss}' 
        }
        count = 0
        items = [] # comment记录
        items_t = [] # t23893238239记录
        users = [] # 评论用户

        appurl = '' # app的url
        newtitle = '' # app的新title
        
        comment_updated = '' #最新评论时间

        start = time.ctime()
        breakmark = False

        # 最新评论默认在第1页，但是当第一页出现page error时，最新评论应该出现在下一页
        page_lastest_comment = 1
        page_lastest_comment_mark = False # 代表是否被更新过，更新过之后，该值为True
        appversion = ""
 
        new_apptitle = self.get_app_title(appid)
        #  app上下架记录标记
        #  默认是被下架的APP
        appinstore_record_mark = 0 

        for pagei in range(1, 11):

            if breakmark: # 已经取完最新的数据了，结束for循环
                break
            url = templateurl.format(pagei, appid)
            
            results_xml = requests.get(url, proxies={'http':random.choice(self.pro)})
            print(str(appid)+': status code : '+str(results_xml.status_code))
            if results_xml.status_code == 403:
                return

            results_text = results_xml.content.decode('utf-8', 'ignore')
          
            try:
                root = ET.fromstring(results_text )
            except ET.ParseError:
                # 如果第一页有问题，就可能导致comment_update不能更新
                continue # xml文件本身有问题

            
            if not page_lastest_comment_mark: 
                page_lastest_comment = pagei
                page_lastest_comment_mark = True

            userid = ''
            authorname = ''
            updated = ''
            title = ''
            content = ''
            rating = ''
            voteSum = ''
            voteCount = ''
            version = ''
            entry_count = 0
            for entries in root.iter(ns['replace_w3org']+'entry'): 
               
                if breakmark:
                    break
                entry_count += 1
                appinstore_record_mark = 1 
                """
                if entry_count ==1:
                    # 第一个entry是app信息 抓取app的信息之后，跳过
                    # 抓取app的信息：title ，artistname，url
                    if pagei == page_lastest_comment:
                        # 在第一页，查找appinfo
                        for entry in  entries:
                            tag = entry.tag.replace(ns['replace_apple'],'')
                            tag = tag.replace(ns['replace_w3org'],'')
 
                            if tag == 'id' : 
                                appurl = entry.text 
                            if tag == 'name' : 
                                new_apptitle = entry.text
                    continue # 第一个entry是app信息 跳过
                """
                itementry = {} 
                user = {}   
                for entry in  entries:  
                    tag = entry.tag.replace(ns['replace_apple'],'')
                    tag = tag.replace(ns['replace_w3org'],'')
                    
                    if tag == 'id' : # 这里的id是comment id
                        pass 
                    if tag =='author':  
                        for name in entry.iter(ns['replace_w3org']+'name'):
                            authorname = name.text
                            itementry['name'] = authorname # 用户名
                            user['name'] = authorname

                        for uri in entry.iter(ns['replace_w3org']+'uri'):
               
                            match = re.search('\d+', uri.text)
                            if match: 
                                userid = match.group() 
                                itementry['userid'] = userid # 用户id
                                user['userid'] = userid
                            else:  
                                break # 非用户评论 
                    if  tag=='version': 
                        version = entry.text
                        if pagei == 1:
                            appversion = version
                        itementry['version'] = version # 应用版本
                             
                    if tag =='updated':
                        
                        updated = entry.text[:19].replace('T', ' ')
                        itementry['updated'] = updated # 用户评论日期
                         
                         
                        if pagei == page_lastest_comment and entry_count == 2: # page为第一页的第二个entry就是最新的  
                            comment_updated = updated
                
                        if lastcomment_updated: # 
                            # 当lastcomment_updated不为None的时候，代表执行的是每日的例行抓取工作
                            # 而不是新来的app进行抓取，注意的是，如果以前抓取的时候，未抓取到任何
                            # 评论数据时，lastcomment_updated也是为空的
                            # updatedtime = datetime.strptime('1993-12-09 18:55:44', '%Y-%m-%d %H:%M:%S')
                            updatedtime = datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')
                            
                            if updatedtime == lastcomment_updated: # 代表取到了上次抓取时抓取的最后一条评论了
                                breakmark = True # 停止所有循环 
                                break
                            elif updatedtime < lastcomment_updated:
                                breakmark = True # 停止所有循环
                                break

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
                        itementry['voteSum'] = voteSum #  

                    if  tag=='voteCount': 
                        voteCount = entry.text
                        itementry['voteCount'] = voteCount #  

                    
                      
                # 将apple用户+新提取的评论插入数据库
                if userid and authorname and not breakmark:
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
              
        
            if count == 0: # 没有抓取到数据，不需要继续往后走了
                break
        # 数据已全部抓取完毕，更新appinof表的counter字段 
         
        
        if count > 0:   
            self.insertmany_appleuser_tb(users)
            self.insertmany_comment_tb(appid, items, items_t)
            self.add_comment_couter(count, appid) 
            appinstore_record_mark = 1
         
            self.update_appin_fetched(appid, appurl, comment_updated)
            print(appid, count, apptitle)
              
            if fake and count > 300: # 刷评检测开启，本次抓取的时候超过400
                # 将app放入刷评app池
                self.insert_fakeapp(appid, apptitle)

        else:
            self.update_appin_fetched(appid, appurl)
        
        # 更新app上下架记录 
        self.update_appinstore_record(appid, appinstore_record_mark)


        if new_apptitle != apptitle and new_apptitle != '': 
            # 跟踪标题修改记录 
            print('OLD : ' + apptitle + ' NEW: ' + new_apptitle)
            f = FetchAppinfo() 
            f.get_latest_title(appid=appid, version=appversion, title=new_apptitle)
            f.update_appin(appid, new_apptitle) 
            f.close()


        # 返回本次抓取的数量
        return count    

    def find_duplicates(self):
        """查找所有有重复数据的table"""
        apps = self.get_all_appin()
        count = 0
        for app in apps:
            count += 1
            sql = """select count(*) from t{} group by userid, updated having count(*) >1;""".format(app[0])
            self.cursor.execute(sql)
            counts = self.cursor.fetchall()
            
            if len(counts) > 0:
                # 有重复数据
                logging.info('Dup:'+str(app[0]))
               
            else:
                addindexsql = """alter table t{0} drop index t{1}index , add unique index t{2}index (userid, updated)""".format(app[0], app[0], app[0])
                try:
                    self.cursor.execute(addindexsql)
                except MySQLdb.Error as e:
                    logging.error(e)
            print (count, app[0])

    def find_new_ones(self):
        """找出那些还没有创建分表的数据"""
        apps = self.get_new_appin()
         
        count = len(apps)
        for app in apps:
            count -= 1
            sql = "select count(*) from t{}".format(app[0])
            print (count, app[0])
            try:
                self.cursor.execute(sql)
            except MySQLdb.Error as e:
                update ='update appinfo set fetched = 2 where id = ' + str(app[0])
                self.cursor.execute(update)
                self.db.commit()
                 
def job(apps):
    f = FetchJob()
    f.runapps(apps)
    f.close()
    print(str(threading.current_thread())+'Done')

def fetchedone(appid, fake=False):
    """
    抓取某个app的最新comment
    350962117, 414478124, 398453262, 989673964
    未抓取的app：368377690
    """

    f = FetchJob()
    #　测试抓取微信的数据

    spider = Spider()
    """ 
    f.find_duplicates()
    """
    oldusercount = spider.count_all_appleusers()
    oldcommentcount = spider.count_all_comments()

    appinfo = f.get_app_comment_updated(appid)
    print(appinfo[0], appinfo[1])
    count = 0
    if appinfo[1]: 
        count = f.init_read(appinfo[0], apptitle=appinfo[2], lastcomment_updated=appinfo[1], fake=fake)
    else:
        # 新来的app不对其进行刷评行为检测
        #f.create_tb(appinfo[0]) # 1 创建app的comment表
        count = f.init_read(appinfo[0], apptitle=appinfo[2])

    if count > 0: # 如果抓取的数量大于0，这时才有必要更新统计数据
        newcommentcount = spider.count_all_comments()
        newusercount = spider.count_all_appleusers()
        spider.insert_stat_newfetched(newcomment=newcommentcount-oldcommentcount, 
                                    newuser=newusercount-oldusercount)
    spider.close() 
    f.close()

@timelog
def fetchall(revert = False, num=None, sql=None):
    """重新抓取所有app的comment"""
    f = FetchJob()
    if revert and sql is None:
        f.revert_appinfo_fetched()
    
    if sql:
        apps = f.get_appin_bysql(sql)
    else:
        apps = f.get_unfetched_appin() 
    
    # 抓取指定数量的app
    if num:
        apps = apps[:num]

   
    length = len(apps)
    step = int(length/4)

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
 
   
    f.close()

@timelog
def fetchallwithout_thr(revert = False, num=None, sql=None):
    """无线程方式获取app"""
    f = FetchJob()
    if revert and sql is None:
        f.revert_appinfo_fetched()
    
    if sql:
        apps = f.get_appin_bysql(sql)
    else:
        apps = f.get_unfetched_appin() 
   
    
    ts = []
    spider = Spider()
    oldusercount = spider.count_all_appleusers()
    oldcommentcount = spider.count_all_comments()
    f.runapps(apps) 
    newusercount = spider.count_all_appleusers()
    newcommentcount = spider.count_all_comments()
    spider.insert_stat_newfetched(newcomment=newcommentcount-oldcommentcount, 
                                  newuser=newusercount-oldusercount)
 

    f.close()
def readlog():
    f = open('fetchapp.log', 'r')
    text = f.read()
    reid='\d{6,}'
    appids = re.findall(reid, text)
    if appids:
        for appid in appids:
            fetchedone(appid)
    f.close()

def fetch_new_without_thr(revert = False, num=None, sql=None, fake=False):
    """
    无线程方式获取app
    fake 表示是否开启刷评检测，默认为否
    """
    f = FetchJob()
    if revert and sql is None:
        f.revert_appinfo_fetched()
    
    if sql:
        apps = f.get_appin_bysql(sql)
    else:
        apps = f.get_unfetched_appin() 
     
    start = time.ctime()
    
    ts = []
 
    f.runapps(apps, fake=fake)
    print ('fetched done' )
  
 
    print('start:', start)
    print('end  :', time.ctime())
    f.close()


@timelog
def fetch_new_without_thr_top(sql, num=None, fake=False ):
    """
    在top表中，无线程方式获取app
    num 代表抓取前num个app的评论
    fake 刷评检测标示
    """
    f = FetchJob()  
    apps = f.get_top_appin_bysql(sql)
     
    if num:
        apps = apps[:num]
    
    for app in apps:
        fetchedone(app[0], fake=fake)
    f.close()

def fetchfakeapp(sql, fake=False):
    """
    在fakeapp表中，无线程方式获取app
    
    fake 刷评检测标示
    """
    f = FetchJob() 
     
    apps = f.get_fakeapp_bysql(sql)
  
    start = time.ctime() 
    for app in apps:
        fetchedone(app[0], fake=fake)
    print ('fetched done' ) 
 
    print('start:', start)
    print('end  :', time.ctime())
    f.close()

def readlog():
    f = open('fetchapp.log', 'r')
    text = f.read()
    reid='\d{6,}'
    appids = re.findall(reid, text)
    if appids:
        for appid in appids:
            fetchedone(appid)
    f.close()
if __name__ == "__main__":
    """
    for i in range(1, 5):
        try:
            fetch_new_without_thr_top(sql="  counter  > 1000  group by appid", fake=True )  
            break
        except :
            continue
    """
    
    fetchedone(382201985) #548608066fe
    
    #fetch_new_without_thr_top(sql=" counter > 100  group by appid", fake=True ) 
    fetch_new_without_thr(sql="counter >= 100  and fetched != 1 ", fake=True)
    for i in range(1, 5):
        try:
            #fetch_new_without_thr(sql="fetched = 1 and counter >= 500  ", fake=True)
            break
        except :
            continue 
     
     
    """
    fetch = FetchComment()
    fetch.create_tables()
    """  
    """

    
    #fetch_new_without_thr(sql="counter < 2000  and counter > 500 ", fake=False)
    #fetch_new_without_thr(sql="category = 6014", fake=True) # 抓取游戏分类下的评论
    #fetch_new_without_thr(sql="category = 6015", fake=True)
      
    f = FetchJob()
    ark={'userid':1234567, 
          'name':'zhangjiwei', 00
          'updated':'2017-12-29 12:34:56', 
          'title':'test', 
          'rating':5, 
          'version':'1.1', 
          'voteSum':10, 
          'voteCount':23, 'content':'this is test'}
    f.insert_comment_tb_tmp(281796108, **ark)



    """
    #f.create_newappleuser_tb()
    #f.insert_fakeapp(342994828, '腾讯斗地主') 
    #fetch_new_without_thr(sql='fetched = 2')
    #f.close()
    #readlog()
    #fetchall(  num=50)
    
 