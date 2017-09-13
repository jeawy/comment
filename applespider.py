# -*- coding:utf-8 -*-
"""
从苹果官网中获取app的信息，主要是app的id
app的id将来用来获取评论数据
"""
import requests
import re
import pdb 
from lxml import html
from lxml import etree
import MySQLdb

class Spider(object):
    
    def __init__(self):

        self.category_json = []
        self.categoryitem = {}

        self.subcategory_json = []
        self.subcategoryitem = {}
        db = MySQLdb.connect(host="192.168.1.103",user="root",passwd="comment",db="comment",charset="utf8mb4") 
        #db = MySQLdb.connect(host="localhost",user="root",passwd="sqlroot",db="comment",charset="utf8mb4") 
        #db = MySQLdb.connect("localhost", 'root', 'sqlroot', 'Comment', 'utf8')
        self.db = db
        self.cursor = self.db.cursor() 
        self.insertnum = 50 #
    def close(self):
        self.db.close()
    def create_category_tb(self):
        """创建分类信息表"""
        sql = """create table if not exists category(
            id int(32) not null, 
            name varchar(4096),
            parentid int(32),
            url varchar(4096), 
            fetched boolean,
            primary key(id)
            ) engine=InnoDB  DEFAULT CHARSET=utf8mb4;"""
        self.cursor.execute(sql)
        self.db.commit()

    def insert_category(self, **kwargs):
        """在APP信息表中插入一条新记录"""
        intsersql = """insert into category (id, name, parentid, url)
                      values (%s, %s, %s, %s)"""
        try:
            self.cursor.execute(intsersql, (kwargs['categoryid'],kwargs['text'],kwargs['parentid'],kwargs['href'])) 
        except UnicodeDecodeError as e:
            print(e, kwargs['categoryid'])
        except MySQLdb.IntegrityError:
            print('duplicated:', kwargs['categoryid'])
        
        self.db.commit()

    def insert_category_db(self):
        """分类插入数据库"""
        self.insert_category(**self.subcategoryitem)
        self.insert_category(**self.categoryitem)

    def getapps(self, categoryurl, categoryid):
        """获取某个分类下所有热门APP的appid"""
       
        response = requests.get(categoryurl)
        tree = html.fromstring(response.content)
        results = tree.xpath('//div[@id= "selectedcontent"]/div/ul/li/a')

        re_id = 'id\d+\?'
        count = 0
         
        for result in results: 
            appid = re.search(re_id, result.get('href')).group()
            appid = appid.replace('id', '')
            appid = appid.replace('?', '')
            appname = result.text
            self.insert_appinfo(appid, appname, categoryid)
            count += 1
            print('app ', appid, 'inserted, count：', count)
            

    def insert_appinfo(self, id, name, categoryid):
        """在appinfo 中一条数据，只插入id和name"""
        intsersql = """insert into appinfo (id, title, category)
                      values (%s, %s, %s)"""
        try:
            self.cursor.execute(intsersql, (id, name, categoryid)) 
        except UnicodeDecodeError as e:
            print(e, id)
        except MySQLdb.IntegrityError:
            print('duplicated:', id) 
        self.db.commit()

    def get_category_internet(self):
        """
        获取所有分类的信息，主要包括分类名称、id、链接、其中分类的链接中可以查看到该分类下所有的app以及所有热门APP
        """
        # apple store 中列出所有分类的url
        category_list_url = 'https://itunes.apple.com/cn/genre/ios/id36?mt=8'

        # 提取分类地址的正则表达式
        re_id = 'id\d+\?'
         
        response = requests.get(category_list_url)
        tree = html.fromstring(response.content)
        results = tree.xpath('//a[contains(@class, "top-level-genre")]')
        print('start get categories from internet..')

        for result in results:
            self.subcategoryitem = {}
            self.categoryitem ['href'] = result.get('href')
            self.categoryitem ['text'] = result.text

            # 获取类别ID
            match = re.search(re_id, self.categoryitem['href'])
            categoryid = match.group().replace('id', '')
            categoryid = categoryid.replace('?', '')
            self.categoryitem ['categoryid'] = categoryid
            self.categoryitem ['parentid'] = -1 # -1 代表没有父分类
            
            print('ID',categoryid)
            self.category_json.append(self.categoryitem)
            next_ele = result.getnext()
            
            if next_ele is not None:# 如'游戏': 有子分类 
                self.subcategory_json = []
                ul = result.getnext()
                lis = ul.getchildren()
                for li in lis:
                    a = li.getchildren()[0]
                    self.subcategoryitem ['parentid'] = categoryid
                    self.subcategoryitem ['href'] = a.get('href')
                    self.subcategoryitem ['text'] = a.text

                    match = re.search(re_id, self.subcategoryitem['href'])
                    categoryid = match.group().replace('id', '')
                    categoryid = categoryid.replace('?', '')

                    self.subcategoryitem ['categoryid'] = categoryid 
                    self.subcategory_json.append(self.subcategoryitem)
                    self.subcategoryitem = {}

                    print('ID',categoryid)
        print('finished to get categories from internet..')

    def create_appinfo(self): 
        
        """创建APP信息表:
           fetched : 是否被抓取过，0表示没有被抓取过，1表示已经抓取过了，每天定时会将该字段重置为0， 以便重新抓取
           comment_updated ： 最新评论的日期
           counter: 评论总数
        """
        sql = """create table if not exists appinfo(
            id int(32) not null, 
            title varchar(4096),
            version varchar(128),
            description text,
            seller varchar(1024),
            artistname varchar(1024),
            icon100 varchar(4096),
            category int(32),
            fetched boolean default 0,
            comment_updated datetime, 
            counter int default 0,
            primary key(id)
            ) engine=InnoDB ;"""
        self.cursor.execute(sql)
        self.db.commit()
    
    def update_appinfo(self): 
        """修改APP信息表"""
        sql = """alter table appinfo add column updated datetime"""
        self.cursor.execute(sql)
        self.db.commit()
    
    def update_category_fetched(self, id): 
        """修改类别信息表，标记指定类别的app信息已获取完毕"""
        sql = """update category set fetched=1 where id={}""".format(id)
        self.cursor.execute(sql)
        self.db.commit()
    
    def get_unfetched_category(self): 
        """修改类别信息表，标记指定类别的app信息已获取完毕"""
        sql = """select id, name, url from category where fetched = 0"""
        self.cursor.execute(sql)
        categories = self.cursor.fetchall() 
        return categories

class SpiderRun(Spider):
    """
    这个类主要以多线程的方式，运行job：
    job1：每天抓取各个分类下新上榜的APP的id以及title更新到数据库中，以便其他程序根据id去
          抓取这个app的评论
    """
    pass