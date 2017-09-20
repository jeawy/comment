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
import logging
from datetime import datetime


logging.basicConfig(level=logging.INFO, datefmt='%a, %d %b %Y %H:%M:%S', 
                     filename='fetchapp.log', filemode='a+')


class Spider(object):
    
    def __init__(self):

        self.category_json = []
        self.categoryitem = {}

        self.subcategory_json = []
        self.subcategoryitem = {}
        db = MySQLdb.connect(host="192.168.1.101",user="root",passwd="comment",db="comment",charset="utf8mb4") 
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
    
    def create_statistics_stat_newfetched(self):
        """创建数据统计表之：每天新抓取的APP数量、每天新抓取的评论数量、以及每天新抓取的用户数量"""
        sql = """create table  if not exists stat_newfetched( 
            date date,
            new_app_num  int default 0,
            new_comment_num  int default 0,
            new_user_num  int default 0,
            primary key(date)
        ) engine=myisam"""
        self.cursor.execute(sql)
        self.db.commit()
    
    def create_title_tb(self):
        """创建一个title表，记录app在不同时期使用的不同title名称"""
        sql = """create table if not exists apptitle (
                    id int(32) not null auto_increment,
                    appid int,
                    title varchar(100),
                    updated date,
                    primary key (id),
                    unique key apptitleindex(appid, title)
                ) engine=myisam auto_increment=1 default charset=utf8;
              """
        self.cursor.execute(sql)
        self.db.commit()
    
    def create_trigger_title(self):
        """创建trigger 在apptitle中，在更新title的时候触发 """
        pass

    def insert_stat_newfetched(self, newapp=0, newuser=0, newcomment=0):
        """更新统计表stat_newfetched， 更新新的用户数量，app数量，评论数量"""
        datenow = datetime.today().date().strftime('%Y-%m-%d')
        if newapp > 0:
            insertnewappsql = """insert into stat_newfetched (date, new_app_num) 
                                 values(%s, %s) on duplicate key update new_app_num =new_app_num+values(new_app_num) """
            self.cursor.execute(insertnewappsql, (datenow, newapp ))
        if newuser > 0:
            insertnewappsql = """insert into stat_newfetched (date, new_user_num) 
                                 values(%s, %s) on duplicate key update new_user_num =new_user_num+values(new_user_num) """
            self.cursor.execute(insertnewappsql, (datenow, newuser))
        if newcomment > 0:
            insertnewappsql = """insert into stat_newfetched (date, new_comment_num) 
                                 values(%s, %s) on duplicate key update new_comment_num =new_comment_num+values(new_comment_num) """
            self.cursor.execute(insertnewappsql, (datenow, newcomment))
        if newapp > 0 or newuser>0 or newcomment >0:
            self.db.commit()
    
  
    def insert_category(self, **kwargs):
        """在APP信息表中插入一条新记录"""
        intsersql = """insert into category (id, name, parentid, url)
                      values (%s, %s, %s, %s)"""
        try:
            self.cursor.execute(intsersql, (kwargs['categoryid'],kwargs['text'],
                                 kwargs['parentid'],kwargs['href'])) 

        except UnicodeDecodeError as e:
            print(e, kwargs['categoryid'])
        except MySQLdb.IntegrityError:
            print('duplicated:', kwargs['categoryid'])
        
        self.db.commit()

    def insert_category_db(self):
        """分类插入数据库"""
        self.insert_category(**self.subcategoryitem)
        self.insert_category(**self.categoryitem)

    
            

    def insert_appinfo(self, id, name, categoryid):
        """在appinfo 中一条数据，只插入id和name以及抓取的日期"""
        datenow = datetime.today().date().strftime('%Y-%m-%d')
        intsersql = """insert into appinfo (id, title, category, fetched_date  )
                      values (%s, %s, %s, %s) """
        try:
            self.cursor.execute(intsersql, (id, name, categoryid,datenow)) 
        except UnicodeDecodeError as e:
            print(e, id)
        except MySQLdb.IntegrityError:
            print('duplicated:', id) 
        self.db.commit()
    
    def insertmany_appinfo(self, args):
        """
        在appinfo 中多条数据，只插入id和name
        insert many 要比insert快300多倍
        args = [(id, name, categoryid), (id, name, categoryid)]
        """
        intsersql = """insert into appinfo (id, title, category)
                      values (%s, %s, %s) on duplicate key update title = values(title)"""
        count = 0
        try:
            self.cursor.executemany(intsersql, args) 
        except UnicodeDecodeError as e:
            logging.info(e)
        except MySQLdb.IntegrityError as e:
            logging.info(e) 
        self.db.commit()

    def count_category_apps(self, categoryid):
        """
        count and return 某category在app数量
        """
        sql = "select count(*) from appinfo where category={}".format(categoryid)
        self.cursor.execute(sql)
        count = self.cursor.fetchone()
        return count[0]
    
    def count_all_apps(self):
        """
        count and return all app数量
        """
        sql = "select count(*) from appinfo " 
        self.cursor.execute(sql)
        count = self.cursor.fetchone()
        return count[0]

    def count_all_appleusers(self):
        """
        count and return all appleuser数量
        """
        sql = "select count(*) from appleuser " 
        self.cursor.execute(sql)
        count = self.cursor.fetchone()
        return count[0]
    
    def count_all_comments(self):
        """
        count and return all comment数量
        """
        sql = "select count(*) from comment " 
        self.cursor.execute(sql)
        count = self.cursor.fetchone()
        return count[0]

    
 
    def create_appinfo(self): 
        
        """创建APP信息表:
           fetched : 是否被抓取过，0表示没有被抓取过，1表示已经抓取过了，每天定时会将该字段重置为0， 以便重新抓取
           comment_updated ： 最新评论的日期
           counter: 评论总数
           free: true,表示是免费app，false代表付费app
           oldcounter: 记录首次抓取时抓取的数量
           activefactor: app的活动因子，首次抓取时抓取的评论数量为500的，activefactor标记为1，
                         以后每天抓取的评论数量来更新这个活动因子，来表明这个app的活跃度

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
            oldcounter int,
            activefactor int default 0,
            free boolean, 
            fetched_date date,
            primary key(id)
            ) engine=InnoDB ;"""
        self.cursor.execute(sql)
        self.db.commit()
    

    def update_category_fetched(self, id): 
        """修改类别信息表，标记指定类别的app信息已获取完毕"""
        sql = """update category set fetched=1 where id={}""".format(id)
        self.cursor.execute(sql)
        self.db.commit()

    def revert_category_fetched(self): 
        """将所有category设置成为未抓取状态，重新抓取"""
        sql = """update category set fetched=0"""
        self.cursor.execute(sql)
        self.db.commit()
    
    def get_unfetched_category(self): 
        """修改类别信息表，标记指定类别的app信息已获取完毕"""
        sql = """select id, name, url from category where fetched = 0"""
        self.cursor.execute(sql)
        categories = self.cursor.fetchall() 
        return categories

    def get_category_byid(self, id):
        """根据类别ID获得类别"""
        sql = """select id, name, url from category where id = {}""".format(id)
        self.cursor.execute(sql)
        categories = self.cursor.fetchall() 
        return categories

class SpiderRun(Spider):
    """
    这个类主要以多线程的方式，运行job：
    job1：每天抓取各个分类下新上榜的APP的id以及title更新到数据库中，以便其他程序根据id去
          抓取这个app的评论
    """

    def get_category_internet(self):
        """
        获取所有分类的信息，主要包括分类名称、id、链接、
        其中分类的链接中可以查看到该分类下所有的app以及所有热门APP
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

    def getapps(self, categoryurl, categoryid):
        """获取某个分类下所有热门APP的appid"""
       
        response = requests.get(categoryurl)
        tree = html.fromstring(response.content)
        results = tree.xpath('//div[@id= "selectedcontent"]/div/ul/li/a')

        re_id = 'id\d+\?'
       
        for result in results: 
            appid = re.search(re_id, result.get('href')).group()
            appid = appid.replace('id', '')
            appid = appid.replace('?', '')
            appname = result.text
            self.insert_appinfo(appid, appname, categoryid)
            

    def get_daily_hot_apps(self):
        """
        获取每日免费和付费热门中的app，并加入appinfo表中
        #　每天要执行这个程序来抓取最新热门的免费和付费的ａｐｐ
        """ 
        # 免费app
        oldcount = self.count_all_apps()
        url = 'https://www.apple.com/cn/itunes/charts/free-apps/'
        self.analyse_daily_apps(url)
        # 收费app
        url = 'https://www.apple.com/cn/itunes/charts/paid-apps/'
        self.analyse_daily_apps(url)
        newcount = self.count_all_apps()
        self.insert_stat_newfetched(newapp=newcount-oldcount)
    
    def analyse_daily_apps(self, url):
        """
        获取每日付费热门中的app，并加入appinfo表中
        """ 
        # url = 'https://www.apple.com/cn/itunes/charts/paid-apps/'
        #url = 'https://www.apple.com/cn/itunes/charts/free-apps/'
        response = requests.get(url)
        tree = html.fromstring(response.content)
        results = tree.xpath('//section[contains(@class, "apps")]/ul/li')
        apps = []

        # 提取ID的正则表达式
        re_id = 'id\d+'
        count = 0
        for result in results:
            
            # 获取appid和app名称
            app_a = result.xpath('h3/a')[0]
            match = re.search(re_id, app_a.get('href')) 
            appid = match.group().replace('id', '')
            title = app_a.text

            category_a = result.xpath('h4/a')[0]
            match = re.search(re_id, category_a.get('href'))
            categoryid = match.group().replace('id', '')

            apps.append((appid, title, categoryid))
            count += 1
            print(count, appid, title, categoryid)
             
        self.insertmany_appinfo(apps)
    def re_fetch_all_category_apps(self):
        self.revert_category_fetched()
        # 获取还没有抓取过app的分类 
        categories = self.get_unfetched_category()
        
        length = len(categories)
        totalcount = 0
        for category in categories:
            url = category[2] 
            # 开始抓取每个分类的热门app
            # 获取抓取之前category app的数量
            oldcount = self.count_category_apps(category[0])
            # 开始抓取APP
            self.getapps(url, category[0])
            # 获取抓取之后category app的数量
            newcount = self.count_category_apps(category[0])
            totalcount += newcount-oldcount # 更新总共新抓取的app 数量
            logging.info('fetched {}'.format(category[0]))
            self.update_category_fetched(category[0])

        self.insert_stat_newfetched(newapp=totalcount)

class SpiderRunTest(SpiderRun):  
    def test_fetch_category_apps(self): 
        categories = self.get_category_byid(6006)
 
        for category in categories:
            url = category[2] 
            # 开始抓取每个分类的热门app
            oldcount = self.count_category_apps(category[0])
            self.getapps(url, category[0])
            newcount = self.count_category_apps(category[0])
            logging.info('fetched {}'.format(category[0]))
            self.update_category_fetched(category[0])
            count = newcount-oldcount
            self.insert_stat_newfetched(newapp=count)
        
        print (newcount, oldcount)
if __name__=="__main__":
    spider = SpiderRun()
    spider.get_daily_hot_apps()
    spider.re_fetch_all_category_apps()
    spider.close()
