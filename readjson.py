# -*- coding:utf-8 -*-
import pdb
import MySQLdb
import re
import ijson
import time

class Readjson(object):
    """"""
    COUNT = 100
    def __init__(self):
        db = MySQLdb.connect(host="192.168.1.103",user="root",passwd="comment",db="comment",charset="utf8mb4") 
        #db = MySQLdb.connect(host="localhost",user="root",passwd="sqlroot",db="comment",charset="utf8mb4") 
        #db = MySQLdb.connect("localhost", 'root', 'sqlroot', 'Comment', 'utf8')
        self.db = db
        self.cursor = self.db.cursor() 
        self.insertnum = self.COUNT # 每COUNT条写一次数据库

    def create_app_tb(self):
        """创建APP信息表"""
        sql = """create table if not exists app(
            id int(32) not null, 
            title varchar(4096),
            version varchar(128),
            description text,
            seller varchar(1024),
            artistname varchar(1024),
            icon100 varchar(4096),
            category varchar(128),
            primary key(id)
            ) engine=InnoDB DEFAULT CHARSET=utf8mb4;"""
        self.cursor.execute(sql)
        self.db.commit()

    def insert_app(self, **kwargs):
        """在APP信息表中插入一条新记录"""
        intsersql = """insert into app (id, title, category, description, version,  seller, artistname, icon100)
                      values (%s, %s, %s, %s, %s, %s, %s, %s)"""
        try:
            self.cursor.execute(intsersql, (kwargs['id'],kwargs['title'],kwargs['category'],kwargs['description'],
                                       kwargs['version'],kwargs['seller'],kwargs['artist_name'],kwargs['icon100'],)) 
        except UnicodeDecodeError as e:
            print(e, kwargs['id'])
        except MySQLdb.IntegrityError:
            print('duplicated:', kwargs['id'])

        if self.insertnum <=0:
            self.db.commit()
            self.insertnum = self.COUNT
        else:
            self.insertnum -= 1

        
    def readappinfo(self, filename):
        start = time.ctime()
        count = 0
        item ={}
        for prefix, the_type, value in ijson.parse(open(filename, 'r')):
            if value:
                if type(value) == str:
                    try:
                        value = bytes(value, 'utf8', 'ignore').decode('utf8')
                    except UnicodeEncodeError as e:
                        print(e)
            else:
                continue
            
            if prefix == 'item.fields.store_id': 
                item['id'] = value # 1
            elif prefix == 'item.fields.title' and value != 'Unknown':
                item['title'] = value # 2
            elif prefix == 'item.fields.category':
                item['category'] = value # 3
            elif prefix == 'item.fields.description':
                item['description'] = value # 4
            elif prefix == 'item.fields.current_version':
                item['version'] = value # 5
            elif prefix == 'item.fields.seller':
                item['seller'] = value # 6
            elif prefix == 'item.fields.artist_name':
                item['artist_name'] = value # 7
            elif prefix == 'item.fields.icon_url100':
                item['icon100'] = value # 8
            elif prefix == 'item.pk':
                if( 'id' in item.keys() and 'title' in item.keys() and
                    'category' in item.keys() and
                    'description' in item.keys() and 
                    'version' in item.keys() and 'seller' in item.keys()
                    and 'artist_name' in item.keys() and 'icon100' in item.keys() ):
                    self.insert_app(**item)
                    print('inserted id ', item['id'])
                    item = {}
                    count += 1
                    continue
        print (start)
        print (count)
        print(time.ctime())