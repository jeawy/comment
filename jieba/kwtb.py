#！ -*- coding:utf-8 -*- 
import sys
import pdb
import codecs
import MySQLdb

sys.path.insert(0, '..')
from conn import ConnectDBA

class CommentJiebaDB(ConnectDBA):
    """
    create kw tables
    """
    STOP_WORDS = 0
    COMPONENT_WORDS = 1
    STOPWORDS_FILE = 'stopwords.txt'
    COMPONENT_FILE = 'component.txt'
    KEYWORD_FILE = 'keywords.txt'


    def create_kw(self):
        """
        create kw table 来自结巴
        """
        sql = """
              create table if not exists kw (
                  id int(32) not null auto_increment,
                  keyword varchar(64),
                  counter int default 1,
                  mark int default 0,
                  primary key (id),
                  unique index kw_index (keyword)
              )engine=myisam auto_increment = 1 default charset = utf8;
        """
        self.cursor.execute(sql)
        self.db.commit()
    
    def create_kw_search(self):
        """
        create kw table
        """
        sql = """
              create table if not exists kw_search (
                  id int(32) not null auto_increment,
                  keyword varchar(64),
                  counter int default 1,
                  mark int default 0,
                  primary key (id),
                  unique index kw_index (keyword)
              )engine=myisam auto_increment = 1 default charset = utf8;
        """
        self.cursor.execute(sql)
        self.db.commit()
    
    def create_kw_dict(self):
        """
        词库:存储无效词type=0，如：标点符号：，、等
             存储组合词type=1，将有特殊意义的词作为一个关键词整体
        """
        sql = """
              create table if not exists kw_dict (
                  id int(32) not null auto_increment,
                  keyword varchar(64),
                  counter int default 1,
                  type tinyint  ,
                  primary key (id), 
                  unique index kw_dict_index (keyword, type)
              )engine=myisam auto_increment = 1 default charset = utf8;
        """
        self.cursor.execute(sql)
        self.db.commit()


    def insertMany_kw_dict(self, kw_list):
        """
        插入数据：kw_dict
        """
        sql = 'insert ignore into kw_dict(keyword, type) values( %s, %s)  '
        try: 
            self.cursor.executemany(sql, kw_list)
            self.db.commit()
        except MySQLdb.Error as e:  
            print ('kw_list:{0} exist already.'.format(str(e)))


    
    def create_kw_cloud(self):
        """
        词云图
        tips:
        中文分词和词性标注的性能对关键词抽取的效果至关重要
        +--------+---------+
        | 词库名称| type值  |
        +--------+---------+
        | 正面评价|   0     |
        +--------+---------+
        | 负面评价|   1     |
        +--------+---------+
        |  bug   |    2    |
        +--------+---------+
        | 建议意见|    3    |
        +--------+---------+ 
        |  刷 评  |    4    |
        +--------+---------+ 
        """
        sql = """
              create table if not exists kw_cloud (
                  id int(32) not null auto_increment,
                  keyword varchar(64),
                  counter int default 1,
                  primary key (id),
                  type tinyint
              )engine=myisam auto_increment = 1 default charset = utf8;
        """
        self.cursor.execute(sql)
        self.db.commit()
    
    

class UpdateKw(CommentJiebaDB):
    """
    更新词库
    """
    def update_dict(self, dict_type = CommentJiebaDB.STOP_WORDS):
        """
        从stopwords.txt中更新无效词/停用词库
        或
        从component.txt中更新组合词库

        默认更新的是无效词库
        dict_type = 1时，更新的是组合词库
        """
        if dict_type == self.STOP_WORDS:
            f = codecs.open(self.STOPWORDS_FILE, 'r', 'utf-8') # 停用词库
        else:
            f = codecs.open(self.COMPONENT_FILE, 'r', 'utf-8') # 组合词库
        
        if f:
            content = f.read() 
            content = content.replace('\r\n', ' ')
            kw_list = content.split(' ')
            try:
                kw_list.remove('') 
            except ValueError:
                pass
            # 
            t_kw = [(x, dict_type ) for x in kw_list]
            self.insertMany_kw_dict(t_kw)
            f.close()
        else:
            print("ERROR: Can not find the files...")
    
    def read_kw(self, kw_tb="kw_search"):
        """
        从kw表或者kw_search表中读取未归类的数据
        当kw_tb == kw的时候，从kw表中读取，否则从kw_search表中读取
        """
        
        sql = "select keyword, id from {0} where mark = 0 limit 10000".format(kw_tb)
        try:
            self.cursor.execute(sql)
            keywords = self.cursor.fetchall()
            content = ''
            for keyword in keywords:
                content = content+','+keyword[0] 
            
            f = codecs.open(self.KEYWORD_FILE, 'a', 'utf-8') # 组合词库
            f.write(content)
            f.close()

            ids = [kw[1] for kw in keywords]
            print(tuple(ids))

            # 更新数据库
            updatesql = "update {0} set mark = 1 where id in {1}".format(kw_tb, tuple(ids))
            self.cursor.execute(updatesql)
            self.db.commit() 
        except MySQLdb.Error as e: 
            print ('ERROR: read_kw:{0}.'.format(str(e))) 
    
if __name__ == "__main__":
    cjd = UpdateKw()
    #cjd.update_dict() # 停用词更新
    cjd.read_kw()
    #cjd.update_dict(cjd.COMPONENT_WORDS) # 更新组合词
    cjd.close()