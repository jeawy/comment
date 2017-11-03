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

    def create_kw(self):
        """
        create kw table 来自结巴
        """
        sql = """
              create table if not exists kw (
                  id int(32) not null auto_increment,
                  keyword varchar(64),
                  counter int default 1,
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
    
    def update_dict(self, dict_type = STOP_WORDS):
        """
        从stopwords.txt中更新无效词/停用词库
        或
        从component.txt中更新组合词库

        默认更新的是无效词库
        dict_type = 1时，更新的是组合词库
        """
        if dict_type == self.STOP_WORDS:
            f = codecs.open('stopwords.txt', 'r', 'utf-8') # 停用词库
        else:
            f = codecs.open('component.txt', 'r', 'utf-8') # 组合词库
        
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


    
if __name__ == "__main__":
    cjd = CommentJiebaDB()
    cjd.update_dict()
    cjd.close()