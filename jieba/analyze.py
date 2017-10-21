# -*- coding:utf-8 -*-

import sys
sys.path.insert(0, '..')

import jieba
import MySQLdb
import pdb
import logging

from conn import ConnectDBA
from decorators import timelog

class Analyze(ConnectDBA):
    """
    """
    def __init__(self):
        logging.basicConfig(level=logging.INFO, datefmt='%a, %d %b %Y %H:%M:%S', 
                     filename='..\\log\\analyze.log', filemode='a+')
        super(Analyze, self).__init__()

    def getcomment(self, num = 0):
        if num > 0:
            sql = """select content, id from comment where jieba = 0 limit {0}""".format(num)
        else:
            sql = """select content, id from comment where jieba = 0 """
        self.cursor.execute(sql)
        comments = self.cursor.fetchall()
        return comments

    def insertmany_kw_tb(self, kws):
        """向苹果用户表中插入用户数据，如果用户id已存在，则不插入"""
       
        insertsql = 'insert into kw(  keyword) values (%s ) on duplicate key update counter = counter + 1' 
 
        try: 
            self.cursor.executemany(insertsql, kws)
            self.db.commit()
        except MySQLdb.Error as e: 
            logging.error ('ERROR:{0}, KW:{1}'.format(str(e), kws))

    def update_comment_tb(self, commentid):
        """更新comment表，代表该comment已经被分词处理过了"""
       
        updatesql = 'update comment set jieba = 1 where id = {0}'.format(commentid)
 
        try: 
            self.cursor.execute(updatesql)
            self.db.commit()
        except MySQLdb.Error as e: 
            logging.error ('ERROR:{0}, Commentid:{1}'.format(str(e), commentid))
    @timelog
    def anlyze_tb(self, comments):
        """
        利用jieba分词
        """
        if len(comments) > 0:
            for comment in comments:
                kwds_list = jieba.cut(comment[0]) 
                self.insertmany_kw_tb(tuple(set(kwds_list)))
                self.update_comment_tb(comment[1])
                print(comment[1])
        else:
            print('no comments...')

if __name__ == "__main__":
    anlyze = Analyze()
    comments = anlyze.getcomment(100)
    anlyze.anlyze_tb(comments)
    anlyze.close()