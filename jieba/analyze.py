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

    def getcomment(self, num = 0, jieba = 0):
        if num > 0:
            sql = """select content, id from comment where jieba = {0} limit {1}""".format(jieba, num)
        else:
            sql = """select content, id from comment where jieba = {0} """.format(jieba )
        self.cursor.execute(sql)
        comments = self.cursor.fetchall()
        return comments

    def insertmany_kw_tb(self, kws, tb='kw'):
        """向苹果用户表中插入用户数据，如果用户id已存在，则不插入"""
       
        insertsql = """insert into {0}(  keyword) values (%s ) 
                       on duplicate key update counter = counter + 1""".format(tb)
 
        try: 
            self.cursor.executemany(insertsql, kws)
            self.db.commit()
        except MySQLdb.Error as e: 
            logging.error ('ERROR:{0}, KW:{1}'.format(str(e), kws))

    def update_comment_tb(self, commentid, jieba = 1):
        """更新comment表，代表该comment已经被分词处理过了"""
       
        updatesql = 'update comment set jieba = {0} where id = {1}'.format(jieba, commentid)
 
        try: 
            self.cursor.execute(updatesql)
            self.db.commit()
        except MySQLdb.Error as e: 
            logging.error ('ERROR:{0}, Commentid:{1}'.format(str(e), commentid))
    @timelog
    def anlyze_tb(self, comments):
        """
        利用jieba的精确模式分词
        """
        if len(comments) > 0:
            for comment in comments:
                kwds_list = jieba.cut(comment[0]) 
                self.insertmany_kw_tb(tuple(set(kwds_list)))
                self.update_comment_tb(comment[1])
                print(comment[1])
        else:
            print('no comments...')
    
    @timelog
    def anlyze_search_tb(self, comments):
        """
        利用jieba搜索模式分词
        """
        if len(comments) > 0:
            for comment in comments:
                kwds_list = jieba.cut_for_search(comment[0]) 
                self.insertmany_kw_tb(tuple(set(kwds_list)), 'kw_search')
                self.update_comment_tb(comment[1], jieba=2)
                print(comment[1])
        else:
            print('no comments...')

if __name__ == "__main__":
    anlyze = Analyze()
    comments = anlyze.getcomment( jieba=1)
    anlyze.anlyze_search_tb(comments)
    anlyze.close()