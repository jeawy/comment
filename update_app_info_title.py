# -*- coding:utf-8 -*-
"""
更新app主表的信息、
并且更新apptitle中的信息，记录app的名字变化情况
"""
import pdb
from datetime import datetime, timedelta

from applespider import SpiderRun
from decorators import timelog


class FetchAppinfo(SpiderRun):
    """
    # 追踪app的title变化情况
    # 1 获取最新的app title
    # 2 获取apptitle中这个app最近使用的一个app title
    #      判断两个title是否相等，如果不相等，则将新的title插入
           apptitle表中，否则跳过
           
    """
    def get_latest_title(self, appid, title, version):
        """
        获取apptitle表中最新的title，并更最新的title进行比较
        如果title相同的话，跳过
        如果不同的话，插入新的title和version信息
        """
        sql = """select appid, title from apptitle where appid = {0} 
                  order by updated desc limit 1 """.format(appid) 
     
        self.cursor.execute(sql)
        app = self.cursor.fetchone()
        if app:
            if app[1] !=  title.strip():
                # 插入apptitle表
                self.insert_title(appid=app[0], title=title, version=version)
            else:
                print(app[0], '................................')
        else:
            # 插入apptitle表
            self.insert_title(appid=appid, title=title, version=version)
    
    def get_apps(self, sql='', limit=0):
        """
        获取apptitle表中需要重新抓取title和version的app
        """
        if limit:
            sql = """select id, url from appinfo where  {0} limit {1}""".format(sql, limit) 
        else:
            sql = """select id, url from appinfo where  {0}  """.format(sql) 
     
        self.cursor.execute(sql)
        apps = self.cursor.fetchall()
        print(len(apps))
        
        return apps

    def insert_title(self, appid, title, version):
        """
        获取apptitle表中最新的title，并更最新的title进行比较
        如果title相同的话，跳过
        如果不同的话，插入新的title和version信息
        """
        today = datetime.today().date()  
        today = today.strftime('%Y-%m-%d')

        sql = """insert into apptitle (appid, title, updated, version) 
                 values(%s, %s, %s, %s)"""
        
        self.cursor.execute(sql, (appid, title, today, version) )
        self.db.commit()

    def update_appin(self, appid ): 
        """
        app的title和version更新完毕后，需要更新appinfo表，表示本次更新完毕
        """ 
        
        sql = "update  appinfo set clean = 1 where id = " + str(appid)
        
        self.cursor.execute(sql)
        self.db.commit()
    
    def update_appin_url(self, appid ): 
        """
        app的url错误，清空
        """ 
        
        sql = "update  appinfo set url = null where id = " + str(appid)
        
        self.cursor.execute(sql)
        self.db.commit()
    
    def update_appin_all(self): 
        """
        app的title和version更新完毕后，重置appinfo的clean，以便下次再次使用
        """  
        sql = "update  appinfo set clean = 0 " 
        self.cursor.execute(sql)
        self.db.commit()



@timelog
def run():
    f = FetchAppinfo()
    apps = f.get_apps(sql='clean = 0 and url is not null', limit = 2000)
    #apps = f.get_apps(sql='id in (385285922, 609834058, 893944982, 724816878, 388882339, 448165862, 457856023, 480079300, 480111612,  486717857, 504533353, 510289916, 529096064, 554594787, 583446403 )', limit = 2000)
    for app in apps:
        url = app[1]
        appid = app[0] 
        if url:
            version, title = f.analyse_appinfo(url) 
            if title and version:
                print(version, title)
                f.get_latest_title(appid=appid, version=version, title=title)
                f.update_appin(appid)
            else:
                print (appid)
                f.update_appin_url(appid)
     
    f.close()
 
def runtest():
    f = FetchAppinfo()
    appid = 350962117
    version ='test version'
    title = '微博'
    f.get_latest_title(appid=appid, version=version, title=title) 
    f.close()

if __name__ == "__main__": 
    for i in range(1, 8):
        try:
            run()
        except Exception as e:
            print (e)
    
