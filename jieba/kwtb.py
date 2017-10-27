import sys
sys.path.insert(0, '..')
from conn import ConnectDBA

class CommentJiebaDB(ConnectDBA):
    """
    create kw tables
    """

    def create_kw(self):
        """
        create kw table
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

if __name__ == "__main__":
    cjd = CommentJiebaDB()
    cjd.create_kw_search()
    cjd.close()