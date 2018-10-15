import MySQLdb


class ConnectDBA(object):
    """
    DB connection management
    """
    pro = ['113.77.240.236:9797', '221.214.214.144:53281', '221.7.49.209:53281', '61.135.217.7','116.6.228.51:808'
    ,'220.166.241.145:8118','61.155.164.107:3128','61.135.217.7:80','118.178.124.33:3128']  
    #pro = ['139.129.166.68:3128', '59.59.146.69:53281', '180.168.179.193:8080', '122.72.32.88', '183.190.74.185']  
    #pro =['111.155.116.203:8123', '123.121.68.220:9000']
    
    def __init__(self ):
        db = MySQLdb.connect(host="localhost",user="root",passwd="sqlroot",db="comment_zh",charset="utf8mb4") 
        #db = MySQLdb.connect(host="192.168.1.105",user="root",passwd="comment",db="comment",charset="utf8mb4") 
        #db = MySQLdb.connect("localhost", 'root', 'sqlroot', 'Comment', 'utf8')
        self.db = db
        self.cursor = self.db.cursor()
    
    def close(self):
        self.db.close()