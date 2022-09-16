from config2 import constant_vars 
from mysql.connector import connect

class Database:
    def __init__(self):

        self.db = connect(
        host=constant_vars['host_mysql'],
        user=constant_vars['user'],
        password=constant_vars['password'])

        if (self.db):
            self.curs=self.db.cursor()
            self.curs.execute('CREATE DATABASE IF NOT EXISTS tset_db;')
            self.curs.execute('CREATE TABLE IF NOT EXISTS tset_db.last_check(share_id bigint PRIMARY KEY,last_update DATE DEFAULT "1980-01-01");')
            self.curs.execute('use tset_db;')


    
    def execute1(self,query):
        self.curs.execute(query)

    def execute2(self,query):
        self.curs.execute(query)
        return self.curs.fetchall()[0]
        
    def commit(self):
        self.db.commit()


