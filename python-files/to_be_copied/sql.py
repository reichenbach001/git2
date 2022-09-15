from config2 import constant_vars 
from mysql.connector import connect

class Database:
    def __init__(self):

        self.db = connect(
        host=constant_vars['host'],
        user=constant_vars['user'],
        password=constant_vars['password'])

        if (self.db):
            self.curs=self.db.cursor()

            print('connected to database')

    
    def execute1(self,query):
        self.curs.execute(query)

    def execute2(self,query):
        self.curs.execute(query)
        return self.curs.fetchall()[0]
        
    def commit(self):
        self.db.commit()


