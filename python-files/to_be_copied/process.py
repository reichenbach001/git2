from sql import Database
from sender import Shoot
from config2 import constant_vars
from datetime import datetime

class Processor:
    def __init__(self):

        self.db=Database()

        pass
    

    def extract(self, mssg):

        list = mssg.split('$$$')
        if list[0] == 'order':

            self.db.execute1(list[1])
            
            return 1

        if list[0] == 'request':

            got = self.db.execute2(list[1])
            got2=got[0].strftime('%Y-%m-%d')
            got2 = ''.join(('response$$$', got2))
            
            shoot=Shoot(constant_vars['qeue_to_crawler'])
            shoot.send(got2)
            
            return 1

        if list[0] == 'response':
            return list[1]
        
        if list[0] == 'commit':
            self.db.commit()
            

