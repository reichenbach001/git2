from sql import Database
from sender import Shoot


class Processor:
    def __init__(self, command):
        if command == 'process1':
            self.db = Database()
            self.shoot = Shoot('q2')
        if command == 'process2':
            pass

    def extract(self, mssg):
        list = mssg.split('$$$')
        if list[0] == 'order':
            self.db.execute1(list[1])

        if list[0] == 'request':
            got = self.db.execute2(list[1])
            got = ''.join(('response$$$', got))
            self.shoot.send(got)

        if list[0] == 'response':
            return list[0]

    def commit(self):
        self.db.commit()        
