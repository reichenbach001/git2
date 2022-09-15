import pika
from config2 import constant_vars
class Shoot:
    def __init__(self, qu, host=constant_vars['host'], port=constant_vars['port']):
        self.qu = qu
        self.host = host
        self.port = port
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
        self.channel = self.connection.channel()

    def send(self, mssg):
        self.channel.queue_declare(queue=self.qu)
        self.channel.basic_publish(exchange='', routing_key=self.qu, body=mssg)
        print(f" [x] Sent {mssg}")


    def terminate(self):
        self.connection.close()
