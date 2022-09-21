import pika
from config2 import constant_vars


class Shoot:
    def __init__(self, qu, host_ip=constant_vars['host_rabbit'], port_number=constant_vars['port']):
        self.qu = qu
        self.host = host_ip
        self.port = port_number
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.qu,durable=True)

    def send(self, mssg):

        self.channel.basic_publish(exchange='', routing_key=self.qu, body=mssg)


            
    def terminate(self):
        self.connection.close()

