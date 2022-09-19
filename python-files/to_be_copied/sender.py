import pika
from config2 import constant_vars
import uuid

class Shoot:
    def __init__(self, qu, host_ip=constant_vars['host_rabbit'],
     port_number=constant_vars['port']):

        self.qu = qu
        self.host = host_ip
        self.port = port_number
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port))
        self.channel = self.connection.channel()

    def send(self, mssg, type=0):

        if type == 1:
            my_qu = self.channel.queue_declare(queue='', exclusive=True)
            callback_queue = my_qu.method.qeue
            corr_id = str(uuid.uuid4())
            self.channel.basic_publish(exchange='',
             routing_key=self.qu,
              properties=pika.BasicProperties(
                reply_to=callback_queue, correlation_id=corr_id),
                 body=mssg)
            return(callback_queue,corr_id)
            

        else:
            
            self.channel.queue_declare(queue=self.qu)
            self.channel.basic_publish(
                exchange='', routing_key=self.qu, body=mssg)

    def terminate(self):
        self.connection.close()
