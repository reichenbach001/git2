import pika
from process import Processor
from config2 import constant_vars

class Hook:
    
    def __init__(self, qu, host=constant_vars['host'], port=constant_vars['port']):
        self.qu = qu
        self.host = host
        self.port = port
        self._body = object
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))

    def start_shit(self):
        process=Processor()
        
        channel=self.connection.channel()
        channel.queue_declare(queue=self.qu)

        def callback(ch, method, properties, body):
            self._body= process.extract(body)
        
        channel.basic_consume(queue=self.qu, on_message_callback=callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
        