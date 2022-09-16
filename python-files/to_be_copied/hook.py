#!/usr/bin/env python
import pika
from process import Processor
from config2 import constant_vars

class Hook:

    def __init__(self, qu, host_ip=constant_vars['host_rabbit'], port_number=constant_vars['port']):
        self.qu = qu
        self.host = host_ip
        self.port = port_number
        self.body = object
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port))
        self.channel = self.connection.channel()
        

    def start_shit(self):
        process = Processor()
        self.channel.queue_declare(queue=self.qu)


        def callback(ch, method, properties, body):
            body=body.decode('utf-8')
            
            self.body = process.extract(body)

            if self.qu==constant_vars['qeue_to_crawler']:
                self.channel.stop_consuming()


        self.channel.basic_consume(
            queue=self.qu, on_message_callback=callback, auto_ack=True)


        print(' [*] Waiting for messages. To exit press CTRL+C : ',self.qu)
        self.channel.start_consuming()

    def terminate(self):

        
        self.connection.close()
