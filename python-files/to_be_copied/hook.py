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

    
    def start(self,qu_temp_name='',corr_id='',type=0):
        process = Processor()

        self.channel.queue_declare(queue=self.qu)


        def callback0(ch, method, properties, body):
            body=body.decode('utf-8')
            
            self.body = process.extract(body)
            
            if self.qu==constant_vars['qeue_to_crawler']:
                self.channel.stop_consuming()
                print(f'on {self.qu} recieved: ',self.body)

        def callback1(ch, method, properties,props, body):
            body=body.decode('utf-8')
            if corr_id==props.correlation_id:
                self.body = process.extract(body)
            
            #if self.qu==constant_vars['qeue_to_crawler']:
            #    self.channel.stop_consuming()
            #    print(f'on {self.qu} recieved: ',self.body)

        if type==1:
            self.channel.basic_consume(
                queue=qu_temp_name, on_message_callback=callback1, auto_ack=True)
        
        else:
            self.channel.basic_consume(
                queue=self.qu, on_message_callback=callback0, auto_ack=True)



        
        self.channel.start_consuming()

    def terminate_channel(self):
        self.channel.close()

    
    def __exit__(self):

        self.connection.close()
    '''
    def __enter__(self, qu='qu2', host_ip=constant_vars['host_rabbit'], port_number=constant_vars['port']):
        self.qu = qu
        self.host = host_ip
        self.port = port_number
        self.body = object
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port))
        self.channel = self.connection.channel()

    '''