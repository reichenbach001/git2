import pika


class Hook:
    
    def __init__(self, qu, host, port):
        self.qu = qu
        self.host = host
        self.port = port
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
    
    def start_shit(self):
        channel=self.connection.channel()
        channel.queue_declare(queue=self.qu)

        def callback(ch, method, properties, body):
            print(body.decode())

        channel.basic_consume(queue='qu1', on_message_callback=callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

