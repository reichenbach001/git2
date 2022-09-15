import pika

class Shoot:
    def __init__(self, mssg2, qu, host, port):
        self.mssg = mssg2
        self.qu = qu
        self.host = host
        self.port = port
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))

    def send(self):
        the_channel = self.connection.channel()
        the_channel.queue_declare(queue=self.qu)
        the_channel.basic_publish(exchange='', routing_key=self.qu, body=self.mssg)
        print(f" [x] Sent {self.mssg}")

    def terminate(self):
        self.connection.close()

