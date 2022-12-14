import json
import pika

class RabbitMQConnection:
    def __init__(self, config_filename):
        self.connection=None
        self.channel=None
        self.num_published = 0
        required_keys = ['host','port','queue_name','username','password']
        with open(config_filename, mode='rt', encoding='utf-8') as config_file:
            self.config_params = json.load(config_file)['rabbitmq_config']
        for required_key in required_keys:
            if required_key not in self.config_params:
                raise Exception('config is missing required key: ' + required_key)

    def connect(self):
        credentials = pika.PlainCredentials(username=self.config_params['username'], password=self.config_params['password'])
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.config_params['host'], port=self.config_params['port'], credentials=credentials))
        self.channel = self.connection.channel()    
        self.channel.queue_declare(self.config_params['queue_name'])

    def publish(self, item):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.config_params['queue_name'], body=item)
        except pika.exceptions.StreamLostError:
            print('RabbitMQ connection lost.....  reconnecting...')
            self.reconnect()
            self.channel.basic_publish(exchange='', routing_key=self.config_params['queue_name'], body=item)
        self.num_published = self.num_published + 1

    def shutdown(self):
        self.connection.close()

    def getNumPublished(self):
        return self.num_published
    
    def reconnect(self):
        try:
            self.shutdown()
        except Exception:
            pass
        self.connect()
