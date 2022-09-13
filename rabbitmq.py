import json
import pika

class RabbitMQConnection:
    def __init__(self, config_filename):
        self.connection=None
        self.channel=None
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
        self.channel.basic_publish(exchange='', routing_key=self.config_params['queue_name'], body=item)

    def shutdown(self):
        self.connection.close()

