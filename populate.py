import rabbitmq
import senzing_server

def run():
    config_file = 'config.json'
    #connect to rabbitmq
    queue = rabbitmq.RabbitMQConnection(config_file)
    queue.connect()

    #connect to senzing
    senzing_handle = senzing_server.SenzingServer(config_file)
    #process
    count = 0
    senzing_handle.exportEntityIDs()
    while True:
        item = senzing_handle.getNextEntityID()
        if item is None:
            break
        #queue item
        queue.publish(item)
        count = count + 1
        if count % 10 == 0:
            print('inserted ' + str(count))
    print('inserted ' + str(count))
    #we are done
    senzing_handle.closeExportEntityIDs()

    #shutdown
    queue.shutdown()

if __name__ == "__main__":
    run()
