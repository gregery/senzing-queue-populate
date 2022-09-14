import rabbitmq
import senzing_server
import work_item

def queueAll(config_file):
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
        queue.publish(work_item.BuildWorkItem(*item))
        count = count + 1
        if count % 10 == 0:
            print('inserted ' + str(count))
    print('inserted ' + str(count))
    #we are done
    senzing_handle.closeExportEntityIDs()

    #shutdown
    queue.shutdown()

if __name__ == "__main__":
    config_file = 'config.json'
    queueAll(config_file)
