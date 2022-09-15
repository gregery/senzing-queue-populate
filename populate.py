import argparse
import rabbitmq
import senzing_server
import work_item
import senzing
import csv

def queueAll(config_file, queue):
    #connect to senzing
    senzing_handle = senzing_server.SenzingServer(config_file)
    #process
    senzing_handle.exportEntityIDs()
    while True:
        item = senzing_handle.getNextEntityID()
        if item is None:
            break
        #queue item
        queue.publish(work_item.BuildWorkItem(*item))
        if queue.getNumPublished() % 100 == 0:
            print(F'published {queue.getNumPublished()}')
    print(F'published {queue.getNumPublished()}')
    #we are done
    senzing_handle.closeExportEntityIDs()

def queueList(config, input_file, queue):
    #connect to senzing
    senzing_handle = senzing_server.SenzingServer(config)

    items = {}

    #look up all of the records and de-dupe on ENTITY_ID
    with open(input_file, mode='rt', encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                item = senzing_handle.getEntityByRecordID(row['DATA_SOURCE'], row['RECORD_ID'])
                if 'RESOLVED_ENTITY' in item:
                    entity_id = item['RESOLVED_ENTITY']['ENTITY_ID']
                    items[entity_id] = (row['DATA_SOURCE'], row['RECORD_ID'])
            except senzing.G2Exception as ex:
                print(F'ERROR: unable to queue item {row}')
                print(ex)

    #queue the items
    for key,value in items.items():
        queue.publish(work_item.BuildWorkItem(key, *value))
        if queue.getNumPublished() % 100 == 0:
            print(F'published {queue.getNumPublished()}')
    print(F'published {queue.getNumPublished()}')


if __name__ == "__main__":  
    action_choices = ['all', 'list']
    action_choices_help = 'all - batch load all resolved entities from senzing into the queue\n'\
                          'list - batch load all resolved entities from a file into the queue'
    parser = argparse.ArgumentParser(description='Utility to load Senzing affected entity work items into RabbitMQ')
    parser.add_argument('action', type=str, choices=action_choices, help=action_choices_help)
    parser.add_argument('-c', '--config', type=str, default='config.json', help='config file with Senzing and RabbitMQ information')
    parser.add_argument('-i', '--inFile', type=str, default='input.csv', help='input file for list mode')
    args = parser.parse_args()

    #connect to rabbitmq
    queue = rabbitmq.RabbitMQConnection(args.config)
    queue.connect()

    if args.action == 'all':
        queueAll(args.config, queue)
    elif args.action == 'list':
        queueList(args.config, args.inFile, queue)

    #shutdown
    queue.shutdown()