import argparse
import csv
import senzing
import senzing_server
import work_item

def queueAll(config_file, queue, outfilename):
    #connect to senzing
    senzing_handle = senzing_server.SenzingServer(config_file)

    if outfilename:
        csvfile = open(outfilename, 'w')
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['ENTITY_ID', ])
        rows_written = 0

    #process
    senzing_handle.exportEntityIDs()
    while True:
        item = senzing_handle.getNextEntityID()
        if item is None:
            break
        if outfilename:
            csvwriter.writerow([item[0],])
            rows_written += 1
        else:
            #queue item
            queue.publish(work_item.BuildWorkItem(*item))
            if queue.getNumPublished() % 100 == 0:
                print(F'published {queue.getNumPublished()}')
    if outfilename:
        csvfile.close()
        print(F'wrote {rows_written}')
    else:
        print(F'published {queue.getNumPublished()}')
    #we are done
    senzing_handle.closeExportEntityIDs()

def queueList(config, input_file, queue, outfilename):
    #connect to senzing
    senzing_handle = senzing_server.SenzingServer(config)

    items = {}

    lookup_count = 0
    #look up all of the records and de-dupe on ENTITY_ID
    with open(input_file, mode='rt', encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                item = senzing_handle.getEntityByRecordID(row['DATA_SOURCE'], row['RECORD_ID'])
                if 'RESOLVED_ENTITY' in item:
                    entity_id = item['RESOLVED_ENTITY']['ENTITY_ID']
                    items[entity_id] = (row['DATA_SOURCE'], row['RECORD_ID'])
                    lookup_count += 1
            except senzing.G2Exception as ex:
                print(F'ERROR: unable to queue item {row}')
                print(ex)
            if lookup_count % 100 == 0:
                print(F'looked up {lookup_count} records')
    print(F'looked up {lookup_count} records')

    if outfilename:
        with open(outfilename, 'w') as csvfile:
            rows_written = 0
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['ENTITY_ID',])
            for key,value in items.items():
                print(value)
                rows_written += 1
        print(F'wrote {rows_written}')
    else:
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
    parser.add_argument('-o', '--outFile', type=str, required=False, help='alternatively, output to a file instead of rabbitmq')

    args = parser.parse_args()

    #connect to rabbitmq
    queue = None
    if not args.outFile:
        import rabbitmq
        queue = rabbitmq.RabbitMQConnection(args.config)
        queue.connect()

    if args.action == 'all':
        queueAll(args.config, queue, args.outFile)
    elif args.action == 'list':
        queueList(args.config, args.inFile, queue, args.outFile)

    #shutdown
    if queue:
        queue.shutdown()