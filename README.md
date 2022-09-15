# senzing-queue-populate

This is a utility that adds work items in batch to RabbitMQ for the use of products downstream from Senzing.

## Dependencies
senzing -- Senzing's Python API must be in the PYTHONPATH
If you don't already have Senzing installed, instructions can be found here [Senzing quickstart guide](https://senzing.zendesk.com/hc/en-us/articles/115002408867-Quickstart-Guide)

pika -- RabbitMQ Client must be installed

### Linux
```
sudo pip install pika
```
### Windows
```
pip install pika
```

## Configuration
sample configuration file can be found in **config.json.SAMPLE**

config.json is the default filename.  alternative config file name can be specified with the **-c** flag.

Config file contains RabbitMQ and Senzing connection info and is very intuitive.
RabbitMQ queue name can be anything the user chooses, and will be automatically created if queue of that name doesn't already exist.

## Utility usage
Usage is built in to the utility
```
python3 populate.py -h
```

## Queue all entities
```
python3 populate.py all
```

## Queue a list of entities from a file
Input file must be of CSV format with a header.  Must contain **DATA_SOURCE** and **RECORD_ID** columns.
The following will load all the entities from the file input.csv
```
python3 populate.py list
```
Input filename can be specified 
```
python3 populate.py list -i somefile.csv
```
