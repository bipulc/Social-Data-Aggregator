#! /usr/bin/env python

'''
Data Writer  library of functions
'''

import os, logging, datetime
import tweepy
from confluent_kafka import Consumer, KafkaError


# Configure logging

def logsetting(logfile, loglevel):
    '''Configure logging'''
    logging.basicConfig(level=loglevel,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=logfile,
                        filemode='a')

# write log messages to logfile and to optionally to stdout
def log(logmessage):
    '''Print message to standard output and log file'''
    logging.info(logmessage)
    print logmessage

def writeToLocalFS(kafka_broker,kafka_topic,consumer_group,consumer_name,max_localfs_file_size,localfs_datadir,localfs_file_format):
    ''' Create a consumer to kafka topic, read from the topic and write to local file system'''

    # build dictionary of parameters required to instantiate a consumer
    consumerConf = {'bootstrap.servers':kafka_broker,'group.id':consumer_group}

    # build output filename
    ofilename = consumer_group+'.'+consumer_name+'.'+datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    ofile = open(os.path.join(localfs_datadir, ofilename), 'w')
    log('{:30} {:30}'.format('output file name',ofile))

    # create a consumer object
    c = Consumer(**consumerConf)

    # subscribe to kafka topic
    c.subscribe([kafka_topic])

    # start a loop to poll messages from kafka topic

    consuming = True
    while consuming:
        msg = c.poll()
        if not msg.error():
            ofile.write(msg.value().decode('utf-8'))
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            log(msg.error())
            consuming = False
    c.close()
    ofile.close()