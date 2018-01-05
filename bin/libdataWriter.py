#! /usr/bin/env python

'''
Data Writer  library of functions
'''

import os
import logging
import datetime
import time
import tweepy
import threading
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


def checkFilesize(ofilename, max_localfs_file_size):

    filesize = os.path.getsize(ofilename)
    #log('{:15} {:30} {:15} {:30}'.format('filename', ofilename, 'filesize', filesize))
    #log('{:20} {:30}'.format('max local file size',max_localfs_file_size))

    if filesize > int(max_localfs_file_size):
        #log('{:60}'.format('file is larger than max allowed...rotating to new file'))
        return True



class kafkaConsumerLocalFS():
    '''Kafka Consumer - Write to local file system'''

    def __init__(self, consumer_name, kafka_broker, kafka_topic, consumer_group, max_localfs_file_size, localfs_datadir, localfs_file_format):

        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.consumer_group = consumer_group
        self.max_localfs_file_size = max_localfs_file_size
        self.localfs_datadir = localfs_datadir
        self.localfs_file_format = localfs_file_format
        self.consumer_name = consumer_name

    def tolocalFS(self):

        consumerConf = {'bootstrap.servers': self.kafka_broker, 'group.id': self.consumer_group}
        ofilename = os.path.join(self.localfs_datadir, self.consumer_group + '.' + self.consumer_name + '.' + datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S"))
        ofile = open(ofilename, 'w')
        log('{:30} {:30}'.format('output file name', ofilename))

        # creare a consumer object
        c = Consumer(**consumerConf)

        # subscribe to kafka topic
        c.subscribe([self.kafka_topic])

        # Set local variable to start an infinite loop and timer to check file size for rotation
        consuming = True
        currtime = time.time()
        checkfreq = 60

        while consuming:

            # Poll a message from Kafka topic
            msg = c.poll()

            # If successfully retrieved message
            if not msg.error():

                # check if the timer expired for file size check and rotate
                if (time.time() - currtime) > checkfreq:
                    #log('{:60} {:30}'.format('checking file size for consumer', self.consumer_name))
                    if checkFilesize(ofilename, self.max_localfs_file_size):

                        # file size > max allowed, Close the old file, build a new filename and open

                        ofile.close()
                        ofilename = os.path.join(self.localfs_datadir, self.consumer_group + '.' + self.consumer_name + '.' + datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S"))
                        ofile = open(ofilename, 'w')
                        log('{:30} {:20}'.format('rotated output file for consumer  ', self.consumer_name))
                        log('{:30} {:40}'.format('new file name ', ofilename))
                        log(' ')
                    # Reset current time for timer
                    currtime = time.time()

                ofile.write(msg.value().decode('utf-8'))
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                log(msg.error())
                consuming = False
        c.close()
        ofile.close()

def writeToLocalFS(kafka_broker,kafka_topic,consumer_group,consumer_names,max_localfs_file_size,localfs_datadir,localfs_file_format):
    ''' Create a consumer to kafka topic, read from the topic and write to local file system'''

    # Build dictionary of config parameters
    consumerParams = {'kafka_broker':kafka_broker,'kafka_topic':kafka_topic,'consumer_group':consumer_group,'max_localfs_file_size':max_localfs_file_size,'localfs_datadir':localfs_datadir,'localfs_file_format':localfs_file_format}

    # Parse consumer_list parameter from configfile and create a KafkaConsumerlocalFS object for each consumer name in the list
    threads = []
    for cn in consumer_names.split(","):
        log('{:30} {:30}'.format('Consumer Name', cn))
        cn = kafkaConsumerLocalFS(cn, **consumerParams)

        # Pass tolocalFS method as reference to the target. If tolocalFS() is passed instead of tolocalFS then it will execute in the main thread
        t = threading.Thread(target=cn.tolocalFS)
        threads.append(t)
        t.start()
    t.join()