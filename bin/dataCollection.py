#! /usr/bin/env python

'''
Data Collector for Sentiment Analysis Project
'''

import argparse, logging, ConfigParser
import libdataCollection

# Collect command line argument

parser = argparse.ArgumentParser()
parser.add_argument("-c", type=str, help="configuration file", required=True)
parser.add_argument("-s", type=str, help="data source", required=True)

args = parser.parse_args()

config_file = args.c
data_source = args.s


# Read and parse config file (Use ConfigParser)

cp = ConfigParser.ConfigParser()
cp.read(config_file)

parameters = dict(cp.items('Section 1'))
logfile = parameters['logfile']
storage_type = parameters['storage_type']
twitter_auth = parameters['twitter_auth']
twitter_tokens = parameters['twitter_tokens']

# convert string of tokens to list of tokens
twitter_tokens = twitter_tokens.split(",")

# Set variables depending on storage_type
if storage_type == 'localfs':
    localfs_datadir = parameters['localfs_datadir']
elif storage_type == 'kafka':
    kafka_broker = parameters['kafka_broker']
    kafka_twitter_topic = parameters['kafka_twitter_topic']

# Set logging and print name of logfile to check
loglevel="INFO"
nloglevel = getattr(logging, loglevel, None)
libdataCollection.logsetting(logfile, nloglevel)

libdataCollection.log(' ')
libdataCollection.log('{:90}'.format("-" * 90))
libdataCollection.log('{:30} {:30}'.format('logfile', logfile))
libdataCollection.log('{:30} {:30}'.format('storage type', storage_type))
libdataCollection.log('{:30} {:30}'.format('twitter auth',twitter_auth))
libdataCollection.log('{:30} {:30}'.format('twitter tokens',twitter_tokens))
if storage_type == 'localfs':
    libdataCollection.log('{:30} {:30}'.format('localfs datadir', localfs_datadir))
if storage_type == 'kafka':
    libdataCollection.log('{:30} {:30}'.format('kafka_broker',kafka_broker))
    libdataCollection.log('{:30} {:30}'.format('kafka_topic',kafka_twitter_topic))
libdataCollection.log('{:90}'.format("-" * 90))
libdataCollection.log(' ')

# Validate the the API key file exists

if libdataCollection.validate_twitter_auth(twitter_auth):
    raise SystemExit(1)

# Read API keys from authorisation file

twitter_auth_dict = libdataCollection.read_twitter_auth(twitter_auth)
libdataCollection.log(twitter_auth_dict)

# Call tweepy to start collecting twitter data
if storage_type == 'localfs':
    try:
        libdataCollection.twitterDataLocalFS(twitter_auth_dict, localfs_datadir, twitter_tokens)
    except KeyboardInterrupt:
        libdataCollection.log('{:90}'.format('Control-C : Program Interrupted'))
        raise SystemExit(1)
elif storage_type == 'kafka':
    try:
        libdataCollection.twitterDataKafka(twitter_auth_dict,kafka_broker,kafka_twitter_topic,twitter_tokens)
    except KeyboardInterrupt:
        libdataCollection.log('{:90}'.format('Control-C : Program Interrupted'))
        raise SystemExit(1)
else:
    libdataCollection.log('{:30} {:30}'.format('unsupported storage type',storage_type))
    raise SystemExit(1)

# Store Data. Should be able to store in one of the following storage system



