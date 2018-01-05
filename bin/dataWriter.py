#! /usr/bin/env python

'''
Data Writer for Social Data Aggregator Project
Writes data to persistent storage either to local file system or HDFS
'''

import argparse, logging, ConfigParser
import libdataWriter

# Collect command line argument

parser = argparse.ArgumentParser()
parser.add_argument("-c", type=str, help="configuration file", required=True)

args = parser.parse_args()

config_file = args.c

# Read and parse config file (Use ConfigParser)

cp = ConfigParser.ConfigParser()
cp.read(config_file)

parameters = dict(cp.items('Section 1'))
logfile_dir = parameters['logfile_dir']
writer_log_file = parameters['writer_log_file']

persistent_storage = parameters['persistent_storage']
consumer_group = parameters['consumer_group']
consumer_names = parameters['consumer_names']
max_localfs_file_size = parameters['max_localfs_file_size']
kafka_broker = parameters['kafka_broker']
kafka_topic = parameters['kafka_topic']



# Set variables depending on storage_type
if persistent_storage == 'localfs':
    localfs_datadir = parameters['localfs_datadir']
    localfs_file_format = parameters['localfs_file_format']
elif persistent_storage == 'hdfs':
    libdataWriter.log('{90}'.format('Writing to HDFS is not yet implemented'))
    raise SystemExit(1)

logfile = logfile_dir + writer_log_file

# Set logging and print name of logfile to check
loglevel="INFO"
nloglevel = getattr(logging, loglevel, None)
libdataWriter.logsetting(logfile, nloglevel)

libdataWriter.log(' ')
libdataWriter.log('{:90}'.format("-" * 90))
libdataWriter.log('{:30} {:30}'.format('logfile', logfile))
libdataWriter.log('{:30} {:30}'.format('persistent storage', persistent_storage))
libdataWriter.log('{:30} {:30}'.format('consumer group', consumer_group))
libdataWriter.log('{:30} {:30}'.format('max file size', max_localfs_file_size))

if persistent_storage == 'localfs':
    libdataWriter.log('{:30} {:30}'.format('localfs datadir', localfs_datadir))

libdataWriter.log('{:90}'.format("-" * 90))
libdataWriter.log(' ')

# Call function from libdataWriter to write to persistent storage
if persistent_storage == 'localfs':
    try:
        libdataWriter.writeToLocalFS(kafka_broker,kafka_topic,consumer_group,consumer_names,max_localfs_file_size,localfs_datadir,localfs_file_format)
    except KeyboardInterrupt:
        libdataWriter.log('{:90}'.format('Control-C : Program Interrupted'))
        raise SystemExit(1)
else:
    libdataWriter.log('{:30} {:30}'.format('unsupported storage type',persistent_storage))
    raise SystemExit(1)

# Store Data. Should be able to store in one of the following storage system



