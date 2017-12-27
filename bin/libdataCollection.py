#! /usr/bin/env python

'''
Data collection library of functions
'''

import os, logging
import tweepy
from confluent_kafka import Producer


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

# check if the twitter api auth file exists
def validate_twitter_auth(twitter_auth):
    ''' check if the auth file with API keys exists'''

    if not os.path.exists(twitter_auth):
        log("Twitter API keys file %s does not exists" %twitter_auth)
        return 1

# Read API Keys and Tokens from Auth file

def read_twitter_auth(twitter_auth):
    '''Read Key and Tokens from auth file and return a dictionary'''

    auth_dict = dict()
    for line in open(twitter_auth):
        (key, value) = line.split(':')
        auth_dict[key] = value.strip('\n')

    return auth_dict


class localFSWriter(tweepy.StreamListener):
    '''class for writing twitter data to local fs'''
    def __init__ (self, ofile):
        self.ofile = ofile

    def on_data(self, data):
        self.ofile.write(data)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            # to protect from danger of rate limiting
            # http://docs.tweepy.org/en/v3.5.0/streaming_how_to.html#handling-errors
            return False


def twitterDataLocalFS(twitter_auth_dict, localfs_datadir, twitter_tokens):
    '''Wrapper to create object of localFSWriter class passing authentication etc and call tweepy Stream '''
    filename = 'twitter_feed.dat'
    ofile = open(os.path.join(localfs_datadir,filename),'w')

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = localFSWriter(ofile)

    auth = tweepy.OAuthHandler(twitter_auth_dict['API_KEY'], twitter_auth_dict['API_SECRET'])
    auth.set_access_token(twitter_auth_dict['ACCESS_TOKEN'], twitter_auth_dict['ACCESS_TOKEN_SECRET'])
    stream = tweepy.Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords
    stream.filter(track=twitter_tokens)

class KafkaProducerTwitter(tweepy.StreamListener):
    '''class for writing twitter data to kafka topic'''
    def __init__ (self, kafkaproducer, topic):
        self.kafkaproducer = kafkaproducer
        self.topic = topic

    def on_data(self, data):
        try:
            #Write to Kafka topic
            self.kafkaproducer.produce(self.topic, data)
        except BufferError as e:
            log('{:30} {:30}'.format('Local queue is full. Awaiting delivery of messages -',len(self.kafkaproducer.produce)))

        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            # to protect from danger of rate limiting
            # http://docs.tweepy.org/en/v3.5.0/streaming_how_to.html#handling-errors
            return False

def twitterDataKafka(twitter_auth_dict, kafka_broker, kafka_topic, twitter_tokens):
    '''Wrapper to create object of KafkaProducerTwitter class passing authentication etc and call tweepy Stream'''
    # Create producer Instance

    producerConf = {'bootstrap.servers':kafka_broker,'acks':0, 'default.topic.config': {'compression.codec': 'snappy'}}

    p = Producer(**producerConf)

    # Create KafkaProducerTwitter Instance

    k = KafkaProducerTwitter(p, kafka_topic)

    # Authenticate

    auth = tweepy.OAuthHandler(twitter_auth_dict['API_KEY'], twitter_auth_dict['API_SECRET'])
    auth.set_access_token(twitter_auth_dict['ACCESS_TOKEN'], twitter_auth_dict['ACCESS_TOKEN_SECRET'])

    # Create tweepy Stream instance

    stream = tweepy.Stream(auth, k)

    # Filter streaming data for interested tokens.

    stream.filter(track=twitter_tokens)
