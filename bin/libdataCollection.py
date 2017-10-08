#! /usr/bin/env python

'''
Data collection library of functions
'''

import os, logging
import tweepy

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


# Class to implement tweepy StreamListener and override on_data method to write to local FS.
class localFSWriter(tweepy.StreamListener):

    def __init__ (self, ofile):
        self.ofile = ofile

    # Write to local Filesystem
    def on_data(self, data):
        self.ofile.write(data)
        return True

    # If disconnected due to rate limit, then do not retry
    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            # to protect from danger of rate limiting
            # http://docs.tweepy.org/en/v3.5.0/streaming_how_to.html#handling-errors
            return False


# Function to create authorisation handler, object of localFSWriter and Streams
# to start collecting data and storing data on local filsystem.

def twitterDataLocalFS(twitter_auth_dict, localfs_datadir, twitter_tokens):

    filename = 'twitter_feed.dat'
    ofile = open(os.path.join(localfs_datadir,filename),'w')

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = localFSWriter(ofile)

    auth = tweepy.OAuthHandler(twitter_auth_dict['API_KEY'], twitter_auth_dict['API_SECRET'])
    auth.set_access_token(twitter_auth_dict['ACCESS_TOKEN'], twitter_auth_dict['ACCESS_TOKEN_SECRET'])
    stream = tweepy.Stream(auth, l)

    #This line filter Twitter Streams to capture data for toekns in variable twirtter_tokens

    stream.filter(track=twitter_tokens)

