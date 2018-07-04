#! /usr/bin/env python

'''
Use Spark (on Google Dataproc) to process Tweet data stored in Google Cloud Storage
Flow:
    Load files from Google Cloud Storage bucket matching a filename pattern
    Read Tweets into a Dataframe
    Count number of tweets for each collected tokens
'''

# Import required packages

import argparse
import json
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower
    
# Initial SparkSession for TweetProcessing application

s_session = SparkSession.builder.appName('TweetProcessing').getOrCreate()

# Read Files from Cloud Storage Bucket

# input_file = "gs://sda_tweet_data/tw*.gz"
# input_file = "gs://sda_tweet_data/twitter_raw_data.c2.2018-05-29-065320.gz"
input_file = "gs://sda_tweet_data/tw*.gz"

# Create a data frame with data from tweet json file
tweet_df = s_session.read.json(input_file)
print 'Total number of tweets --> ' + str(tweet_df.count())
    
token_to_search = '%microsoft%'
numtweets = tweet_df.filter(lower(tweet_df.text).like(token_to_search)).count()
print 'Number of tweets for ' + token_to_search + ' --> ' + str(numtweets)

token_to_search = '%oracle%'
numtweets = tweet_df.filter(lower(tweet_df.text).like(token_to_search)).count()
print 'Number of tweets for ' + token_to_search + ' --> ' + str(numtweets)

token_to_search = '%google%'
numtweets = tweet_df.filter(lower(tweet_df.text).like(token_to_search)).count()
print 'Number of tweets for ' + token_to_search + ' --> ' + str(numtweets)

token_to_search = '%blockchain%'
numtweets = tweet_df.filter(lower(tweet_df.text).like(token_to_search)).count()
print 'Number of tweets for ' + token_to_search + ' --> ' + str(numtweets)

s_session.stop()

