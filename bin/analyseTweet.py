#! /usr/bin/env python

'''
Use spark to process Tweet data from localFS or HDFS
Flow:
    Load a single file ( later on load all files from a directory)
    Read Tweets in JSON format to a RDD
    Count number of tweets for each collected tokens
'''

# Import required packages

import argparse
import json
import os
import shutil
import pyspark
from pyspark import SparkContext

'''
class SearchTokenInTweetText(object):
    def __init__(self,token):
        self.token = token
    def getMatches(self,rdd):
        token = self.token
'''
# Class to filter tags from Tweet JSON and create a smaller JSON with fewer tags
class filterJSONtags:
    def __init__(self, tweet, tags):
        self.tweet_json = tweet
        self.tags_list = tags
        #self.output_file = ofile

    def pullTags(self):
        
        # List to collect Values of required tags
        jsondata_list = []
        tweet_json = self.tweet_json
        tags_list = self.tags_list

        # dump JSON in a dictionary
        try:
            json_dict = json.loads(tweet_json)
        except ValueError:
            # most likely a malformed JSON. Do nothing
            pass

        for tag in tags_list:
            try:
                if 'user' in tag:
                    t = []
                    for tag_tokens in tag.split('.'):
                        t.append(tag_tokens)
                    value_list.append(json_dict[t[0]][t[1]])
                else:
                    #print tags,json_dict[tags]
                    #print type(json_dict[tags])
                    if type(json_dict[tag]) is unicode:
                        #print tags
                        value_string = json_dict[tag]
                        value_string = value_string.replace('\n', ' ').replace('\r', '')
                        json_string = '"'+tag+'"'+':'+'"'+value_string+'"'
                        # print value_string
                        jsondata_list.append(json_string)
                    else:
                        json_string = '"'+tag+'"'+':'+'"'+str(json_dict[tag])+'"'
                        #print json_string
                        jsondata_list.append(json_string)
                # print value_list
            except:
                pass
        # print jsondata_list
        try:
            jsondata_string = ",".join(map(str,jsondata_list))
            return '{'+jsondata_string+'}'
        except:
            # if malformed JSON or text can not be converted to ascii
            pass
        


def filterJSON(tweet_json, tags_list, ofile):
    c = filterJSONtags(tweet_json, tags_list)
    ofile.write(c.pullTags())
    ofile.write('\n')

def filterJSON_RDD(tweet_json):
    tags_list = ['created_at','id','text','lang']
    c = filterJSONtags(tweet_json, tags_list)
    return c.pullTags()
        


if __name__ == "__main__":

    # read input params

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", type=str, help="Tweet file containing data in JSON format", required=True)
    parser.add_argument("-o", type=str, help="name of output file", required=True)

    args = parser.parse_args()
    input_file = args.i
    output_file = args.o

    # remove output directory if exists

    if os.path.isdir(output_file):
        shutil.rmtree(output_file)
    
    # define the cluster to run Spark
    master = "local"
    
    # Initial SparkContext for TweetProcessing application
    sc = SparkContext(master,"TweetProcessing")

    # Load Tweet JSONs from input file
    tweet_rdd = sc.textFile(input_file)
    
    # Persist the RDD in Memory
    tweet_rdd.persist(pyspark.StorageLevel.MEMORY_ONLY)

    tags_list = ['created_at','id','text','lang']

    selective_tags_rdd = tweet_rdd.map(filterJSON_RDD)
    # print 'Number of Rows in RDD --> ' + str(selective_tags_rdd.count())
    #print selective_tags_rdd.take(10)
    selective_tags_rdd.saveAsTextFile(output_file)
    #ofile=open(output_file,'w')
    #for tweet in open(input_file,'r'):
        # call the function to process and write
    #    filterJSON(tweet, tags_list, ofile)
    
    # Select all tweets for 'oracle'

    token_to_search = 'microsoft'
    token_tweets = tweet_rdd.filter(lambda x : token_to_search in x.lower())
    print 'Number of tweets for ' + token_to_search + ' --> ' + str(token_tweets.count())

    token_to_search = 'oracle'
    token_tweets = tweet_rdd.filter(lambda x : token_to_search in x.lower())
    print 'Number of tweets for ' + token_to_search + ' --> ' + str(token_tweets.count())

    token_to_search = 'google'
    token_tweets = tweet_rdd.filter(lambda x : token_to_search in x.lower())
    print 'Number of tweets for ' + token_to_search + ' --> ' + str(token_tweets.count())

    token_to_search = 'blockchain'
    token_tweets = tweet_rdd.filter(lambda x : token_to_search in x.lower())
    print 'Number of tweets for ' + token_to_search + ' --> ' + str(token_tweets.count())

    sc.stop()





