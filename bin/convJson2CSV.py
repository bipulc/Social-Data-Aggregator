#! /usr/bin/env python

'''
Python tool to pull a set of tags (input via a file) from JSON and write to a csv file ( Todo - or database table )
'''

# Import required packages

import argparse
import os
import json

# read input params

parser = argparse.ArgumentParser()
parser.add_argument("-i", type=str, help="Tweet file containing data in JSON format", required=True)
parser.add_argument("-t", type=str, help="file with list of tags to pull. Tags separated by newline", required=True)
parser.add_argument("-o", type=str, help="name of output file", required=True)

args = parser.parse_args()
input_file = args.i
tags_file = args.t
output_file = args.o

# validate input params

if not os.path.exists(input_file):
    print ('{:15} {:30} {:30}'.format('Input file - ',input_file,' does not exist .. exit'))
    exit(1)

if not os.path.exists(tags_file):
    print ('{:15} {:30} {:30}'.format('Tags file - ',tags_file,' does not exist .. exit'))

# Read file containing set of tags to pull and store in a list
tags_list = []
for tags in open(tags_file,'r'):
    tags_list.append(tags.strip())

# Class to convert JSON to CSV
#   __init__ - initialise an object (input, one line from the JSON tweet file and list containing set of tags, output fle name)
#            - build dictionary from JSON data
#   Function - PullTags
#   Function - WriteTags (append to putput file name)

class ConvertJSON:
    def __init__(self, tweet, tags, ofile):
        self.tweet_json = tweet
        self.tags_list = tags
        self.output_file = ofile

    def pullTags(self):
        # dump JSON in a dictionary
        value_list = []
        try:
            json_dict = json.loads(self.tweet_json)
        except ValueError:
            # most likely a malformed JSON. Do nothing
            pass

        for tags in tags_list:
            try:
                if 'user' in tags:
                    t = []
                    for tags_token in tags.split('.'):
                        t.append(tags_token)
                    value_list.append(json_dict[t[0]][t[1]])
                else:
                    #print tags,json_dict[tags]
                    #print type(json_dict[tags])
                    if type(json_dict[tags]) is unicode:
                        #print tags
                        value_string = json_dict[tags]
                        value_string = value_string.replace('\n', ' ').replace('\r', '')
                        # print value_string
                        value_list.append(value_string)
                    else:
                        value_list.append(json_dict[tags])
                #print value_list
            except:
                pass
        value_string = ",".join(map(str,value_list))
        ofile.write(value_string)
        ofile.write('\n')

def callConvertJson(tweet_json, tags_list, ofile):
    c = ConvertJSON(tweet_json, tags_list, ofile)
    c.pullTags()

# Read file containing tweets

if os.path.exists(output_file):
    os.remove(output_file)
    
ofile=open(output_file,'w')
ofile.write('created_at,lang,id,retweet_count,user_id,user_followers_count,user_friends_count,user_screen_name\n')
for tweet in open(input_file,'r'):
    # call the function to process and write
    callConvertJson(tweet, tags_list, ofile)



