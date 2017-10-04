#! /usr/bin/env python

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

consumer_key = 'RlJR1V2EXLRn8DZ6IPFZh3YDF'
consumer_secret = 'ZPa28wPv1IkfmyNrSe9RD2Eu5Ju4XJMuN4ptunynJZhnot6yoX'
access_token = '711176417191526400-SLh28ze926pG5DD3KQJAPFDF3f6s51g'
access_token_secret = 'ogU1gC79hxrSBLceSaPbr1TaAh468MDp1sJXk2fsm6I7c'

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print status

    #This handles Twitter authetification and the connection to Twitter Streaming API

    if __name__ == '__main__':

        l = StdOutListener()
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = Stream(auth, l)

        #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
        stream.filter(track=['python', 'javascript', 'ruby'])
