import tweepy

consumer_key = 'RlJR1V2EXLRn8DZ6IPFZh3YDF'
consumer_secret = 'ZPa28wPv1IkfmyNrSe9RD2Eu5Ju4XJMuN4ptunynJZhnot6yoX'
access_token = '711176417191526400-SLh28ze926pG5DD3KQJAPFDF3f6s51g'
access_token_secret = 'ogU1gC79hxrSBLceSaPbr1TaAh468MDp1sJXk2fsm6I7c'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

#public_tweets = api.home_timeline()
#for tweet in public_tweets:
#    print (tweet.text)

print(api.rate_limit_status())

