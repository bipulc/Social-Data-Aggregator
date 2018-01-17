# Social Data Aggregator Project

#### Started - 29 Sep 2017
#### In progress

This project attempts to build an application to collect user feedback for a brand (e.g. Nike) or an institution (e.g. Harrow Council) from social media sites primarily twitter and facebook, categorise as positive or negative sentiment and provides an overall trend of 'like' or 'dislike' for a brand or an institution.

## High Level use cases:

- Ability to analyse Tweets or Facebook feed in real time - Streaming
- Ability to analyse historical data.
- Categorisation of collected data points in a 'Positive' or 'Negative' sentiment.
- User should be able to provide a token (brand name or topic) for analysis.
    e.g. - What people are saying about ‘Harrow Council’ ?
- User should be able to provide up to 3 tokens (brand name or topic) for analysis and comparison.
    e.g. - Compare popularity of Nike and Reebok
- Result can optionally be categorised geographically.
    e.g. - Where was XBOX most popular in last 24 hours ?

## Logical Architecture

![Logical Architecture](https://github.com/bipulc/sentiment_analysis/blob/master/logical_architecture.jpg)

## Data Collector and Loader

Data Collector: Python script dataCollection.py in bin directory will implement the following 

- To collect data on local filesystem (completed), using [Tweepy](http://docs.tweepy.org/en/v3.5.0/getting_started.html). 
- To collect data in Kafka (completed), using kafka producer and tweepy.
- To write data from kafka topic to local filesystem (completed).
- Kafka to HDFS using flume (to be developed)

## Collecting Data

dataCollection.py usage:
```
usage: dataCollection.py [-h] -c C -s S

optional arguments:
  -h, --help  show this help message and exit
  -c C        configuration file
  -s S        data source
```
Example of collecting twitter streaming data and store to local FS

```
./dataCollection.py -c ../etc/configfile -s twitter
```

## Analytics 
To analyse data in real-time, I will be using Spark. Detailed analysis of components required as well as algorithm for analytics is pending. I may use an out of box analytics service available on Oracle or Google Cloud Platform. Details TBD.

## User Interface
There will be two distinct user interface. 
1.  To allow users to input the tokens for which they would like to collect data and analyse.
2.  To view the result of analysis.

UI will be very simple, unless I find a contributor willing to spend time on developing UI. It will be written using Python Flask framework.

