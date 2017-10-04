# Sentiment Analytics Project

This project attempt to build an application to collect user feedback for a brand (e.g. Nike) or an institution (e.g. Harrow Council) from social media sites primarily twitter and facebook, and categorise as positive or negative sentiment.

## High Level use cases:

- Ability to analyse Tweets or Facebook feed in real time - Streaming
- Ability to analyse historical data.
- User should be able to provide a token (brand name or topic) for analysis.
    e.g. - What people are saying about ‘Harrow Council’ ?
- User should be able to provide up to 3 tokens (brand name or topic) for analysis and comparison.
    e.g. - Compare popularity of Nike and Reebok
- Result can optionally be categorised geographically.
    e.g. - Where was XBOX most popular in last 24 hours ?

## Logical Architecture

![Logical Architecture](https://github.com/bipulc/sentiment_analysis/blob/master/logical_architecture.jpg)

## Data Collector and Loader

Data Collector is written in Python using [Tweepy](http://docs.tweepy.org/en/v3.5.0/getting_started.html). The basic structure of Collector is pretty simple. It implements following features (some of them still to be written).

