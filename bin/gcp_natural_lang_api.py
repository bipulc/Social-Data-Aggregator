#! /usr/bin/env python

import google.cloud.language

# Create a Language client.
language_client = google.cloud.language.LanguageServiceClient()

# TODO (Developer): Replace this with the text you want to analyze.
text = u'@BarclaysUKHelp are customers going to receive compensation due to the fact you have duplicated debit card transactions?! Sounds like some people have got no funds left and this has been going on for hours. Luckily it hasnt completely wiped me out. Disgraceful from Barclays!!!!!'
document = google.cloud.language.types.Document(
    content=text,
    type=google.cloud.language.enums.Document.Type.PLAIN_TEXT)

# Use Language to detect the sentiment of the text.
response = language_client.analyze_sentiment(document=document)
sentiment = response.document_sentiment

print(u'Text: {}'.format(text))
print(u'Sentiment: Score: {}, Magnitude: {}'.format(
sentiment.score, sentiment.magnitude))
                 
                