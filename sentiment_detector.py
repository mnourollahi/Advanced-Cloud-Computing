from __future__ import print_function
import boto3
import json
import uuid
from urllib.parse import unquote_plus

def lambda_handler(event, context):
    pattern = event['pattern']
    s3_client = boto3.client('s3')

    bucket = 'tp3-bucket'
    key = 'input.json'
    json_file = s3_client.get_object(Bucket=bucket, Key=key)
    content = json_file["Body"].read().decode('utf-8')
    tweets = json.loads(content)

    comprehend = boto3.client('comprehend')
    lambda_client = boto3.client('lambda')
    sentiment_bucket = 'sentiment-b'
    s3_sentiment = 'sentiment.json'
    sentiments = {}

    for item in tweets:
        id = tweets[item]['id']
        text = tweets[item]['text']
        date = tweets[item]['date']
        sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='en')['Sentiment']

        if sentiment == 'POSITIVE':
            sentiment = 'Positive'
        elif sentiment == 'NEGATIVE':
            sentiment = 'Negative'
        else:
            sentiment = 'Neutral'

        score = comprehend.detect_sentiment(Text=text, LanguageCode='en')['SentimentScore'][sentiment]
        sentiments.update({item: {'id': id, 'sentiment': sentiment, 'score': score, 'text': text, 'date': date}})

    s3_client.put_object(Body=(bytes(json.dumps(sentiments).encode('UTF-8'))), Bucket=sentiment_bucket,
                         Key=s3_sentiment)
    payload = {"pattern": pattern}
    response = lambda_client.invoke(FunctionName='db_patterns', InvocationType='RequestResponse', Payload=json.dumps(payload))

    return str(response)
