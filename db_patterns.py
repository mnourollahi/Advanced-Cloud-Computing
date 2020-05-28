from boto3 import client as boto3_client
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import json
import random
import boto3
from botocore.vendored import requests

def lambda_handler(event, context):
    _hostname = 'sentiments.c4ttzoyttspm.us-east-1.rds.amazonaws.com'
    _username = 'postgres'
    _password = '1qaz2wsx'
    _dbname = 'sentiments'
    schemas = {'proxy1', 'proxy2', 'sharding1', 'sharding2'}

    pattern = event['pattern']

    conn = psycopg2.connect(host=_hostname, user=_username, password=_password, dbname=_dbname)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    q2 = "create schema IF NOT EXISTS " + pattern + "1;"
    cur.execute(q2)
    q5 = "create schema IF NOT EXISTS " + pattern + "2;"
    cur.execute(q5)
    q1 = "DROP TABLE IF EXISTS " + pattern + "1.tweet_tbl;"
    cur.execute(q1)
    q6 = "DROP TABLE IF EXISTS " + pattern + "2.tweet_tbl;"
    cur.execute(q6)
    q3 = "CREATE TABLE  IF NOT EXISTS " + pattern + "1.tweet_tbl(TweetID TEXT  PRIMARY KEY, " \
         "Sentiment TEXT NOT NULL, Score TEXT  NOT NULL, Text  TEXT  NOT NULL, Date  TEXT  NOT NULL);"
    cur.execute(q3)
    q7 = "CREATE TABLE  IF NOT EXISTS " + pattern + "2.tweet_tbl(TweetID TEXT  PRIMARY KEY, " \
         "Sentiment TEXT NOT NULL, Score TEXT  NOT NULL, Text  TEXT  NOT NULL, Date  TEXT  NOT NULL);"
    cur.execute(q7)
    conn.commit()

    s3_client = boto3.client('s3')
    bucket = 'sentiment-b'
    key = 'sentiment.json'

    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    sentiments = json.loads(content)

    n = random.randint(0, (len(sentiments)-1))  # number of records for sharding

    for item in sentiments:
        id = sentiments[item]['id']
        sentiment = sentiments[item]['sentiment']
        score = sentiments[item]['score']
        text = sentiments[item]['text']
        date = sentiments[item]['date']

        if pattern == 'proxy':
            random_selection = random.randint(0, 1)
            if random_selection == 0:
                q4 = "INSERT INTO " + pattern + "1.tweet_tbl(TweetID, Sentiment, Score, Text, Date) VALUES (%s,%s,%s,%s,%s);"
                record_to_insert = (id, sentiment, score, text, date)
                cur.execute(q4, record_to_insert)
            else:
                q4 = "INSERT INTO " + pattern + "2.tweet_tbl(TweetID, Sentiment, Score, Text, Date) VALUES (%s,%s,%s,%s,%s);"
                record_to_insert = (id, sentiment, score, text, date)
                cur.execute(q4, record_to_insert)

        elif pattern == 'sharding':
            if n > 0:
                q4 = "INSERT INTO " + pattern + "1.tweet_tbl(TweetID, Sentiment, Score, Text, Date) VALUES (%s,%s,%s,%s,%s);"
                record_to_insert = (id, sentiment, score, text, date)
                cur.execute(q4, record_to_insert)
                n = n - 1
            else:
                q4 = "INSERT INTO " + pattern + "2.tweet_tbl(TweetID, Sentiment, Score, Text, Date) VALUES (%s,%s,%s,%s,%s);"
                record_to_insert = (id, sentiment, score, text, date)
                cur.execute(q4, record_to_insert)

    conn.commit()
    conn.close()

    return {'statusCode': 200}
