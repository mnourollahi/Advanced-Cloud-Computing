from botocore.exceptions import ClientError
import boto3
import json
from flask import Flask
from flask import request
import psycopg2
from psycopg2.extras import RealDictCursor
import time

app = Flask(__name__)
s3 = boto3.resource('s3')
bucket= 'tp3-bucket'
s3_file= 'input.json'
#inputJson= 'content'

_hostname = 'sentiments.c4ttzoyttspm.us-east-1.rds.amazonaws.com'
_username = 'postgres'
_password = '1qaz2wsx'
_dbname = 'sentiments'
schemas = {'proxy1', 'proxy2', 'sharding1', 'sharding2'}

conn = psycopg2.connect(host=_hostname, user=_username, password=_password, dbname=_dbname)
cur = conn.cursor(cursor_factory=RealDictCursor)
"""
@app.route('/postres', methods=['POST'])
def postres():
    if request == (200):
        flag = True

    return 'OK'
"""
@app.route('/postjson', methods = ['POST'])
def postJsonHandler():
    flag = True
    if request.is_json:
        start = time.time()
        content = request.get_json()
        s3object = s3.Object(bucket, s3_file)
        s3object.put(Body=(bytes(json.dumps(content).encode('utf8'))))
elif request == 'OK':
        flag = False
        result= 'ok'
    else:
        result= 'failure'
    while flag:
        time.sleep(1)

    execution_time= "Time taken for execution: {0: .2f} seconds".format(time.time() - start)

    q1 = "select * from proxy1.tweet_tbl;"
    cur.execute(q1)
    res1 = cur.fetchall()

    q2 = "select * from proxy2.tweet_tbl;"
    cur.execute(q2)
    res2 = cur.fetchall()

    q3 = "select * from sharding1.tweet_tbl;"
    cur.execute(q3)
    res3 = cur.fetchall()

    q4 = "select * from sharding2.tweet_tbl;"
    cur.execute(q4)
    res4 = cur.fetchall()
    result= execution_time, json.dumps(
        {"result proxy1: ": res1, "result proxy2: ": res2, "result sharding1: ": res3, "result sharding2: ": res4})

    return result
#@app.route('/getsentiment', methods=['GET'])
#def getsentiment():

app.run(host='0.0.0.0', port= 5000)
