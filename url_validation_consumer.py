import requests
import boto3
import json
import psycopg2
import configparser
import os


if __name__ == '__main__':
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor()

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential[0],
        aws_secret_access_key=aws_credential[1],
        region_name=aws_credential[2]
    )
    sqs = aws_session.resource('sqs')

    tweets_queue_empty = False
    tweets_queue = sqs.get_queue_by_name(QueueName='tweet-urls')
    while not tweets_queue_empty:
        message = tweets_queue.receive_messages()
        if len(message) == 0:
            tweets_queue_empty = True
        else:
            received_message = json.loads(message[0].body)
            message[0].delete()
            for tweet in received_message['tweets']:
                failed = False
                error = None
                status_code = None
                url = None
                try:
                    r = requests.head(tweet['url'], allow_redirects = True, verify = False, timeout=5)
                    url = r.url
                    status_code = r.status_code
                except Exception as e:
                    url = tweet['url']
                    error = repr(e)

                cur.execute("""insert into tweet_url
                              (query_alias, tweet_id, created_at, url, expanded_url, error, status_code)
                              values 
                              (%s, %s, to_timestamp(%s,'Dy Mon DD HH24:MI:SS +0000 YYYY'), %s, %s, %s, %s)
                              ON CONFLICT DO NOTHING""",
                            (received_message['query_alias'],
                             tweet['tweet_id'],
                             tweet['created_at'],
                             tweet['url'],
                             url,
                             error,
                             status_code))

                conn.commit()

    conn.close()