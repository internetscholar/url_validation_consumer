import requests
import boto3
import json
import psycopg2
from psycopg2 import extras
import configparser
import os
import logging
import urllib3
import traceback
import tldextract
from urllib.parse import urlparse


def main():
    # todo: collect the text too (in case Watson is unable to retrieve it).
    # todo: create a module to find URLs on Wayback machine.
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if 'DISPLAY' not in os.environ:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)
    logging.info('Connected to database %s', config['database']['db_name'])

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=config['aws']['region_queues']
    )
    sqs = aws_session.resource('sqs')
    logging.info('Connected to AWS in %s', aws_credential['region_name'])

    ip = requests.get('http://checkip.amazonaws.com').text.rstrip()
    urls_queue_empty = False
    url = None
    received_message = None
    urls_queue = sqs.get_queue_by_name(QueueName='url_validation')
    user_agent = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0'}
    try:
        while not urls_queue_empty:
            message = urls_queue.receive_messages()
            if len(message) == 0:
                urls_queue_empty = True
                logging.info('No more messages')
            else:
                received_message = json.loads(message[0].body)
                message[0].delete()
                logging.info('Message received with %d urls', len(received_message))
                for url in received_message:
                    logging.info('Accessing %s - %s', url['project_name'], url['url'])

                    if urlparse(url['url']).scheme not in ['https', 'http']:
                        final_url = url['url']
                        status_code = 601    # Code for scheme different from https and http
                        headers = None
                        history = None
                    else:
                        # noinspection PyBroadException
                        try:
                            r = requests.head(url['url'], headers=user_agent,
                                              allow_redirects=True, verify=False, timeout=15)
                        except Exception:
                            logging.info('Exception!')
                            final_url = url['url']
                            status_code = 600    # Code for exception during URL validation.
                            headers = None
                            history = None
                            cur.execute("insert into error (current_record, error, module, ip) VALUES (%s, %s, %s, %s)",
                                        (json.dumps(url),
                                         traceback.format_exc(),
                                         'url_validation_consumer-internal',
                                         ip), )
                            conn.commit()
                            logging.info('Saved record with error information')
                        else:
                            final_url = r.url
                            status_code = r.status_code
                            if len(r.history) == 0:
                                history = None
                            else:
                                history = list()
                                for history_element in r.history:
                                    history.append({'url': history_element.url,
                                                    'status_code': history_element.status_code})
                            headers = dict(r.headers)

                    tld = dict(tldextract.extract(final_url)._asdict())
                    components = dict(urlparse(final_url)._asdict())
                    cur.execute("""
                        insert into url
                            (url, status_code, project_name, headers, components, tld)
                        VALUES
                            (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                                """,
                                (final_url,
                                 status_code,
                                 url['project_name'],
                                 None if headers is None else json.dumps(headers),
                                 json.dumps(components),
                                 json.dumps(tld),))
                    logging.info('Saved record with url')
                    cur.execute("""
                        insert into url_history
                            (url, project_name, history)
                        VALUES
                            (%s, %s, %s)
                                """,
                                (final_url,
                                 url['project_name'],
                                 None if history is None else json.dumps(history),))
                    logging.info('Saved record with history')

                    conn.commit()
    except Exception:
        conn.rollback()
        # add record indicating error.
        cur.execute("insert into error (current_record, error, module, ip) VALUES (%s, %s, %s, %s)",
                    (json.dumps(url),
                     traceback.format_exc(),
                     'url_validation_consumer',
                     ip), )
        logging.info('Saved record with error information')
        conn.commit()
        if received_message is not None:
            urls_queue.send_message(MessageBody=json.dumps(received_message))
            logging.info('Enqueued URLs after exception')
        raise

    conn.close()
    logging.info('Disconnected from database')


if __name__ == '__main__':
    main()
