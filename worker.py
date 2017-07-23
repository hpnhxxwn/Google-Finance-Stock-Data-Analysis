#!/usr/bin/python 
# -*- coding: utf-8 -*-

import atexit
import logging
import json
import sys
import time

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
#from pyspark_cassandra import streaming

from zipkin.api import api as zipkin_api
from py_zipkin.zipkin import zipkin_span
import requests
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

import configparser

# instrumentation
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

topic = None
target_topic = None
target_topic_2 = None
brokers = None
kafka_producer = None

# Ziplin transport handler instantiated through a RESTFUL POST request
def http_transport_handler(span):
    body = '\x0c\x00\x00\x00\x01' + span
    requests.post(
        "http://localhost:9411/api/v1/spans",
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
        )

@zipkin_span(service_name='assignment6', span_name='get data from Kafka and store into Cassandra')
def persist_data(message, session):
    """
    persist stock data into cassandra
    :param stock_data:
        the stock data looks like this:
        [{
            "Index": "NASDAQ",
            "LastTradeWithCurrency": "109.36",
            "LastTradeDateTime": "2016-08-19T16:00:02Z",
            "LastTradePrice": "109.36",
            "LastTradeTime": "4:00PM EDT",
            "LastTradeDateTimeLong": "Aug 19, 4:00PM EDT",
            "StockSymbol": "AAPL",
            "ID": "22144"
        }]
    :param cassandra_session:
    :return: None
    """
    try:
        logger.debug("start to save message %s \n" % message)
        parsed = json.loads(message)[0]
        print(parsed)
        symbol = parsed.get('StockSymbol')
        price = float(parsed.get('LastTradePrice'))
        trade_time = parsed.get('LastTradeDateTime')

        statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)" % (table, symbol, trade_time, price)

        session.execute(statement)
        logger.info("saved message to cassandra\n")
    except Exception:       
        logger.error("cannot save message\n")

@zipkin_span(service_name='assignment6', span_name='Shutdown kafka')
def shutdown_hook(producer, session):
    """
    a shutdown hook to be called before the shutdown
    :param producer: instance of a kafka producer
    :return: None
    """
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
            logger.info('Closing Cassandra Session')
            session.shutdown()
        except Exception as e:
            logger.warn('Failed to close kafka or cassandra connection, caused by: %s', e.message)

@zipkin_span(service_name='assignment6', span_name='Start Cassandra')
def startCassandraSession(contact_points):
    """
    Start a Cassandra session
    :param contact_points: contact point to start the a Cassandra session
    :return: Cassandra session
    """
    cassandra_cluster = Cluster(contact_points=contact_points.split(","))
    global session
    session = cassandra_cluster.connect()
    return session

@zipkin_span(service_name='assignment6', span_name='create keyspace and table in Cassandra')
def cassandraCreateTable(session, keyspace, table):
    """
    Create keyspace and tables if not exist
    :param session: Cassandra session
    :param keyspace: Cassandra keyspace to be created
    :param table: Cassandra table to be created
    :return: None
    """

    session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'" % keyspace)
    session.set_keyspace(keyspace)
    if table == "stock_average_price":
        session.execute("CREATE TABLE IF NOT EXISTS %s (stock_symbol text, record_time timestamp, average_price float, PRIMARY KEY (stock_symbol, record_time))" %  table)
    elif (table == "stock_trend"):
        session.execute("CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, trend text, stock_price_difference float, time_elapsed text, PRIMARY KEY (stock_symbol, trade_time) )" % table)
    elif table == "stock":
        session.execute("CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY (stock_symbol,trade_time))" %  table)
    elif table == "stock_tweet":
        session.execute("CREATE TABLE IF NOT EXISTS %s (stock_symbol text, user_id text, user_screen_name text, tweet_create_time timestamp, tweet text, PRIMARY KEY (stock_symbol, user_id))" %  table)

@zipkin_span(service_name='assignment6', span_name='Process stock and tweet streaming data')
def process_stock_stream(stream, keyspace, stock_table, averate_price_table, stock_trend_table, session):
    """
    Process stock stream
    :param stream: stock stream
    :param keyspace: Cassandra keyspace to be created
    :param stock_table: stock table that stores stock trade_time and trade_price
    :param averate_price_table: stock average price table that stores average price of stocks
    :param stock_trend_table: stock trend table that shows stock trend
    :param session: Cassandra session
    :return: None
    """

    # - Send the average stock price back to Kafka
    def send_to_kafka(rdd):
        results = rdd.collect()
        if (len(results) == 0):
            return
        for r in results:
            data = json.dumps(
                {
                    'symbol': r[0],
                    'timestamp': time.time(),
                    'average': r[1]
                }
            )
            try:
                logger.info('Sending average price %s to kafka' % data)
                kafka_producer.send(target_topic, value=data)
            except KafkaError as error:
                logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)

    def pair(data):
        record = json.loads(data[1].decode('utf-8'))[0]
        return record.get('StockSymbol'), (float(record.get('LastTradePrice')), 1, record.get('LastTradeDateTime'))

    def store_average_price_to_cassandra(rdd):
        if (rdd.count() == 0):
            return

        results = rdd.collect()
        for a in results:
            stmt = "INSERT INTO %s (stock_symbol, record_time, average_price) VALUES ('%s', '%s', %f)" % (averate_price_table, a[0], long(time.time() * 1e6), a[1])
            session.execute(stmt)

    def store_stock_data_to_cassandra(rdd):
        results = rdd.collect()

        for s in results:
            stmt = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)" % (stock_table, s[0], s[1], s[2])
            session.execute(stmt)

    # - stock trend, resolution is days
    def store_stock_trend_to_cassandra(rdd):
        
        results = rdd.collect()
        
        for tuple in results:
            stock_symbol = tuple[0].encode('utf-8')
            logger.info('query stock data')
            statement = "SELECT trade_time, trade_price FROM %s WHERE stock_symbol='%s'" % (stock_table, stock_symbol)
            row = session.execute(statement)

            if (len(row._current_rows) != 0):
                current_trade_price = tuple[1][1]
                
                ct = tuple[1][0].encode('utf-8')
                current_time = datetime.strptime(ct, "%Y-%m-%dT%H:%M:%SZ")
                #current_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%S.%f')
                #current_time = datetime.strptime(str(time.time()), format="%Y-%m-%d %H:%M:%S.%f")
                history_time = row[len(row._current_rows)-1].trade_time
                #history_time.date()
                #history_time = datetime.strptime(ht, format="%Y-%m-%dT%H:%M:%S.%f")
                price_diff = current_trade_price - row[len(row._current_rows)-1].trade_price
                time_diff = (current_time.date() - history_time.date()).days
                if time_diff < 1:
                    continue
                stock_price_change = "up"
                if (price_diff < 0.001 and price_diff > -0.001):
                    stock_price_change = "not changed"
                    price_diff = 0
                elif (price_diff <- 0.01):
                    stock_price_change = "down"

                session.execute("INSERT INTO %s (stock_symbol, trade_time, trend, stock_price_difference, time_elapsed) VALUES ('%s', '%s', '%s', %f, '%s')" % (stock_trend_table, stock_symbol, current_time, stock_price_change, price_diff, time_diff))
    
                data = json.dumps(
                    {
                        'symbol': stock_symbol,
                        'trade_time': ct,
                        'trend': stock_price_change,
                        'stock_price_difference': price_diff,
                        'time_elapsed': time_diff
                    }
                )
                try:
                    logger.info('Sending average price %s to kafka' % data)
                    kafka_producer.send(target_topic_2, value=data)
                except KafkaError as error:
                    logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)

    # - This data is used many times during transformation, so better to cache it in memory to faster transformation
    post_process_stream = stream.map(pair).cache()
    stock_stream = post_process_stream.map(lambda x: (x[0], x[1][2], x[1][0])) # stock_symbol, trade_time, trade_price
    

    average_stock_price_stream = post_process_stream.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda (k, v): (k, v[0]/v[1]))
    average_stock_price_stream.foreachRDD(send_to_kafka)
    average_stock_price_stream.map(lambda x: {"stock_symbol":x[0], "record_time":time.time(), "average_price":x[1]}) #.saveToCassandra(keyspace, averate_price_table)

    average_stock_price_stream.foreachRDD(store_average_price_to_cassandra)
    #stock_stream.pprint()
    stock_trend_stream = stock_stream.map(lambda x: (x[0], (x[1], x[2]))).reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: x[0]))
    
    logger.debug("DEBUGGING INFO:")
    stock_trend_stream.pprint()
    stock_trend_stream.foreachRDD(store_stock_trend_to_cassandra)
    
    stock_stream.foreachRDD(store_stock_data_to_cassandra)

@zipkin_span(service_name='assignment6', span_name='Process tweet streaming data')
def process_tweet_stream(stream, keyspace, tweet_table, session):
    """
    Process tweet stream
    :param stream: tweet stream
    :param keyspace: Cassandra keyspace to be created
    :param tweet_table: Cassandra tweet table that stores tweets per stock
    :param session: Cassandra session
    :return: None
    """

    def send_tweet_to_cassandra(rdd):
        results = rdd.collect()
        try:
            for tweet in results:
                symbol = tweet[0]
                user_id = int(tweet[1])
                user_name = tweet[2]
                tweet_create_time = tweet[3]
                tweet_text = str(tweet[4].encode('utf-8'))
                tweet_text = tweet_text.replace("\"", "")
                tweet_text = tweet_text.replace("\"", "")

                stmt = "INSERT INTO %s (stock_symbol, user_id, user_screen_name, tweet_create_time, tweet) VALUES ('%s', '%s', '%s', '%s', '%s')" % (tweet_table, symbol.encode('utf-8'), user_id, user_name.encode('utf-8'), tweet_create_time.encode('utf-8'), tweet_text)
                
                #print(stmt)
                session.execute(stmt)
        except Exception as e:
            logger.debug("Failed to insert tweet due to %s" % e.message)

    tweets = stream.map(lambda x: x[1].split('^$$^')).filter(lambda y: len(y[0]) > 0)

    tweets.foreachRDD(send_tweet_to_cassandra)

# - main program
if __name__ == '__main__':
    #if len(sys.argv) != 11:
    #    print("Usage: stream-process.py [topic] [tweet_topic] [target-topic] \
    #                                    [broker-list] [keyspace] [table] [average price table] \
    #                                    [stock trend table] [stock tweet table] [cassandra_host]")
    #    exit(1)

    settings = configparser.ConfigParser()
    settings._interpolation = configparser.ExtendedInterpolation()
    settings.read('config.ini')
    # - create SparkContext and StreamingContext
    sc = SparkContext("local[*]", "StockAveragePrice")
    sc.setLogLevel('INFO')
    # every 5 seconds
    ssc = StreamingContext(sc, 5)

    #topic, tweet_topic, target_topic, brokers, keyspace, stock_table, \
    #averate_price_table, stock_trend_table, stock_tweet_table, cassandra_host = sys.argv[1:]

    topic               = settings.get('assignment6', 'CONFIG_KAFKA_TOPIC')
    tweet_topic         = settings.get('assignment6', 'CONFIG_KAFKA_TWITTER_TOPIC')
    target_topic        = settings.get('assignment6', 'CONFIG_TARGET_TOPIC')
    target_topic_2      = settings.get('assignment6', 'CONFIG_TARGET_TOPIC_2')
    brokers             = settings.get('assignment6', 'CONFIG_KAFKA_ENDPOINT')
    kafka_broker_port   = settings.get('assignment6', 'CONFIG_KAFKA_PORT')
    keyspace            = settings.get('assignment6', 'CONFIG_CASSANDRA_KEYSPACE')
    stock_table         = settings.get('assignment6', 'CONDIG_CASSANDRA_STOCK_TABLE')
    averate_price_table = settings.get('assignment6', 'CONDIG_CASSANDRA_STOCK_AVERAGE_PRICE_TABLE')
    stock_trend_table   = settings.get('assignment6', 'CONFIG_CASSANDRA_STOCK_TREND')
    stock_tweet_table   = settings.get('assignment6', 'CONFIG_CASSANDRA_STOCK_TWEET')
    cassandra_host      = settings.get('assignment6', 'CONFIG_CASSANDRA_CONTACT_POINT')

    kafka_brokers       = brokers+":"+kafka_broker_port

    with zipkin_span(
        service_name='assignment6',
        span_name='Spark streaming to process stock price and calculate average price',
        transport_handler=http_transport_handler,
        sample_rate=100.0, # Value between 0.0 and 100.0
    ):
        # - create Cassandra session, keyspace and tables
        session = startCassandraSession(cassandra_host)
        cassandraCreateTable(session, keyspace, stock_table)
        cassandraCreateTable(session, keyspace, averate_price_table)
        cassandraCreateTable(session, keyspace, stock_trend_table)
        cassandraCreateTable(session, keyspace, stock_tweet_table)

        # - instantiate a simple kafka producer
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_brokers
        )

        # - instantiate a kafka stock stream and a tweet stream for processing
        stockDirectKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': kafka_brokers})
        tweetDirectKafkaStream = KafkaUtils.createDirectStream(ssc, [tweet_topic], {'metadata.broker.list': kafka_brokers})
        process_stock_stream(stockDirectKafkaStream, keyspace, stock_table, averate_price_table, stock_trend_table, session)
        process_tweet_stream(tweetDirectKafkaStream, keyspace, stock_tweet_table, session)
        
        # - setup proper shutdown hook
        atexit.register(shutdown_hook, kafka_producer, session)

        ssc.start()
        ssc.awaitTermination()
