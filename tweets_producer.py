from googlefinance import getQuotes
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from apscheduler.schedulers.background import BackgroundScheduler

import logging
import json
import argparse
import time
import atexit

from flask import Flask, request, jsonify
import tweepy
from tweepy.auth import OAuthHandler
from tweepy.error import TweepError

consumer_key = 'rFHcyGScTaCBuQrMvo396G0Ti'
consumer_secret = 'J3ce0jC1BrP59qPCSDoenNncSbH4QccVEYFKVDZekGt6ETP11c'
access_token = '833853766050799618-O1Gg0RSJDIi3iVp1qWpqxuzlM7NwHkO'
access_secret = 'mg9tGFQZoRoU6SN6EqB9SjMCa50yM7bFdmQzaiVc4sgE3'
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()


logger_format = "%(asctime)-15s %(levelname)-8s %(message)s"

logging.basicConfig(format=logger_format)

logger = logging.getLogger("data-producer")

# DEBUG INFO WARNING ERROR
logger.setLevel(logging.DEBUG)

app = Flask(__name__)
app.config.from_envvar('ENV_CONFIG_FILE')
kafka_broker = app.config['CONFIG_KAFKA_ENDPOINT']
topic_name = app.config['CONFIG_KAFKA_TOPIC']
tweet_topic = app.config['CONFIG_KAFKA_TWITTER_TOPIC']

logger.debug("Kafka broker is %s" % kafka_broker)
logger.debug("Topic name is %s" % topic_name)

symbols = set()

producer = KafkaProducer(bootstrap_servers=kafka_broker)

stream = None

# - Tweepy listener class
class Listener(tweepy.StreamListener):
    def __init__(self, api, producer, topic_name, symbols):
    	"""
	    Listener initilizer
	    :param api: tweepy api
	    :param producer: Kafka producer
	    :param topic_name: tweet topic the kafka producer sent by
	    :param symbols: stock list
	    :return: None
	    """
        self.api = api    
        self.producer = producer
        self.topic_name = topic_name
        self.symbols = symbols
        super(tweepy.StreamListener, self).__init__()    
        print("Initiating Listener, topic is %s" % self.topic_name)

    def on_status(self, status):
    	"""
	    on_status method that is called when there is tweet
	    :param status: tweet status
	    :return: None
	    """
        
        # - filter the retweeted tweet so to keep tweet unique
        if not status.retweeted and 'RT @' not in status.text:
        	symbol = ""
        	for s in symbols:
        		if status.text.lower().find(s.lower()) > -1:
        			symbol += s
        			break

	        # - message to sent by Kafka producer    
	        message =  symbol + '^$$^' + str(status.user.id) + '^$$^' + status.user.screen_name.encode('utf-8') + '^$$^' + str(status.created_at) + '^$$^' + status.text.encode('utf-8') 
	        
	        try:
	        	logger.debug("Start to send messsage %s to Kafka with topic %s" % (message, self.topic_name))
	        	
	        	self.producer.send(topic=self.topic_name, value=message, timestamp_ms=time.time())
	        except KafkaTimeoutError as timeout_error:
	            logger.warn("Failed to send tweet caused by: %s" % (timeout_error.message))
	            return False
	        except Exception as e:
				logger.warn("Failed to send tweet caused by: %s" % e.message)
				return False
        else:
        	logger.debug("This is a re-tweeted message!!! Ignore it...")

        return True

    def on_error(self, status_code):
    	"""
	    action when error happens
	    :param status: tweet status_code (e.g. 420)
	    :return: True (always to return true in order to continue listening)
	    """

    	if status_code == 420:
        	logger.warn('Got an error with status code: ' + str(status_code))
        return True # To continue listening
 
    def on_timeout(self):
    	"""
	    action when timeout happens
	    :param: None
	    :return: True (always to return true in order to continue listening)
	    """

        logger.warn('Timeout...')
        return True # To continue listening

def fetch_twitter_status(producer, symbols):
	"""
    Retrieve English tweet associated with stock symbols (e.g. SNAP, AAPL, GOOG, etc)
    :param producer: Kafka producer
    :param symbols: stock symbol list
    :return: None
    """

	try:
		global stream
		if stream is not None and stream.running is True:
			logger.debug("Tweet streamming is running")
			stream.disconnect()
			del stream

		logger.info("Fetching tweets")
		stream_listener = Listener(api, producer, tweet_topic, symbols)
		stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
		stream.filter(track=symbols, async=True, languages=["en"])
	except TweepError as te:
		logger.debug("TweepyExeption: Failed to get tweet for stocks caused by: %s" % te.message)
	except Exception as e:
		logger.warn("Eception: Failed to get tweet for stocks caused by: %s" % e.message)



def fetch_price(producer, symbol):
	"""
    Retrieve stock price by stock symbol (e.g. SNAP, AAPL, GOOG, etc)
    :param producer: Kafka producer
    :param symbol: stock symbol 
    :return: None
    ->json.dumps(getQuotes(symbol)
    '[{"Index": "NASDAQ", "LastTradeWithCurrency": "152.78", "LastTradeDateTime": "2017-05-02T16:00:02Z", "LastTradePrice": "152.78", "Yield": "", "LastTradeTime": "4:00PM EDT", "LastTradeDateTimeLong": "May 2, 4:00PM EDT", "Dividend": "", "StockSymbol": "FB", "ID": "296878244325128"}]'
    """
	try:
		price = json.dumps(getQuotes(symbol))
		logger.debug('received stock price %s' % price)
		producer.send(topic=topic_name, value=price, timestamp_ms=time.time())
	except KafkaTimeoutError as timeout_error:
		#logger.error("Failed to send stock price for %s to Kafka" % symbol)
		logger.warn("Failed to send stock price for %s to Kafka, caused by: %s" % (symbol, timeout_error.message))
	except Exception:
		logger.warn("Failed to send stock price for %s to Kafka" % symbol)


def shutdown_hook():
	"""
    a shutdown hook to be called before the shutdown
    :param: None
    :return: None
    """
	try:
		logger.info("Flushing pending messages to Kafka, timeout is set to 10s")
		producer.flush(10)	
		logger.info("Finish flushing messages to Kafka")
	except KafkaError as ke:
		logger.warn("Failed to flush pending messages to kafka, caused by: %s" % ke.message)
	finally:
		try:
			logger.info("Closing Kafka connection")
			producer.close(10)
		except Exception as e:
			logger.error('Failed to close kafka connection, caused by: %s', e.message)

	try:
		logger.info('Shutting down scheduler')
		schedule.shutdown()
	except Exception as e:
		logger.error('Failed to shutdown scheduler, caused by: %s', e.message)

@app.route("/<symbol>", methods=["POST"])
def add_stock_record(symbol):
	"""
    User can add stock to retrieve stock data via sending a POST RESTFUL request
    :param symbol: stock symbol
    :return: updated stocks list and response code 200
    """
	print("add_stock_record")
	if not symbol:
		return jsonify({"error": "Stock name cannot be empty"}), 400

	if symbol in symbols:
		pass
	else:
		symbol = symbol.encode('utf-8')
		logger.info("Adding stock retrive job for %s" % symbol)
		symbols.add(symbol)
		schedule.add_job(fetch_price, "interval", [producer, symbol], seconds=1, id=symbol)
		tid = "tweet"
		fetch_twitter_status(producer, symbols)
		#schedule.add_job(fetch_twitter_status, "interval", [producer, symbols], seconds=5, id=tid)
		
	return jsonify(results=list(symbols)), 200


@app.route("/<symbol>", methods=["DELETE"])
def delete_stock_record(symbol):
	"""
    User can delete stock from stock list via sending a DELETE RESTFUL request
    :param symbol: stock symbol
    :return: update stocks list and response code 200
    """
	print("delete_stock_record")
	if not symbol:
		return jsonify({"error": "Stock name cannot by empty"}), 400

	if symbol not in symbols:
		pass
	else:
		logger.info("Deleting stock %s" % symbol)
		symbols.remove(symbol)
		schedule.remove_job(symbol)
		fetch_twitter_status(producer, symbols)
		#schedule.remove_job("tweet")


	return jsonify(results=list(symbols)), 200


if __name__ == "__main__":
	# - main program
	# - register shutdown_hool
	atexit.register(shutdown_hook)

	# - Load config into Flask app
	app.run(port=app.config['CONFIG_APPLICATION_PORT'])
