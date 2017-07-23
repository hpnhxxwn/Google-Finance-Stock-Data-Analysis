# read from any kafka broker and topic
# relay to redis channel

import argparse
from kafka import KafkaConsumer
import redis
import logging
import atexit

from apscheduler.schedulers.background import BackgroundScheduler
import thread
from multiprocessing import Pool
#import schedule
import threading, time

logging.basicConfig()
logger = logging.getLogger('redis-publisher')
logger.setLevel(logging.DEBUG)


schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

def shutdown_hook(kafka_consumer):
	"""
	a shutdown hook to be called before the shutdown
	:param: None
	:return: None
	"""
	try:
		for consumer in kafka_consumer:
			logger.info('closing kafka client')
			consumer.close()	
			logger.info("Finish closing kafka consumer")
	except KafkaError as ke:
		logger.warn("Failed to flush pending messages to kafka, caused by: %s" % ke.message)

	try:
		logger.info('Shutting down scheduler')
		schedule.shutdown()
	except Exception as e:
		logger.error('Failed to shutdown scheduler, caused by: %s', e.message)


class Consumer(threading.Thread):
	daemon = True
	def __init__(self, redis_client, redis_channel, consumer):
		threading.Thread.__init__(self)
		self.redis_channel = redis_channel
		self.redis_client = redis_client
		self.consumer = consumer

	def run(self):
		print(self.redis_channel)
		
		for msg in self.consumer:
			if (self.redis_channel == "trend"):
				logger.info('received message from kafka %s and send it via channel %s' % (msg.value, self.redis_channel))
			self.redis_client.publish(self.redis_channel, msg.value)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name', help="the kafka average stock price topic")
	parser.add_argument('tweet_topic_name', help="the kafka stock tweet topic")
	parser.add_argument('trend_topic_name', help="the kafka stock trend topic")
	parser.add_argument('kafka_broker', help='the location of kafka')
	parser.add_argument('redis_host', help="the ip of the redis server")
	parser.add_argument('redis_port', help='the port of redis server')
	parser.add_argument('redis_channel', help='the channel to publish to')
	parser.add_argument('redis_tweet_channel', help='the tweet channel to publish to')
	parser.add_argument('redis_trend_channel', help='the tweet channel to publish to')

	args 				= parser.parse_args()
	topic_name 			= args.topic_name
	tweet_topic_name 	= args.tweet_topic_name
	trend_topic_name	= args.trend_topic_name
	kafka_broker 		= args.kafka_broker
	redis_host 			= args.redis_host
	redis_port 			= args.redis_port
	redis_channel 		= args.redis_channel
	redis_tweet_channel = args.redis_tweet_channel
	redis_trend_channel = args.redis_trend_channel

	# - create three kafka client
	average_stock_price_kafka_consumer  = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)
	stock_tweet_kafka_consumer			= KafkaConsumer(tweet_topic_name, bootstrap_servers=kafka_broker)
	stock_trend_consumer				= KafkaConsumer(trend_topic_name, bootstrap_servers=kafka_broker)


	# - create redis client, single redis host, so strictredis
	redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

	atexit.register(shutdown_hook, [average_stock_price_kafka_consumer, stock_tweet_kafka_consumer, stock_trend_consumer])

	# - pass the redis client and kafka consumers to three threads. Process the three topic separately
        thread1 = Consumer(redis_client, redis_channel, average_stock_price_kafka_consumer)
        thread2 = Consumer(redis_client, redis_tweet_channel, stock_tweet_kafka_consumer)
        thread3 = Consumer(redis_client, redis_trend_channel, stock_trend_consumer)
    
        thread1.start()
        thread2.start()
        thread3.start()
        
        while True:
            pass

