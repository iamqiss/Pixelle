#!/usr/bin/env python3
import argparse
import signal
import sys
from .ingestion.consumer import InteractionsConsumer
from .config import settings

def run_consumer():
	"""Run the Kafka consumer to process interactions and build features."""
	print(f"Starting Overdrive consumer for topic: {settings.kafka_topic_interactions}")
	print(f"Connecting to Kafka brokers: {settings.kafka_brokers}")
	
	consumer = InteractionsConsumer(
		settings.kafka_brokers,
		settings.kafka_topic_interactions,
		'overdrive-consumer'
	)
	
	# Handle graceful shutdown
	def signal_handler(signum, frame):
		print("\nShutting down consumer...")
		consumer.stop()
		sys.exit(0)
	
	signal.signal(signal.SIGINT, signal_handler)
	signal.signal(signal.SIGTERM, signal_handler)
	
	try:
		consumer.run()
	except KeyboardInterrupt:
		print("\nConsumer interrupted")
	finally:
		consumer.stop()

def main():
	parser = argparse.ArgumentParser(description='Overdrive ML Service')
	parser.add_argument('command', choices=['api', 'consumer'], help='Command to run')
	
	args = parser.parse_args()
	
	if args.command == 'api':
		from .app import run
		run()
	elif args.command == 'consumer':
		run_consumer()

if __name__ == '__main__':
	main()