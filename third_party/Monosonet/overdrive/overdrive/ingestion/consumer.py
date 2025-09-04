from typing import Iterable, Dict, Any
import json
import threading
from confluent_kafka import Consumer, KafkaError

from ..config import settings
from ..feature_store import FeatureStore
from ..features.extractors import extract_user_features

class InteractionsConsumer:
	def __init__(self, brokers: str, topic: str, group_id: str) -> None:
		self._topic = topic
		self._consumer = Consumer({
			'bootstrap.servers': brokers,
			'group.id': group_id,
			'auto.offset.reset': 'latest',
		})
		self._fs = FeatureStore()
		self._stop = threading.Event()

	def run(self) -> None:
		self._consumer.subscribe([self._topic])
		while not self._stop.is_set():
			msg = self._consumer.poll(0.2)
			if msg is None:
				continue
			if msg.error():
				if msg.error().code() == KafkaError._PARTITION_EOF:
					continue
				else:
					print("Kafka error:", msg.error())
					continue
			try:
				payload = json.loads(msg.value().decode('utf-8'))
				# Expect { interactions: [ ... ] }
				interactions = payload.get('interactions', []) if isinstance(payload, dict) else []
				self.process_batch(interactions)
			except Exception as e:
				print("Failed to process message:", e)
				continue

	def stop(self) -> None:
		self._stop.set()
		self._consumer.close()

	def process_batch(self, events: Iterable[Dict[str, Any]]) -> None:
		for ev in events:
			user_id = ev.get('user_id') or ev.get('actor') or 'unknown'
			if user_id == 'unknown':
				continue
			features = extract_user_features(ev)
			self._fs.set_user_features(user_id, features)