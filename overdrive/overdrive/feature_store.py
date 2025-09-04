from __future__ import annotations
from typing import Dict, Any, List
import json
import redis

from .config import settings

class FeatureStore:
	def __init__(self, redis_url: str | None = None) -> None:
		self._r = redis.Redis.from_url(redis_url or settings.redis_url, decode_responses=True)
		self._ttl = settings.feature_ttl_seconds

	def set_user_features(self, user_id: str, features: Dict[str, Any]) -> None:
		key = f"feat:user:{user_id}"
		self._r.hset(key, mapping={k: json.dumps(v) for k, v in features.items()})
		if self._ttl:
			self._r.expire(key, self._ttl)

	def get_user_features(self, user_id: str) -> Dict[str, Any]:
		key = f"feat:user:{user_id}"
		data = self._r.hgetall(key)
		return {k: json.loads(v) for k, v in data.items()}

	def set_item_features(self, note_id: str, features: Dict[str, Any]) -> None:
		key = f"feat:item:{note_id}"
		self._r.hset(key, mapping={k: json.dumps(v) for k, v in features.items()})
		if self._ttl:
			self._r.expire(key, self._ttl)

	def get_item_features(self, note_id: str) -> Dict[str, Any]:
		key = f"feat:item:{note_id}"
		data = self._r.hgetall(key)
		return {k: json.loads(v) for k, v in data.items()}

	def mget_item_features(self, note_ids: List[str]) -> Dict[str, Dict[str, Any]]:
		pipe = self._r.pipeline()
		keys = [f"feat:item:{nid}" for nid in note_ids]
		for k in keys:
			pipe.hgetall(k)
		raw = pipe.execute()
		result: Dict[str, Dict[str, Any]] = {}
		for nid, obj in zip(note_ids, raw):
			result[nid] = {k: json.loads(v) for k, v in obj.items()}
		return result