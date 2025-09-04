from __future__ import annotations
from typing import Dict, Any
from datetime import datetime

# Placeholder extractors; in production, call NLP/CV/ASR models and aggregations

def extract_user_features(event: Dict[str, Any]) -> Dict[str, Any]:
	features: Dict[str, Any] = {}
	features["last_event_ts"] = datetime.utcnow().isoformat()
	features["session_interaction_count"] = int(event.get("metadata", {}).get("interaction_count", 0))
	features["device_platform"] = event.get("metadata", {}).get("platform", "unknown")
	return features


def extract_item_features(note: Dict[str, Any]) -> Dict[str, Any]:
	features: Dict[str, Any] = {}
	features["has_media"] = bool(note.get("media"))
	features["text_length"] = len(note.get("content", ""))
	features["created_at"] = note.get("created_at")
	return features