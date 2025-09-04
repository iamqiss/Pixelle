from __future__ import annotations
from typing import Dict, Any, List, Optional
import numpy as np
from sentence_transformers import SentenceTransformer
import hashlib
import json
from datetime import datetime

class FeatureExtractor:
	def __init__(self):
		# Initialize text embedding model (multilingual, good for social media)
		try:
			self.text_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
			print("Loaded text embedding model: paraphrase-multilingual-MiniLM-L12-v2")
		except Exception as e:
			print(f"Failed to load text model: {e}")
			self.text_model = None
		
		# Feature dimensions
		self.text_dim = 384
		self.user_dim = 128
		self.item_dim = 256
	
	def extract_text_features(self, text: str) -> Dict[str, Any]:
		"""Extract text-based features including embeddings."""
		features = {}
		
		if not text or len(text.strip()) == 0:
			features["text_length"] = 0
			features["text_embedding"] = np.zeros(self.text_dim).tolist()
			features["hashtags"] = []
			features["mentions"] = []
			features["urls"] = []
			return features
		
		# Basic text features
		features["text_length"] = len(text)
		features["word_count"] = len(text.split())
		features["char_count"] = len(text.replace(" ", ""))
		
		# Extract hashtags, mentions, URLs
		features["hashtags"] = [word for word in text.split() if word.startswith('#')]
		features["mentions"] = [word for word in text.split() if word.startswith('@')]
		features["urls"] = [word for word in text.split() if word.startswith('http')]
		
		# Text embedding
		if self.text_model:
			try:
				embedding = self.text_model.encode(text, convert_to_numpy=True)
				features["text_embedding"] = embedding.tolist()
			except Exception as e:
				print(f"Text embedding failed: {e}")
				features["text_embedding"] = np.zeros(self.text_dim).tolist()
		else:
			features["text_embedding"] = np.zeros(self.text_dim).tolist()
		
		# Language detection (simple heuristic)
		features["language"] = self._detect_language(text)
		
		# Sentiment features (basic)
		features["sentiment_score"] = self._basic_sentiment(text)
		
		return features
	
	def extract_user_features(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
		"""Extract user features from interaction history."""
		features = {}
		
		if not events:
			return self._empty_user_features()
		
		# Temporal features
		timestamps = [event.get("timestamp") for event in events if event.get("timestamp")]
		if timestamps:
			features["last_activity"] = max(timestamps)
			features["activity_frequency"] = len(events) / max(1, (max(timestamps) - min(timestamps)).days)
		
		# Interaction patterns
		interaction_counts = {}
		for event in events:
			event_type = event.get("event")
			interaction_counts[event_type] = interaction_counts.get(event_type, 0) + 1
		
		features["interaction_counts"] = interaction_counts
		features["total_interactions"] = len(events)
		
		# Content preferences
		content_types = [event.get("metadata", {}).get("content_type") for event in events]
		features["preferred_content_types"] = list(set(filter(None, content_types)))
		
		# Session features
		session_durations = [event.get("metadata", {}).get("session_duration", 0) for event in events]
		features["avg_session_duration"] = np.mean(session_durations) if session_durations else 0
		
		# Device/platform features
		platforms = [event.get("metadata", {}).get("platform") for event in events]
		features["primary_platform"] = max(set(platforms), key=platforms.count) if platforms else "unknown"
		
		# Engagement depth
		engagement_scores = []
		for event in events:
			score = 0
			if event.get("event") in ["like", "renote", "reply"]:
				score += 1
			if event.get("event") == "share":
				score += 2
			if event.get("event") == "follow":
				score += 3
			engagement_scores.append(score)
		
		features["avg_engagement_score"] = np.mean(engagement_scores) if engagement_scores else 0
		
		return features
	
	def extract_item_features(self, note: Dict[str, Any]) -> Dict[str, Any]:
		"""Extract item features from note content and metadata."""
		features = {}
		
		# Basic content features
		content = note.get("content", "")
		features.update(self.extract_text_features(content))
		
		# Media features
		media = note.get("media", [])
		features["has_media"] = len(media) > 0
		features["media_count"] = len(media)
		features["media_types"] = [m.get("type") for m in media if m.get("type")]
		
		# Video-specific features
		video_media = [m for m in media if m.get("type") == "video"]
		if video_media:
			features["has_video"] = True
			features["video_duration"] = video_media[0].get("duration", 0)
			features["video_quality"] = video_media[0].get("quality", "unknown")
		else:
			features["has_video"] = False
		
		# Image-specific features
		image_media = [m for m in media if m.get("type") == "image"]
		if image_media:
			features["has_image"] = True
			features["image_count"] = len(image_media)
		else:
			features["has_image"] = False
		
		# Author features
		author = note.get("author", {})
		features["author_followers"] = author.get("followers_count", 0)
		features["author_verified"] = author.get("verified", False)
		features["author_created_at"] = author.get("created_at")
		
		# Engagement features
		metrics = note.get("metrics", {})
		features["like_count"] = metrics.get("likes", 0)
		features["renote_count"] = metrics.get("renotes", 0)
		features["reply_count"] = metrics.get("replies", 0)
		features["view_count"] = metrics.get("views", 0)
		
		# Engagement rate
		total_engagements = features["like_count"] + features["renote_count"] + features["reply_count"]
		features["engagement_rate"] = total_engagements / max(1, features["view_count"])
		
		# Freshness
		created_at = note.get("created_at")
		if created_at:
			age_hours = (datetime.utcnow() - created_at).total_seconds() / 3600
			features["age_hours"] = age_hours
			features["freshness_score"] = np.exp(-age_hours / 24)  # Exponential decay
		
		# Content quality heuristics
		features["quality_score"] = self._calculate_content_quality(features)
		
		return features
	
	def _detect_language(self, text: str) -> str:
		"""Simple language detection based on character sets."""
		# Basic heuristics - in production, use proper language detection
		if any('\u4e00' <= char <= '\u9fff' for char in text):
			return "zh"
		elif any('\u3040' <= char <= '\u309f' for char in text):
			return "ja"
		elif any('\uac00' <= char <= '\ud7af' for char in text):
			return "ko"
		elif any('\u0600' <= char <= '\u06ff' for char in text):
			return "ar"
		else:
			return "en"  # Default to English
	
	def _basic_sentiment(self, text: str) -> float:
		"""Basic sentiment analysis using keyword matching."""
		positive_words = {"good", "great", "awesome", "love", "like", "happy", "amazing", "wonderful"}
		negative_words = {"bad", "terrible", "hate", "awful", "horrible", "sad", "angry", "disappointed"}
		
		words = set(text.lower().split())
		positive_count = len(words.intersection(positive_words))
		negative_count = len(words.intersection(negative_words))
		
		if positive_count == 0 and negative_count == 0:
			return 0.0
		
		return (positive_count - negative_count) / (positive_count + negative_count)
	
	def _calculate_content_quality(self, features: Dict[str, Any]) -> float:
		"""Calculate content quality score based on various factors."""
		score = 0.5  # Base score
		
		# Text quality
		text_length = features.get("text_length", 0)
		if 50 <= text_length <= 280:
			score += 0.2  # Optimal length
		elif text_length > 0:
			score += 0.1  # Has content
		
		# Media quality
		if features.get("has_media"):
			score += 0.1
		
		# Engagement quality
		engagement_rate = features.get("engagement_rate", 0)
		if engagement_rate > 0.1:
			score += 0.2
		elif engagement_rate > 0.05:
			score += 0.1
		
		# Author quality
		if features.get("author_verified"):
			score += 0.1
		if features.get("author_followers", 0) > 1000:
			score += 0.1
		
		return min(1.0, max(0.0, score))
	
	def _empty_user_features(self) -> Dict[str, Any]:
		"""Return empty user features for new users."""
		return {
			"last_activity": datetime.utcnow().isoformat(),
			"activity_frequency": 0.0,
			"interaction_counts": {},
			"total_interactions": 0,
			"preferred_content_types": [],
			"avg_session_duration": 0.0,
			"primary_platform": "unknown",
			"avg_engagement_score": 0.0
		}