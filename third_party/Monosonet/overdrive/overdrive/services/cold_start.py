from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple
import json
import random
from datetime import datetime, timedelta
import logging

from ..feature_store import FeatureStore
from ..features.embeddings import FeatureExtractor

logger = logging.getLogger(__name__)

class ColdStartService:
	"""Service for handling cold start recommendations for new users."""
	
	def __init__(self, feature_store: FeatureStore):
		self.feature_store = feature_store
		self.feature_extractor = FeatureExtractor()
		
		# Interest-based content mapping
		self.interest_content_map = {
			'art': {
				'content_types': ['image', 'video'],
				'hashtags': ['art', 'painting', 'drawing', 'design', 'creative'],
				'engagement_patterns': ['like', 'renote', 'follow'],
				'content_quality_threshold': 0.7
			},
			'gaming': {
				'content_types': ['video', 'image'],
				'hashtags': ['gaming', 'game', 'esports', 'streamer', 'gamer'],
				'engagement_patterns': ['like', 'reply', 'share'],
				'content_quality_threshold': 0.6
			},
			'sports': {
				'content_types': ['video', 'image'],
				'hashtags': ['sports', 'football', 'basketball', 'soccer', 'athlete'],
				'engagement_patterns': ['like', 'renote', 'follow'],
				'content_quality_threshold': 0.8
			},
			'comics': {
				'content_types': ['image'],
				'hashtags': ['comics', 'manga', 'anime', 'cartoon', 'illustration'],
				'engagement_patterns': ['like', 'renote'],
				'content_quality_threshold': 0.7
			},
			'music': {
				'content_types': ['video', 'audio'],
				'hashtags': ['music', 'song', 'artist', 'concert', 'album'],
				'engagement_patterns': ['like', 'share', 'follow'],
				'content_quality_threshold': 0.8
			},
			'politics': {
				'content_types': ['text', 'image'],
				'hashtags': ['politics', 'news', 'government', 'policy', 'election'],
				'engagement_patterns': ['reply', 'share', 'follow'],
				'content_quality_threshold': 0.9
			},
			'photography': {
				'content_types': ['image'],
				'hashtags': ['photography', 'photo', 'camera', 'nature', 'portrait'],
				'engagement_patterns': ['like', 'renote', 'follow'],
				'content_quality_threshold': 0.8
			},
			'science': {
				'content_types': ['text', 'image', 'video'],
				'hashtags': ['science', 'research', 'technology', 'discovery', 'innovation'],
				'engagement_patterns': ['like', 'reply', 'share'],
				'content_quality_threshold': 0.9
			},
			'news': {
				'content_types': ['text', 'image', 'video'],
				'hashtags': ['news', 'breaking', 'update', 'report', 'coverage'],
				'engagement_patterns': ['like', 'share', 'follow'],
				'content_quality_threshold': 0.8
			},
			'technology': {
				'content_types': ['text', 'image', 'video'],
				'hashtags': ['tech', 'technology', 'innovation', 'startup', 'ai'],
				'engagement_patterns': ['like', 'reply', 'share'],
				'content_quality_threshold': 0.8
			},
			'food': {
				'content_types': ['image', 'video'],
				'hashtags': ['food', 'cooking', 'recipe', 'restaurant', 'chef'],
				'engagement_patterns': ['like', 'renote', 'follow'],
				'content_quality_threshold': 0.7
			},
			'travel': {
				'content_types': ['image', 'video'],
				'hashtags': ['travel', 'vacation', 'trip', 'destination', 'adventure'],
				'engagement_patterns': ['like', 'renote', 'follow'],
				'content_quality_threshold': 0.8
			},
			'fashion': {
				'content_types': ['image', 'video'],
				'hashtags': ['fashion', 'style', 'outfit', 'trend', 'designer'],
				'engagement_patterns': ['like', 'renote', 'follow'],
				'content_quality_threshold': 0.7
			}
		}
		
		# Default interests for users who don't select any
		self.default_interests = ['news', 'technology', 'culture']
	
	def get_cold_start_recommendations(self, user_id: str, user_interests: List[str], 
									candidate_items: List[Dict[str, Any]], 
									limit: int = 20) -> List[Dict[str, Any]]:
		"""
		Get cold start recommendations based on user interests.
		
		Args:
			user_id: User identifier
			user_interests: List of interest tags from signup
			candidate_items: List of candidate items to rank
			limit: Maximum number of recommendations to return
		
		Returns:
			List of ranked items with cold start scores
		"""
		if not user_interests:
			user_interests = self.default_interests
		
		logger.info(f"Generating cold start recommendations for user {user_id} with interests: {user_interests}")
		
		# Score items based on interests
		scored_items = []
		for item in candidate_items:
			score = self._calculate_interest_score(item, user_interests)
			scored_items.append({
				'item': item,
				'score': score,
				'reason': f"Cold start based on interests: {', '.join(user_interests)}"
			})
		
		# Sort by score and return top items
		scored_items.sort(key=lambda x: x['score'], reverse=True)
		
		# Add diversity by ensuring not all items are from the same interest
		diverse_items = self._add_diversity(scored_items, user_interests, limit)
		
		# Store cold start features for future learning
		self._store_cold_start_features(user_id, user_interests, diverse_items)
		
		return diverse_items[:limit]
	
	def _calculate_interest_score(self, item: Dict[str, Any], user_interests: List[str]) -> float:
		"""Calculate interest-based score for an item."""
		score = 0.0
		
		# Extract item features
		content = item.get('content', '')
		hashtags = item.get('hashtags', [])
		media_types = item.get('media_types', [])
		engagement_rate = item.get('engagement_rate', 0.0)
		quality_score = item.get('quality_score', 0.5)
		freshness_score = item.get('freshness_score', 0.5)
		
		# Score based on interest matching
		for interest in user_interests:
			if interest in self.interest_content_map:
				interest_config = self.interest_content_map[interest]
				
				# Content type matching
				if any(media_type in interest_config['content_types'] for media_type in media_types):
					score += 0.3
				
				# Hashtag matching
				interest_hashtags = interest_config['hashtags']
				hashtag_matches = sum(1 for tag in hashtags if tag.lower() in interest_hashtags)
				if hashtag_matches > 0:
					score += 0.2 * min(hashtag_matches, 3)  # Cap at 3 matches
				
				# Content quality threshold
				if quality_score >= interest_config['content_quality_threshold']:
					score += 0.2
		
		# Boost for high engagement content
		if engagement_rate > 0.1:
			score += 0.1
		
		# Boost for fresh content
		if freshness_score > 0.7:
			score += 0.1
		
		# Normalize score
		score = min(1.0, max(0.0, score))
		
		return score
	
	def _add_diversity(self, scored_items: List[Dict[str, Any]], 
					  user_interests: List[str], limit: int) -> List[Dict[str, Any]]:
		"""Add diversity to recommendations by ensuring different interests are represented."""
		if len(scored_items) <= limit:
			return scored_items
		
		# Group items by primary interest
		interest_groups = {}
		for item in scored_items:
			primary_interest = self._get_primary_interest(item['item'], user_interests)
			if primary_interest not in interest_groups:
				interest_groups[primary_interest] = []
			interest_groups[primary_interest].append(item)
		
		# Ensure each interest gets representation
		diverse_items = []
		items_per_interest = max(1, limit // len(user_interests))
		
		for interest in user_interests:
			if interest in interest_groups:
				# Take top items from this interest
				interest_items = interest_groups[interest][:items_per_interest]
				diverse_items.extend(interest_items)
		
		# Fill remaining slots with highest scoring items
		remaining_slots = limit - len(diverse_items)
		if remaining_slots > 0:
			# Get items not already included
			included_ids = {item['item'].get('id') for item in diverse_items}
			remaining_items = [item for item in scored_items if item['item'].get('id') not in included_ids]
			diverse_items.extend(remaining_items[:remaining_slots])
		
		return diverse_items
	
	def _get_primary_interest(self, item: Dict[str, Any], user_interests: List[str]) -> str:
		"""Determine the primary interest for an item."""
		content = item.get('content', '').lower()
		hashtags = [tag.lower() for tag in item.get('hashtags', [])]
		
		# Find the interest with the most matches
		best_interest = user_interests[0]
		best_score = 0
		
		for interest in user_interests:
			if interest in self.interest_content_map:
				interest_config = self.interest_content_map[interest]
				score = 0
				
				# Check content matching
				for hashtag in interest_config['hashtags']:
					if hashtag in content or hashtag in hashtags:
						score += 1
				
				if score > best_score:
					best_score = score
					best_interest = interest
		
		return best_interest
	
	def _store_cold_start_features(self, user_id: str, user_interests: List[str], 
								  recommended_items: List[Dict[str, Any]]):
		"""Store cold start features for future ML training."""
		try:
			# Create cold start user features
			cold_start_features = {
				'user_id': user_id,
				'interests': user_interests,
				'cold_start_timestamp': datetime.utcnow().isoformat(),
				'recommendation_count': len(recommended_items),
				'interest_diversity': len(set(user_interests)),
				'primary_interests': user_interests[:3],  # Top 3 interests
				'cold_start_score': sum(item['score'] for item in recommended_items) / len(recommended_items)
			}
			
			# Store in feature store
			self.feature_store.set_user_features(user_id, cold_start_features)
			
			logger.info(f"Stored cold start features for user {user_id}")
			
		except Exception as e:
			logger.error(f"Failed to store cold start features for user {user_id}: {e}")
	
	def get_cold_start_boost(self, user_id: str, user_interests: List[str]) -> float:
		"""
		Get cold start boost factor for ranking.
		Higher boost for newer users, decreases over time.
		"""
		try:
			# Get user features to determine cold start status
			user_features = self.feature_store.get_user_features(user_id)
			
			if not user_features:
				# New user, full cold start boost
				return 1.0
			
			cold_start_timestamp = user_features.get('cold_start_timestamp')
			if not cold_start_timestamp:
				return 1.0
			
			# Calculate time since cold start
			cold_start_time = datetime.fromisoformat(cold_start_timestamp)
			time_since_cold_start = datetime.utcnow() - cold_start_time
			
			# Cold start boost decreases over 30 days
			days_since_cold_start = time_since_cold_start.days
			if days_since_cold_start >= 30:
				return 0.0  # No more cold start boost
			
			# Exponential decay
			decay_factor = 0.95 ** days_since_cold_start
			return max(0.0, decay_factor)
			
		except Exception as e:
			logger.error(f"Failed to calculate cold start boost for user {user_id}: {e}")
			return 0.0
	
	def should_use_cold_start(self, user_id: str) -> bool:
		"""Determine if user should still use cold start recommendations."""
		try:
			user_features = self.feature_store.get_user_features(user_id)
			
			if not user_features:
				return True  # New user, use cold start
			
			# Check if user has enough interaction history
			total_interactions = user_features.get('total_interactions', 0)
			if total_interactions < 50:
				return True  # Not enough data, use cold start
			
			# Check cold start boost
			cold_start_boost = self.get_cold_start_boost(user_id, [])
			if cold_start_boost > 0.1:
				return True  # Still in cold start period
			
			return False
			
		except Exception as e:
			logger.error(f"Failed to determine cold start status for user {user_id}: {e}")
			return True  # Default to cold start on error