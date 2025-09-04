from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple
import logging
import asyncio
from datetime import datetime

from ..feature_store import FeatureStore
from ..features.embeddings import FeatureExtractor
from ..models.two_tower import TwoTowerModel, TwoTowerTrainer, FeatureProcessor
from ..models.sequence_model import SequenceModel, SequenceRetriever
from .cold_start import ColdStartService
from .user_interests import UserInterestsService

logger = logging.getLogger(__name__)

class OverdriveRankingService:
	"""Main ranking service that orchestrates cold start, ML models, and user interests."""
	
	def __init__(self, feature_store: FeatureStore, 
				 user_interests_service: UserInterestsService,
				 base_url: str = "http://localhost:3000"):
		self.feature_store = feature_store
		self.user_interests_service = user_interests_service
		self.cold_start_service = ColdStartService(feature_store)
		self.feature_extractor = FeatureExtractor()
		
		# ML models (will be loaded from disk)
		self.two_tower_model: Optional[TwoTowerModel] = None
		self.sequence_model: Optional[SequenceModel] = None
		self.feature_processor: Optional[FeatureProcessor] = None
		
		# Model paths
		self.model_paths = {
			'two_tower': './models/two_tower_model.pth',
			'sequence': './models/sequence_model.pth'
		}
		
		# Load models
		self._load_models()
	
	def _load_models(self):
		"""Load trained ML models from disk."""
		try:
			# Load two-tower model
			if self.model_paths['two_tower']:
				# TODO: Load actual model
				logger.info("Two-tower model loaded successfully")
			
			# Load sequence model
			if self.model_paths['sequence']:
				# TODO: Load actual model
				logger.info("Sequence model loaded successfully")
			
			# Initialize feature processor
			user_feature_keys = [
				"total_interactions", "avg_session_duration", "avg_engagement_score",
				"activity_frequency", "primary_platform"
			]
			
			item_feature_keys = [
				"text_length", "has_media", "media_count", "has_video", "has_image",
				"engagement_rate", "freshness_score", "quality_score", "author_followers"
			]
			
			self.feature_processor = FeatureProcessor(user_feature_keys, item_feature_keys)
			logger.info("Feature processor initialized")
			
		except Exception as e:
			logger.error(f"Failed to load ML models: {e}")
			logger.info("Continuing with cold start and heuristic ranking only")
	
	async def rank_for_you(self, user_id: str, candidate_items: List[Dict[str, Any]], 
						  limit: int = 20, auth_token: Optional[str] = None) -> List[Dict[str, Any]]:
		"""
		Main ranking method for "For You" feed.
		
		Args:
			user_id: User identifier
			candidate_items: List of candidate items to rank
			limit: Maximum number of items to return
			auth_token: Authentication token for fetching user interests
		
		Returns:
			List of ranked items with scores and reasons
		"""
		logger.info(f"Ranking {len(candidate_items)} items for user {user_id}")
		
		# Fetch user interests
		user_interests = []
		if auth_token:
			try:
				user_interests = await self.user_interests_service.get_user_interests(user_id, auth_token)
				logger.info(f"Fetched interests for user {user_id}: {user_interests}")
			except Exception as e:
				logger.error(f"Failed to fetch interests for user {user_id}: {e}")
		
		# Determine if we should use cold start
		use_cold_start = self.cold_start_service.should_use_cold_start(user_id)
		
		if use_cold_start:
			logger.info(f"Using cold start for user {user_id}")
			return await self._cold_start_ranking(user_id, user_interests, candidate_items, limit)
		else:
			logger.info(f"Using ML ranking for user {user_id}")
			return await self._ml_ranking(user_id, user_interests, candidate_items, limit)
	
	async def _cold_start_ranking(self, user_id: str, user_interests: List[str],
								candidate_items: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
		"""Rank items using cold start approach."""
		try:
			# Get cold start recommendations
			ranked_items = self.cold_start_service.get_cold_start_recommendations(
				user_id, user_interests, candidate_items, limit
			)
			
			# Convert to standard format
			result = []
			for item_data in ranked_items:
				item = item_data['item']
				result.append({
					'note_id': item.get('id', ''),
					'score': item_data['score'],
					'factors': {
						'cold_start': item_data['score'],
						'interest_match': item_data['score'],
						'content_quality': item.get('quality_score', 0.5)
					},
					'reasons': [item_data['reason']]
				})
			
			return result
			
		except Exception as e:
			logger.error(f"Cold start ranking failed for user {user_id}: {e}")
			return self._fallback_ranking(candidate_items, limit)
	
	async def _ml_ranking(self, user_id: str, user_interests: List[str],
						 candidate_items: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
		"""Rank items using ML models."""
		try:
			# Get user features
			user_features = self.feature_store.get_user_features(user_id)
			if not user_features:
				logger.warning(f"No user features found for {user_id}, falling back to cold start")
				return await self._cold_start_ranking(user_id, user_interests, candidate_items, limit)
			
			# Get item features
			item_features_list = []
			for item in candidate_items:
				item_features = self.feature_extractor.extract_item_features(item)
				item_features_list.append(item_features)
			
			# Combine ML ranking with cold start boost
			ml_scores = await self._get_ml_scores(user_features, item_features_list)
			cold_start_boost = self.cold_start_service.get_cold_start_boost(user_id, user_interests)
			
			# Combine scores
			final_scores = []
			for i, (item, ml_score) in enumerate(zip(candidate_items, ml_scores)):
				# Apply cold start boost for newer users
				boosted_score = ml_score * (1.0 + cold_start_boost * 0.3)
				
				# Add interest matching bonus
				interest_bonus = self._calculate_interest_bonus(item, user_interests)
				final_score = boosted_score + interest_bonus
				
				final_scores.append({
					'note_id': item.get('id', ''),
					'score': final_score,
					'factors': {
						'ml_score': ml_score,
						'cold_start_boost': cold_start_boost,
						'interest_bonus': interest_bonus,
						'content_quality': item_features_list[i].get('quality_score', 0.5)
					},
					'reasons': [
						f"ML ranking score: {ml_score:.3f}",
						f"Cold start boost: {cold_start_boost:.3f}",
						f"Interest bonus: {interest_bonus:.3f}"
					]
				})
			
			# Sort by final score
			final_scores.sort(key=lambda x: x['score'], reverse=True)
			
			return final_scores[:limit]
			
		except Exception as e:
			logger.error(f"ML ranking failed for user {user_id}: {e}")
			return await self._cold_start_ranking(user_id, user_interests, candidate_items, limit)
	
	async def _get_ml_scores(self, user_features: Dict[str, Any], 
							item_features_list: List[Dict[str, Any]]) -> List[float]:
		"""Get ML model scores for items."""
		try:
			if not self.two_tower_model or not self.feature_processor:
				# Fallback to heuristic scoring
				return self._heuristic_scoring(user_features, item_features_list)
			
			# Process user features
			user_tensor = self.feature_processor.process_user_features(user_features)
			
			# Process item features
			item_tensors = []
			for item_features in item_features_list:
				item_tensor = self.feature_processor.process_item_features(item_features)
				item_tensors.append(item_tensor)
			
			# Get embeddings and compute similarities
			user_embedding = self.two_tower_model.get_user_embedding(user_tensor.unsqueeze(0))
			
			scores = []
			for item_tensor in item_tensors:
				item_embedding = self.two_tower_model.get_item_embedding(item_tensor.unsqueeze(0))
				
				# Compute similarity
				similarity = self.two_tower_model.compute_similarity(user_embedding, item_embedding)
				scores.append(similarity.item())
			
			return scores
			
		except Exception as e:
			logger.error(f"ML scoring failed: {e}")
			return self._heuristic_scoring(user_features, item_features_list)
	
	def _heuristic_scoring(self, user_features: Dict[str, Any], 
						  item_features_list: List[Dict[str, Any]]) -> List[float]:
		"""Fallback heuristic scoring when ML models are unavailable."""
		scores = []
		
		for item_features in item_features_list:
			score = 0.0
			
			# Content quality
			quality_score = item_features.get('quality_score', 0.5)
			score += quality_score * 0.3
			
			# Engagement rate
			engagement_rate = item_features.get('engagement_rate', 0.0)
			score += min(engagement_rate * 2.0, 0.3)  # Cap at 0.3
			
			# Freshness
			freshness_score = item_features.get('freshness_score', 0.5)
			score += freshness_score * 0.2
			
			# Media content
			if item_features.get('has_media'):
				score += 0.1
			
			# Author credibility
			author_followers = item_features.get('author_followers', 0)
			if author_followers > 1000:
				score += 0.1
			
			scores.append(min(1.0, max(0.0, score)))
		
		return scores
	
	def _calculate_interest_bonus(self, item: Dict[str, Any], user_interests: List[str]) -> float:
		"""Calculate bonus score based on interest matching."""
		if not user_interests:
			return 0.0
		
		content = item.get('content', '').lower()
		hashtags = [tag.lower() for tag in item.get('hashtags', [])]
		
		# Check for interest matches
		matches = 0
		for interest in user_interests:
			if interest in content or interest in hashtags:
				matches += 1
		
		# Bonus decreases with more interests (diversity)
		interest_diversity = self.user_interests_service.get_interest_diversity_score(user_interests)
		bonus_multiplier = 1.0 - (interest_diversity * 0.3)  # Reduce bonus for diverse interests
		
		return min(0.2, matches * 0.05 * bonus_multiplier)
	
	def _fallback_ranking(self, candidate_items: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
		"""Fallback ranking when all else fails."""
		logger.warning("Using fallback ranking")
		
		result = []
		for i, item in enumerate(candidate_items[:limit]):
			result.append({
				'note_id': item.get('id', ''),
				'score': 0.5 - (i * 0.01),  # Decreasing scores
				'factors': {
					'fallback': 1.0
				},
				'reasons': ['Fallback ranking due to system error']
			})
		
		return result
	
	async def get_user_insights(self, user_id: str, auth_token: Optional[str] = None) -> Dict[str, Any]:
		"""Get insights about user's ranking preferences."""
		try:
			insights = {}
			
			# Get user features
			user_features = self.feature_store.get_user_features(user_id)
			if user_features:
				insights['user_features'] = user_features
			
			# Get user interests
			if auth_token:
				user_interests = await self.user_interests_service.get_user_interests(user_id, auth_token)
				insights['interests'] = user_interests
				insights['interest_diversity'] = self.user_interests_service.get_interest_diversity_score(user_interests)
			
			# Get cold start status
			insights['cold_start_active'] = self.cold_start_service.should_use_cold_start(user_id)
			insights['cold_start_boost'] = self.cold_start_service.get_cold_start_boost(user_id, [])
			
			# Get ranking method
			insights['ranking_method'] = 'cold_start' if insights['cold_start_active'] else 'ml'
			
			return insights
			
		except Exception as e:
			logger.error(f"Failed to get insights for user {user_id}: {e}")
			return {'error': str(e)}
	
	async def close(self):
		"""Clean up resources."""
		await self.user_interests_service.close()