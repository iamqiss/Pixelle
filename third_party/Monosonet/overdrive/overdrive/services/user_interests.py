from __future__ import annotations
from typing import Dict, List, Any, Optional
import httpx
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class UserInterestsService:
	"""Service for fetching and managing user interests."""
	
	def __init__(self, base_url: str = "http://localhost:3000"):
		self.base_url = base_url
		self.client = httpx.AsyncClient(timeout=30.0)
		
		# Interest categories and their weights
		self.interest_weights = {
			'art': 1.2,
			'gaming': 1.1,
			'sports': 1.3,
			'comics': 1.0,
			'music': 1.4,
			'politics': 1.5,
			'photography': 1.2,
			'science': 1.6,
			'news': 1.3,
			'technology': 1.4,
			'food': 1.1,
			'travel': 1.2,
			'fashion': 1.0,
			'culture': 1.1,
			'books': 1.0,
			'comedy': 1.1,
			'dev': 1.3,
			'animals': 1.0,
			'business': 1.2,
			'education': 1.4
		}
	
	async def get_user_interests(self, user_id: str, auth_token: str) -> List[str]:
		"""
		Fetch user interests from the client preferences API.
		
		Args:
			user_id: User identifier
			auth_token: Authentication token
		
		Returns:
			List of user interest tags
		"""
		try:
			headers = {
				'Authorization': f'Bearer {auth_token}',
				'Content-Type': 'application/json'
			}
			
			# Try to fetch from preferences endpoint
			response = await self.client.get(
				f"{self.base_url}/v1/preferences",
				headers=headers
			)
			
			if response.status_code == 200:
				preferences = response.json()
				interests = preferences.get('interests', {}).get('tags', [])
				
				if interests:
					logger.info(f"Fetched {len(interests)} interests for user {user_id}: {interests}")
					return interests
			
			# Fallback: try to fetch from user profile
			profile_response = await self.client.get(
				f"{self.base_url}/v1/users/{user_id}/profile",
				headers=headers
			)
			
			if profile_response.status_code == 200:
				profile = profile_response.json()
				# Extract interests from bio or other profile fields
				bio_interests = self._extract_interests_from_bio(profile.get('description', ''))
				if bio_interests:
					logger.info(f"Extracted {len(bio_interests)} interests from bio for user {user_id}")
					return bio_interests
			
			# If no interests found, return default interests
			logger.warning(f"No interests found for user {user_id}, using defaults")
			return self._get_default_interests()
			
		except Exception as e:
			logger.error(f"Failed to fetch interests for user {user_id}: {e}")
			return self._get_default_interests()
	
	async def get_user_preferences(self, user_id: str, auth_token: str) -> Dict[str, Any]:
		"""
		Fetch complete user preferences including interests.
		
		Args:
			user_id: User identifier
			auth_token: Authentication token
		
		Returns:
			User preferences dictionary
		"""
		try:
			headers = {
				'Authorization': f'Bearer {auth_token}',
				'Content-Type': 'application/json'
			}
			
			response = await self.client.get(
				f"{self.base_url}/v1/preferences",
				headers=headers
			)
			
			if response.status_code == 200:
				return response.json()
			else:
				logger.warning(f"Failed to fetch preferences for user {user_id}: {response.status_code}")
				return {}
				
		except Exception as e:
			logger.error(f"Failed to fetch preferences for user {user_id}: {e}")
			return {}
	
	def _extract_interests_from_bio(self, bio: str) -> List[str]:
		"""Extract interest tags from user bio text."""
		if not bio:
			return []
		
		bio_lower = bio.lower()
		found_interests = []
		
		# Check for hashtags and interest keywords
		for interest, weight in self.interest_weights.items():
			if interest in bio_lower or f"#{interest}" in bio_lower:
				found_interests.append(interest)
		
		# Also check for common variations
		interest_variations = {
			'tech': 'technology',
			'photo': 'photography',
			'gamer': 'gaming',
			'artist': 'art',
			'chef': 'food',
			'developer': 'dev',
			'programmer': 'dev',
			'coder': 'dev'
		}
		
		for variation, interest in interest_variations.items():
			if variation in bio_lower and interest not in found_interests:
				found_interests.append(interest)
		
		return found_interests[:5]  # Limit to top 5 interests
	
	def _get_default_interests(self) -> List[str]:
		"""Get default interests for users with no preferences."""
		return ['news', 'technology', 'culture']
	
	def get_weighted_interests(self, interests: List[str]) -> Dict[str, float]:
		"""
		Get interests with their weights for ranking.
		
		Args:
			interests: List of interest tags
		
		Returns:
			Dictionary mapping interests to their weights
		"""
		weighted = {}
		for interest in interests:
			weight = self.interest_weights.get(interest, 1.0)
			weighted[interest] = weight
		
		return weighted
	
	def get_interest_embeddings(self, interests: List[str]) -> List[float]:
		"""
		Convert interests to a numerical representation for ML models.
		
		Args:
			interests: List of interest tags
		
		Returns:
			List of numerical values representing interests
		"""
		# Create a fixed-size vector for all possible interests
		all_interests = list(self.interest_weights.keys())
		interest_vector = [0.0] * len(all_interests)
		
		for interest in interests:
			if interest in all_interests:
				idx = all_interests.index(interest)
				weight = self.interest_weights[interest]
				interest_vector[idx] = weight
		
		return interest_vector
	
	async def update_user_interests(self, user_id: str, interests: List[str], 
								  auth_token: str) -> bool:
		"""
		Update user interests in the system.
		
		Args:
			user_id: User identifier
			interests: New list of interests
			auth_token: Authentication token
		
		Returns:
			True if successful, False otherwise
		"""
		try:
			headers = {
				'Authorization': f'Bearer {auth_token}',
				'Content-Type': 'application/json'
			}
			
			payload = {
				'interests': {
					'tags': interests
				}
			}
			
			response = await self.client.put(
				f"{self.base_url}/v1/preferences/interests",
				headers=headers,
				json=payload
			)
			
			if response.status_code == 200:
				logger.info(f"Updated interests for user {user_id}: {interests}")
				return True
			else:
				logger.warning(f"Failed to update interests for user {user_id}: {response.status_code}")
				return False
				
		except Exception as e:
			logger.error(f"Failed to update interests for user {user_id}: {e}")
			return False
	
	def get_interest_similarity(self, interests1: List[str], interests2: List[str]) -> float:
		"""
		Calculate similarity between two sets of interests.
		
		Args:
			interests1: First set of interests
			interests2: Second set of interests
		
		Returns:
			Similarity score between 0 and 1
		"""
		if not interests1 or not interests2:
			return 0.0
		
		# Convert to sets for intersection calculation
		set1 = set(interests1)
		set2 = set(interests2)
		
		# Jaccard similarity
		intersection = len(set1.intersection(set2))
		union = len(set1.union(set2))
		
		if union == 0:
			return 0.0
		
		return intersection / union
	
	def get_interest_diversity_score(self, interests: List[str]) -> float:
		"""
		Calculate diversity score for a set of interests.
		Higher diversity means more varied interests.
		
		Args:
			interests: List of interest tags
		
		Returns:
			Diversity score between 0 and 1
		"""
		if not interests:
			return 0.0
		
		# Count unique interests
		unique_interests = len(set(interests))
		total_interests = len(interests)
		
		# Diversity is higher when there are more unique interests
		# and they're more evenly distributed
		if total_interests == 0:
			return 0.0
		
		diversity = unique_interests / total_interests
		
		# Boost for having multiple interests
		if unique_interests >= 5:
			diversity += 0.2
		elif unique_interests >= 3:
			diversity += 0.1
		
		return min(1.0, diversity)
	
	async def close(self):
		"""Close the HTTP client."""
		await self.client.aclose()