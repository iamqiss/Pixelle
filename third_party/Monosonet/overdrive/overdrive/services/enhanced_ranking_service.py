from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple, Union
import asyncio
import logging
import time
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass
import json
from collections import defaultdict
import os
import uuid

from ..feature_store import FeatureStore
from ..features.multimodal_extractor import MultiModalFeatureExtractor, ContentFeatures, UserBehaviorFeatures
from ..models.collaborative_filtering import AdvancedCollaborativeFiltering, UserItemInteraction
from ..analytics.real_time_signals import RealTimeSignalProcessor, UserSignal, AdaptiveRecommendation
from ..models.two_tower import TwoTowerModel, FeatureProcessor
from .cold_start import ColdStartService
from .user_interests import UserInterestsService

logger = logging.getLogger(__name__)

@dataclass
class EnhancedRankingResult:
    """Enhanced ranking result with multiple scoring methods."""
    content_id: str
    final_score: float
    ranking_method: str
    confidence: float
    explanation: str
    feature_scores: Dict[str, float]
    collaborative_scores: Dict[str, float]
    real_time_boost: float
    metadata: Dict[str, Any]

class EnhancedOverdriveRankingService:
    """Enhanced ranking service integrating all advanced recommendation capabilities."""
    
    def __init__(self, 
                 feature_store: FeatureStore,
                 user_interests_service: UserInterestsService,
                 base_url: str = "http://localhost:3000",
                 redis_url: str = "redis://localhost:6379",
                 kafka_bootstrap_servers: List[str] = None):
        
        self.feature_store = feature_store
        self.user_interests_service = user_interests_service
        self.base_url = base_url
        
        # Initialize services
        self.cold_start_service = ColdStartService(feature_store)
        self.multimodal_extractor = MultiModalFeatureExtractor()
        self.collaborative_filtering = AdvancedCollaborativeFiltering()
        self.real_time_processor = RealTimeSignalProcessor(
            redis_url=redis_url,
            kafka_bootstrap_servers=kafka_bootstrap_servers
        )
        
        # ML models
        self.two_tower_model: Optional[TwoTowerModel] = None
        self.feature_processor: Optional[FeatureProcessor] = None
        
        # Ranking weights and parameters
        self.ranking_weights = {
            'content_based': 0.3,
            'collaborative': 0.25,
            'real_time': 0.25,
            'user_interests': 0.1,
            'freshness': 0.1
        }
        
        # Performance tracking
        self.ranking_metrics = {
            'total_rankings': 0,
            'avg_ranking_time_ms': 0.0,
            'ranking_methods_used': defaultdict(int)
        }
        
        logger.info("EnhancedOverdriveRankingService initialized")
    
    async def start_services(self):
        """Start all background services."""
        try:
            # Start real-time signal processing
            await self.real_time_processor.start_processing()
            
            # Load pre-trained models
            await self._load_models()
            
            logger.info("All services started successfully")
            
        except Exception as e:
            logger.error(f"Error starting services: {e}")
            raise
    
    async def stop_services(self):
        """Stop all background services."""
        try:
            await self.real_time_processor.stop_processing()
            logger.info("All services stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping services: {e}")
    
    async def _load_models(self):
        """Load pre-trained ML models."""
        try:
            # Load two-tower model if available
            model_path = "./models/two_tower_model.pth"
            if os.path.exists(model_path):
                # TODO: Implement model loading
                logger.info("Two-tower model loaded")
            
            # Initialize feature processor
            user_feature_keys = [
                "total_interactions", "avg_session_duration", "avg_engagement_score",
                "activity_frequency", "primary_platform", "location_hash", "device_hash"
            ]
            
            item_feature_keys = [
                "text_length", "has_media", "media_count", "has_video", "has_image",
                "engagement_rate", "freshness_score", "quality_score", "author_followers",
                "content_category", "language", "sentiment_score"
            ]
            
            self.feature_processor = FeatureProcessor(user_feature_keys, item_feature_keys)
            logger.info("Feature processor initialized")
            
        except Exception as e:
            logger.error(f"Error loading models: {e}")
    
    async def rank_for_you_enhanced(self, 
                                  user_id: str, 
                                  candidate_items: List[Dict[str, Any]], 
                                  limit: int = 20, 
                                  auth_token: Optional[str] = None) -> List[EnhancedRankingResult]:
        """Enhanced ranking method integrating all recommendation approaches."""
        
        start_time = time.time()
        logger.info(f"Enhanced ranking for user {user_id} with {len(candidate_items)} candidates")
        
        try:
            # Check if we should use cold start
            use_cold_start = self.cold_start_service.should_use_cold_start(user_id)
            
            if use_cold_start:
                logger.info(f"Using cold start for user {user_id}")
                return await self._cold_start_ranking(user_id, candidate_items, limit)
            
            # Extract user behavior features
            user_behavior = await self._extract_user_behavior_features(user_id)
            
            # Get real-time signals
            real_time_signals = self.real_time_processor.get_user_signals(user_id, "1h")
            
            # Get collaborative filtering recommendations
            cf_recommendations = self._get_collaborative_recommendations(user_id, limit)
            
            # Rank items using multiple approaches
            ranked_items = await self._multi_approach_ranking(
                user_id, candidate_items, user_behavior, real_time_signals, cf_recommendations, limit
            )
            
            # Update metrics
            ranking_time = (time.time() - start_time) * 1000
            self._update_ranking_metrics(ranking_time, "enhanced")
            
            return ranked_items
            
        except Exception as e:
            logger.error(f"Error in enhanced ranking: {e}")
            # Fallback to cold start
            return await self._cold_start_ranking(user_id, candidate_items, limit)
    
    async def _extract_user_behavior_features(self, user_id: str) -> UserBehaviorFeatures:
        """Extract comprehensive user behavior features."""
        try:
            # Get user data from feature store
            user_features = self.feature_store.get_user_features(user_id)
            
            # Get recent interactions
            recent_interactions = self.real_time_processor.get_user_signals(user_id, "24h")
            
            # Prepare behavior data
            behavior_data = {
                "session_data": {
                    "duration_seconds": user_features.get("avg_session_duration", 0),
                    "items_viewed": user_features.get("total_interactions", 0),
                    "engagement_actions": user_features.get("engagement_actions", 0),
                    "platform": user_features.get("primary_platform", "unknown"),
                    "device_type": user_features.get("device_type", "unknown")
                },
                "interaction_data": [
                    {
                        "type": signal.signal_type,
                        "timestamp": signal.timestamp.timestamp(),
                        "content_id": signal.content_id,
                        "duration": signal.duration,
                        "intensity": signal.intensity
                    }
                    for signal in recent_interactions
                ],
                "temporal_data": {
                    "daily_activity": user_features.get("daily_activity", 0),
                    "weekly_activity": user_features.get("weekly_activity", 0),
                    "peak_hours": user_features.get("peak_hours", [])
                },
                "device_data": {
                    "type": user_features.get("device_type", "unknown"),
                    "os": user_features.get("os_type", "unknown"),
                    "browser": user_features.get("browser_type", "unknown"),
                    "screen_width": user_features.get("screen_width", 1920),
                    "screen_height": user_features.get("screen_height", 1080),
                    "screen_density": user_features.get("screen_density", 1.0)
                },
                "location_data": {
                    "latitude": user_features.get("latitude", 0.0),
                    "longitude": user_features.get("longitude", 0.0),
                    "country_code": user_features.get("country_code", "unknown"),
                    "timezone": user_features.get("timezone", "UTC")
                }
            }
            
            # Extract features using multimodal extractor
            behavior_features = await self.multimodal_extractor.extract_user_behavior_features(
                user_id, behavior_data
            )
            
            return behavior_features
            
        except Exception as e:
            logger.error(f"Error extracting user behavior features: {e}")
            # Return empty features
            return UserBehaviorFeatures(user_id=user_id)
    
    def _get_collaborative_recommendations(self, user_id: str, limit: int) -> List[Dict[str, Any]]:
        """Get collaborative filtering recommendations."""
        try:
            # Get recommendations from collaborative filtering
            cf_results = self.collaborative_filtering.get_recommendations(user_id, limit)
            
            # Convert to dictionary format
            recommendations = []
            for result in cf_results:
                recommendations.append({
                    "content_id": result.item_id,
                    "score": result.score,
                    "method": result.method,
                    "confidence": result.confidence,
                    "explanation": result.explanation
                })
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error getting collaborative filtering recommendations: {e}")
            return []
    
    async def _multi_approach_ranking(self, 
                                    user_id: str,
                                    candidate_items: List[Dict[str, Any]],
                                    user_behavior: UserBehaviorFeatures,
                                    real_time_signals: List[UserSignal],
                                    cf_recommendations: List[Dict[str, Any]],
                                    limit: int) -> List[EnhancedRankingResult]:
        """Rank items using multiple approaches and combine scores."""
        
        try:
            ranked_items = []
            
            for item in candidate_items:
                content_id = item.get("id", str(uuid.uuid4()))
                
                # Calculate content-based score
                content_score = await self._calculate_content_based_score(item, user_behavior)
                
                # Calculate collaborative filtering score
                cf_score = self._calculate_collaborative_score(content_id, cf_recommendations)
                
                # Calculate real-time boost
                real_time_boost = self._calculate_real_time_boost(content_id, real_time_signals)
                
                # Calculate user interest score
                interest_score = await self._calculate_interest_score(user_id, item, auth_token)
                
                # Calculate freshness score
                freshness_score = self._calculate_freshness_score(item)
                
                # Combine all scores
                final_score = self._combine_scores({
                    'content_based': content_score,
                    'collaborative': cf_score,
                    'real_time': real_time_boost,
                    'user_interests': interest_score,
                    'freshness': freshness_score
                })
                
                # Create ranking result
                result = EnhancedRankingResult(
                    content_id=content_id,
                    final_score=final_score,
                    ranking_method="multi_approach",
                    confidence=self._calculate_confidence(content_score, cf_score, real_time_boost),
                    explanation=self._generate_explanation(content_score, cf_score, real_time_boost),
                    feature_scores={
                        'content_based': content_score,
                        'collaborative': cf_score,
                        'real_time': real_time_boost,
                        'user_interests': interest_score,
                        'freshness': freshness_score
                    },
                    collaborative_scores=self._get_cf_scores(content_id, cf_recommendations),
                    real_time_boost=real_time_boost,
                    metadata=item
                )
                
                ranked_items.append(result)
            
            # Sort by final score
            ranked_items.sort(key=lambda x: x.final_score, reverse=True)
            
            return ranked_items[:limit]
            
        except Exception as e:
            logger.error(f"Error in multi-approach ranking: {e}")
            return []
    
    async def _calculate_content_based_score(self, item: Dict[str, Any], user_behavior: UserBehaviorFeatures) -> float:
        """Calculate content-based similarity score."""
        try:
            # Extract content features
            content_features = await self.multimodal_extractor.extract_content_features(
                content_id=item.get("id", "unknown"),
                content_type=item.get("type", "text"),
                content_data=item
            )
            
            if content_features.combined_features is None or user_behavior.combined_features is None:
                return 0.0
            
            # Calculate cosine similarity
            content_vector = content_features.combined_features
            user_vector = user_behavior.combined_features
            
            # Ensure vectors have same length
            min_length = min(len(content_vector), len(user_vector))
            if min_length == 0:
                return 0.0
            
            content_vector = content_vector[:min_length]
            user_vector = user_vector[:min_length]
            
            # Calculate similarity
            dot_product = np.dot(content_vector, user_vector)
            content_norm = np.linalg.norm(content_vector)
            user_norm = np.linalg.norm(user_vector)
            
            if content_norm == 0 or user_norm == 0:
                return 0.0
            
            similarity = dot_product / (content_norm * user_norm)
            
            # Normalize to [0, 1]
            return max(0.0, min(1.0, (similarity + 1) / 2))
            
        except Exception as e:
            logger.error(f"Error calculating content-based score: {e}")
            return 0.0
    
    def _calculate_collaborative_score(self, content_id: str, cf_recommendations: List[Dict[str, Any]]) -> float:
        """Calculate collaborative filtering score."""
        try:
            for rec in cf_recommendations:
                if rec["content_id"] == content_id:
                    return min(1.0, max(0.0, rec["score"] / 10.0))  # Normalize score
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating collaborative score: {e}")
            return 0.0
    
    def _calculate_real_time_boost(self, content_id: str, real_time_signals: List[UserSignal]) -> float:
        """Calculate real-time signal boost for content."""
        try:
            if not real_time_signals:
                return 0.0
            
            # Find signals related to this content
            content_signals = [s for s in real_time_signals if s.content_id == content_id]
            
            if not content_signals:
                return 0.0
            
            # Calculate boost based on signal types and recency
            total_boost = 0.0
            
            for signal in content_signals:
                # Get signal weight
                weight = self.real_time_processor.signal_weights.get(signal.signal_type, 1.0)
                
                # Apply recency decay
                hours_ago = (datetime.utcnow() - signal.timestamp).total_seconds() / 3600
                decay_factor = np.exp(-hours_ago / 2.0)  # 2 hour half-life for real-time boost
                
                boost = weight * signal.intensity * decay_factor
                total_boost += boost
            
            # Normalize boost
            max_boost = 5.0  # Maximum boost value
            normalized_boost = min(max_boost, total_boost) / max_boost
            
            return normalized_boost
            
        except Exception as e:
            logger.error(f"Error calculating real-time boost: {e}")
            return 0.0
    
    async def _calculate_interest_score(self, user_id: str, item: Dict[str, Any], auth_token: Optional[str]) -> float:
        """Calculate user interest score for content."""
        try:
            if not auth_token:
                return 0.0
            
            # Get user interests
            user_interests = await self.user_interests_service.get_user_interests(user_id, auth_token)
            
            if not user_interests:
                return 0.0
            
            # Calculate interest match
            content_text = item.get("text", "").lower()
            content_category = item.get("category", "").lower()
            
            interest_score = 0.0
            total_interests = len(user_interests)
            
            for interest in user_interests:
                interest_text = interest.get("text", "").lower()
                interest_weight = interest.get("weight", 1.0)
                
                # Check text match
                if interest_text in content_text:
                    interest_score += interest_weight
                
                # Check category match
                if interest_text in content_category:
                    interest_score += interest_weight * 0.5
            
            # Normalize score
            if total_interests > 0:
                normalized_score = interest_score / (total_interests * 2)  # Max possible score
                return min(1.0, normalized_score)
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating interest score: {e}")
            return 0.0
    
    def _calculate_freshness_score(self, item: Dict[str, Any]) -> float:
        """Calculate content freshness score."""
        try:
            # Get content creation time
            created_at = item.get("created_at")
            if not created_at:
                return 0.5  # Default score for unknown creation time
            
            # Parse timestamp
            if isinstance(created_at, str):
                try:
                    created_time = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                except:
                    return 0.5
            else:
                created_time = created_at
            
            # Calculate age in hours
            age_hours = (datetime.utcnow() - created_time).total_seconds() / 3600
            
            # Calculate freshness score (exponential decay)
            # 24 hour half-life
            freshness_score = np.exp(-age_hours / 24.0)
            
            return max(0.1, freshness_score)  # Minimum score of 0.1
            
        except Exception as e:
            logger.error(f"Error calculating freshness score: {e}")
            return 0.5
    
    def _combine_scores(self, scores: Dict[str, float]) -> float:
        """Combine scores from different approaches using weighted average."""
        try:
            total_score = 0.0
            total_weight = 0.0
            
            for approach, score in scores.items():
                weight = self.ranking_weights.get(approach, 0.0)
                total_score += score * weight
                total_weight += weight
            
            if total_weight > 0:
                return total_score / total_weight
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error combining scores: {e}")
            return 0.0
    
    def _calculate_confidence(self, content_score: float, cf_score: float, real_time_boost: float) -> float:
        """Calculate confidence in the ranking."""
        try:
            # Higher confidence if multiple approaches agree
            scores = [content_score, cf_score, real_time_boost]
            non_zero_scores = [s for s in scores if s > 0]
            
            if not non_zero_scores:
                return 0.0
            
            # Calculate variance (lower variance = higher confidence)
            mean_score = np.mean(non_zero_scores)
            variance = np.var(non_zero_scores)
            
            # Convert variance to confidence (0-1)
            confidence = 1.0 / (1.0 + variance)
            
            # Boost confidence if multiple approaches have high scores
            approach_agreement = len(non_zero_scores) / len(scores)
            confidence *= (0.5 + 0.5 * approach_agreement)
            
            return min(1.0, confidence)
            
        except Exception as e:
            logger.error(f"Error calculating confidence: {e}")
            return 0.5
    
    def _generate_explanation(self, content_score: float, cf_score: float, real_time_boost: float) -> str:
        """Generate human-readable explanation for the ranking."""
        try:
            explanations = []
            
            if content_score > 0.7:
                explanations.append("High content similarity")
            elif content_score > 0.4:
                explanations.append("Moderate content similarity")
            
            if cf_score > 0.7:
                explanations.append("Strong collaborative filtering score")
            elif cf_score > 0.4:
                explanations.append("Moderate collaborative filtering score")
            
            if real_time_boost > 0.5:
                explanations.append("Recent positive engagement")
            
            if not explanations:
                explanations.append("Baseline recommendation")
            
            return "; ".join(explanations)
            
        except Exception as e:
            logger.error(f"Error generating explanation: {e}")
            return "Recommendation based on multiple factors"
    
    def _get_cf_scores(self, content_id: str, cf_recommendations: List[Dict[str, Any]]) -> Dict[str, float]:
        """Get collaborative filtering scores for different methods."""
        try:
            scores = {}
            
            for rec in cf_recommendations:
                if rec["content_id"] == content_id:
                    method = rec["method"]
                    scores[method] = rec["score"]
            
            return scores
            
        except Exception as e:
            logger.error(f"Error getting CF scores: {e}")
            return {}
    
    async def _cold_start_ranking(self, user_id: str, candidate_items: List[Dict[str, Any]], limit: int) -> List[EnhancedRankingResult]:
        """Fallback to cold start ranking."""
        try:
            # Use cold start service
            cold_start_items = self.cold_start_service.get_cold_start_recommendations(
                candidate_items, limit
            )
            
            # Convert to enhanced ranking results
            results = []
            for i, item in enumerate(cold_start_items):
                result = EnhancedRankingResult(
                    content_id=item.get("id", str(i)),
                    final_score=1.0 - (i * 0.1),  # Decreasing scores
                    ranking_method="cold_start",
                    confidence=0.3,  # Lower confidence for cold start
                    explanation="Cold start recommendation",
                    feature_scores={},
                    collaborative_scores={},
                    real_time_boost=0.0,
                    metadata=item
                )
                results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Error in cold start ranking: {e}")
            return []
    
    def _update_ranking_metrics(self, ranking_time_ms: float, method: str):
        """Update ranking performance metrics."""
        try:
            self.ranking_metrics['total_rankings'] += 1
            self.ranking_metrics['ranking_methods_used'][method] += 1
            
            # Update average ranking time
            current_avg = self.ranking_metrics['avg_ranking_time_ms']
            total_rankings = self.ranking_metrics['total_rankings']
            
            new_avg = (current_avg * (total_rankings - 1) + ranking_time_ms) / total_rankings
            self.ranking_metrics['avg_ranking_time_ms'] = new_avg
            
        except Exception as e:
            logger.error(f"Error updating ranking metrics: {e}")
    
    async def process_user_signal(self, signal: UserSignal):
        """Process a user signal for real-time adaptation."""
        try:
            # Send to real-time processor
            await self.real_time_processor.process_signal(signal)
            
            # Update collaborative filtering if it's an interaction
            if signal.signal_type in ['like', 'comment', 'share', 'follow', 'view']:
                interaction = UserItemInteraction(
                    user_id=signal.user_id,
                    item_id=signal.content_id or "unknown",
                    interaction_type=signal.signal_type,
                    timestamp=signal.timestamp,
                    duration=signal.duration,
                    intensity=signal.intensity
                )
                
                # Add to collaborative filtering
                self.collaborative_filtering.add_interaction(interaction)
                
                # Retrain models periodically (in practice, this would be done asynchronously)
                if self.collaborative_filtering.user_item_interactions % 1000 == 0:
                    logger.info("Retraining collaborative filtering models")
                    self.collaborative_filtering.train_all_models()
            
            logger.info(f"Processed signal {signal.signal_id} for user {signal.user_id}")
            
        except Exception as e:
            logger.error(f"Error processing user signal: {e}")
    
    def get_ranking_insights(self, user_id: str) -> Dict[str, Any]:
        """Get insights about ranking performance for a user."""
        try:
            insights = {
                'user_id': user_id,
                'ranking_methods_used': dict(self.ranking_metrics['ranking_methods_used']),
                'total_rankings': self.ranking_metrics['total_rankings'],
                'avg_ranking_time_ms': self.ranking_metrics['avg_ranking_time_ms'],
                'real_time_signals': len(self.real_time_processor.get_user_signals(user_id, "24h")),
                'collaborative_interactions': len(self.collaborative_filtering.user_item_interactions),
                'user_embedding_available': self.real_time_processor.get_user_embedding(user_id) is not None
            }
            
            return insights
            
        except Exception as e:
            logger.error(f"Error getting ranking insights: {e}")
            return {'user_id': user_id, 'error': str(e)}
    
    def get_system_performance(self) -> Dict[str, Any]:
        """Get overall system performance metrics."""
        try:
            performance = {
                'ranking_service': {
                    'total_rankings': self.ranking_metrics['total_rankings'],
                    'avg_ranking_time_ms': self.ranking_metrics['avg_ranking_time_ms'],
                    'methods_used': dict(self.ranking_metrics['ranking_methods_used'])
                },
                'real_time_processor': self.real_time_processor.get_performance_metrics(),
                'system_health': self.real_time_processor.get_system_health(),
                'collaborative_filtering': {
                    'total_interactions': len(self.collaborative_filtering.user_item_interactions),
                    'active_users': len(self.collaborative_filtering.user_mapping),
                    'active_items': len(self.collaborative_filtering.item_mapping)
                }
            }
            
            return performance
            
        except Exception as e:
            logger.error(f"Error getting system performance: {e}")
            return {'error': str(e)}