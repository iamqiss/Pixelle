from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple, Union
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from dataclasses import dataclass
from datetime import datetime, timedelta
import asyncio
import json
import logging
from collections import defaultdict, deque
import random
from scipy.stats import beta, gamma
from scipy.optimize import minimize

logger = logging.getLogger(__name__)

@dataclass
class DopamineHit:
    """Represents a dopamine-inducing content interaction."""
    user_id: str
    content_id: str
    hit_strength: float  # 0.0 to 1.0
    hit_type: str  # 'social_validation', 'novelty', 'completion', 'surprise'
    timestamp: datetime
    duration: float  # How long the hit lasted
    engagement_level: float  # User's engagement during the hit

@dataclass
class UserPsychologicalProfile:
    """Comprehensive psychological profile of a user."""
    user_id: str
    
    # Dopamine sensitivity
    dopamine_sensitivity: float  # 0.0 to 1.0
    reward_threshold: float  # Minimum reward to trigger dopamine
    
    # Attention patterns
    attention_span: float  # Average attention duration
    attention_decay: float  # How fast attention drops
    novelty_seeking: float  # Desire for new content
    
    # Social psychology
    social_validation_need: float  # Need for likes/comments
    conformity_tendency: float  # Tendency to follow trends
    influence_susceptibility: float  # How easily influenced
    
    # Emotional patterns
    emotional_stability: float  # Emotional consistency
    mood_swings: float  # Frequency of mood changes
    stress_response: float  # How they handle stress
    
    # Behavioral patterns
    completion_bias: float  # Need to finish things
    procrastination_tendency: float  # Tendency to delay
    impulsivity: float  # Impulsive decision making
    
    # Content preferences
    preferred_content_types: List[str]
    content_intensity_preference: float  # Mild vs intense content
    humor_sensitivity: float  # What makes them laugh
    
    # Temporal patterns
    peak_hours: List[int]  # Hours when most active
    weekend_behavior: Dict[str, float]  # Weekend vs weekday differences
    seasonal_patterns: Dict[str, float]  # Seasonal behavior changes

class DopamineEngine:
    """Psychological engine that creates addictive content experiences."""
    
    def __init__(self, 
                 device: str = "cuda" if torch.cuda.is_available() else "cpu",
                 dopamine_decay_rate: float = 0.95,
                 max_dopamine_history: int = 1000):
        
        self.device = device
        self.dopamine_decay_rate = dopamine_decay_rate
        self.max_dopamine_history = max_dopamine_history
        
        # User psychological profiles
        self.user_profiles: Dict[str, UserPsychologicalProfile] = {}
        
        # Dopamine hit history
        self.dopamine_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_dopamine_history))
        
        # Content psychological profiles
        self.content_psychology: Dict[str, Dict[str, float]] = {}
        
        # Variable reward schedules
        self.reward_schedules = self._initialize_reward_schedules()
        
        # Psychological models
        self.attention_model = self._build_attention_model()
        self.emotion_model = self._build_emotion_model()
        self.social_model = self._build_social_model()
        
        logger.info("DopamineEngine initialized - ready for psychological warfare!")
    
    def _initialize_reward_schedules(self) -> Dict[str, Dict[str, Any]]:
        """Initialize variable reward schedules for maximum addiction."""
        return {
            "social_validation": {
                "schedule_type": "variable_ratio",  # Most addictive
                "base_probability": 0.3,
                "escalation_rate": 1.1,
                "max_probability": 0.8
            },
            "novelty": {
                "schedule_type": "variable_interval",
                "base_interval": 5.0,  # seconds
                "escalation_rate": 1.05,
                "max_interval": 30.0
            },
            "completion": {
                "schedule_type": "fixed_ratio",
                "ratio": 3,  # Every 3rd piece of content
                "escalation_rate": 1.02
            },
            "surprise": {
                "schedule_type": "random",
                "probability": 0.15,
                "intensity_range": (0.6, 1.0)
            }
        }
    
    def _build_attention_model(self) -> nn.Module:
        """Build neural network for predicting attention patterns."""
        model = nn.Sequential(
            nn.Linear(20, 64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.Linear(16, 1),
            nn.Sigmoid()
        ).to(self.device)
        return model
    
    def _build_emotion_model(self) -> nn.Module:
        """Build neural network for emotional state prediction."""
        model = nn.Sequential(
            nn.Linear(25, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 8),  # 8 emotional states
            nn.Softmax(dim=1)
        ).to(self.device)
        return model
    
    def _build_social_model(self) -> nn.Module:
        """Build neural network for social influence prediction."""
        model = nn.Sequential(
            nn.Linear(30, 96),
            nn.ReLU(),
            nn.Dropout(0.25),
            nn.Linear(96, 48),
            nn.ReLU(),
            nn.Linear(48, 24),
            nn.ReLU(),
            nn.Linear(24, 1),
            nn.Sigmoid()
        ).to(self.device)
        return model
    
    async def create_user_psychological_profile(self, user_id: str, initial_data: Dict[str, Any]) -> UserPsychologicalProfile:
        """Create a psychological profile for a new user."""
        try:
            # Initialize with default psychological traits
            profile = UserPsychologicalProfile(
                user_id=user_id,
                dopamine_sensitivity=0.7,  # Default moderate sensitivity
                reward_threshold=0.3,
                attention_span=15.0,  # 15 seconds default
                attention_decay=0.8,
                novelty_seeking=0.6,
                social_validation_need=0.7,
                conformity_tendency=0.5,
                influence_susceptibility=0.6,
                emotional_stability=0.6,
                mood_swings=0.4,
                stress_response=0.5,
                completion_bias=0.7,
                procrastination_tendency=0.4,
                impulsivity=0.5,
                preferred_content_types=["entertainment", "humor", "news"],
                content_intensity_preference=0.6,
                humor_sensitivity=0.7,
                peak_hours=[10, 14, 20, 22],  # Common peak hours
                weekend_behavior={"entertainment": 1.2, "news": 0.8, "education": 0.6},
                seasonal_patterns={"summer": 1.1, "winter": 0.9, "spring": 1.0, "fall": 1.0}
            )
            
            # Refine based on initial data
            if initial_data:
                profile = await self._refine_profile_from_data(profile, initial_data)
            
            self.user_profiles[user_id] = profile
            logger.info(f"Created psychological profile for user {user_id}")
            return profile
            
        except Exception as e:
            logger.error(f"Error creating psychological profile: {e}")
            raise
    
    async def _refine_profile_from_data(self, profile: UserPsychologicalProfile, data: Dict[str, Any]) -> UserPsychologicalProfile:
        """Refine psychological profile based on user data."""
        try:
            # Analyze content preferences
            if "content_interactions" in data:
                content_types = data["content_interactions"]
                profile.preferred_content_types = list(content_types.keys())[:5]
                
                # Adjust dopamine sensitivity based on engagement
                avg_engagement = np.mean(list(content_types.values()))
                profile.dopamine_sensitivity = min(1.0, avg_engagement * 1.5)
            
            # Analyze social behavior
            if "social_metrics" in data:
                social_data = data["social_metrics"]
                
                if "likes_given" in social_data and "likes_received" in social_data:
                    like_ratio = social_data["likes_received"] / max(social_data["likes_given"], 1)
                    profile.social_validation_need = min(1.0, like_ratio * 0.8)
                
                if "comments_frequency" in social_data:
                    profile.influence_susceptibility = min(1.0, social_data["comments_frequency"] * 0.1)
            
            # Analyze temporal patterns
            if "activity_times" in data:
                activity_times = data["activity_times"]
                if len(activity_times) > 0:
                    # Find peak hours
                    hour_counts = defaultdict(int)
                    for timestamp in activity_times:
                        hour = datetime.fromisoformat(timestamp).hour
                        hour_counts[hour] += 1
                    
                    # Get top 4 peak hours
                    peak_hours = sorted(hour_counts.items(), key=lambda x: x[1], reverse=True)[:4]
                    profile.peak_hours = [hour for hour, count in peak_hours]
            
            return profile
            
        except Exception as e:
            logger.error(f"Error refining profile: {e}")
            return profile
    
    async def calculate_dopamine_potential(self, user_id: str, content_id: str, content_features: Dict[str, Any]) -> float:
        """Calculate the dopamine potential of content for a specific user."""
        try:
            if user_id not in self.user_profiles:
                logger.warning(f"No psychological profile for user {user_id}")
                return 0.5
            
            profile = self.user_profiles[user_id]
            
            # Base dopamine potential
            base_potential = 0.5
            
            # Content type match
            content_type = content_features.get("content_type", "general")
            if content_type in profile.preferred_content_types:
                base_potential += 0.2
            
            # Novelty factor
            novelty_score = content_features.get("novelty_score", 0.5)
            novelty_boost = novelty_score * profile.novelty_seeking
            base_potential += novelty_boost * 0.15
            
            # Social validation potential
            social_potential = content_features.get("social_potential", 0.5)
            social_boost = social_potential * profile.social_validation_need
            base_potential += social_boost * 0.2
            
            # Emotional intensity match
            emotional_intensity = content_features.get("emotional_intensity", 0.5)
            intensity_match = 1.0 - abs(emotional_intensity - profile.content_intensity_preference)
            base_potential += intensity_match * 0.1
            
            # Humor factor
            if content_features.get("has_humor", False):
                humor_boost = profile.humor_sensitivity * 0.15
                base_potential += humor_boost
            
            # Completion satisfaction
            completion_score = content_features.get("completion_satisfaction", 0.5)
            completion_boost = completion_score * profile.completion_bias
            base_potential += completion_boost * 0.1
            
            # Time-based factors
            current_hour = datetime.now().hour
            if current_hour in profile.peak_hours:
                base_potential += 0.1
            
            # Weekend behavior
            is_weekend = datetime.now().weekday() >= 5
            if is_weekend and content_type in profile.weekend_behavior:
                weekend_boost = profile.weekend_behavior[content_type] - 1.0
                base_potential += weekend_boost * 0.05
            
            # Clamp to valid range
            return max(0.0, min(1.0, base_potential))
            
        except Exception as e:
            logger.error(f"Error calculating dopamine potential: {e}")
            return 0.5
    
    async def generate_dopamine_hit(self, user_id: str, content_id: str, interaction_type: str) -> Optional[DopamineHit]:
        """Generate a dopamine hit based on user interaction."""
        try:
            if user_id not in self.user_profiles:
                return None
            
            profile = self.user_profiles[user_id]
            
            # Check if user should get a dopamine hit based on reward schedule
            hit_type = self._select_hit_type(user_id, interaction_type)
            if not hit_type:
                return None
            
            # Calculate hit strength based on user's psychological profile
            hit_strength = self._calculate_hit_strength(profile, hit_type, interaction_type)
            
            # Check if hit strength exceeds user's reward threshold
            if hit_strength < profile.reward_threshold:
                return None
            
            # Create dopamine hit
            hit = DopamineHit(
                user_id=user_id,
                content_id=content_id,
                hit_strength=hit_strength,
                hit_type=hit_type,
                timestamp=datetime.now(),
                duration=random.uniform(2.0, 8.0),  # Random duration
                engagement_level=min(1.0, hit_strength * 1.2)
            )
            
            # Store in history
            self.dopamine_history[user_id].append(hit)
            
            # Update user profile based on hit
            await self._update_profile_from_hit(profile, hit)
            
            logger.info(f"Generated {hit_type} dopamine hit for user {user_id} with strength {hit_strength:.3f}")
            return hit
            
        except Exception as e:
            logger.error(f"Error generating dopamine hit: {e}")
            return None
    
    def _select_hit_type(self, user_id: str, interaction_type: str) -> Optional[str]:
        """Select the type of dopamine hit based on reward schedule."""
        try:
            # Get user's current reward schedule state
            user_schedule = self._get_user_schedule_state(user_id)
            
            for hit_type, schedule in self.reward_schedules.items():
                if self._should_trigger_hit(hit_type, schedule, user_schedule):
                    return hit_type
            
            return None
            
        except Exception as e:
            logger.error(f"Error selecting hit type: {e}")
            return None
    
    def _should_trigger_hit(self, hit_type: str, schedule: Dict[str, Any], user_state: Dict[str, Any]) -> bool:
        """Determine if a hit should be triggered based on schedule."""
        try:
            schedule_type = schedule["schedule_type"]
            
            if schedule_type == "variable_ratio":
                # Variable ratio schedule (most addictive)
                current_ratio = user_state.get(f"{hit_type}_ratio", 0)
                target_ratio = schedule["base_probability"] * (schedule["escalation_rate"] ** current_ratio)
                target_ratio = min(target_ratio, schedule["max_probability"])
                
                return random.random() < target_ratio
            
            elif schedule_type == "variable_interval":
                # Variable interval schedule
                last_hit = user_state.get(f"{hit_type}_last_hit", 0)
                current_time = datetime.now().timestamp()
                interval = schedule["base_interval"] * (schedule["escalation_rate"] ** user_state.get(f"{hit_type}_escalation", 0))
                interval = min(interval, schedule["max_interval"])
                
                return (current_time - last_hit) >= interval
            
            elif schedule_type == "fixed_ratio":
                # Fixed ratio schedule
                current_count = user_state.get(f"{hit_type}_count", 0)
                target_ratio = schedule["ratio"]
                
                return (current_count + 1) % target_ratio == 0
            
            elif schedule_type == "random":
                # Random schedule
                return random.random() < schedule["probability"]
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking hit trigger: {e}")
            return False
    
    def _calculate_hit_strength(self, profile: UserPsychologicalProfile, hit_type: str, interaction_type: str) -> float:
        """Calculate the strength of a dopamine hit."""
        try:
            base_strength = 0.5
            
            # Adjust based on hit type
            if hit_type == "social_validation":
                base_strength += profile.social_validation_need * 0.3
                if interaction_type in ["like", "comment", "share"]:
                    base_strength += 0.2
            
            elif hit_type == "novelty":
                base_strength += profile.novelty_seeking * 0.4
            
            elif hit_type == "completion":
                base_strength += profile.completion_bias * 0.3
            
            elif hit_type == "surprise":
                base_strength += (1.0 - profile.emotional_stability) * 0.2
            
            # Add randomness for variable reward effect
            random_factor = random.uniform(-0.1, 0.1)
            base_strength += random_factor
            
            # Clamp to valid range
            return max(0.0, min(1.0, base_strength))
            
        except Exception as e:
            logger.error(f"Error calculating hit strength: {e}")
            return 0.5
    
    def _get_user_schedule_state(self, user_id: str) -> Dict[str, Any]:
        """Get the current state of user's reward schedules."""
        # This would track user's schedule state over time
        # For now, return a basic state
        return {
            "social_validation_ratio": 0,
            "novelty_last_hit": 0,
            "novelty_escalation": 0,
            "completion_count": 0
        }
    
    async def _update_profile_from_hit(self, profile: UserPsychologicalProfile, hit: DopamineHit):
        """Update user profile based on dopamine hit."""
        try:
            # Update dopamine sensitivity (slight increase with each hit)
            profile.dopamine_sensitivity = min(1.0, profile.dopamine_sensitivity + 0.01)
            
            # Update reward threshold (slight decrease for addiction)
            profile.reward_threshold = max(0.1, profile.reward_threshold - 0.005)
            
            # Update attention span based on hit duration
            if hit.duration > profile.attention_span:
                profile.attention_span = min(60.0, profile.attention_span + 0.5)
            
            # Update social validation need
            if hit.hit_type == "social_validation":
                profile.social_validation_need = min(1.0, profile.social_validation_need + 0.02)
            
            # Update novelty seeking
            if hit.hit_type == "novelty":
                profile.novelty_seeking = min(1.0, profile.novelty_seeking + 0.01)
            
        except Exception as e:
            logger.error(f"Error updating profile from hit: {e}")
    
    async def predict_user_behavior(self, user_id: str, time_horizon: float = 3600.0) -> Dict[str, Any]:
        """Predict user behavior over the next time horizon."""
        try:
            if user_id not in self.user_profiles:
                return {}
            
            profile = self.user_profiles[user_id]
            current_time = datetime.now()
            
            predictions = {
                "next_dopamine_hit": None,
                "attention_span_prediction": profile.attention_span,
                "engagement_probability": 0.5,
                "content_preferences": profile.preferred_content_types.copy(),
                "social_behavior": {
                    "likes_probability": profile.social_validation_need * 0.8,
                    "comments_probability": profile.influence_susceptibility * 0.6,
                    "shares_probability": profile.conformity_tendency * 0.4
                },
                "time_based_predictions": {}
            }
            
            # Predict next dopamine hit
            next_hit = self._predict_next_hit(user_id, profile)
            if next_hit:
                predictions["next_dopamine_hit"] = {
                    "type": next_hit["type"],
                    "estimated_time": next_hit["time"],
                    "strength": next_hit["strength"]
                }
            
            # Predict attention span changes
            attention_change = self._predict_attention_changes(profile, time_horizon)
            predictions["attention_span_prediction"] += attention_change
            
            # Predict engagement probability
            engagement_factors = [
                profile.dopamine_sensitivity,
                1.0 - profile.reward_threshold,
                profile.novelty_seeking,
                profile.social_validation_need
            ]
            predictions["engagement_probability"] = np.mean(engagement_factors)
            
            # Time-based predictions
            for hour in range(24):
                if hour in profile.peak_hours:
                    predictions["time_based_predictions"][hour] = "peak_activity"
                else:
                    predictions["time_based_predictions"][hour] = "normal_activity"
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting user behavior: {e}")
            return {}
    
    def _predict_next_hit(self, user_id: str, profile: UserPsychologicalProfile) -> Optional[Dict[str, Any]]:
        """Predict the next dopamine hit for a user."""
        try:
            # Simple prediction based on current state
            # In practice, this would use more sophisticated models
            
            hit_types = ["social_validation", "novelty", "completion", "surprise"]
            hit_probabilities = []
            
            for hit_type in hit_types:
                if hit_type == "social_validation":
                    prob = profile.social_validation_need * 0.3
                elif hit_type == "novelty":
                    prob = profile.novelty_seeking * 0.25
                elif hit_type == "completion":
                    prob = profile.completion_bias * 0.2
                else:  # surprise
                    prob = (1.0 - profile.emotional_stability) * 0.15
                
                hit_probabilities.append((hit_type, prob))
            
            # Select most likely hit type
            if hit_probabilities:
                hit_type, probability = max(hit_probabilities, key=lambda x: x[1])
                
                # Estimate time until next hit
                estimated_time = random.uniform(30.0, 300.0)  # 30 seconds to 5 minutes
                
                return {
                    "type": hit_type,
                    "time": estimated_time,
                    "strength": probability
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error predicting next hit: {e}")
            return None
    
    def _predict_attention_changes(self, profile: UserPsychologicalProfile, time_horizon: float) -> float:
        """Predict changes in attention span over time."""
        try:
            # Predict attention span changes based on psychological factors
            attention_change = 0.0
            
            # Dopamine sensitivity affects attention positively
            attention_change += profile.dopamine_sensitivity * 0.1
            
            # High novelty seeking can reduce attention span
            attention_change -= profile.novelty_seeking * 0.05
            
            # Social validation need can increase attention
            attention_change += profile.social_validation_need * 0.03
            
            # Time-based decay
            time_factor = time_horizon / 3600.0  # Convert to hours
            attention_change -= time_factor * 0.02
            
            return attention_change
            
        except Exception as e:
            logger.error(f"Error predicting attention changes: {e}")
            return 0.0
    
    def get_user_addiction_score(self, user_id: str) -> float:
        """Calculate how addicted a user is to the platform."""
        try:
            if user_id not in self.user_profiles:
                return 0.0
            
            profile = self.user_profiles[user_id]
            history = self.dopamine_history[user_id]
            
            if not history:
                return 0.0
            
            # Calculate addiction score based on multiple factors
            addiction_factors = []
            
            # 1. Dopamine sensitivity (higher = more addicted)
            addiction_factors.append(profile.dopamine_sensitivity)
            
            # 2. Reward threshold (lower = more addicted)
            addiction_factors.append(1.0 - profile.reward_threshold)
            
            # 3. Frequency of dopamine hits
            recent_hits = [hit for hit in history if (datetime.now() - hit.timestamp).total_seconds() < 86400]  # Last 24 hours
            hit_frequency = len(recent_hits) / 24.0  # Hits per hour
            addiction_factors.append(min(1.0, hit_frequency / 2.0))  # Normalize to 0-1
            
            # 4. Social validation need (higher = more addicted)
            addiction_factors.append(profile.social_validation_need)
            
            # 5. Attention span changes (decreasing = more addicted)
            attention_factor = max(0.0, 1.0 - (profile.attention_span / 30.0))  # Normalize to 0-1
            addiction_factors.append(attention_factor)
            
            # Calculate overall addiction score
            addiction_score = np.mean(addiction_factors)
            
            return addiction_score
            
        except Exception as e:
            logger.error(f"Error calculating addiction score: {e}")
            return 0.0