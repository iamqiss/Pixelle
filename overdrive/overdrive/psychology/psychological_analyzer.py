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
import re
from textblob import TextBlob
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

logger = logging.getLogger(__name__)

@dataclass
class PsychologicalContent:
    """Content with psychological analysis."""
    content_id: str
    content_type: str  # 'note', 'comment', 'search', 'interaction'
    raw_text: str
    psychological_features: Dict[str, float]
    emotional_state: Dict[str, float]
    personality_indicators: Dict[str, float]
    behavioral_patterns: Dict[str, float]
    addiction_potential: float

@dataclass
class UserPsychologicalInsights:
    """Psychological insights about a user."""
    user_id: str
    personality_profile: Dict[str, float]
    emotional_patterns: Dict[str, float]
    behavioral_tendencies: Dict[str, float]
    content_preferences: Dict[str, float]
    addiction_indicators: Dict[str, float]
    psychological_triggers: List[str]

class PsychologicalAnalyzer:
    """Analyzes user psychology from content to create addictive recommendations."""
    
    def __init__(self, device: str = "cuda" if torch.cuda.is_available() else "cpu"):
        self.device = device
        
        # Psychological models
        self.personality_model = self._build_personality_model()
        self.emotion_model = self._build_emotion_model()
        self.behavior_model = self._build_behavior_model()
        
        # Psychological dictionaries
        self.psychological_indicators = self._initialize_psychological_indicators()
        
        # User psychological profiles
        self.user_profiles: Dict[str, UserPsychologicalInsights] = {}
        
        # Content psychological profiles
        self.content_profiles: Dict[str, PsychologicalContent] = {}
        
        logger.info("PsychologicalAnalyzer initialized - ready to analyze user psychology!")
    
    def _build_personality_model(self) -> nn.Module:
        """Build neural network for personality analysis."""
        model = nn.Sequential(
            nn.Linear(50, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.Linear(16, 5),  # Big 5 personality traits
            nn.Softmax(dim=1)
        ).to(self.device)
        return model
    
    def _build_emotion_model(self) -> nn.Module:
        """Build neural network for emotional state analysis."""
        model = nn.Sequential(
            nn.Linear(40, 96),
            nn.ReLU(),
            nn.Dropout(0.25),
            nn.Linear(96, 48),
            nn.ReLU(),
            nn.Linear(48, 24),
            nn.ReLU(),
            nn.Linear(24, 8),  # 8 emotional states
            nn.Softmax(dim=1)
        ).to(self.device)
        return model
    
    def _build_behavior_model(self) -> nn.Module:
        """Build neural network for behavioral pattern analysis."""
        model = nn.Sequential(
            nn.Linear(35, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid()
        ).to(self.device)
        return model
    
    def _initialize_psychological_indicators(self) -> Dict[str, Dict[str, List[str]]]:
        """Initialize psychological indicators for analysis."""
        return {
            "personality_traits": {
                "openness": ["creative", "imaginative", "curious", "adventurous", "artistic"],
                "conscientiousness": ["organized", "responsible", "diligent", "disciplined", "careful"],
                "extraversion": ["outgoing", "energetic", "sociable", "talkative", "enthusiastic"],
                "agreeableness": ["friendly", "compassionate", "cooperative", "trusting", "helpful"],
                "neuroticism": ["anxious", "worried", "moody", "irritable", "sensitive"]
            },
            "emotional_states": {
                "joy": ["happy", "excited", "thrilled", "delighted", "ecstatic"],
                "anger": ["angry", "furious", "irritated", "annoyed", "mad"],
                "sadness": ["sad", "depressed", "melancholy", "gloomy", "sorrowful"],
                "fear": ["afraid", "scared", "terrified", "anxious", "worried"],
                "surprise": ["surprised", "shocked", "amazed", "astonished", "stunned"],
                "disgust": ["disgusted", "revolted", "repulsed", "appalled", "sickened"],
                "trust": ["trusting", "confident", "secure", "assured", "reliable"],
                "anticipation": ["excited", "eager", "enthusiastic", "optimistic", "hopeful"]
            },
            "behavioral_patterns": {
                "addiction_prone": ["binge", "obsessed", "can't stop", "always", "constantly"],
                "social_seeking": ["friends", "social", "party", "meet", "connect"],
                "validation_seeking": ["like", "approval", "recognition", "praise", "attention"],
                "escapism": ["escape", "distract", "avoid", "forget", "numb"],
                "competition": ["win", "beat", "compete", "challenge", "victory"],
                "curiosity": ["wonder", "curious", "discover", "explore", "learn"]
            }
        }
    
    async def analyze_user_psychology(self, user_id: str, user_content: List[Dict[str, Any]]) -> UserPsychologicalInsights:
        """Analyze user psychology from their content."""
        try:
            # Analyze individual content pieces
            psychological_contents = []
            for content in user_content:
                analyzed_content = await self.analyze_content_psychology(content)
                psychological_contents.append(analyzed_content)
                self.content_profiles[analyzed_content.content_id] = analyzed_content
            
            # Aggregate psychological insights
            insights = await self._aggregate_psychological_insights(user_id, psychological_contents)
            
            # Store user profile
            self.user_profiles[user_id] = insights
            
            logger.info(f"Analyzed psychology for user {user_id}")
            return insights
            
        except Exception as e:
            logger.error(f"Error analyzing user psychology: {e}")
            raise
    
    async def analyze_content_psychology(self, content: Dict[str, Any]) -> PsychologicalContent:
        """Analyze the psychological aspects of content."""
        try:
            content_id = content.get("id", "unknown")
            content_type = content.get("type", "unknown")
            raw_text = content.get("text", "")
            
            # Extract psychological features
            psychological_features = await self._extract_psychological_features(raw_text)
            
            # Analyze emotional state
            emotional_state = await self._analyze_emotional_state(raw_text)
            
            # Analyze personality indicators
            personality_indicators = await self._analyze_personality_indicators(raw_text)
            
            # Analyze behavioral patterns
            behavioral_patterns = await self._analyze_behavioral_patterns(raw_text)
            
            # Calculate addiction potential
            addiction_potential = await self._calculate_addiction_potential(
                psychological_features, emotional_state, personality_indicators, behavioral_patterns
            )
            
            psychological_content = PsychologicalContent(
                content_id=content_id,
                content_type=content_type,
                raw_text=raw_text,
                psychological_features=psychological_features,
                emotional_state=emotional_state,
                personality_indicators=personality_indicators,
                behavioral_patterns=behavioral_patterns,
                addiction_potential=addiction_potential
            )
            
            return psychological_content
            
        except Exception as e:
            logger.error(f"Error analyzing content psychology: {e}")
            raise
    
    async def _extract_psychological_features(self, text: str) -> Dict[str, float]:
        """Extract psychological features from text."""
        try:
            features = {}
            
            # Text complexity
            words = word_tokenize(text.lower())
            features["word_count"] = len(words)
            features["avg_word_length"] = np.mean([len(word) for word in words]) if words else 0
            
            # Emotional intensity
            features["emotional_intensity"] = self._calculate_emotional_intensity(text)
            
            # Social indicators
            features["social_references"] = self._count_social_references(text)
            features["self_references"] = self._count_self_references(text)
            
            # Urgency indicators
            features["urgency_indicators"] = self._count_urgency_indicators(text)
            
            # Novelty indicators
            features["novelty_indicators"] = self._count_novelty_indicators(text)
            
            # Completion indicators
            features["completion_indicators"] = self._count_completion_indicators(text)
            
            # Sentiment analysis
            blob = TextBlob(text)
            features["sentiment_polarity"] = blob.sentiment.polarity
            features["sentiment_subjectivity"] = blob.sentiment.subjectivity
            
            # Normalize features
            for key, value in features.items():
                if isinstance(value, (int, float)):
                    features[key] = min(1.0, max(0.0, value / 10.0))  # Normalize to 0-1
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting psychological features: {e}")
            return {}
    
    def _calculate_emotional_intensity(self, text: str) -> float:
        """Calculate emotional intensity of text."""
        try:
            emotional_words = 0
            total_words = len(word_tokenize(text.lower()))
            
            for emotion, words in self.psychological_indicators["emotional_states"].items():
                for word in words:
                    if word in text.lower():
                        emotional_words += 1
            
            return min(1.0, emotional_words / max(total_words, 1))
            
        except Exception as e:
            logger.error(f"Error calculating emotional intensity: {e}")
            return 0.0
    
    def _count_social_references(self, text: str) -> float:
        """Count social references in text."""
        try:
            social_words = ["friend", "family", "people", "group", "community", "social", "party", "meet"]
            count = sum(1 for word in social_words if word in text.lower())
            return min(1.0, count / 3.0)  # Normalize to 0-1
            
        except Exception as e:
            logger.error(f"Error counting social references: {e}")
            return 0.0
    
    def _count_self_references(self, text: str) -> float:
        """Count self-references in text."""
        try:
            self_words = ["i", "me", "my", "myself", "i'm", "i've", "i'll"]
            count = sum(1 for word in self_words if word in text.lower())
            return min(1.0, count / 5.0)  # Normalize to 0-1
            
        except Exception as e:
            logger.error(f"Error counting self references: {e}")
            return 0.0
    
    def _count_urgency_indicators(self, text: str) -> float:
        """Count urgency indicators in text."""
        try:
            urgency_words = ["now", "urgent", "quick", "fast", "immediate", "hurry", "rush", "deadline"]
            count = sum(1 for word in urgency_words if word in text.lower())
            return min(1.0, count / 4.0)  # Normalize to 0-1
            
        except Exception as e:
            logger.error(f"Error counting urgency indicators: {e}")
            return 0.0
    
    def _count_novelty_indicators(self, text: str) -> float:
        """Count novelty indicators in text."""
        try:
            novelty_words = ["new", "first", "never", "amazing", "incredible", "unbelievable", "wow", "omg"]
            count = sum(1 for word in novelty_words if word in text.lower())
            return min(1.0, count / 4.0)  # Normalize to 0-1
            
        except Exception as e:
            logger.error(f"Error counting novelty indicators: {e}")
            return 0.0
    
    def _count_completion_indicators(self, text: str) -> float:
        """Count completion indicators in text."""
        try:
            completion_words = ["finish", "complete", "done", "end", "final", "last", "conclusion"]
            count = sum(1 for word in completion_words if word in text.lower())
            return min(1.0, count / 3.0)  # Normalize to 0-1
            
        except Exception as e:
            logger.error(f"Error counting completion indicators: {e}")
            return 0.0
    
    async def _analyze_emotional_state(self, text: str) -> Dict[str, float]:
        """Analyze emotional state from text."""
        try:
            emotional_scores = {}
            
            for emotion, words in self.psychological_indicators["emotional_states"].items():
                score = 0.0
                for word in words:
                    if word in text.lower():
                        score += 1.0
                
                # Normalize score
                emotional_scores[emotion] = min(1.0, score / 3.0)
            
            # If no emotions detected, assign neutral state
            if not any(emotional_scores.values()):
                emotional_scores["neutral"] = 1.0
            
            return emotional_scores
            
        except Exception as e:
            logger.error(f"Error analyzing emotional state: {e}")
            return {"neutral": 1.0}
    
    async def _analyze_personality_indicators(self, text: str) -> Dict[str, float]:
        """Analyze personality indicators from text."""
        try:
            personality_scores = {}
            
            for trait, words in self.psychological_indicators["personality_traits"].items():
                score = 0.0
                for word in words:
                    if word in text.lower():
                        score += 1.0
                
                # Normalize score
                personality_scores[trait] = min(1.0, score / 3.0)
            
            return personality_scores
            
        except Exception as e:
            logger.error(f"Error analyzing personality indicators: {e}")
            return {}
    
    async def _analyze_behavioral_patterns(self, text: str) -> Dict[str, float]:
        """Analyze behavioral patterns from text."""
        try:
            behavioral_scores = {}
            
            for pattern, words in self.psychological_indicators["behavioral_patterns"].items():
                score = 0.0
                for word in words:
                    if word in text.lower():
                        score += 1.0
                
                # Normalize score
                behavioral_scores[pattern] = min(1.0, score / 3.0)
            
            return behavioral_scores
            
        except Exception as e:
            logger.error(f"Error analyzing behavioral patterns: {e}")
            return {}
    
    async def _calculate_addiction_potential(self, psychological_features: Dict[str, float], 
                                           emotional_state: Dict[str, float], 
                                           personality_indicators: Dict[str, float], 
                                           behavioral_patterns: Dict[str, float]) -> float:
        """Calculate addiction potential of content."""
        try:
            addiction_factors = []
            
            # High emotional intensity increases addiction potential
            if "emotional_intensity" in psychological_features:
                addiction_factors.append(psychological_features["emotional_intensity"] * 0.3)
            
            # High urgency increases addiction potential
            if "urgency_indicators" in psychological_features:
                addiction_factors.append(psychological_features["urgency_indicators"] * 0.25)
            
            # High novelty increases addiction potential
            if "novelty_indicators" in psychological_features:
                addiction_factors.append(psychological_features["novelty_indicators"] * 0.2)
            
            # High completion satisfaction increases addiction potential
            if "completion_indicators" in psychological_features:
                addiction_factors.append(psychological_features["completion_indicators"] * 0.15)
            
            # High social references increase addiction potential
            if "social_references" in psychological_features:
                addiction_factors.append(psychological_features["social_references"] * 0.1)
            
            # Behavioral patterns that indicate addiction proneness
            if "addiction_prone" in behavioral_patterns:
                addiction_factors.append(behavioral_patterns["addiction_prone"] * 0.3)
            
            # Emotional states that increase addiction potential
            high_addiction_emotions = ["joy", "excitement", "anticipation", "surprise"]
            for emotion in high_addiction_emotions:
                if emotion in emotional_state:
                    addiction_factors.append(emotional_state[emotion] * 0.2)
            
            # Calculate overall addiction potential
            if addiction_factors:
                addiction_potential = np.mean(addiction_factors)
            else:
                addiction_potential = 0.5  # Neutral default
            
            return min(1.0, max(0.0, addiction_potential))
            
        except Exception as e:
            logger.error(f"Error calculating addiction potential: {e}")
            return 0.5
    
    async def _aggregate_psychological_insights(self, user_id: str, 
                                              psychological_contents: List[PsychologicalContent]) -> UserPsychologicalInsights:
        """Aggregate psychological insights across multiple content pieces."""
        try:
            if not psychological_contents:
                return UserPsychologicalInsights(
                    user_id=user_id,
                    personality_profile={},
                    emotional_patterns={},
                    behavioral_tendencies={},
                    content_preferences={},
                    addiction_indicators={},
                    psychological_triggers=[]
                )
            
            # Aggregate personality indicators
            personality_profile = self._aggregate_personality_profile(psychological_contents)
            
            # Aggregate emotional patterns
            emotional_patterns = self._aggregate_emotional_patterns(psychological_contents)
            
            # Aggregate behavioral tendencies
            behavioral_tendencies = self._aggregate_behavioral_tendencies(psychological_contents)
            
            # Analyze content preferences
            content_preferences = self._analyze_content_preferences(psychological_contents)
            
            # Analyze addiction indicators
            addiction_indicators = self._analyze_addiction_indicators(psychological_contents)
            
            # Identify psychological triggers
            psychological_triggers = self._identify_psychological_triggers(
                personality_profile, emotional_patterns, behavioral_tendencies
            )
            
            insights = UserPsychologicalInsights(
                user_id=user_id,
                personality_profile=personality_profile,
                emotional_patterns=emotional_patterns,
                behavioral_tendencies=behavioral_tendencies,
                content_preferences=content_preferences,
                addiction_indicators=addiction_indicators,
                psychological_triggers=psychological_triggers
            )
            
            return insights
            
        except Exception as e:
            logger.error(f"Error aggregating psychological insights: {e}")
            raise
    
    def _aggregate_personality_profile(self, contents: List[PsychologicalContent]) -> Dict[str, float]:
        """Aggregate personality profile across content pieces."""
        try:
            personality_scores = defaultdict(list)
            
            for content in contents:
                for trait, score in content.personality_indicators.items():
                    personality_scores[trait].append(score)
            
            # Calculate average scores
            aggregated_profile = {}
            for trait, scores in personality_scores.items():
                aggregated_profile[trait] = np.mean(scores)
            
            return aggregated_profile
            
        except Exception as e:
            logger.error(f"Error aggregating personality profile: {e}")
            return {}
    
    def _aggregate_emotional_patterns(self, contents: List[PsychologicalContent]) -> Dict[str, float]:
        """Aggregate emotional patterns across content pieces."""
        try:
            emotional_scores = defaultdict(list)
            
            for content in contents:
                for emotion, score in content.emotional_state.items():
                    emotional_scores[emotion].append(score)
            
            # Calculate average scores
            aggregated_patterns = {}
            for emotion, scores in emotional_scores.items():
                aggregated_patterns[emotion] = np.mean(scores)
            
            return aggregated_patterns
            
        except Exception as e:
            logger.error(f"Error aggregating emotional patterns: {e}")
            return {}
    
    def _aggregate_behavioral_tendencies(self, contents: List[PsychologicalContent]) -> Dict[str, float]:
        """Aggregate behavioral tendencies across content pieces."""
        try:
            behavioral_scores = defaultdict(list)
            
            for content in contents:
                for pattern, score in content.behavioral_patterns.items():
                    behavioral_scores[pattern].append(score)
            
            # Calculate average scores
            aggregated_tendencies = {}
            for pattern, scores in behavioral_scores.items():
                aggregated_tendencies[pattern] = np.mean(scores)
            
            return aggregated_tendencies
            
        except Exception as e:
            logger.error(f"Error aggregating behavioral tendencies: {e}")
            return {}
    
    def _analyze_content_preferences(self, contents: List[PsychologicalContent]) -> Dict[str, float]:
        """Analyze content preferences based on psychological analysis."""
        try:
            preferences = {
                "high_emotion": 0.0,
                "social_content": 0.0,
                "novelty_seeking": 0.0,
                "completion_focused": 0.0,
                "urgency_driven": 0.0
            }
            
            for content in contents:
                # High emotion preference
                if content.psychological_features.get("emotional_intensity", 0) > 0.6:
                    preferences["high_emotion"] += 1
                
                # Social content preference
                if content.psychological_features.get("social_references", 0) > 0.5:
                    preferences["social_content"] += 1
                
                # Novelty seeking
                if content.psychological_features.get("novelty_indicators", 0) > 0.5:
                    preferences["novelty_seeking"] += 1
                
                # Completion focused
                if content.psychological_features.get("completion_indicators", 0) > 0.5:
                    preferences["completion_focused"] += 1
                
                # Urgency driven
                if content.psychological_features.get("urgency_indicators", 0) > 0.5:
                    preferences["urgency_driven"] += 1
            
            # Normalize preferences
            total_content = len(contents)
            for key in preferences:
                preferences[key] = preferences[key] / total_content if total_content > 0 else 0.0
            
            return preferences
            
        except Exception as e:
            logger.error(f"Error analyzing content preferences: {e}")
            return {}
    
    def _analyze_addiction_indicators(self, contents: List[PsychologicalContent]) -> Dict[str, float]:
        """Analyze addiction indicators across content."""
        try:
            indicators = {
                "overall_addiction_potential": 0.0,
                "emotional_dependency": 0.0,
                "social_validation_seeking": 0.0,
                "novelty_addiction": 0.0,
                "completion_addiction": 0.0
            }
            
            addiction_potentials = [content.addiction_potential for content in contents]
            indicators["overall_addiction_potential"] = np.mean(addiction_potentials) if addiction_potentials else 0.0
            
            # Emotional dependency
            emotional_intensities = [content.psychological_features.get("emotional_intensity", 0) for content in contents]
            indicators["emotional_dependency"] = np.mean(emotional_intensities) if emotional_intensities else 0.0
            
            # Social validation seeking
            social_references = [content.psychological_features.get("social_references", 0) for content in contents]
            indicators["social_validation_seeking"] = np.mean(social_references) if social_references else 0.0
            
            # Novelty addiction
            novelty_indicators = [content.psychological_features.get("novelty_indicators", 0) for content in contents]
            indicators["novelty_addiction"] = np.mean(novelty_indicators) if novelty_indicators else 0.0
            
            # Completion addiction
            completion_indicators = [content.psychological_features.get("completion_indicators", 0) for content in contents]
            indicators["completion_addiction"] = np.mean(completion_indicators) if completion_indicators else 0.0
            
            return indicators
            
        except Exception as e:
            logger.error(f"Error analyzing addiction indicators: {e}")
            return {}
    
    def _identify_psychological_triggers(self, personality_profile: Dict[str, float], 
                                       emotional_patterns: Dict[str, float], 
                                       behavioral_tendencies: Dict[str, float]) -> List[str]:
        """Identify psychological triggers for the user."""
        try:
            triggers = []
            
            # Personality-based triggers
            if personality_profile.get("neuroticism", 0) > 0.6:
                triggers.append("anxiety_relief")
                triggers.append("comfort_content")
            
            if personality_profile.get("extraversion", 0) > 0.6:
                triggers.append("social_validation")
                triggers.append("group_activities")
            
            if personality_profile.get("openness", 0) > 0.6:
                triggers.append("novelty_seeking")
                triggers.append("creative_content")
            
            # Emotional-based triggers
            if emotional_patterns.get("sadness", 0) > 0.5:
                triggers.append("mood_improvement")
                triggers.append("uplifting_content")
            
            if emotional_patterns.get("fear", 0) > 0.5:
                triggers.append("reassurance")
                triggers.append("safety_content")
            
            # Behavioral-based triggers
            if behavioral_tendencies.get("addiction_prone", 0) > 0.6:
                triggers.append("variable_rewards")
                triggers.append("completion_loops")
            
            if behavioral_tendencies.get("validation_seeking", 0) > 0.6:
                triggers.append("like_counts")
                triggers.append("social_proof")
            
            return triggers
            
        except Exception as e:
            logger.error(f"Error identifying psychological triggers: {e}")
            return []
    
    async def get_psychological_recommendations(self, user_id: str, target_content: Dict[str, Any]) -> Dict[str, Any]:
        """Get psychological recommendations for content targeting."""
        try:
            if user_id not in self.user_profiles:
                return {}
            
            user_profile = self.user_profiles[user_id]
            
            # Calculate psychological match score
            match_score = await self._calculate_psychological_match(user_profile, target_content)
            
            # Generate psychological recommendations
            recommendations = {
                "psychological_match_score": match_score,
                "targeted_triggers": self._identify_targeted_triggers(user_profile, target_content),
                "content_optimization": self._suggest_content_optimization(user_profile, target_content),
                "timing_recommendations": self._suggest_timing_recommendations(user_profile),
                "addiction_optimization": self._suggest_addiction_optimization(user_profile)
            }
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error getting psychological recommendations: {e}")
            return {}
    
    async def _calculate_psychological_match(self, user_profile: UserPsychologicalInsights, 
                                           target_content: Dict[str, Any]) -> float:
        """Calculate psychological match between user and content."""
        try:
            match_factors = []
            
            # Personality match
            if "personality_traits" in target_content:
                for trait, score in target_content["personality_traits"].items():
                    if trait in user_profile.personality_profile:
                        user_score = user_profile.personality_profile[trait]
                        match = 1.0 - abs(score - user_score)
                        match_factors.append(match)
            
            # Emotional match
            if "emotional_state" in target_content:
                for emotion, score in target_content["emotional_state"].items():
                    if emotion in user_profile.emotional_patterns:
                        user_score = user_profile.emotional_patterns[emotion]
                        match = 1.0 - abs(score - user_score)
                        match_factors.append(match)
            
            # Behavioral match
            if "behavioral_patterns" in target_content:
                for pattern, score in target_content["behavioral_patterns"].items():
                    if pattern in user_profile.behavioral_tendencies:
                        user_score = user_profile.behavioral_tendencies[pattern]
                        match = 1.0 - abs(score - user_score)
                        match_factors.append(match)
            
            # Calculate overall match score
            if match_factors:
                return np.mean(match_factors)
            else:
                return 0.5  # Neutral default
                
        except Exception as e:
            logger.error(f"Error calculating psychological match: {e}")
            return 0.5
    
    def _identify_targeted_triggers(self, user_profile: UserPsychologicalInsights, 
                                   target_content: Dict[str, Any]) -> List[str]:
        """Identify psychological triggers to use for this user and content."""
        try:
            triggers = []
            
            # Use user's identified triggers
            triggers.extend(user_profile.psychological_triggers)
            
            # Add content-specific triggers
            if target_content.get("addiction_potential", 0) > 0.7:
                triggers.append("strong_hook")
                triggers.append("curiosity_gap")
            
            if target_content.get("social_potential", 0) > 0.6:
                triggers.append("social_proof")
                triggers.append("viral_signals")
            
            return list(set(triggers))  # Remove duplicates
            
        except Exception as e:
            logger.error(f"Error identifying targeted triggers: {e}")
            return []
    
    def _suggest_content_optimization(self, user_profile: UserPsychologicalInsights, 
                                     target_content: Dict[str, Any]) -> Dict[str, Any]:
        """Suggest content optimization based on user psychology."""
        try:
            optimization = {
                "hook_strength": 0.7,  # Default
                "emotional_intensity": 0.6,  # Default
                "novelty_factor": 0.5,  # Default
                "completion_satisfaction": 0.6,  # Default
                "social_elements": 0.5  # Default
            }
            
            # Adjust based on user personality
            if user_profile.personality_profile.get("neuroticism", 0) > 0.6:
                optimization["emotional_intensity"] = 0.4  # Lower intensity for anxious users
                optimization["completion_satisfaction"] = 0.8  # Higher satisfaction for reassurance
            
            if user_profile.personality_profile.get("extraversion", 0) > 0.6:
                optimization["social_elements"] = 0.8  # Higher social elements for extroverts
            
            if user_profile.personality_profile.get("openness", 0) > 0.6:
                optimization["novelty_factor"] = 0.8  # Higher novelty for open users
            
            # Adjust based on behavioral tendencies
            if user_profile.behavioral_tendencies.get("addiction_prone", 0) > 0.6:
                optimization["hook_strength"] = 0.9  # Stronger hooks for addiction-prone users
                optimization["completion_satisfaction"] = 0.7  # Moderate satisfaction to maintain engagement
            
            return optimization
            
        except Exception as e:
            logger.error(f"Error suggesting content optimization: {e}")
            return {}
    
    def _suggest_timing_recommendations(self, user_profile: UserPsychologicalInsights) -> Dict[str, Any]:
        """Suggest timing recommendations based on user psychology."""
        try:
            timing = {
                "optimal_hours": [10, 14, 20, 22],  # Default peak hours
                "session_duration": 30,  # Default 30 minutes
                "content_pacing": "moderate",  # Default pacing
                "break_frequency": "low"  # Default break frequency
            }
            
            # Adjust based on personality
            if user_profile.personality_profile.get("neuroticism", 0) > 0.6:
                timing["session_duration"] = 20  # Shorter sessions for anxious users
                timing["break_frequency"] = "high"  # More breaks for anxious users
            
            if user_profile.personality_profile.get("extraversion", 0) > 0.6:
                timing["session_duration"] = 45  # Longer sessions for extroverts
                timing["content_pacing"] = "fast"  # Faster pacing for extroverts
            
            # Adjust based on addiction indicators
            if user_profile.addiction_indicators.get("overall_addiction_potential", 0) > 0.7:
                timing["session_duration"] = 60  # Longer sessions for highly addicted users
                timing["break_frequency"] = "minimal"  # Minimal breaks for addicted users
            
            return timing
            
        except Exception as e:
            logger.error(f"Error suggesting timing recommendations: {e}")
            return {}
    
    def _suggest_addiction_optimization(self, user_profile: UserPsychologicalInsights) -> Dict[str, Any]:
        """Suggest addiction optimization strategies."""
        try:
            optimization = {
                "reward_schedule": "variable_ratio",  # Most addictive
                "dopamine_intensity": 0.7,  # Default
                "withdrawal_prevention": "moderate",  # Default
                "escalation_strategy": "gradual"  # Default
            }
            
            # Adjust based on addiction indicators
            if user_profile.addiction_indicators.get("overall_addiction_potential", 0) > 0.8:
                optimization["reward_schedule"] = "variable_interval"  # More addictive for highly addicted users
                optimization["dopamine_intensity"] = 0.9  # Higher intensity
                optimization["withdrawal_prevention"] = "aggressive"  # Prevent withdrawal
                optimization["escalation_strategy"] = "rapid"  # Rapid escalation
            
            elif user_profile.addiction_indicators.get("overall_addiction_potential", 0) < 0.3:
                optimization["reward_schedule"] = "fixed_ratio"  # Less addictive for non-addicted users
                optimization["dopamine_intensity"] = 0.5  # Lower intensity
                optimization["withdrawal_prevention"] = "minimal"  # Minimal prevention
                optimization["escalation_strategy"] = "slow"  # Slow escalation
            
            return optimization
            
        except Exception as e:
            logger.error(f"Error suggesting addiction optimization: {e}")
            return {}