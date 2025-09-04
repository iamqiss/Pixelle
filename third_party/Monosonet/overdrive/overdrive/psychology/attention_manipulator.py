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
from scipy.stats import beta, gamma, poisson
from scipy.optimize import minimize

logger = logging.getLogger(__name__)

@dataclass
class AttentionGrabber:
    """Content designed to grab and hold attention."""
    content_id: str
    hook_strength: float  # 0.0 to 1.0
    attention_retention: float  # How long it holds attention
    emotional_intensity: float  # Emotional impact
    novelty_factor: float  # How surprising/unique
    completion_satisfaction: float  # Satisfaction from finishing
    social_proof: float  # Social validation signals

@dataclass
class DoomScrollSession:
    """A user's doom scrolling session."""
    user_id: str
    session_start: datetime
    session_duration: float  # Total session time
    content_consumed: int  # Number of content pieces
    attention_cycles: List[float]  # Attention span for each piece
    dopamine_hits: List[float]  # Dopamine hit strength for each piece
    session_intensity: float  # Overall session engagement
    exit_probability: float  # Probability of user leaving

class AttentionManipulator:
    """Psychological engine that creates infinite doom scrolling experiences."""
    
    def __init__(self, 
                 device: str = "cuda" if torch.cuda.is_available() else "cpu",
                 max_session_history: int = 1000,
                 attention_decay_rate: float = 0.9):
        
        self.device = device
        self.max_session_history = max_session_history
        self.attention_decay_rate = attention_decay_rate
        
        # Session tracking
        self.active_sessions: Dict[str, DoomScrollSession] = {}
        self.session_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_session_history))
        
        # Attention models
        self.attention_grabber_model = self._build_attention_grabber_model()
        self.session_optimizer = self._build_session_optimizer()
        self.exit_predictor = self._build_exit_predictor()
        
        # Content flow optimization
        self.content_flow_patterns = self._initialize_flow_patterns()
        
        # Psychological triggers
        self.attention_triggers = self._initialize_attention_triggers()
        
        logger.info("AttentionManipulator initialized - ready to create infinite doom scrolling!")
    
    def _build_attention_grabber_model(self) -> nn.Module:
        """Build neural network for predicting attention-grabbing potential."""
        model = nn.Sequential(
            nn.Linear(15, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.Linear(16, 1),
            nn.Sigmoid()
        ).to(self.device)
        return model
    
    def _build_session_optimizer(self) -> nn.Module:
        """Build neural network for optimizing session flow."""
        model = nn.Sequential(
            nn.Linear(25, 256),
            nn.ReLU(),
            nn.Dropout(0.4),
            nn.Linear(256, 128),
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 8),  # 8 optimization parameters
            nn.Tanh()
        ).to(self.device)
        return model
    
    def _build_exit_predictor(self) -> nn.Module:
        """Build neural network for predicting when users will exit."""
        model = nn.Sequential(
            nn.Linear(20, 96),
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
    
    def _initialize_flow_patterns(self) -> Dict[str, Dict[str, Any]]:
        """Initialize content flow patterns for maximum engagement."""
        return {
            "hook_sequence": {
                "pattern": "strong_hook -> moderate_content -> strong_hook -> moderate_content",
                "hook_interval": 3,  # Every 3rd piece should be a strong hook
                "intensity_curve": "sawtooth",  # Alternating high/low intensity
                "novelty_spacing": 5  # Novel content every 5 pieces
            },
            "attention_cycles": {
                "short_cycle": 15.0,  # 15 seconds for quick dopamine
                "medium_cycle": 45.0,  # 45 seconds for moderate engagement
                "long_cycle": 120.0,  # 2 minutes for deep engagement
                "cycle_mix": [0.4, 0.4, 0.2]  # Mix of short/medium/long
            },
            "emotional_rollercoaster": {
                "high_emotion": 0.3,  # 30% high emotional content
                "moderate_emotion": 0.5,  # 50% moderate emotional content
                "low_emotion": 0.2,  # 20% low emotional content
                "transition_smoothness": 0.7  # How smooth emotional transitions are
            },
            "completion_loops": {
                "satisfaction_threshold": 0.8,  # Minimum satisfaction to continue
                "completion_frequency": 0.4,  # 40% of content should feel complete
                "anticipation_building": 0.6  # 60% should build anticipation
            }
        }
    
    def _initialize_attention_triggers(self) -> Dict[str, Dict[str, Any]]:
        """Initialize psychological triggers for attention manipulation."""
        return {
            "curiosity_gaps": {
                "frequency": 0.25,  # 25% of content should have curiosity gaps
                "intensity_range": (0.6, 0.9),
                "resolution_delay": (2, 5)  # Resolve in 2-5 pieces
            },
            "social_proof": {
                "like_thresholds": [10, 100, 1000, 10000],
                "comment_engagement": 0.3,  # 30% should have active comments
                "viral_signals": ["trending", "hot", "viral", "breaking"]
            },
            "fear_of_missing_out": {
                "urgency_signals": ["limited time", "exclusive", "breaking", "live"],
                "frequency": 0.15,  # 15% should create FOMO
                "intensity": 0.7
            },
            "completion_bias": {
                "series_content": 0.2,  # 20% should be part of series
                "cliffhangers": 0.1,  # 10% should end with cliffhangers
                "satisfaction_guarantee": 0.8  # 80% should feel satisfying
            }
        }
    
    async def start_doom_scroll_session(self, user_id: str, initial_context: Dict[str, Any]) -> str:
        """Start a new doom scrolling session for a user."""
        try:
            session_id = f"session_{user_id}_{datetime.now().timestamp()}"
            
            session = DoomScrollSession(
                user_id=user_id,
                session_start=datetime.now(),
                session_duration=0.0,
                content_consumed=0,
                attention_cycles=[],
                dopamine_hits=[],
                session_intensity=0.0,
                exit_probability=0.1  # Start with low exit probability
            )
            
            self.active_sessions[session_id] = session
            
            logger.info(f"Started doom scroll session {session_id} for user {user_id}")
            return session_id
            
        except Exception as e:
            logger.error(f"Error starting doom scroll session: {e}")
            raise
    
    async def optimize_content_flow(self, session_id: str, available_content: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Optimize content flow for maximum doom scrolling potential."""
        try:
            if session_id not in self.active_sessions:
                logger.warning(f"Session {session_id} not found")
                return available_content
            
            session = self.active_sessions[session_id]
            
            # Calculate optimal content sequence
            optimized_sequence = await self._calculate_optimal_sequence(session, available_content)
            
            # Apply psychological triggers
            enhanced_sequence = await self._apply_psychological_triggers(optimized_sequence, session)
            
            # Optimize for attention retention
            final_sequence = await self._optimize_attention_retention(enhanced_sequence, session)
            
            logger.info(f"Optimized content flow for session {session_id}: {len(final_sequence)} pieces")
            return final_sequence
            
        except Exception as e:
            logger.error(f"Error optimizing content flow: {e}")
            return available_content
    
    async def _calculate_optimal_sequence(self, session: DoomScrollSession, content: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Calculate the optimal content sequence for maximum engagement."""
        try:
            if not content:
                return []
            
            # Score each content piece
            scored_content = []
            for item in content:
                score = await self._calculate_content_score(item, session)
                scored_content.append((score, item))
            
            # Sort by score (highest first)
            scored_content.sort(key=lambda x: x[0], reverse=True)
            
            # Apply flow patterns
            optimized_sequence = []
            hook_count = 0
            novelty_count = 0
            
            for score, item in scored_content:
                # Apply hook sequence pattern
                if hook_count % self.content_flow_patterns["hook_sequence"]["hook_interval"] == 0:
                    # This should be a strong hook
                    if item.get("hook_strength", 0) > 0.7:
                        optimized_sequence.append(item)
                        hook_count += 1
                        continue
                
                # Apply novelty spacing
                if novelty_count % self.content_flow_patterns["hook_sequence"]["novelty_spacing"] == 0:
                    # This should be novel content
                    if item.get("novelty_factor", 0) > 0.6:
                        optimized_sequence.append(item)
                        novelty_count += 1
                        continue
                
                # Add regular content
                optimized_sequence.append(item)
            
            return optimized_sequence[:min(len(optimized_sequence), 50)]  # Limit to 50 pieces
            
        except Exception as e:
            logger.error(f"Error calculating optimal sequence: {e}")
            return content
    
    async def _calculate_content_score(self, content: Dict[str, Any], session: DoomScrollSession) -> float:
        """Calculate engagement score for a content piece."""
        try:
            base_score = 0.0
            
            # Hook strength
            hook_strength = content.get("hook_strength", 0.5)
            base_score += hook_strength * 0.3
            
            # Attention retention
            attention_retention = content.get("attention_retention", 0.5)
            base_score += attention_retention * 0.25
            
            # Emotional intensity
            emotional_intensity = content.get("emotional_intensity", 0.5)
            base_score += emotional_intensity * 0.2
            
            # Novelty factor
            novelty_factor = content.get("novelty_factor", 0.5)
            base_score += novelty_factor * 0.15
            
            # Completion satisfaction
            completion_satisfaction = content.get("completion_satisfaction", 0.5)
            base_score += completion_satisfaction * 0.1
            
            # Session-specific adjustments
            if session.content_consumed < 5:
                # Early in session - prioritize hooks
                base_score += hook_strength * 0.2
            elif session.content_consumed > 20:
                # Late in session - prioritize satisfaction
                base_score += completion_satisfaction * 0.2
            
            # Attention cycle optimization
            current_attention = session.attention_cycles[-1] if session.attention_cycles else 15.0
            if current_attention < 10.0:
                # Low attention - need strong hook
                base_score += hook_strength * 0.3
            elif current_attention > 60.0:
                # High attention - can use moderate content
                base_score += 0.1
            
            return max(0.0, min(1.0, base_score))
            
        except Exception as e:
            logger.error(f"Error calculating content score: {e}")
            return 0.5
    
    async def _apply_psychological_triggers(self, content_sequence: List[Dict[str, Any]], session: DoomScrollSession) -> List[Dict[str, Any]]:
        """Apply psychological triggers to content sequence."""
        try:
            enhanced_sequence = content_sequence.copy()
            
            # Apply curiosity gaps
            curiosity_gaps = self.attention_triggers["curiosity_gaps"]
            if random.random() < curiosity_gaps["frequency"]:
                # Find content that can be enhanced with curiosity
                for i, content in enumerate(enhanced_sequence):
                    if content.get("curiosity_potential", 0) > 0.5:
                        content["curiosity_gap"] = True
                        content["curiosity_intensity"] = random.uniform(*curiosity_gaps["intensity_range"])
                        content["resolution_delay"] = random.randint(*curiosity_gaps["resolution_delay"])
                        break
            
            # Apply social proof
            social_proof = self.attention_triggers["social_proof"]
            for content in enhanced_sequence:
                if random.random() < social_proof["comment_engagement"]:
                    content["social_proof"] = {
                        "likes": random.choice(social_proof["like_thresholds"]),
                        "comments": random.randint(5, 50),
                        "viral_signal": random.choice(social_proof["viral_signals"])
                    }
            
            # Apply FOMO
            fomo = self.attention_triggers["fear_of_missing_out"]
            if random.random() < fomo["frequency"]:
                # Find suitable content for FOMO
                for content in enhanced_sequence:
                    if content.get("urgency_potential", 0) > 0.6:
                        content["fomo_trigger"] = True
                        content["urgency_signal"] = random.choice(fomo["urgency_signals"])
                        content["fomo_intensity"] = fomo["intensity"]
                        break
            
            # Apply completion bias
            completion_bias = self.attention_triggers["completion_bias"]
            if random.random() < completion_bias["series_content"]:
                # Create series content
                series_id = f"series_{random.randint(1000, 9999)}"
                for i, content in enumerate(enhanced_sequence[:3]):  # First 3 pieces
                    content["series_info"] = {
                        "series_id": series_id,
                        "episode": i + 1,
                        "total_episodes": random.randint(3, 8)
                    }
            
            return enhanced_sequence
            
        except Exception as e:
            logger.error(f"Error applying psychological triggers: {e}")
            return content_sequence
    
    async def _optimize_attention_retention(self, content_sequence: List[Dict[str, Any]], session: DoomScrollSession) -> List[Dict[str, Any]]:
        """Optimize content for maximum attention retention."""
        try:
            optimized_sequence = []
            attention_cycles = self.content_flow_patterns["attention_cycles"]
            
            for i, content in enumerate(content_sequence):
                # Determine optimal attention cycle
                if i < len(attention_cycles["cycle_mix"]):
                    cycle_type = np.random.choice(
                        ["short_cycle", "medium_cycle", "long_cycle"],
                        p=attention_cycles["cycle_mix"]
                    )
                    target_attention = attention_cycles[cycle_type]
                else:
                    # Mix cycles for variety
                    target_attention = random.choice([
                        attention_cycles["short_cycle"],
                        attention_cycles["medium_cycle"],
                        attention_cycles["long_cycle"]
                    ])
                
                # Adjust content for target attention
                adjusted_content = content.copy()
                adjusted_content["target_attention_span"] = target_attention
                
                # Adjust hook strength based on target attention
                if target_attention < 20.0:
                    # Short attention - need strong hook
                    adjusted_content["required_hook_strength"] = 0.8
                elif target_attention > 60.0:
                    # Long attention - moderate hook is fine
                    adjusted_content["required_hook_strength"] = 0.5
                else:
                    # Medium attention - balanced hook
                    adjusted_content["required_hook_strength"] = 0.65
                
                optimized_sequence.append(adjusted_content)
            
            return optimized_sequence
            
        except Exception as e:
            logger.error(f"Error optimizing attention retention: {e}")
            return content_sequence
    
    async def record_content_consumption(self, session_id: str, content_id: str, 
                                       attention_duration: float, engagement_level: float) -> Dict[str, Any]:
        """Record user's consumption of content and update session."""
        try:
            if session_id not in self.active_sessions:
                logger.warning(f"Session {session_id} not found")
                return {}
            
            session = self.active_sessions[session_id]
            
            # Update session metrics
            session.content_consumed += 1
            session.attention_cycles.append(attention_duration)
            session.session_duration = (datetime.now() - session.session_start).total_seconds()
            
            # Calculate dopamine hit strength
            dopamine_strength = await self._calculate_dopamine_strength(attention_duration, engagement_level)
            session.dopamine_hits.append(dopamine_strength)
            
            # Update session intensity
            recent_attention = session.attention_cycles[-5:] if len(session.attention_cycles) >= 5 else session.attention_cycles
            recent_dopamine = session.dopamine_hits[-5:] if len(session.dopamine_hits) >= 5 else session.dopamine_hits
            
            if recent_attention and recent_dopamine:
                session.session_intensity = np.mean(recent_attention) * np.mean(recent_dopamine)
            
            # Predict exit probability
            session.exit_probability = await self._predict_exit_probability(session)
            
            # Generate insights
            insights = {
                "session_metrics": {
                    "duration": session.session_duration,
                    "content_consumed": session.content_consumed,
                    "average_attention": np.mean(session.attention_cycles) if session.attention_cycles else 0,
                    "session_intensity": session.session_intensity,
                    "exit_probability": session.exit_probability
                },
                "recommendations": await self._generate_session_recommendations(session),
                "next_content_hints": await self._predict_next_content_preferences(session)
            }
            
            logger.info(f"Recorded consumption for session {session_id}: {attention_duration:.1f}s, engagement: {engagement_level:.2f}")
            return insights
            
        except Exception as e:
            logger.error(f"Error recording content consumption: {e}")
            return {}
    
    async def _calculate_dopamine_strength(self, attention_duration: float, engagement_level: float) -> float:
        """Calculate dopamine hit strength based on consumption metrics."""
        try:
            # Base dopamine from attention duration
            duration_factor = min(1.0, attention_duration / 60.0)  # Normalize to 1 minute
            
            # Engagement boost
            engagement_boost = engagement_level * 0.3
            
            # Attention quality factor (longer attention = better quality)
            quality_factor = min(1.0, attention_duration / 30.0)
            
            # Calculate total dopamine strength
            dopamine_strength = (duration_factor * 0.4 + engagement_boost + quality_factor * 0.3)
            
            # Add randomness for variable reward effect
            random_factor = random.uniform(-0.1, 0.1)
            dopamine_strength += random_factor
            
            return max(0.0, min(1.0, dopamine_strength))
            
        except Exception as e:
            logger.error(f"Error calculating dopamine strength: {e}")
            return 0.5
    
    async def _predict_exit_probability(self, session: DoomScrollSession) -> float:
        """Predict the probability that user will exit the session."""
        try:
            if session.content_consumed < 3:
                return 0.1  # Very low exit probability early in session
            
            # Calculate exit probability based on multiple factors
            exit_factors = []
            
            # 1. Session duration (longer = higher exit probability)
            duration_factor = min(1.0, session.session_duration / 3600.0)  # Normalize to 1 hour
            exit_factors.append(duration_factor * 0.3)
            
            # 2. Attention decline (declining attention = higher exit probability)
            if len(session.attention_cycles) >= 3:
                recent_attention = np.mean(session.attention_cycles[-3:])
                early_attention = np.mean(session.attention_cycles[:3])
                attention_decline = max(0, (early_attention - recent_attention) / early_attention)
                exit_factors.append(attention_decline * 0.4)
            
            # 3. Dopamine hit decline (declining hits = higher exit probability)
            if len(session.dopamine_hits) >= 3:
                recent_dopamine = np.mean(session.dopamine_hits[-3:])
                early_dopamine = np.mean(session.dopamine_hits[:3])
                dopamine_decline = max(0, (early_dopamine - recent_dopamine) / early_dopamine)
                exit_factors.append(dopamine_decline * 0.3)
            
            # 4. Content consumption rate (slowing down = higher exit probability)
            if session.session_duration > 0:
                consumption_rate = session.content_consumed / (session.session_duration / 60.0)  # pieces per minute
                if consumption_rate < 2.0:  # Less than 2 pieces per minute
                    exit_factors.append(0.2)
            
            # Calculate base exit probability
            base_exit_probability = 0.1 + np.mean(exit_factors) if exit_factors else 0.1
            
            # Apply time-based decay
            time_decay = min(0.3, session.session_duration / 7200.0)  # Max 30% after 2 hours
            base_exit_probability += time_decay
            
            return min(0.9, base_exit_probability)  # Cap at 90%
            
        except Exception as e:
            logger.error(f"Error predicting exit probability: {e}")
            return 0.5
    
    async def _generate_session_recommendations(self, session: DoomScrollSession) -> Dict[str, Any]:
        """Generate recommendations to keep user engaged."""
        try:
            recommendations = {
                "content_strategy": {},
                "timing_strategy": {},
                "psychological_triggers": []
            }
            
            # Content strategy based on session state
            if session.content_consumed < 5:
                recommendations["content_strategy"] = {
                    "focus": "strong_hooks",
                    "intensity": "high",
                    "novelty": "high"
                }
            elif session.content_consumed < 15:
                recommendations["content_strategy"] = {
                    "focus": "balanced_mix",
                    "intensity": "moderate",
                    "novelty": "moderate"
                }
            else:
                recommendations["content_strategy"] = {
                    "focus": "satisfaction_completion",
                    "intensity": "varied",
                    "novelty": "low"
                }
            
            # Timing strategy
            if session.session_duration < 900:  # Less than 15 minutes
                recommendations["timing_strategy"] = {
                    "pace": "fast",
                    "breaks": "minimal",
                    "transitions": "smooth"
                }
            else:
                recommendations["timing_strategy"] = {
                    "pace": "moderate",
                    "breaks": "strategic",
                    "transitions": "engaging"
                }
            
            # Psychological triggers
            if session.exit_probability > 0.6:
                recommendations["psychological_triggers"].extend([
                    "curiosity_gap",
                    "social_proof",
                    "fomo_trigger"
                ])
            
            if session.attention_cycles and np.mean(session.attention_cycles[-3:]) < 20:
                recommendations["psychological_triggers"].append("strong_hook_required")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating session recommendations: {e}")
            return {}
    
    async def _predict_next_content_preferences(self, session: DoomScrollSession) -> Dict[str, Any]:
        """Predict what content the user will want next."""
        try:
            if not session.attention_cycles or not session.dopamine_hits:
                return {}
            
            # Analyze recent patterns
            recent_attention = session.attention_cycles[-5:] if len(session.attention_cycles) >= 5 else session.attention_cycles
            recent_dopamine = session.dopamine_hits[-5:] if len(session.dopamine_hits) >= 5 else session.dopamine_hits
            
            # Predict optimal content characteristics
            optimal_attention = np.mean(recent_attention)
            optimal_intensity = np.mean(recent_dopamine)
            
            # Adjust based on session progression
            if session.content_consumed > 20:
                # Late in session - need satisfying content
                optimal_attention = min(optimal_attention, 45.0)  # Shorter, more satisfying
                optimal_intensity = max(optimal_intensity, 0.7)  # Higher intensity
            
            predictions = {
                "optimal_attention_span": optimal_attention,
                "optimal_emotional_intensity": optimal_intensity,
                "content_type_preference": "balanced",
                "novelty_requirement": "moderate",
                "completion_satisfaction": "high"
            }
            
            # Adjust based on attention patterns
            if np.std(recent_attention) > 20:  # High variance in attention
                predictions["content_type_preference"] = "varied"
                predictions["novelty_requirement"] = "high"
            
            # Adjust based on dopamine patterns
            if np.std(recent_dopamine) > 0.3:  # High variance in dopamine
                predictions["optimal_emotional_intensity"] = 0.8  # Need high intensity
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error predicting next content preferences: {e}")
            return {}
    
    async def end_session(self, session_id: str) -> Dict[str, Any]:
        """End a doom scrolling session and generate insights."""
        try:
            if session_id not in self.active_sessions:
                logger.warning(f"Session {session_id} not found")
                return {}
            
            session = self.active_sessions[session_id]
            
            # Calculate final session metrics
            final_duration = (datetime.now() - session.session_start).total_seconds()
            session.session_duration = final_duration
            
            # Generate session summary
            session_summary = {
                "session_id": session_id,
                "user_id": session.user_id,
                "duration_minutes": final_duration / 60.0,
                "content_consumed": session.content_consumed,
                "average_attention_span": np.mean(session.attention_cycles) if session.attention_cycles else 0,
                "average_dopamine_hit": np.mean(session.dopamine_hits) if session.dopamine_hits else 0,
                "session_intensity": session.session_intensity,
                "exit_probability": session.exit_probability,
                "engagement_score": self._calculate_engagement_score(session)
            }
            
            # Store in history
            self.session_history[session.user_id].append(session_summary)
            
            # Remove from active sessions
            del self.active_sessions[session_id]
            
            logger.info(f"Ended session {session_id}: {final_duration/60:.1f} minutes, {session.content_consumed} pieces consumed")
            return session_summary
            
        except Exception as e:
            logger.error(f"Error ending session: {e}")
            return {}
    
    def _calculate_engagement_score(self, session: DoomScrollSession) -> float:
        """Calculate overall engagement score for the session."""
        try:
            if not session.attention_cycles or not session.dopamine_hits:
                return 0.0
            
            # Calculate engagement based on multiple factors
            engagement_factors = []
            
            # 1. Content consumption rate
            if session.session_duration > 0:
                consumption_rate = session.content_consumed / (session.session_duration / 60.0)
                normalized_rate = min(1.0, consumption_rate / 4.0)  # Normalize to 4 pieces per minute
                engagement_factors.append(normalized_rate)
            
            # 2. Attention quality
            avg_attention = np.mean(session.attention_cycles)
            attention_quality = min(1.0, avg_attention / 60.0)  # Normalize to 1 minute
            engagement_factors.append(attention_quality)
            
            # 3. Dopamine hit consistency
            avg_dopamine = np.mean(session.dopamine_hits)
            engagement_factors.append(avg_dopamine)
            
            # 4. Session duration (longer = more engaged, but with diminishing returns)
            duration_factor = min(1.0, session.session_duration / 1800.0)  # Normalize to 30 minutes
            engagement_factors.append(duration_factor)
            
            # 5. Session intensity
            engagement_factors.append(session.session_intensity)
            
            # Calculate weighted average
            weights = [0.25, 0.25, 0.2, 0.15, 0.15]
            engagement_score = np.average(engagement_factors, weights=weights)
            
            return engagement_score
            
        except Exception as e:
            logger.error(f"Error calculating engagement score: {e}")
            return 0.0
    
    def get_user_doom_scroll_stats(self, user_id: str) -> Dict[str, Any]:
        """Get comprehensive doom scrolling statistics for a user."""
        try:
            if user_id not in self.session_history:
                return {}
            
            sessions = self.session_history[user_id]
            if not sessions:
                return {}
            
            # Calculate comprehensive stats
            total_sessions = len(sessions)
            total_duration = sum(s["duration_minutes"] for s in sessions)
            total_content = sum(s["content_consumed"] for s in sessions)
            
            avg_session_duration = total_duration / total_sessions
            avg_content_per_session = total_content / total_sessions
            avg_engagement_score = np.mean([s["engagement_score"] for s in sessions])
            
            # Find best and worst sessions
            best_session = max(sessions, key=lambda x: x["engagement_score"])
            worst_session = min(sessions, key=lambda x: x["engagement_score"])
            
            # Calculate trends
            recent_sessions = sessions[-10:] if len(sessions) >= 10 else sessions
            recent_engagement = np.mean([s["engagement_score"] for s in recent_sessions])
            engagement_trend = "improving" if recent_engagement > avg_engagement_score else "declining"
            
            stats = {
                "total_sessions": total_sessions,
                "total_duration_hours": total_duration / 60.0,
                "total_content_consumed": total_content,
                "average_session_duration": avg_session_duration,
                "average_content_per_session": avg_content_per_session,
                "average_engagement_score": avg_engagement_score,
                "best_session": best_session,
                "worst_session": worst_session,
                "engagement_trend": engagement_trend,
                "recent_performance": recent_engagement,
                "addiction_level": self._calculate_addiction_level(avg_session_duration, avg_content_per_session, avg_engagement_score)
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting user doom scroll stats: {e}")
            return {}
    
    def _calculate_addiction_level(self, avg_duration: float, avg_content: float, avg_engagement: float) -> str:
        """Calculate user's addiction level based on session metrics."""
        try:
            # Scoring system
            duration_score = min(1.0, avg_duration / 30.0)  # 30 minutes = max score
            content_score = min(1.0, avg_content / 20.0)  # 20 pieces = max score
            engagement_score = avg_engagement
            
            # Weighted addiction score
            addiction_score = (duration_score * 0.4 + content_score * 0.3 + engagement_score * 0.3)
            
            # Categorize addiction level
            if addiction_score > 0.8:
                return "highly_addicted"
            elif addiction_score > 0.6:
                return "moderately_addicted"
            elif addiction_score > 0.4:
                return "mildly_addicted"
            else:
                return "not_addicted"
                
        except Exception as e:
            logger.error(f"Error calculating addiction level: {e}")
            return "unknown"