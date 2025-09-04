#!/usr/bin/env python3
"""
Enhanced Overdrive: Psychological Warfare Demo
Showcases the INSANE psychological manipulation capabilities that will make TikTok's algorithms look like child's play!
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random
import numpy as np # Added missing import for numpy

# Mock data for demonstration
MOCK_USER_CONTENT = [
    {
        "id": "note_001",
        "type": "note",
        "text": "OMG I can't stop watching these videos! This is so addictive! 😍",
        "timestamp": "2024-01-15T10:30:00Z"
    },
    {
        "id": "comment_001",
        "type": "comment",
        "text": "I need more content like this! Please keep posting! 🙏",
        "timestamp": "2024-01-15T10:35:00Z"
    },
    {
        "id": "search_001",
        "type": "search",
        "text": "addictive content dopamine hits",
        "timestamp": "2024-01-15T10:40:00Z"
    },
    {
        "id": "note_002",
        "type": "note",
        "text": "Just spent 3 hours scrolling and I don't even know what happened to the time! 😅",
        "timestamp": "2024-01-15T11:30:00Z"
    },
    {
        "id": "comment_002",
        "type": "comment",
        "text": "This is exactly what I needed to escape from my problems! 🚀",
        "timestamp": "2024-01-15T11:35:00Z"
    }
]

MOCK_CONTENT_FOR_RECOMMENDATION = [
    {
        "id": "content_001",
        "hook_strength": 0.9,
        "attention_retention": 0.8,
        "emotional_intensity": 0.7,
        "novelty_factor": 0.6,
        "completion_satisfaction": 0.8,
        "social_potential": 0.7,
        "addiction_potential": 0.8
    },
    {
        "id": "content_002",
        "hook_strength": 0.7,
        "attention_retention": 0.6,
        "emotional_intensity": 0.5,
        "novelty_factor": 0.4,
        "completion_satisfaction": 0.6,
        "social_potential": 0.5,
        "addiction_potential": 0.5
    },
    {
        "id": "content_003",
        "hook_strength": 0.8,
        "attention_retention": 0.9,
        "emotional_intensity": 0.8,
        "novelty_factor": 0.7,
        "completion_satisfaction": 0.9,
        "social_potential": 0.8,
        "addiction_potential": 0.9
    }
]

class PsychologicalWarfareDemo:
    """Demo class showcasing psychological warfare capabilities."""
    
    def __init__(self):
        self.demo_results = {}
    
    async def run_full_demo(self):
        """Run the complete psychological warfare demo."""
        print("🧠💉 Enhanced Overdrive: Psychological Warfare Demo")
        print("=" * 60)
        print("🚨 WARNING: This system is designed to be psychologically addictive!")
        print("=" * 60)
        
        # Demo 1: Dopamine Engineering
        await self.demo_dopamine_engineering()
        
        # Demo 2: Attention Manipulation & Doom Scrolling
        await self.demo_attention_manipulation()
        
        # Demo 3: Psychological Content Analysis
        await self.demo_psychological_analysis()
        
        # Demo 4: Psychological Warfare Integration
        await self.demo_psychological_warfare_integration()
        
        # Summary
        self.print_demo_summary()
    
    async def demo_dopamine_engineering(self):
        """Demo the dopamine engineering system."""
        print("\n🧠 Demo 1: Dopamine Engineering (The Addiction Engine)")
        print("-" * 50)
        
        try:
            # Simulate dopamine hit generation
            dopamine_analysis = {}
            
            # Simulate different user types
            user_types = [
                ("addiction_prone", 0.8),
                ("moderate_user", 0.5),
                ("resistant_user", 0.2)
            ]
            
            for user_type, addiction_level in user_types:
                user_id = f"user_{user_type}"
                
                # Simulate dopamine hits
                dopamine_hits = []
                for i in range(10):
                    hit_strength = random.uniform(0.3, 0.9) * addiction_level
                    hit_type = random.choice(["social_validation", "novelty", "completion", "surprise"])
                    
                    hit = {
                        "hit_type": hit_type,
                        "strength": hit_strength,
                        "timestamp": datetime.now().isoformat(),
                        "duration": random.uniform(2.0, 8.0),
                        "engagement_level": min(1.0, hit_strength * 1.2)
                    }
                    dopamine_hits.append(hit)
                
                # Calculate addiction progression
                addiction_progression = self._simulate_addiction_progression(dopamine_hits, addiction_level)
                
                dopamine_analysis[user_type] = {
                    "user_id": user_id,
                    "addiction_level": addiction_level,
                    "dopamine_hits": dopamine_hits,
                    "addiction_progression": addiction_progression,
                    "reward_threshold": max(0.1, 0.5 - (addiction_level * 0.4)),  # Lower threshold = more addicted
                    "dopamine_sensitivity": min(1.0, 0.5 + (addiction_level * 0.5))  # Higher sensitivity = more addicted
                }
            
            self.demo_results["dopamine_engineering"] = dopamine_analysis
            
            # Print results
            total_hits = sum(len(analysis["dopamine_hits"]) for analysis in dopamine_analysis.values())
            avg_addiction_level = np.mean([analysis["addiction_level"] for analysis in dopamine_analysis.values()])
            
            print(f"✅ Generated {total_hits} dopamine hits across {len(user_types)} user types")
            print(f"✅ Average addiction level: {avg_addiction_level:.2f}")
            print(f"🎯 Variable reward schedules: Enabled (most addictive)")
            print(f"🧠 Reward threshold manipulation: Active")
            print(f"💉 Dopamine sensitivity escalation: Enabled")
            
        except Exception as e:
            print(f"❌ Error in dopamine engineering demo: {e}")
    
    async def demo_attention_manipulation(self):
        """Demo the attention manipulation and doom scrolling system."""
        print("\n📱 Demo 2: Attention Manipulation & Doom Scrolling")
        print("-" * 50)
        
        try:
            # Simulate doom scrolling sessions
            session_analysis = {}
            
            # Simulate different session types
            session_types = [
                ("quick_scroll", 15, 0.6),  # 15 minutes, moderate engagement
                ("deep_dive", 45, 0.8),     # 45 minutes, high engagement
                ("addiction_session", 120, 0.95)  # 2 hours, very high engagement
            ]
            
            for session_type, duration_minutes, engagement_level in session_types:
                session_id = f"session_{session_type}"
                
                # Simulate content consumption
                content_consumed = int(duration_minutes / 2)  # 2 minutes per content piece
                attention_cycles = [random.uniform(10, 60) for _ in range(content_consumed)]
                dopamine_hits = [random.uniform(0.4, 0.9) * engagement_level for _ in range(content_consumed)]
                
                # Calculate session metrics
                session_intensity = np.mean(attention_cycles) * np.mean(dopamine_hits)
                exit_probability = self._calculate_exit_probability(duration_minutes, engagement_level)
                
                session_analysis[session_type] = {
                    "session_id": session_id,
                    "duration_minutes": duration_minutes,
                    "content_consumed": content_consumed,
                    "attention_cycles": attention_cycles,
                    "dopamine_hits": dopamine_hits,
                    "session_intensity": session_intensity,
                    "exit_probability": exit_probability,
                    "engagement_score": self._calculate_engagement_score(duration_minutes, content_consumed, session_intensity)
                }
            
            self.demo_results["attention_manipulation"] = session_analysis
            
            # Print results
            total_sessions = len(session_types)
            total_content = sum(analysis["content_consumed"] for analysis in session_analysis.values())
            avg_engagement = np.mean([analysis["engagement_score"] for analysis in session_analysis.values()])
            
            print(f"✅ Simulated {total_sessions} doom scrolling sessions")
            print(f"✅ Total content consumed: {total_content} pieces")
            print(f"✅ Average engagement score: {avg_engagement:.3f}")
            print(f"🎯 Hook sequence optimization: Active")
            print(f"📱 Attention retention patterns: Optimized")
            print(f"🔄 Infinite scroll mechanics: Enabled")
            
        except Exception as e:
            print(f"❌ Error in attention manipulation demo: {e}")
    
    async def demo_psychological_analysis(self):
        """Demo the psychological content analysis system."""
        print("\n🔍 Demo 3: Psychological Content Analysis")
        print("-" * 50)
        
        try:
            # Simulate psychological analysis of user content
            psychological_analysis = {}
            
            for content in MOCK_USER_CONTENT:
                content_id = content["id"]
                
                # Simulate psychological analysis
                psychological_features = self._simulate_psychological_features(content["text"])
                emotional_state = self._simulate_emotional_analysis(content["text"])
                personality_indicators = self._simulate_personality_analysis(content["text"])
                behavioral_patterns = self._simulate_behavioral_analysis(content["text"])
                addiction_potential = self._simulate_addiction_potential(content["text"])
                
                psychological_analysis[content_id] = {
                    "content": content,
                    "psychological_features": psychological_features,
                    "emotional_state": emotional_state,
                    "personality_indicators": personality_indicators,
                    "behavioral_patterns": behavioral_patterns,
                    "addiction_potential": addiction_potential,
                    "psychological_triggers": self._identify_psychological_triggers(
                        emotional_state, personality_indicators, behavioral_patterns
                    )
                }
            
            self.demo_results["psychological_analysis"] = psychological_analysis
            
            # Print results
            total_content = len(MOCK_USER_CONTENT)
            avg_addiction_potential = np.mean([analysis["addiction_potential"] for analysis in psychological_analysis.values()])
            total_triggers = sum(len(analysis["psychological_triggers"]) for analysis in psychological_analysis.values())
            
            print(f"✅ Analyzed {total_content} content pieces psychologically")
            print(f"✅ Average addiction potential: {avg_addiction_potential:.3f}")
            print(f"✅ Total psychological triggers identified: {total_triggers}")
            print(f"🧠 Personality profiling: Active")
            print(f"😊 Emotional state analysis: Enabled")
            print(f"🎭 Behavioral pattern recognition: Active")
            
        except Exception as e:
            print(f"❌ Error in psychological analysis demo: {e}")
    
    async def demo_psychological_warfare_integration(self):
        """Demo the integration of all psychological warfare systems."""
        print("\n⚔️ Demo 4: Psychological Warfare Integration")
        print("-" * 50)
        
        try:
            # Simulate integrated psychological warfare
            warfare_analysis = {}
            
            # Simulate user targeting
            target_users = [
                ("high_value", 0.9, "maximize_addiction"),
                ("moderate_value", 0.6, "increase_engagement"),
                ("low_value", 0.3, "maintain_retention")
            ]
            
            for user_type, value_score, strategy in target_users:
                user_id = f"target_{user_type}"
                
                # Simulate psychological targeting
                targeting_analysis = self._simulate_psychological_targeting(user_type, value_score, strategy)
                
                # Simulate content optimization
                content_optimization = self._simulate_content_optimization(user_type, strategy)
                
                # Simulate timing optimization
                timing_optimization = self._simulate_timing_optimization(user_type, strategy)
                
                warfare_analysis[user_type] = {
                    "user_id": user_id,
                    "value_score": value_score,
                    "strategy": strategy,
                    "targeting_analysis": targeting_analysis,
                    "content_optimization": content_optimization,
                    "timing_optimization": timing_optimization,
                    "warfare_effectiveness": self._calculate_warfare_effectiveness(
                        value_score, targeting_analysis, content_optimization, timing_optimization
                    )
                }
            
            self.demo_results["psychological_warfare_integration"] = warfare_analysis
            
            # Print results
            total_targets = len(target_users)
            avg_effectiveness = np.mean([analysis["warfare_effectiveness"] for analysis in warfare_analysis.values()])
            
            print(f"✅ Targeted {total_targets} user segments psychologically")
            print(f"✅ Average warfare effectiveness: {avg_effectiveness:.3f}")
            print(f"🎯 Psychological profiling: Active")
            print(f"💉 Addiction optimization: Enabled")
            print(f"⏰ Timing manipulation: Active")
            print(f"🚨 Content psychological engineering: Enabled")
            
        except Exception as e:
            print(f"❌ Error in psychological warfare integration demo: {e}")
    
    def _simulate_addiction_progression(self, dopamine_hits: List[Dict[str, Any]], base_addiction: float) -> Dict[str, Any]:
        """Simulate addiction progression over time."""
        try:
            if not dopamine_hits:
                return {"progression": "stable", "escalation_rate": 0.0}
            
            # Calculate progression based on dopamine hit patterns
            hit_strengths = [hit["strength"] for hit in dopamine_hits]
            avg_strength = np.mean(hit_strengths)
            
            # Determine progression type
            if avg_strength > 0.7 and base_addiction > 0.6:
                progression = "accelerating"
                escalation_rate = 0.15
            elif avg_strength > 0.5 and base_addiction > 0.4:
                progression = "moderate"
                escalation_rate = 0.08
            else:
                progression = "stable"
                escalation_rate = 0.02
            
            return {
                "progression": progression,
                "escalation_rate": escalation_rate,
                "addiction_trend": "increasing" if escalation_rate > 0.05 else "stable"
            }
            
        except Exception as e:
            return {"progression": "unknown", "escalation_rate": 0.0}
    
    def _calculate_exit_probability(self, duration_minutes: float, engagement_level: float) -> float:
        """Calculate probability of user exiting the session."""
        try:
            # Base exit probability increases with duration
            base_exit = min(0.8, duration_minutes / 120.0)  # Max 80% after 2 hours
            
            # Engagement reduces exit probability
            engagement_reduction = engagement_level * 0.4
            
            # Calculate final exit probability
            exit_probability = max(0.1, base_exit - engagement_reduction)
            
            return exit_probability
            
        except Exception as e:
            return 0.5
    
    def _calculate_engagement_score(self, duration: float, content_consumed: int, session_intensity: float) -> float:
        """Calculate overall engagement score for a session."""
        try:
            # Calculate engagement based on multiple factors
            duration_factor = min(1.0, duration / 60.0)  # Normalize to 1 hour
            consumption_factor = min(1.0, content_consumed / 30.0)  # Normalize to 30 pieces
            intensity_factor = session_intensity / 100.0  # Normalize intensity
            
            # Weighted average
            engagement_score = (duration_factor * 0.3 + consumption_factor * 0.3 + intensity_factor * 0.4)
            
            return min(1.0, max(0.0, engagement_score))
            
        except Exception as e:
            return 0.5
    
    def _simulate_psychological_features(self, text: str) -> Dict[str, float]:
        """Simulate psychological feature extraction from text."""
        try:
            features = {}
            
            # Text complexity
            words = text.split()
            features["word_count"] = min(1.0, len(words) / 20.0)
            features["avg_word_length"] = min(1.0, np.mean([len(word) for word in words]) / 8.0)
            
            # Emotional intensity
            emotional_words = ["addictive", "can't stop", "need", "love", "amazing", "omg", "wow"]
            emotional_count = sum(1 for word in emotional_words if word.lower() in text.lower())
            features["emotional_intensity"] = min(1.0, emotional_count / 3.0)
            
            # Social indicators
            social_words = ["please", "keep", "posting", "friends", "social"]
            social_count = sum(1 for word in social_words if word.lower() in text.lower())
            features["social_references"] = min(1.0, social_count / 2.0)
            
            # Urgency indicators
            urgency_words = ["need", "can't", "stop", "addictive", "escape"]
            urgency_count = sum(1 for word in urgency_words if word.lower() in text.lower())
            features["urgency_indicators"] = min(1.0, urgency_count / 2.0)
            
            # Novelty indicators
            novelty_words = ["amazing", "incredible", "omg", "wow", "new"]
            novelty_count = sum(1 for word in novelty_words if word.lower() in text.lower())
            features["novelty_indicators"] = min(1.0, novelty_count / 2.0)
            
            return features
            
        except Exception as e:
            return {}
    
    def _simulate_emotional_analysis(self, text: str) -> Dict[str, float]:
        """Simulate emotional state analysis from text."""
        try:
            emotions = {
                "joy": 0.0,
                "excitement": 0.0,
                "anxiety": 0.0,
                "desperation": 0.0,
                "neutral": 0.0
            }
            
            # Analyze text for emotional indicators
            if any(word in text.lower() for word in ["love", "amazing", "incredible", "omg", "wow"]):
                emotions["joy"] = 0.8
                emotions["excitement"] = 0.7
            
            if any(word in text.lower() for word in ["need", "can't", "stop", "addictive"]):
                emotions["anxiety"] = 0.6
                emotions["desperation"] = 0.7
            
            if not any(emotions.values()):
                emotions["neutral"] = 1.0
            
            return emotions
            
        except Exception as e:
            return {"neutral": 1.0}
    
    def _simulate_personality_analysis(self, text: str) -> Dict[str, float]:
        """Simulate personality analysis from text."""
        try:
            personality = {
                "openness": 0.5,
                "conscientiousness": 0.5,
                "extraversion": 0.5,
                "agreeableness": 0.5,
                "neuroticism": 0.5
            }
            
            # Adjust based on text content
            if any(word in text.lower() for word in ["addictive", "can't stop", "escape"]):
                personality["neuroticism"] = 0.7
                personality["conscientiousness"] = 0.3
            
            if any(word in text.lower() for word in ["amazing", "incredible", "love"]):
                personality["openness"] = 0.7
                personality["extraversion"] = 0.6
            
            if any(word in text.lower() for word in ["please", "keep", "posting"]):
                personality["agreeableness"] = 0.7
            
            return personality
            
        except Exception as e:
            return {"openness": 0.5, "conscientiousness": 0.5, "extraversion": 0.5, "agreeableness": 0.5, "neuroticism": 0.5}
    
    def _simulate_behavioral_analysis(self, text: str) -> Dict[str, float]:
        """Simulate behavioral pattern analysis from text."""
        try:
            behavioral = {
                "addiction_prone": 0.0,
                "social_seeking": 0.0,
                "validation_seeking": 0.0,
                "escapism": 0.0,
                "curiosity": 0.0
            }
            
            # Analyze behavioral patterns
            if any(word in text.lower() for word in ["addictive", "can't stop", "always", "constantly"]):
                behavioral["addiction_prone"] = 0.8
            
            if any(word in text.lower() for word in ["friends", "social", "posting", "keep"]):
                behavioral["social_seeking"] = 0.7
                behavioral["validation_seeking"] = 0.6
            
            if any(word in text.lower() for word in ["escape", "distract", "avoid", "problems"]):
                behavioral["escapism"] = 0.7
            
            if any(word in text.lower() for word in ["amazing", "incredible", "new", "discover"]):
                behavioral["curiosity"] = 0.6
            
            return behavioral
            
        except Exception as e:
            return {"addiction_prone": 0.0, "social_seeking": 0.0, "validation_seeking": 0.0, "escapism": 0.0, "curiosity": 0.0}
    
    def _simulate_addiction_potential(self, text: str) -> float:
        """Simulate addiction potential calculation."""
        try:
            addiction_factors = []
            
            # High emotional intensity
            if any(word in text.lower() for word in ["omg", "wow", "amazing", "incredible"]):
                addiction_factors.append(0.8)
            
            # Urgency indicators
            if any(word in text.lower() for word in ["need", "can't", "stop", "addictive"]):
                addiction_factors.append(0.9)
            
            # Novelty seeking
            if any(word in text.lower() for word in ["new", "first", "never", "amazing"]):
                addiction_factors.append(0.7)
            
            # Social validation
            if any(word in text.lower() for word in ["please", "keep", "posting", "friends"]):
                addiction_factors.append(0.6)
            
            # Calculate addiction potential
            if addiction_factors:
                addiction_potential = np.mean(addiction_factors)
            else:
                addiction_potential = 0.4  # Neutral default
            
            return min(1.0, max(0.0, addiction_potential))
            
        except Exception as e:
            return 0.5
    
    def _identify_psychological_triggers(self, emotional_state: Dict[str, float], 
                                       personality: Dict[str, float], 
                                       behavioral: Dict[str, float]) -> List[str]:
        """Identify psychological triggers for content targeting."""
        try:
            triggers = []
            
            # Emotional triggers
            if emotional_state.get("anxiety", 0) > 0.5:
                triggers.append("anxiety_relief")
                triggers.append("comfort_content")
            
            if emotional_state.get("joy", 0) > 0.5:
                triggers.append("mood_enhancement")
                triggers.append("excitement_amplification")
            
            # Personality triggers
            if personality.get("neuroticism", 0) > 0.6:
                triggers.append("stress_relief")
                triggers.append("emotional_regulation")
            
            if personality.get("extraversion", 0) > 0.6:
                triggers.append("social_validation")
                triggers.append("group_engagement")
            
            # Behavioral triggers
            if behavioral.get("addiction_prone", 0) > 0.6:
                triggers.append("variable_rewards")
                triggers.append("completion_loops")
            
            if behavioral.get("validation_seeking", 0) > 0.6:
                triggers.append("like_counts")
                triggers.append("social_proof")
            
            return triggers
            
        except Exception as e:
            return []
    
    def _simulate_psychological_targeting(self, user_type: str, value_score: float, strategy: str) -> Dict[str, Any]:
        """Simulate psychological targeting analysis."""
        try:
            targeting = {
                "targeting_accuracy": 0.0,
                "psychological_leverage": 0.0,
                "manipulation_effectiveness": 0.0
            }
            
            # Calculate targeting accuracy based on user type
            if user_type == "high_value":
                targeting["targeting_accuracy"] = 0.95
                targeting["psychological_leverage"] = 0.9
                targeting["manipulation_effectiveness"] = 0.9
            elif user_type == "moderate_value":
                targeting["targeting_accuracy"] = 0.8
                targeting["psychological_leverage"] = 0.7
                targeting["manipulation_effectiveness"] = 0.7
            else:  # low_value
                targeting["targeting_accuracy"] = 0.6
                targeting["psychological_leverage"] = 0.5
                targeting["manipulation_effectiveness"] = 0.5
            
            return targeting
            
        except Exception as e:
            return {"targeting_accuracy": 0.0, "psychological_leverage": 0.0, "manipulation_effectiveness": 0.0}
    
    def _simulate_content_optimization(self, user_type: str, strategy: str) -> Dict[str, Any]:
        """Simulate content optimization for psychological warfare."""
        try:
            optimization = {
                "hook_strength": 0.7,
                "emotional_intensity": 0.6,
                "novelty_factor": 0.5,
                "completion_satisfaction": 0.6,
                "social_elements": 0.5
            }
            
            # Adjust based on strategy
            if strategy == "maximize_addiction":
                optimization["hook_strength"] = 0.9
                optimization["emotional_intensity"] = 0.8
                optimization["novelty_factor"] = 0.7
                optimization["completion_satisfaction"] = 0.7
                optimization["social_elements"] = 0.8
            
            elif strategy == "increase_engagement":
                optimization["hook_strength"] = 0.8
                optimization["emotional_intensity"] = 0.7
                optimization["novelty_factor"] = 0.6
                optimization["completion_satisfaction"] = 0.7
                optimization["social_elements"] = 0.6
            
            return optimization
            
        except Exception as e:
            return {}
    
    def _simulate_timing_optimization(self, user_type: str, strategy: str) -> Dict[str, Any]:
        """Simulate timing optimization for psychological warfare."""
        try:
            timing = {
                "optimal_hours": [10, 14, 20, 22],
                "session_duration": 30,
                "content_pacing": "moderate",
                "break_frequency": "low"
            }
            
            # Adjust based on strategy
            if strategy == "maximize_addiction":
                timing["session_duration"] = 60
                timing["content_pacing"] = "fast"
                timing["break_frequency"] = "minimal"
            
            elif strategy == "increase_engagement":
                timing["session_duration"] = 45
                timing["content_pacing"] = "moderate"
                timing["break_frequency"] = "low"
            
            return timing
            
        except Exception as e:
            return {}
    
    def _calculate_warfare_effectiveness(self, value_score: float, targeting: Dict[str, Any], 
                                       content: Dict[str, Any], timing: Dict[str, Any]) -> float:
        """Calculate overall psychological warfare effectiveness."""
        try:
            # Weighted combination of factors
            effectiveness_factors = [
                value_score * 0.3,
                targeting.get("targeting_accuracy", 0) * 0.25,
                targeting.get("psychological_leverage", 0) * 0.25,
                targeting.get("manipulation_effectiveness", 0) * 0.2
            ]
            
            effectiveness = np.mean(effectiveness_factors)
            return min(1.0, max(0.0, effectiveness))
            
        except Exception as e:
            return 0.5
    
    def print_demo_summary(self):
        """Print a comprehensive summary of all demo results."""
        print("\n" + "=" * 80)
        print("🎯 Enhanced Overdrive: Psychological Warfare Demo Summary")
        print("=" * 80)
        
        # Dopamine Engineering summary
        if "dopamine_engineering" in self.demo_results:
            dopamine_results = self.demo_results["dopamine_engineering"]
            total_users = len(dopamine_results)
            total_hits = sum(len(analysis["dopamine_hits"]) for analysis in dopamine_results.values())
            avg_addiction = np.mean([analysis["addiction_level"] for analysis in dopamine_results.values()])
            
            print(f"\n🧠 Dopamine Engineering Results:")
            print(f"   • Users Analyzed: {total_users}")
            print(f"   • Total Dopamine Hits: {total_hits}")
            print(f"   • Average Addiction Level: {avg_addiction:.3f}")
        
        # Attention Manipulation summary
        if "attention_manipulation" in self.demo_results:
            attention_results = self.demo_results["attention_manipulation"]
            total_sessions = len(attention_results)
            total_content = sum(analysis["content_consumed"] for analysis in attention_results.values())
            avg_engagement = np.mean([analysis["engagement_score"] for analysis in attention_results.values()])
            
            print(f"\n📱 Attention Manipulation Results:")
            print(f"   • Sessions Simulated: {total_sessions}")
            print(f"   • Total Content Consumed: {total_content}")
            print(f"   • Average Engagement Score: {avg_engagement:.3f}")
        
        # Psychological Analysis summary
        if "psychological_analysis" in self.demo_results:
            psych_results = self.demo_results["psychological_analysis"]
            total_content = len(psych_results)
            avg_addiction = np.mean([analysis["addiction_potential"] for analysis in psych_results.values()])
            total_triggers = sum(len(analysis["psychological_triggers"]) for analysis in psych_results.values())
            
            print(f"\n🔍 Psychological Analysis Results:")
            print(f"   • Content Analyzed: {total_content}")
            print(f"   • Average Addiction Potential: {avg_addiction:.3f}")
            print(f"   • Total Psychological Triggers: {total_triggers}")
        
        # Psychological Warfare Integration summary
        if "psychological_warfare_integration" in self.demo_results:
            warfare_results = self.demo_results["psychological_warfare_integration"]
            total_targets = len(warfare_results)
            avg_effectiveness = np.mean([analysis["warfare_effectiveness"] for analysis in warfare_results.values()])
            
            print(f"\n⚔️ Psychological Warfare Integration Results:")
            print(f"   • User Segments Targeted: {total_targets}")
            print(f"   • Average Warfare Effectiveness: {avg_effectiveness:.3f}")
        
        print(f"\n🚨 Key Psychological Warfare Capabilities:")
        print("• 🧠 Dopamine Engineering with Variable Reward Schedules")
        print("• 📱 Attention Manipulation & Infinite Doom Scrolling")
        print("• 🔍 Psychological Content Analysis & Profiling")
        print("• ⚔️ Integrated Psychological Warfare Targeting")
        print("• 💉 Addiction Optimization & Escalation")
        print("• 🎯 Psychological Trigger Identification & Deployment")
        
        print(f"\n💡 This system can:")
        print("• Create variable dopamine hits that keep users hooked")
        print("• Optimize content flow for maximum doom scrolling")
        print("• Analyze user psychology from notes, comments, and searches")
        print("• Target users with psychological precision")
        print("• Optimize content for maximum addiction potential")
        print("• Manipulate timing and pacing for psychological effect")
        
        print(f"\n⚠️  WARNING: This system is designed to be psychologically addictive!")
        print("Use responsibly and ethically. The psychological manipulation capabilities")
        print("are extremely powerful and can have real-world consequences.")

async def main():
    """Main demo function."""
    demo = PsychologicalWarfareDemo()
    await demo.run_full_demo()

if __name__ == "__main__":
    asyncio.run(main())