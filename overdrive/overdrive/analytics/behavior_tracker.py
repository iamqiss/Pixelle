from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple
import json
import time
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict, deque
import logging
from dataclasses import dataclass, asdict

from ..feature_store import FeatureStore

logger = logging.getLogger(__name__)

@dataclass
class UserSession:
    """Track user session data."""
    session_id: str
    user_id: str
    start_time: datetime
    last_activity: datetime
    platform: str
    device_info: Dict[str, Any]
    total_items_viewed: int = 0
    total_engagement_actions: int = 0
    session_duration_seconds: float = 0.0
    scroll_depth: int = 0
    exit_reason: Optional[str] = None

@dataclass
class ContentInteraction:
    """Track individual content interactions."""
    interaction_id: str
    user_id: str
    content_id: str
    timestamp: datetime
    interaction_type: str  # view, like, share, comment, follow, etc.
    duration_seconds: float = 0.0
    scroll_position: Optional[int] = None
    device_info: Optional[Dict[str, Any]] = None
    session_id: Optional[str] = None

@dataclass
class EngagementMetrics:
    """Aggregated engagement metrics."""
    user_id: str
    time_window: str  # 1h, 24h, 7d, 30d
    total_interactions: int
    unique_content_viewed: int
    total_session_time: float
    avg_session_duration: float
    engagement_rate: float
    content_preferences: Dict[str, float]
    platform_usage: Dict[str, int]
    peak_activity_hours: List[int]

class RealTimeBehaviorTracker:
    """Real-time user behavior analytics system."""
    
    def __init__(self, feature_store: FeatureStore):
        self.feature_store = feature_store
        
        # In-memory tracking for real-time analytics
        self.active_sessions: Dict[str, UserSession] = {}
        self.user_sessions: Dict[str, List[UserSession]] = defaultdict(list)
        self.content_interactions: Dict[str, List[ContentInteraction]] = defaultdict(list)
        
        # Real-time metrics
        self.hourly_metrics: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
        self.daily_metrics: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # Engagement patterns
        self.engagement_patterns: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        
        # Content affinity tracking
        self.content_affinity: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        
        # Session analytics
        self.session_analytics: Dict[str, Dict[str, Any]] = defaultdict(dict)
    
    async def track_session_start(self, user_id: str, platform: str, 
                                device_info: Dict[str, Any]) -> str:
        """Track when a user starts a new session."""
        session_id = f"{user_id}_{int(time.time())}"
        
        session = UserSession(
            session_id=session_id,
            user_id=user_id,
            start_time=datetime.utcnow(),
            last_activity=datetime.utcnow(),
            platform=platform,
            device_info=device_info
        )
        
        self.active_sessions[session_id] = session
        self.user_sessions[user_id].append(session)
        
        # Update real-time metrics
        current_hour = datetime.utcnow().hour
        self.hourly_metrics[user_id][current_hour] += 1
        
        logger.info(f"Session started: {session_id} for user {user_id}")
        return session_id
    
    async def track_session_end(self, session_id: str, exit_reason: str = "user_exit"):
        """Track when a user ends their session."""
        if session_id not in self.active_sessions:
            return
        
        session = self.active_sessions[session_id]
        session.exit_reason = exit_reason
        session.last_activity = datetime.utcnow()
        session.session_duration_seconds = (
            session.last_activity - session.start_time
        ).total_seconds()
        
        # Calculate session analytics
        self._calculate_session_analytics(session)
        
        # Store session data
        await self._store_session_data(session)
        
        # Remove from active sessions
        del self.active_sessions[session_id]
        
        logger.info(f"Session ended: {session_id}, duration: {session.session_duration_seconds:.1f}s")
    
    async def track_content_view(self, user_id: str, content_id: str, 
                               session_id: Optional[str] = None,
                               duration_seconds: float = 0.0,
                               scroll_position: Optional[int] = None,
                               device_info: Optional[Dict[str, Any]] = None):
        """Track when a user views content."""
        interaction = ContentInteraction(
            interaction_id=f"{user_id}_{content_id}_{int(time.time())}",
            user_id=user_id,
            content_id=content_id,
            timestamp=datetime.utcnow(),
            interaction_type="view",
            duration_seconds=duration_seconds,
            scroll_position=scroll_position,
            device_info=device_info,
            session_id=session_id
        )
        
        self.content_interactions[user_id].append(interaction)
        
        # Update active session
        if session_id and session_id in self.active_sessions:
            session = self.active_sessions[session_id]
            session.total_items_viewed += 1
            session.last_activity = datetime.utcnow()
        
        # Update content affinity
        self.content_affinity[user_id][content_id] = self.content_affinity[user_id].get(content_id, 0) + 1
        
        # Update real-time metrics
        current_hour = datetime.utcnow().hour
        self.hourly_metrics[user_id][current_hour] += 1
        
        logger.debug(f"Content view tracked: {user_id} -> {content_id}")
    
    async def track_engagement(self, user_id: str, content_id: str, 
                             interaction_type: str, session_id: Optional[str] = None,
                             device_info: Optional[Dict[str, Any]] = None):
        """Track user engagement actions (like, share, comment, follow)."""
        interaction = ContentInteraction(
            interaction_id=f"{user_id}_{content_id}_{interaction_type}_{int(time.time())}",
            user_id=user_id,
            content_id=content_id,
            timestamp=datetime.utcnow(),
            interaction_type=interaction_type,
            device_info=device_info,
            session_id=session_id
        )
        
        self.content_interactions[user_id].append(interaction)
        
        # Update engagement patterns
        self.engagement_patterns[user_id][interaction_type] += 1
        
        # Update active session
        if session_id and session_id in self.active_sessions:
            session = self.active_sessions[session_id]
            session.total_engagement_actions += 1
            session.last_activity = datetime.utcnow()
        
        # Update content affinity (engagement is worth more than views)
        engagement_weight = self._get_engagement_weight(interaction_type)
        self.content_affinity[user_id][content_id] = (
            self.content_affinity[user_id].get(content_id, 0) + engagement_weight
        )
        
        logger.info(f"Engagement tracked: {user_id} {interaction_type} on {content_id}")
    
    async def track_scroll_behavior(self, user_id: str, session_id: str, 
                                  scroll_depth: int, scroll_direction: str):
        """Track user scrolling behavior."""
        if session_id in self.active_sessions:
            session = self.active_sessions[session_id]
            session.scroll_depth = max(session.scroll_depth, scroll_depth)
            session.last_activity = datetime.utcnow()
        
        # Store scroll analytics
        scroll_data = {
            "user_id": user_id,
            "session_id": session_id,
            "scroll_depth": scroll_depth,
            "scroll_direction": scroll_direction,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self._store_scroll_data(scroll_data)
    
    async def get_user_analytics(self, user_id: str, time_window: str = "24h") -> EngagementMetrics:
        """Get comprehensive user analytics for a time window."""
        end_time = datetime.utcnow()
        
        if time_window == "1h":
            start_time = end_time - timedelta(hours=1)
        elif time_window == "24h":
            start_time = end_time - timedelta(days=1)
        elif time_window == "7d":
            start_time = end_time - timedelta(days=7)
        elif time_window == "30d":
            start_time = end_time - timedelta(days=30)
        else:
            start_time = end_time - timedelta(days=1)
        
        # Filter interactions by time window
        recent_interactions = [
            interaction for interaction in self.content_interactions[user_id]
            if start_time <= interaction.timestamp <= end_time
        ]
        
        # Filter sessions by time window
        recent_sessions = [
            session for session in self.user_sessions[user_id]
            if start_time <= session.start_time <= end_time
        ]
        
        # Calculate metrics
        total_interactions = len(recent_interactions)
        unique_content = len(set(interaction.content_id for interaction in recent_interactions))
        total_session_time = sum(session.session_duration_seconds for session in recent_sessions)
        avg_session_duration = total_session_time / len(recent_sessions) if recent_sessions else 0
        
        # Calculate engagement rate
        engagement_actions = sum(
            1 for interaction in recent_interactions
            if interaction.interaction_type != "view"
        )
        engagement_rate = engagement_actions / total_interactions if total_interactions > 0 else 0
        
        # Content preferences
        content_preferences = self._calculate_content_preferences(user_id, recent_interactions)
        
        # Platform usage
        platform_usage = defaultdict(int)
        for session in recent_sessions:
            platform_usage[session.platform] += 1
        
        # Peak activity hours
        peak_hours = self._calculate_peak_activity_hours(user_id, time_window)
        
        return EngagementMetrics(
            user_id=user_id,
            time_window=time_window,
            total_interactions=total_interactions,
            unique_content_viewed=unique_content,
            total_session_time=total_session_time,
            avg_session_duration=avg_session_duration,
            engagement_rate=engagement_rate,
            content_preferences=dict(content_preferences),
            platform_usage=dict(platform_usage),
            peak_activity_hours=peak_hours
        )
    
    async def get_real_time_insights(self, user_id: str) -> Dict[str, Any]:
        """Get real-time insights for a user."""
        current_hour = datetime.utcnow().hour
        
        # Active session info
        active_session = None
        for session in self.active_sessions.values():
            if session.user_id == user_id:
                active_session = session
                break
        
        # Current hour activity
        current_hour_activity = self.hourly_metrics[user_id].get(current_hour, 0)
        
        # Recent engagement patterns
        recent_engagement = dict(self.engagement_patterns[user_id])
        
        # Content affinity (top content)
        top_content = sorted(
            self.content_affinity[user_id].items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        insights = {
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat(),
            "active_session": asdict(active_session) if active_session else None,
            "current_hour_activity": current_hour_activity,
            "recent_engagement": recent_engagement,
            "top_content_affinity": top_content,
            "session_count_today": len([
                s for s in self.user_sessions[user_id]
                if s.start_time.date() == datetime.utcnow().date()
            ])
        }
        
        return insights
    
    async def get_system_analytics(self) -> Dict[str, Any]:
        """Get system-wide analytics."""
        total_active_users = len(set(session.user_id for session in self.active_sessions.values()))
        total_sessions_today = sum(
            1 for sessions in self.user_sessions.values()
            for session in sessions
            if session.start_time.date() == datetime.utcnow().date()
        )
        
        # Platform distribution
        platform_distribution = defaultdict(int)
        for session in self.active_sessions.values():
            platform_distribution[session.platform] += 1
        
        # Engagement distribution
        engagement_distribution = defaultdict(int)
        for user_patterns in self.engagement_patterns.values():
            for interaction_type, count in user_patterns.items():
                engagement_distribution[interaction_type] += count
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_active_users": total_active_users,
            "total_sessions_today": total_sessions_today,
            "platform_distribution": dict(platform_distribution),
            "engagement_distribution": dict(engagement_distribution),
            "active_sessions": len(self.active_sessions)
        }
    
    def _calculate_session_analytics(self, session: UserSession):
        """Calculate analytics for a completed session."""
        analytics = {
            "session_id": session.session_id,
            "user_id": session.user_id,
            "duration_seconds": session.session_duration_seconds,
            "items_viewed": session.total_items_viewed,
            "engagement_actions": session.total_engagement_actions,
            "engagement_rate": (
                session.total_engagement_actions / session.total_items_viewed
                if session.total_items_viewed > 0 else 0
            ),
            "scroll_depth": session.scroll_depth,
            "platform": session.platform,
            "exit_reason": session.exit_reason
        }
        
        self.session_analytics[session.session_id] = analytics
    
    def _calculate_content_preferences(self, user_id: str, 
                                    interactions: List[ContentInteraction]) -> Dict[str, float]:
        """Calculate user content preferences based on interactions."""
        preferences = defaultdict(float)
        
        for interaction in interactions:
            # Different interaction types have different weights
            weight = self._get_engagement_weight(interaction.interaction_type)
            preferences[interaction.content_id] += weight
        
        # Normalize preferences
        total_weight = sum(preferences.values())
        if total_weight > 0:
            preferences = {k: v / total_weight for k, v in preferences.items()}
        
        return preferences
    
    def _calculate_peak_activity_hours(self, user_id: str, time_window: str) -> List[int]:
        """Calculate peak activity hours for a user."""
        if time_window == "24h":
            hours = range(24)
        else:
            # For longer periods, use daily averages
            hours = range(24)
        
        # Get activity for each hour
        hour_activity = [self.hourly_metrics[user_id].get(hour, 0) for hour in hours]
        
        # Find peak hours (top 3)
        peak_indices = sorted(range(len(hour_activity)), key=lambda i: hour_activity[i], reverse=True)[:3]
        return [hours[i] for i in peak_indices]
    
    def _get_engagement_weight(self, interaction_type: str) -> float:
        """Get weight for different engagement types."""
        weights = {
            "view": 1.0,
            "like": 2.0,
            "share": 3.0,
            "comment": 2.5,
            "follow": 4.0,
            "bookmark": 2.0,
            "report": -1.0
        }
        return weights.get(interaction_type, 1.0)
    
    async def _store_session_data(self, session: UserSession):
        """Store session data in feature store."""
        try:
            session_data = asdict(session)
            session_data["start_time"] = session.start_time.isoformat()
            session_data["last_activity"] = session.last_activity.isoformat()
            
            # Store in feature store
            await self.feature_store.set_user_features(
                f"{session.user_id}_session_{session.session_id}",
                session_data
            )
        except Exception as e:
            logger.error(f"Failed to store session data: {e}")
    
    async def _store_scroll_data(self, scroll_data: Dict[str, Any]):
        """Store scroll behavior data."""
        try:
            await self.feature_store.set_user_features(
                f"{scroll_data['user_id']}_scroll_{scroll_data['session_id']}",
                scroll_data
            )
        except Exception as e:
            logger.error(f"Failed to store scroll data: {e}")
    
    async def cleanup_old_data(self, max_age_hours: int = 24):
        """Clean up old analytics data."""
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        
        # Clean up old interactions
        for user_id in list(self.content_interactions.keys()):
            self.content_interactions[user_id] = [
                interaction for interaction in self.content_interactions[user_id]
                if interaction.timestamp > cutoff_time
            ]
        
        # Clean up old sessions
        for user_id in list(self.user_sessions.keys()):
            self.user_sessions[user_id] = [
                session for session in self.user_sessions[user_id]
                if session.start_time > cutoff_time
            ]
        
        # Clean up old metrics
        for user_id in list(self.hourly_metrics.keys()):
            # Keep only recent hours
            current_hour = datetime.utcnow().hour
            for hour in range(24):
                if hour < current_hour - max_age_hours:
                    self.hourly_metrics[user_id].pop(hour, None)
        
        logger.info(f"Cleaned up analytics data older than {max_age_hours} hours")