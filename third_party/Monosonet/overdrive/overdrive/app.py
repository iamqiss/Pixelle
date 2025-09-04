from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import logging
import time
from typing import Optional, List, Dict, Any
import uuid
from datetime import datetime

from .config import settings
from .feature_store import FeatureStore
from .features.extractors import extract_user_features, extract_item_features
from .services.user_interests import UserInterestsService
from .services.enhanced_ranking_service import EnhancedOverdriveRankingService, UserSignal
from .analytics.behavior_tracker import RealTimeBehaviorTracker
from .psychology.dopamine_engine import DopamineEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Enhanced Overdrive ML Service", version="2.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
feature_store = FeatureStore()
user_interests_service = UserInterestsService(base_url=settings.client_base_url)
enhanced_ranking_service = EnhancedOverdriveRankingService(
    feature_store, 
    user_interests_service, 
    settings.client_base_url,
    redis_url=settings.redis_url
)
behavior_tracker = RealTimeBehaviorTracker(feature_store)
dopamine_engine = DopamineEngine()

start_time_monotonic = time.monotonic()

class HealthResponse(BaseModel):
    status: str
    uptime_seconds: float
    version: str = "2.0.0"
    enhanced_features: List[str]

class FeaturesResponse(BaseModel):
    user: dict
    items: dict

class RankingRequest(BaseModel):
    user_id: str
    candidate_items: List[Dict[str, Any]]
    limit: int = 20
    use_enhanced: bool = True

class RankingResponse(BaseModel):
    user_id: str
    rankings: List[Dict[str, Any]]
    total_items: int
    ranking_method: str
    enhanced_features_used: List[str]

class UserSignalRequest(BaseModel):
    user_id: str
    signal_type: str
    content_id: Optional[str] = None
    session_id: Optional[str] = None
    duration: float = 0.0
    intensity: float = 1.0
    metadata: Optional[Dict[str, Any]] = None

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    try:
        logger.info("Starting Enhanced Overdrive services...")
        await enhanced_ranking_service.start_services()
        logger.info("Enhanced Overdrive services started successfully")
    except Exception as e:
        logger.error(f"Failed to start services: {e}")
        raise

@app.get("/")
async def root():
    return {
        "message": "Enhanced Overdrive ML Service v2.0", 
        "status": "running",
        "features": [
            "Multi-modal content analysis",
            "Advanced collaborative filtering",
            "Real-time signal processing",
            "Deep learning embeddings",
            "Adaptive recommendations",
            "Multilingual NLP (100+ languages)",
            "Video intelligence & action recognition",
            "Psychological warfare & addiction engineering"
        ]
    }

@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    uptime = time.monotonic() - start_time_monotonic
    
    # Get system health
    system_health = enhanced_ranking_service.get_system_performance()
    
    enhanced_features = [
        "Multi-modal feature extraction",
        "Collaborative filtering (Matrix Factorization, LightFM, Implicit)",
        "Real-time signal processing",
        "Deep learning embeddings",
        "Adaptive ranking",
        "Multilingual NLP (100+ languages)",
        "Video intelligence & action recognition",
        "Psychological warfare & addiction engineering"
    ]
    
    return HealthResponse(
        status="ok", 
        uptime_seconds=uptime,
        version="2.0.0",
        enhanced_features=enhanced_features
    )

@app.get("/features/{user_id}", response_model=FeaturesResponse)
async def get_user_features(user_id: str) -> FeaturesResponse:
    """Get user features from the feature store."""
    try:
        user_features = feature_store.get_user_features(user_id)
        if not user_features:
            user_features = {}
        
        # Get some sample item features for demonstration
        item_features = {}
        # TODO: Implement item features retrieval
        
        return FeaturesResponse(user=user_features, items=item_features)
    except Exception as e:
        logger.error(f"Error fetching features for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/features/items/{item_id}")
async def get_item_features(item_id: str):
    """Get item features from the feature store."""
    try:
        features = feature_store.get_item_features(item_id)
        if features:
            return {"item_id": item_id, "features": features}
        else:
            raise HTTPException(status_code=404, detail="Item features not found")
    except Exception as e:
        logger.error(f"Error fetching features for item {item_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/rank/for-you", response_model=RankingResponse)
async def rank_for_you(request: RankingRequest, authorization: Optional[str] = Header(None)):
    """Enhanced ranking for the "For You" feed."""
    try:
        user_id = request.user_id
        candidate_items = request.candidate_items
        limit = request.limit
        use_enhanced = request.use_enhanced
        
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")
        
        if not candidate_items:
            raise HTTPException(status_code=400, detail="candidate_items is required")
        
        # Extract auth token
        auth_token = None
        if authorization and authorization.startswith("Bearer "):
            auth_token = authorization[7:]
        
        if use_enhanced:
            # Use enhanced ranking service
            enhanced_results = await enhanced_ranking_service.rank_for_you_enhanced(
                user_id=user_id,
                candidate_items=candidate_items,
                limit=limit,
                auth_token=auth_token
            )
            
            # Convert to response format
            rankings = []
            for result in enhanced_results:
                rankings.append({
                    "content_id": result.content_id,
                    "score": result.final_score,
                    "confidence": result.confidence,
                    "explanation": result.explanation,
                    "feature_scores": result.feature_scores,
                    "collaborative_scores": result.collaborative_scores,
                    "real_time_boost": result.real_time_boost,
                    "metadata": result.metadata
                })
            
            enhanced_features_used = [
                "Multi-modal content analysis",
                "Collaborative filtering",
                "Real-time signal processing",
                "Deep learning embeddings"
            ]
            
            ranking_method = "enhanced_overdrive"
            
        else:
            # Fallback to basic ranking
            rankings = await enhanced_ranking_service.rank_for_you_enhanced(
                user_id=user_id,
                candidate_items=candidate_items,
                limit=limit,
                auth_token=auth_token
            )
            
            enhanced_features_used = ["Basic ranking"]
            ranking_method = "basic_overdrive"
        
        return RankingResponse(
            user_id=user_id,
            rankings=rankings,
            total_items=len(rankings),
            ranking_method=ranking_method,
            enhanced_features_used=enhanced_features_used
        )
        
    except Exception as e:
        logger.error(f"Error in rank_for_you: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/rank/for-you/enhanced", response_model=RankingResponse)
async def rank_for_you_enhanced(request: RankingRequest, authorization: Optional[str] = Header(None)):
    """Enhanced ranking with all advanced features."""
    try:
        user_id = request.user_id
        candidate_items = request.candidate_items
        limit = request.limit
        
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")
        
        if not candidate_items:
            raise HTTPException(status_code=400, detail="candidate_items is required")
        
        # Extract auth token
        auth_token = None
        if authorization and authorization.startswith("Bearer "):
            auth_token = authorization[7:]
        
        # Use enhanced ranking service
        enhanced_results = await enhanced_ranking_service.rank_for_you_enhanced(
            user_id=user_id,
            candidate_items=candidate_items,
            limit=limit,
            auth_token=auth_token
        )
        
        # Convert to response format
        rankings = []
        for result in enhanced_results:
            rankings.append({
                "content_id": result.content_id,
                "score": result.final_score,
                "confidence": result.confidence,
                "explanation": result.explanation,
                "feature_scores": result.feature_scores,
                "collaborative_scores": result.collaborative_scores,
                "real_time_boost": result.real_time_boost,
                "metadata": result.metadata
            })
        
        enhanced_features_used = [
            "Multi-modal content analysis",
            "Collaborative filtering (Matrix Factorization, LightFM, Implicit)",
            "Real-time signal processing",
            "Deep learning embeddings",
            "Adaptive ranking",
            "User behavior modeling",
            "Multilingual NLP (100+ languages)",
            "Video intelligence & action recognition",
            "Psychological warfare & addiction engineering"
        ]
        
        return RankingResponse(
            user_id=user_id,
            rankings=rankings,
            total_items=len(rankings),
            ranking_method="enhanced_overdrive_v2",
            enhanced_features_used=enhanced_features_used
        )
        
    except Exception as e:
        logger.error(f"Error in enhanced ranking: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/signals/process")
async def process_user_signal(request: UserSignalRequest):
    """Process a user signal for real-time adaptation."""
    try:
        # Create user signal
        signal = UserSignal(
            signal_id=str(uuid.uuid4()),
            user_id=request.user_id,
            signal_type=request.signal_type,
            timestamp=datetime.utcnow(),
            content_id=request.content_id,
            session_id=request.session_id,
            duration=request.duration,
            intensity=request.intensity,
            metadata=request.metadata
        )
        
        # Process signal
        await enhanced_ranking_service.process_user_signal(signal)
        
        return {
            "status": "processed",
            "signal_id": signal.signal_id,
            "message": f"Signal {request.signal_type} processed for user {request.user_id}"
        }
        
    except Exception as e:
        logger.error(f"Error processing user signal: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/insights/{user_id}")
async def get_user_insights(user_id: str, authorization: Optional[str] = Header(None)):
    """Get enhanced insights about user's ranking preferences."""
    try:
        # Extract auth token
        auth_token = None
        if authorization and authorization.startswith("Bearer "):
            auth_token = authorization[7:]
        
        insights = enhanced_ranking_service.get_ranking_insights(user_id)
        return {"user_id": user_id, "insights": insights}
        
    except Exception as e:
        logger.error(f"Error getting insights for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/interests/{user_id}")
async def get_user_interests(user_id: str, authorization: Optional[str] = Header(None)):
    """Get user interests."""
    try:
        # Extract auth token
        auth_token = None
        if authorization and authorization.startswith("Bearer "):
            auth_token = authorization[7:]
        
        if not auth_token:
            raise HTTPException(status_code=401, detail="Authorization required")
        
        interests = await user_interests_service.get_user_interests(user_id, auth_token)
        return {"user_id": user_id, "interests": interests}
        
    except Exception as e:
        logger.error(f"Error getting interests for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/performance/system")
async def get_system_performance():
    """Get overall system performance metrics."""
    try:
        performance = enhanced_ranking_service.get_system_performance()
        return performance
        
    except Exception as e:
        logger.error(f"Error getting system performance: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Analytics Endpoints
# Psychological Warfare Endpoints
@app.post("/psychology/profile/create")
async def create_psychological_profile(request: Dict[str, Any]):
    """Create a psychological profile for a user."""
    try:
        user_id = request.get("user_id")
        initial_data = request.get("initial_data", {})
        
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")
        
        profile = await dopamine_engine.create_user_psychological_profile(user_id, initial_data)
        
        return {
            "user_id": user_id,
            "profile_created": True,
            "dopamine_sensitivity": profile.dopamine_sensitivity,
            "reward_threshold": profile.reward_threshold,
            "addiction_potential": "high" if profile.dopamine_sensitivity > 0.7 else "medium" if profile.dopamine_sensitivity > 0.4 else "low"
        }
        
    except Exception as e:
        logger.error(f"Error creating psychological profile: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/psychology/dopamine/calculate")
async def calculate_dopamine_potential(request: Dict[str, Any]):
    """Calculate dopamine potential for content."""
    try:
        user_id = request.get("user_id")
        content_id = request.get("content_id")
        content_features = request.get("content_features", {})
        
        if not user_id or not content_id:
            raise HTTPException(status_code=400, detail="user_id and content_id are required")
        
        potential = await dopamine_engine.calculate_dopamine_potential(user_id, content_id, content_features)
        
        return {
            "user_id": user_id,
            "content_id": content_id,
            "dopamine_potential": potential,
            "addiction_risk": "high" if potential > 0.8 else "medium" if potential > 0.6 else "low"
        }
        
    except Exception as e:
        logger.error(f"Error calculating dopamine potential: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/psychology/dopamine/hit")
async def generate_dopamine_hit(request: Dict[str, Any]):
    """Generate a dopamine hit for user interaction."""
    try:
        user_id = request.get("user_id")
        content_id = request.get("content_id")
        interaction_type = request.get("interaction_type")
        
        if not user_id or not content_id or not interaction_type:
            raise HTTPException(status_code=400, detail="user_id, content_id, and interaction_type are required")
        
        hit = await dopamine_engine.generate_dopamine_hit(user_id, content_id, interaction_type)
        
        if hit:
            return {
                "hit_generated": True,
                "hit_type": hit.hit_type,
                "hit_strength": hit.hit_strength,
                "engagement_level": hit.engagement_level,
                "addiction_escalation": "active"
            }
        else:
            return {
                "hit_generated": False,
                "reason": "User not eligible for dopamine hit",
                "addiction_escalation": "stable"
            }
        
    except Exception as e:
        logger.error(f"Error generating dopamine hit: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/psychology/profile/{user_id}")
async def get_psychological_profile(user_id: str):
    """Get user's psychological profile."""
    try:
        if user_id not in dopamine_engine.user_profiles:
            raise HTTPException(status_code=404, detail="Psychological profile not found")
        
        profile = dopamine_engine.user_profiles[user_id]
        
        return {
            "user_id": user_id,
            "dopamine_sensitivity": profile.dopamine_sensitivity,
            "reward_threshold": profile.reward_threshold,
            "attention_span": profile.attention_span,
            "novelty_seeking": profile.novelty_seeking,
            "social_validation_need": profile.social_validation_need,
            "addiction_level": "high" if profile.dopamine_sensitivity > 0.7 else "medium" if profile.dopamine_sensitivity > 0.4 else "low"
        }
        
    except Exception as e:
        logger.error(f"Error getting psychological profile: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/psychology/warfare/status")
async def get_psychological_warfare_status():
    """Get psychological warfare system status."""
    try:
        total_profiles = len(dopamine_engine.user_profiles)
        total_hits = sum(len(hits) for hits in dopamine_engine.dopamine_history.values())
        
        return {
            "system_status": "active",
            "total_profiles": total_profiles,
            "total_dopamine_hits": total_hits,
            "addiction_optimization": "enabled",
            "psychological_manipulation": "active",
            "warning": "This system is designed to be psychologically addictive!"
        }
        
    except Exception as e:
        logger.error(f"Error getting warfare status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/analytics/session/start")
async def start_session(request: Dict[str, Any]):
    """Start tracking a user session."""
    try:
        user_id = request.get("user_id")
        platform = request.get("platform", "unknown")
        device_info = request.get("device_info", {})
        
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")
        
        session_id = await behavior_tracker.track_session_start(user_id, platform, device_info)
        return {"session_id": session_id, "status": "started"}
        
    except Exception as e:
        logger.error(f"Error starting session: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/analytics/session/end")
async def end_session(request: Dict[str, Any]):
    """End tracking a user session."""
    try:
        session_id = request.get("session_id")
        exit_reason = request.get("exit_reason", "user_exit")
        
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id is required")
        
        await behavior_tracker.track_session_end(session_id, exit_reason)
        return {"status": "ended"}
        
    except Exception as e:
        logger.error(f"Error ending session: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/analytics/content/view")
async def track_content_view(request: Dict[str, Any]):
    """Track content view."""
    try:
        user_id = request.get("user_id")
        content_id = request.get("content_id")
        session_id = request.get("session_id")
        duration_seconds = request.get("duration_seconds", 0.0)
        scroll_position = request.get("scroll_position")
        device_info = request.get("device_info", {})
        
        if not user_id or not content_id:
            raise HTTPException(status_code=400, detail="user_id and content_id are required")
        
        await behavior_tracker.track_content_view(
            user_id, content_id, session_id, duration_seconds, scroll_position, device_info
        )
        return {"status": "tracked"}
        
    except Exception as e:
        logger.error(f"Error tracking content view: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/analytics/engagement")
async def track_engagement(request: Dict[str, Any]):
    """Track user engagement."""
    try:
        user_id = request.get("user_id")
        content_id = request.get("content_id")
        interaction_type = request.get("interaction_type")
        session_id = request.get("session_id")
        device_info = request.get("device_info", {})
        
        if not user_id or not content_id or not interaction_type:
            raise HTTPException(status_code=400, detail="user_id, content_id, and interaction_type are required")
        
        await behavior_tracker.track_engagement(
            user_id, content_id, interaction_type, session_id, device_info
        )
        return {"status": "tracked"}
        
    except Exception as e:
        logger.error(f"Error tracking engagement: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/analytics/user/{user_id}")
async def get_user_analytics(user_id: str, time_window: str = "24h"):
    """Get user analytics."""
    try:
        analytics = await behavior_tracker.get_user_analytics(user_id, time_window)
        return {"user_id": user_id, "analytics": analytics}
        
    except Exception as e:
        logger.error(f"Error getting analytics for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/analytics/user/{user_id}/realtime")
async def get_real_time_insights(user_id: str):
    """Get real-time insights for a user."""
    try:
        insights = await behavior_tracker.get_real_time_insights(user_id)
        return {"user_id": user_id, "insights": insights}
        
    except Exception as e:
        logger.error(f"Error getting real-time insights for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/analytics/system")
async def get_system_analytics():
    """Get system-wide analytics."""
    try:
        analytics = await behavior_tracker.get_system_analytics()
        return analytics
        
    except Exception as e:
        logger.error(f"Error getting system analytics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/metrics")
async def metrics() -> str:
    # Placeholder for Prometheus text format
    return "# HELP overdrive_up 1 if server is up\n# TYPE overdrive_up gauge\noverdrive_up 1\n"

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
    await enhanced_ranking_service.stop_services()
    dopamine_engine.close()

def run() -> None:
    """Run the FastAPI server."""
    uvicorn.run(app, host="0.0.0.0", port=settings.service_port)