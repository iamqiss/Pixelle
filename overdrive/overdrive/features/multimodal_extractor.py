from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple, Union
import torch
import torch.nn as nn
import torch.nn.functional as F
from transformers import (
    AutoTokenizer, AutoModel, CLIPProcessor, CLIPModel,
    WhisperProcessor, WhisperModel, pipeline
)
from sentence_transformers import SentenceTransformer
import numpy as np
import cv2
import librosa
from PIL import Image
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

@dataclass
class ContentFeatures:
    """Extracted features from content."""
    content_id: str
    content_type: str  # text, image, video, audio
    text_features: Optional[np.ndarray] = None
    image_features: Optional[np.ndarray] = None
    video_features: Optional[np.ndarray] = None
    audio_features: Optional[np.ndarray] = None
    metadata_features: Optional[np.ndarray] = None
    combined_features: Optional[np.ndarray] = None
    feature_dim: int = 0
    extraction_timestamp: Optional[datetime] = None

@dataclass
class UserBehaviorFeatures:
    """Extracted features from user behavior."""
    user_id: str
    session_features: Optional[np.ndarray] = None
    interaction_features: Optional[np.ndarray] = None
    temporal_features: Optional[np.ndarray] = None
    device_features: Optional[np.ndarray] = None
    location_features: Optional[np.ndarray] = None
    combined_features: Optional[np.ndarray] = None
    feature_dim: int = 0
    extraction_timestamp: Optional[datetime] = None

class MultiModalFeatureExtractor:
    """Advanced multi-modal feature extractor for content and user behavior."""
    
    def __init__(self, 
                 device: str = "cuda" if torch.cuda.is_available() else "cpu",
                 cache_dir: str = "./model_cache"):
        self.device = device
        self.cache_dir = cache_dir
        
        # Initialize models
        self._load_models()
        
        # Feature dimensions
        self.text_dim = 768
        self.image_dim = 512
        self.video_dim = 512
        self.audio_dim = 128
        self.behavior_dim = 256
        
        # Thread pool for async processing
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        logger.info(f"MultiModalFeatureExtractor initialized on {device}")
    
    def _load_models(self):
        """Load all required ML models."""
        try:
            # Text models
            self.text_tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
            self.text_model = SentenceTransformer("all-MiniLM-L6-v2").to(self.device)
            
            # Image models
            self.clip_processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
            self.clip_model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(self.device)
            
            # Video models (using CLIP for frame analysis)
            self.video_processor = self.clip_processor
            
            # Audio models
            self.whisper_processor = WhisperProcessor.from_pretrained("openai/whisper-base")
            self.whisper_model = WhisperModel.from_pretrained("openai/whisper-base").to(self.device)
            
            # Sentiment analysis
            self.sentiment_pipeline = pipeline("sentiment-analysis", device=0 if self.device == "cuda" else -1)
            
            logger.info("All models loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load models: {e}")
            raise
    
    async def extract_content_features(self, 
                                    content_id: str,
                                    content_type: str,
                                    content_data: Dict[str, Any]) -> ContentFeatures:
        """Extract features from content asynchronously."""
        
        features = ContentFeatures(
            content_id=content_id,
            content_type=content_type,
            extraction_timestamp=datetime.utcnow()
        )
        
        try:
            # Extract text features
            if content_data.get("text"):
                features.text_features = await self._extract_text_features(content_data["text"])
            
            # Extract image features
            if content_data.get("image_url") or content_data.get("image_data"):
                features.image_features = await self._extract_image_features(content_data)
            
            # Extract video features
            if content_data.get("video_url") or content_data.get("video_data"):
                features.video_features = await self._extract_video_features(content_data)
            
            # Extract audio features
            if content_data.get("audio_url") or content_data.get("audio_data"):
                features.audio_features = await self._extract_audio_features(content_data)
            
            # Extract metadata features
            if content_data.get("metadata"):
                features.metadata_features = await self._extract_metadata_features(content_data["metadata"])
            
            # Combine all features
            features.combined_features = await self._combine_content_features(features)
            features.feature_dim = len(features.combined_features) if features.combined_features is not None else 0
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting features for content {content_id}: {e}")
            return features
    
    async def extract_user_behavior_features(self, 
                                          user_id: str,
                                          behavior_data: Dict[str, Any]) -> UserBehaviorFeatures:
        """Extract features from user behavior data."""
        
        features = UserBehaviorFeatures(
            user_id=user_id,
            extraction_timestamp=datetime.utcnow()
        )
        
        try:
            # Extract session features
            if behavior_data.get("session_data"):
                features.session_features = await self._extract_session_features(behavior_data["session_data"])
            
            # Extract interaction features
            if behavior_data.get("interaction_data"):
                features.interaction_features = await self._extract_interaction_features(behavior_data["interaction_data"])
            
            # Extract temporal features
            if behavior_data.get("temporal_data"):
                features.temporal_features = await self._extract_temporal_features(behavior_data["temporal_data"])
            
            # Extract device features
            if behavior_data.get("device_data"):
                features.device_features = await self._extract_device_features(behavior_data["device_data"])
            
            # Extract location features
            if behavior_data.get("location_data"):
                features.location_features = await self._extract_location_features(behavior_data["location_data"])
            
            # Combine all features
            features.combined_features = await self._combine_behavior_features(features)
            features.feature_dim = len(features.combined_features) if features.combined_features is not None else 0
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting behavior features for user {user_id}: {e}")
            return features
    
    async def _extract_text_features(self, text: str) -> np.ndarray:
        """Extract features from text content."""
        try:
            # Get sentence embeddings
            embeddings = self.text_model.encode(text)
            
            # Extract additional text features
            text_length = len(text)
            word_count = len(text.split())
            avg_word_length = np.mean([len(word) for word in text.split()]) if word_count > 0 else 0
            
            # Sentiment analysis
            sentiment_result = self.sentiment_pipeline(text[:512])[0]  # Limit text length
            sentiment_score = 1.0 if sentiment_result["label"] == "POSITIVE" else -1.0
            sentiment_confidence = sentiment_result["score"]
            
            # Combine features
            features = np.concatenate([
                embeddings,
                [text_length, word_count, avg_word_length, sentiment_score, sentiment_confidence]
            ])
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting text features: {e}")
            return np.zeros(self.text_dim + 5)
    
    async def _extract_image_features(self, content_data: Dict[str, Any]) -> np.ndarray:
        """Extract features from image content."""
        try:
            # Load image
            if content_data.get("image_url"):
                # TODO: Implement image loading from URL
                image = Image.new('RGB', (224, 224), color='gray')
            elif content_data.get("image_data"):
                image = Image.open(content_data["image_data"])
            else:
                return np.zeros(self.image_dim)
            
            # Preprocess for CLIP
            inputs = self.clip_processor(images=image, return_tensors="pt").to(self.device)
            
            # Extract features
            with torch.no_grad():
                image_features = self.clip_model.get_image_features(**inputs)
                image_features = F.normalize(image_features, p=2, dim=1)
            
            # Convert to numpy
            features = image_features.cpu().numpy().flatten()
            
            # Add image metadata
            width, height = image.size
            aspect_ratio = width / height if height > 0 else 1.0
            
            features = np.concatenate([features, [width, height, aspect_ratio]])
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting image features: {e}")
            return np.zeros(self.image_dim + 3)
    
    async def _extract_video_features(self, content_data: Dict[str, Any]) -> np.ndarray:
        """Extract features from video content."""
        try:
            # TODO: Implement video frame extraction and analysis
            # For now, return placeholder features
            
            # Extract key frames and analyze with CLIP
            # Analyze video metadata (duration, resolution, etc.)
            
            features = np.zeros(self.video_dim)
            
            # Add video metadata
            duration = content_data.get("duration", 0.0)
            resolution = content_data.get("resolution", "unknown")
            fps = content_data.get("fps", 30.0)
            
            features = np.concatenate([features, [duration, fps]])
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting video features: {e}")
            return np.zeros(self.video_dim + 2)
    
    async def _extract_audio_features(self, content_data: Dict[str, Any]) -> np.ndarray:
        """Extract features from audio content."""
        try:
            # TODO: Implement audio feature extraction
            # Extract MFCC, spectral features, etc.
            
            features = np.zeros(self.audio_dim)
            
            # Add audio metadata
            duration = content_data.get("duration", 0.0)
            sample_rate = content_data.get("sample_rate", 44100)
            
            features = np.concatenate([features, [duration, sample_rate]])
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting audio features: {e}")
            return np.zeros(self.audio_dim + 2)
    
    async def _extract_metadata_features(self, metadata: Dict[str, Any]) -> np.ndarray:
        """Extract features from content metadata."""
        try:
            features = []
            
            # Author features
            author_followers = metadata.get("author_followers", 0)
            author_verified = 1.0 if metadata.get("author_verified", False) else 0.0
            
            # Content features
            content_age_hours = metadata.get("content_age_hours", 0)
            content_language = metadata.get("language", "en")
            content_category = metadata.get("category", "general")
            
            # Engagement features
            likes_count = metadata.get("likes_count", 0)
            comments_count = metadata.get("comments_count", 0)
            shares_count = metadata.get("shares_count", 0)
            
            # Normalize numerical features
            features = [
                np.log1p(author_followers),
                author_verified,
                np.log1p(content_age_hours),
                likes_count / max(likes_count + 1, 1),
                comments_count / max(comments_count + 1, 1),
                shares_count / max(shares_count + 1, 1)
            ]
            
            return np.array(features)
            
        except Exception as e:
            logger.error(f"Error extracting metadata features: {e}")
            return np.zeros(6)
    
    async def _extract_session_features(self, session_data: Dict[str, Any]) -> np.ndarray:
        """Extract features from user session data."""
        try:
            features = []
            
            # Session duration and activity
            session_duration = session_data.get("duration_seconds", 0.0)
            items_viewed = session_data.get("items_viewed", 0)
            engagement_actions = session_data.get("engagement_actions", 0)
            
            # Platform and device
            platform = session_data.get("platform", "unknown")
            device_type = session_data.get("device_type", "unknown")
            
            # Normalize features
            features = [
                np.log1p(session_duration),
                np.log1p(items_viewed),
                np.log1p(engagement_actions),
                hash(platform) % 1000 / 1000.0,  # Hash to numerical
                hash(device_type) % 1000 / 1000.0
            ]
            
            return np.array(features)
            
        except Exception as e:
            logger.error(f"Error extracting session features: {e}")
            return np.zeros(5)
    
    async def _extract_interaction_features(self, interaction_data: List[Dict[str, Any]]) -> np.ndarray:
        """Extract features from user interaction data."""
        try:
            if not interaction_data:
                return np.zeros(10)
            
            # Aggregate interaction patterns
            total_interactions = len(interaction_data)
            interaction_types = [i.get("type", "unknown") for i in interaction_data]
            
            # Count interaction types
            type_counts = {}
            for itype in interaction_types:
                type_counts[itype] = type_counts.get(itype, 0) + 1
            
            # Normalize counts
            features = [
                np.log1p(total_interactions),
                type_counts.get("like", 0) / max(total_interactions, 1),
                type_counts.get("comment", 0) / max(total_interactions, 1),
                type_counts.get("share", 0) / max(total_interactions, 1),
                type_counts.get("follow", 0) / max(total_interactions, 1),
                type_counts.get("view", 0) / max(total_interactions, 1)
            ]
            
            # Add temporal patterns
            if len(interaction_data) > 1:
                timestamps = [i.get("timestamp", 0) for i in interaction_data]
                time_diffs = np.diff(sorted(timestamps))
                avg_time_between = np.mean(time_diffs) if len(time_diffs) > 0 else 0
                features.extend([np.log1p(avg_time_between), 0, 0, 0])
            else:
                features.extend([0, 0, 0, 0])
            
            return np.array(features)
            
        except Exception as e:
            logger.error(f"Error extracting interaction features: {e}")
            return np.zeros(10)
    
    async def _extract_temporal_features(self, temporal_data: Dict[str, Any]) -> np.ndarray:
        """Extract temporal features from user behavior."""
        try:
            features = []
            
            # Time-based patterns
            current_time = datetime.utcnow()
            hour_of_day = current_time.hour
            day_of_week = current_time.weekday()
            
            # Activity frequency
            daily_activity = temporal_data.get("daily_activity", 0)
            weekly_activity = temporal_data.get("weekly_activity", 0)
            
            # Peak activity hours
            peak_hours = temporal_data.get("peak_hours", [])
            peak_hour_1 = peak_hours[0] if len(peak_hours) > 0 else 0
            peak_hour_2 = peak_hours[1] if len(peak_hours) > 1 else 0
            
            features = [
                hour_of_day / 24.0,
                day_of_week / 7.0,
                np.log1p(daily_activity),
                np.log1p(weekly_activity),
                peak_hour_1 / 24.0,
                peak_hour_2 / 24.0
            ]
            
            return np.array(features)
            
        except Exception as e:
            logger.error(f"Error extracting temporal features: {e}")
            return np.zeros(6)
    
    async def _extract_device_features(self, device_data: Dict[str, Any]) -> np.ndarray:
        """Extract features from device information."""
        try:
            features = []
            
            # Device type
            device_type = device_data.get("type", "unknown")
            os_type = device_data.get("os", "unknown")
            browser_type = device_data.get("browser", "unknown")
            
            # Screen resolution
            screen_width = device_data.get("screen_width", 1920)
            screen_height = device_data.get("screen_height", 1080)
            screen_density = device_data.get("screen_density", 1.0)
            
            # Hash categorical features to numerical
            features = [
                hash(device_type) % 1000 / 1000.0,
                hash(os_type) % 1000 / 1000.0,
                hash(browser_type) % 1000 / 1000.0,
                screen_width / 10000.0,
                screen_height / 10000.0,
                screen_density
            ]
            
            return np.array(features)
            
        except Exception as e:
            logger.error(f"Error extracting device features: {e}")
            return np.zeros(6)
    
    async def _extract_location_features(self, location_data: Dict[str, Any]) -> np.ndarray:
        """Extract features from location information."""
        try:
            features = []
            
            # Geographic features
            latitude = location_data.get("latitude", 0.0)
            longitude = location_data.get("longitude", 0.0)
            country_code = location_data.get("country_code", "unknown")
            timezone = location_data.get("timezone", "UTC")
            
            # Hash categorical features
            features = [
                latitude / 90.0,  # Normalize to [-1, 1]
                longitude / 180.0,  # Normalize to [-1, 1]
                hash(country_code) % 1000 / 1000.0,
                hash(timezone) % 1000 / 1000.0
            ]
            
            return np.array(features)
            
        except Exception as e:
            logger.error(f"Error extracting location features: {e}")
            return np.zeros(4)
    
    async def _combine_content_features(self, features: ContentFeatures) -> np.ndarray:
        """Combine all content features into a single vector."""
        try:
            feature_vectors = []
            
            if features.text_features is not None:
                feature_vectors.append(features.text_features)
            
            if features.image_features is not None:
                feature_vectors.append(features.image_features)
            
            if features.video_features is not None:
                feature_vectors.append(features.video_features)
            
            if features.audio_features is not None:
                feature_vectors.append(features.audio_features)
            
            if features.metadata_features is not None:
                feature_vectors.append(features.metadata_features)
            
            if not feature_vectors:
                return np.zeros(100)  # Default size
            
            # Concatenate all features
            combined = np.concatenate(feature_vectors)
            
            # Normalize to unit length
            norm = np.linalg.norm(combined)
            if norm > 0:
                combined = combined / norm
            
            return combined
            
        except Exception as e:
            logger.error(f"Error combining content features: {e}")
            return np.zeros(100)
    
    async def _combine_behavior_features(self, features: UserBehaviorFeatures) -> np.ndarray:
        """Combine all behavior features into a single vector."""
        try:
            feature_vectors = []
            
            if features.session_features is not None:
                feature_vectors.append(features.session_features)
            
            if features.interaction_features is not None:
                feature_vectors.append(features.interaction_features)
            
            if features.temporal_features is not None:
                feature_vectors.append(features.temporal_features)
            
            if features.device_features is not None:
                feature_vectors.append(features.device_features)
            
            if features.location_features is not None:
                feature_vectors.append(features.location_features)
            
            if not feature_vectors:
                return np.zeros(50)  # Default size
            
            # Concatenate all features
            combined = np.concatenate(feature_vectors)
            
            # Normalize to unit length
            norm = np.linalg.norm(combined)
            if norm > 0:
                combined = combined / norm
            
            return combined
            
        except Exception as e:
            logger.error(f"Error combining behavior features: {e}")
            return np.zeros(50)
    
    def close(self):
        """Clean up resources."""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)