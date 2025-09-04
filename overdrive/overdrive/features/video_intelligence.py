from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple, Union
import torch
import torch.nn as nn
import torch.nn.functional as F
from transformers import (
    VideoMAEFeatureExtractor, VideoMAEModel, CLIPProcessor, CLIPModel,
    WhisperProcessor, WhisperModel, pipeline
)
import cv2
import numpy as np
import librosa
from PIL import Image
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
import asyncio
from concurrent.futures import ThreadPoolExecutor
import tempfile
import os
from moviepy.editor import VideoFileClip
import whisper
from ultralytics import YOLO
import mediapipe as mp

logger = logging.getLogger(__name__)

@dataclass
class VideoFrame:
    """Individual video frame with analysis."""
    frame_number: int
    timestamp: float
    image: np.ndarray
    objects_detected: List[Dict[str, Any]]
    scene_features: Optional[np.ndarray] = None
    action_features: Optional[np.ndarray] = None

@dataclass
class VideoSegment:
    """Video segment with temporal analysis."""
    start_time: float
    end_time: float
    duration: float
    frames: List[VideoFrame]
    dominant_actions: List[str]
    dominant_objects: List[str]
    scene_type: str
    audio_features: Optional[np.ndarray] = None

@dataclass
class VideoIntelligence:
    """Comprehensive video understanding and analysis."""
    video_id: str
    duration: float
    fps: float
    resolution: Tuple[int, int]
    total_frames: int
    
    # Frame-level analysis
    key_frames: List[VideoFrame]
    frame_features: np.ndarray
    
    # Segment-level analysis
    segments: List[VideoSegment]
    temporal_features: np.ndarray
    
    # Content understanding
    actions_detected: List[str]
    objects_detected: List[str]
    scenes_identified: List[str]
    audio_content: Dict[str, Any]
    
    # High-level understanding
    content_summary: str
    content_category: str
    engagement_potential: float
    viral_indicators: Dict[str, float]

class AdvancedVideoIntelligence:
    """Advanced video content analysis system that understands what's happening."""
    
    def __init__(self, 
                 device: str = "cuda" if torch.cuda.is_available() else "cpu",
                 cache_dir: str = "./model_cache",
                 max_frames: int = 300,  # Max frames to analyze
                 segment_duration: float = 5.0):  # Segment duration in seconds
        
        self.device = device
        self.cache_dir = cache_dir
        self.max_frames = max_frames
        self.segment_duration = segment_duration
        
        # Initialize video analysis models
        self._load_video_models()
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        logger.info(f"AdvancedVideoIntelligence initialized on {device}")
    
    def _load_video_models(self):
        """Load all required video analysis models."""
        try:
            # Video understanding models
            self.videomae_extractor = VideoMAEFeatureExtractor.from_pretrained("MCG-NJU/videomae-base")
            self.videomae_model = VideoMAEModel.from_pretrained("MCG-NJU/videomae-base").to(self.device)
            
            # CLIP for frame understanding
            self.clip_processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
            self.clip_model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(self.device)
            
            # Audio analysis
            self.whisper_processor = WhisperProcessor.from_pretrained("openai/whisper-base")
            self.whisper_model = WhisperModel.from_pretrained("openai/whisper-base").to(self.device)
            
            # Object detection (YOLO)
            try:
                self.yolo_model = YOLO('yolov8n.pt')
                logger.info("YOLO model loaded successfully")
            except:
                logger.warning("YOLO model not available, using basic detection")
                self.yolo_model = None
            
            # MediaPipe for pose and face detection
            self.mp_pose = mp.solutions.pose
            self.mp_face = mp.solutions.face_detection
            self.mp_hands = mp.solutions.hands
            
            # Action recognition pipeline
            self.action_pipeline = pipeline(
                "video-classification",
                model="microsoft/xclip-base-patch32",
                device=0 if self.device == "cuda" else -1
            )
            
            # Scene understanding
            self.scene_pipeline = pipeline(
                "image-classification",
                model="microsoft/resnet-50",
                device=0 if self.device == "cuda" else -1
            )
            
            logger.info("All video analysis models loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load video models: {e}")
            raise
    
    async def analyze_video(self, 
                           video_path: str,
                           video_id: str,
                           metadata: Optional[Dict[str, Any]] = None) -> VideoIntelligence:
        """Comprehensive video analysis and understanding."""
        
        try:
            logger.info(f"Starting comprehensive analysis of video: {video_id}")
            
            # Load video
            video_clip = VideoFileClip(video_path)
            duration = video_clip.duration
            fps = video_clip.fps
            resolution = (video_clip.w, video_clip.h)
            total_frames = int(duration * fps)
            
            # Extract key frames
            key_frames = await self._extract_key_frames(video_clip, total_frames)
            
            # Analyze frames
            analyzed_frames = await self._analyze_frames(key_frames)
            
            # Extract frame features
            frame_features = await self._extract_frame_features(analyzed_frames)
            
            # Segment video
            segments = await self._segment_video(analyzed_frames, duration)
            
            # Temporal analysis
            temporal_features = await self._extract_temporal_features(segments, duration)
            
            # Audio analysis
            audio_content = await self._analyze_audio(video_path)
            
            # Content understanding
            actions_detected = await self._identify_actions(segments)
            objects_detected = await self._identify_objects(segments)
            scenes_identified = await self._identify_scenes(segments)
            
            # High-level analysis
            content_summary = await self._generate_content_summary(actions_detected, objects_detected, scenes_identified)
            content_category = await self._categorize_content(actions_detected, objects_detected, scenes_identified)
            engagement_potential = await self._calculate_engagement_potential(segments, audio_content)
            viral_indicators = await self._analyze_viral_potential(segments, audio_content, metadata)
            
            # Create video intelligence object
            video_intelligence = VideoIntelligence(
                video_id=video_id,
                duration=duration,
                fps=fps,
                resolution=resolution,
                total_frames=total_frames,
                key_frames=analyzed_frames,
                frame_features=frame_features,
                segments=segments,
                temporal_features=temporal_features,
                actions_detected=actions_detected,
                objects_detected=objects_detected,
                scenes_identified=scenes_identified,
                audio_content=audio_content,
                content_summary=content_summary,
                content_category=content_category,
                engagement_potential=engagement_potential,
                viral_indicators=viral_indicators
            )
            
            # Clean up
            video_clip.close()
            
            logger.info(f"Video analysis completed for {video_id}")
            return video_intelligence
            
        except Exception as e:
            logger.error(f"Error analyzing video {video_id}: {e}")
            raise
    
    async def _extract_key_frames(self, video_clip: VideoFileClip, total_frames: int) -> List[VideoFrame]:
        """Extract key frames from video for analysis."""
        try:
            frames = []
            frame_interval = max(1, total_frames // self.max_frames)
            
            for i in range(0, total_frames, frame_interval):
                timestamp = i / video_clip.fps
                frame = video_clip.get_frame(timestamp)
                
                # Convert to RGB if needed
                if len(frame.shape) == 3 and frame.shape[2] == 3:
                    frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                else:
                    frame_rgb = frame
                
                video_frame = VideoFrame(
                    frame_number=i,
                    timestamp=timestamp,
                    image=frame_rgb,
                    objects_detected=[],
                    scene_features=None,
                    action_features=None
                )
                frames.append(video_frame)
            
            logger.info(f"Extracted {len(frames)} key frames")
            return frames
            
        except Exception as e:
            logger.error(f"Error extracting key frames: {e}")
            return []
    
    async def _analyze_frames(self, frames: List[VideoFrame]) -> List[VideoFrame]:
        """Analyze individual frames for objects, scenes, and actions."""
        try:
            analyzed_frames = []
            
            for frame in frames:
                try:
                    # Object detection
                    objects = await self._detect_objects(frame.image)
                    frame.objects_detected = objects
                    
                    # Scene understanding
                    scene_features = await self._extract_scene_features(frame.image)
                    frame.scene_features = scene_features
                    
                    # Action features
                    action_features = await self._extract_action_features(frame.image)
                    frame.action_features = action_features
                    
                    analyzed_frames.append(frame)
                    
                except Exception as e:
                    logger.warning(f"Error analyzing frame {frame.frame_number}: {e}")
                    analyzed_frames.append(frame)
            
            return analyzed_frames
            
        except Exception as e:
            logger.error(f"Error analyzing frames: {e}")
            return frames
    
    async def _detect_objects(self, image: np.ndarray) -> List[Dict[str, Any]]:
        """Detect objects in image using YOLO and MediaPipe."""
        try:
            objects = []
            
            # YOLO object detection
            if self.yolo_model:
                try:
                    results = self.yolo_model(image)
                    for result in results:
                        boxes = result.boxes
                        if boxes is not None:
                            for box in boxes:
                                x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                                confidence = box.conf[0].cpu().numpy()
                                class_id = int(box.cls[0].cpu().numpy())
                                class_name = result.names[class_id]
                                
                                objects.append({
                                    'type': 'object',
                                    'class': class_name,
                                    'confidence': float(confidence),
                                    'bbox': [float(x1), float(y1), float(x2), float(y2)],
                                    'detector': 'yolo'
                                })
                except Exception as e:
                    logger.warning(f"YOLO detection failed: {e}")
            
            # MediaPipe pose detection
            try:
                with self.mp_pose.Pose(static_image_mode=True, min_detection_confidence=0.5) as pose:
                    results = pose.process(cv2.cvtColor(image, cv2.COLOR_RGB2BGR))
                    if results.pose_landmarks:
                        objects.append({
                            'type': 'pose',
                            'class': 'human_pose',
                            'confidence': 0.8,
                            'landmarks': len(results.pose_landmarks.landmark),
                            'detector': 'mediapipe'
                        })
            except Exception as e:
                logger.warning(f"Pose detection failed: {e}")
            
            # MediaPipe face detection
            try:
                with self.mp_face.FaceDetection(model_selection=0, min_detection_confidence=0.5) as face_detection:
                    results = face_detection.process(cv2.cvtColor(image, cv2.COLOR_RGB2BGR))
                    if results.detections:
                        for detection in results.detections:
                            bbox = detection.location_data.relative_bounding_box
                            objects.append({
                                'type': 'face',
                                'class': 'human_face',
                                'confidence': float(detection.score[0]),
                                'bbox': [float(bbox.xmin), float(bbox.ymin), 
                                        float(bbox.xmin + bbox.width), float(bbox.ymin + bbox.height)],
                                'detector': 'mediapipe'
                            })
            except Exception as e:
                logger.warning(f"Face detection failed: {e}")
            
            return objects
            
        except Exception as e:
            logger.error(f"Error detecting objects: {e}")
            return []
    
    async def _extract_scene_features(self, image: np.ndarray) -> np.ndarray:
        """Extract scene understanding features from image."""
        try:
            # Convert to PIL Image for CLIP
            pil_image = Image.fromarray(image)
            
            # CLIP scene understanding
            inputs = self.clip_processor(images=pil_image, return_tensors="pt").to(self.device)
            with torch.no_grad():
                image_features = self.clip_model.get_image_features(**inputs)
                scene_features = F.normalize(image_features, p=2, dim=1)
            
            return scene_features.cpu().numpy().flatten()
            
        except Exception as e:
            logger.error(f"Error extracting scene features: {e}")
            return np.zeros(512)
    
    async def _extract_action_features(self, image: np.ndarray) -> np.ndarray:
        """Extract action-related features from image."""
        try:
            # Use VideoMAE for action understanding
            # Convert image to video format (single frame)
            video_input = np.expand_dims(image, axis=0)  # Add time dimension
            
            inputs = self.videomae_extractor(video_input, return_tensors="pt")
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                outputs = self.videomae_model(**inputs)
                action_features = outputs.last_hidden_state[:, 0, :]  # Use [CLS] token
            
            return action_features.cpu().numpy().flatten()
            
        except Exception as e:
            logger.error(f"Error extracting action features: {e}")
            return np.zeros(768)
    
    async def _segment_video(self, frames: List[VideoFrame], duration: float) -> List[VideoSegment]:
        """Segment video into temporal segments for analysis."""
        try:
            segments = []
            segment_duration = self.segment_duration
            
            for start_time in np.arange(0, duration, segment_duration):
                end_time = min(start_time + segment_duration, duration)
                
                # Get frames in this segment
                segment_frames = [
                    frame for frame in frames 
                    if start_time <= frame.timestamp < end_time
                ]
                
                if segment_frames:
                    # Analyze segment
                    dominant_actions = await self._analyze_segment_actions(segment_frames)
                    dominant_objects = await self._analyze_segment_objects(segment_frames)
                    scene_type = await self._classify_segment_scene(segment_frames)
                    
                    # Audio features for this segment
                    audio_features = None  # Will be filled later
                    
                    segment = VideoSegment(
                        start_time=start_time,
                        end_time=end_time,
                        duration=end_time - start_time,
                        frames=segment_frames,
                        dominant_actions=dominant_actions,
                        dominant_objects=dominant_objects,
                        scene_type=scene_type,
                        audio_features=audio_features
                    )
                    segments.append(segment)
            
            logger.info(f"Created {len(segments)} video segments")
            return segments
            
        except Exception as e:
            logger.error(f"Error segmenting video: {e}")
            return []
    
    async def _analyze_segment_actions(self, frames: List[VideoFrame]) -> List[str]:
        """Analyze dominant actions in a video segment."""
        try:
            action_counts = {}
            
            for frame in frames:
                if frame.action_features is not None:
                    # Use action features to classify actions
                    # This is a simplified approach - in practice, you'd use a trained action classifier
                    action_type = self._classify_action_from_features(frame.action_features)
                    action_counts[action_type] = action_counts.get(action_type, 0) + 1
            
            # Get top actions
            sorted_actions = sorted(action_counts.items(), key=lambda x: x[1], reverse=True)
            return [action for action, count in sorted_actions[:3]]
            
        except Exception as e:
            logger.error(f"Error analyzing segment actions: {e}")
            return []
    
    async def _analyze_segment_objects(self, frames: List[VideoFrame]) -> List[str]:
        """Analyze dominant objects in a video segment."""
        try:
            object_counts = {}
            
            for frame in frames:
                for obj in frame.objects_detected:
                    obj_class = obj['class']
                    object_counts[obj_class] = object_counts.get(obj_class, 0) + 1
            
            # Get top objects
            sorted_objects = sorted(object_counts.items(), key=lambda x: x[1], reverse=True)
            return [obj for obj, count in sorted_objects[:5]]
            
        except Exception as e:
            logger.error(f"Error analyzing segment objects: {e}")
            return []
    
    async def _classify_segment_scene(self, frames: List[VideoFrame]) -> str:
        """Classify the scene type for a video segment."""
        try:
            scene_scores = {}
            
            for frame in frames:
                if frame.scene_features is not None:
                    # Use scene features to classify scene
                    scene_type = self._classify_scene_from_features(frame.scene_features)
                    scene_scores[scene_type] = scene_scores.get(scene_type, 0) + 1
            
            # Get most common scene
            if scene_scores:
                return max(scene_scores.items(), key=lambda x: x[1])[0]
            else:
                return "unknown"
                
        except Exception as e:
            logger.error(f"Error classifying segment scene: {e}")
            return "unknown"
    
    def _classify_action_from_features(self, action_features: np.ndarray) -> str:
        """Classify action from extracted features."""
        try:
            # Simplified action classification based on feature patterns
            # In practice, you'd use a trained action classifier
            
            # Analyze feature patterns to determine action type
            feature_mean = np.mean(action_features)
            feature_std = np.std(action_features)
            
            if feature_std > 0.1:
                if feature_mean > 0.5:
                    return "active_movement"
                else:
                    return "subtle_action"
            else:
                return "static"
                
        except Exception as e:
            logger.error(f"Error classifying action: {e}")
            return "unknown"
    
    def _classify_scene_from_features(self, scene_features: np.ndarray) -> str:
        """Classify scene from extracted features."""
        try:
            # Simplified scene classification based on feature patterns
            # In practice, you'd use a trained scene classifier
            
            feature_mean = np.mean(scene_features)
            feature_std = np.std(scene_features)
            
            if feature_std > 0.1:
                if feature_mean > 0.6:
                    return "outdoor"
                elif feature_mean > 0.3:
                    return "indoor"
                else:
                    return "mixed"
            else:
                return "uniform"
                
        except Exception as e:
            logger.error(f"Error classifying scene: {e}")
            return "unknown"
    
    async def _extract_temporal_features(self, segments: List[VideoSegment], duration: float) -> np.ndarray:
        """Extract temporal patterns and features from video."""
        try:
            temporal_features = []
            
            # Segment duration patterns
            segment_durations = [seg.duration for seg in segments]
            temporal_features.extend([
                np.mean(segment_durations),
                np.std(segment_durations),
                len(segments) / duration  # Segments per second
            ])
            
            # Action dynamics
            action_transitions = []
            for i in range(1, len(segments)):
                prev_actions = set(segments[i-1].dominant_actions)
                curr_actions = set(segments[i].dominant_actions)
                transition_score = len(prev_actions.intersection(curr_actions)) / max(len(prev_actions.union(curr_actions)), 1)
                action_transitions.append(transition_score)
            
            if action_transitions:
                temporal_features.extend([
                    np.mean(action_transitions),
                    np.std(action_transitions)
                ])
            else:
                temporal_features.extend([0.0, 0.0])
            
            # Scene consistency
            scene_types = [seg.scene_type for seg in segments]
            unique_scenes = len(set(scene_types))
            temporal_features.append(unique_scenes / len(segments) if segments else 0.0)
            
            # Object persistence
            all_objects = []
            for seg in segments:
                all_objects.extend(seg.dominant_objects)
            
            if all_objects:
                object_counts = {}
                for obj in all_objects:
                    object_counts[obj] = object_counts.get(obj, 0) + 1
                
                temporal_features.extend([
                    len(object_counts),  # Unique objects
                    np.mean(list(object_counts.values())  # Average object frequency
                ])
            else:
                temporal_features.extend([0.0, 0.0])
            
            return np.array(temporal_features)
            
        except Exception as e:
            logger.error(f"Error extracting temporal features: {e}")
            return np.zeros(8)
    
    async def _analyze_audio(self, video_path: str) -> Dict[str, Any]:
        """Analyze audio content of video."""
        try:
            audio_content = {}
            
            # Extract audio from video
            video_clip = VideoFileClip(video_path)
            audio = video_clip.audio
            
            if audio is not None:
                # Save audio to temporary file
                with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_audio:
                    audio.write_audiofile(temp_audio.name, verbose=False, logger=None)
                    temp_audio_path = temp_audio.name
                
                try:
                    # Whisper speech recognition
                    whisper_model = whisper.load_model("base")
                    result = whisper_model.transcribe(temp_audio_path)
                    
                    audio_content.update({
                        'transcription': result['text'],
                        'language': result['language'],
                        'segments': len(result['segments']),
                        'has_speech': len(result['text'].strip()) > 0
                    })
                    
                    # Audio features
                    y, sr = librosa.load(temp_audio_path)
                    
                    # Spectral features
                    spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
                    spectral_rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr)[0]
                    
                    # MFCC features
                    mfccs = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13)
                    
                    # Rhythm features
                    tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
                    
                    audio_content.update({
                        'spectral_centroid_mean': float(np.mean(spectral_centroids)),
                        'spectral_rolloff_mean': float(np.mean(spectral_rolloff)),
                        'mfcc_mean': float(np.mean(mfccs)),
                        'tempo': float(tempo),
                        'duration': float(librosa.get_duration(y=y, sr=sr))
                    })
                    
                finally:
                    # Clean up temporary file
                    os.unlink(temp_audio_path)
                
                audio.close()
            else:
                audio_content.update({
                    'transcription': '',
                    'language': 'unknown',
                    'segments': 0,
                    'has_speech': False,
                    'spectral_centroid_mean': 0.0,
                    'spectral_rolloff_mean': 0.0,
                    'mfcc_mean': 0.0,
                    'tempo': 0.0,
                    'duration': 0.0
                })
            
            video_clip.close()
            return audio_content
            
        except Exception as e:
            logger.error(f"Error analyzing audio: {e}")
            return {
                'transcription': '',
                'language': 'unknown',
                'segments': 0,
                'has_speech': False,
                'spectral_centroid_mean': 0.0,
                'spectral_rolloff_mean': 0.0,
                'mfcc_mean': 0.0,
                'tempo': 0.0,
                'duration': 0.0
            }
    
    async def _identify_actions(self, segments: List[VideoSegment]) -> List[str]:
        """Identify all actions present in the video."""
        try:
            all_actions = []
            for segment in segments:
                all_actions.extend(segment.dominant_actions)
            
            # Count and rank actions
            action_counts = {}
            for action in all_actions:
                action_counts[action] = action_counts.get(action, 0) + 1
            
            # Return top actions
            sorted_actions = sorted(action_counts.items(), key=lambda x: x[1], reverse=True)
            return [action for action, count in sorted_actions[:10]]
            
        except Exception as e:
            logger.error(f"Error identifying actions: {e}")
            return []
    
    async def _identify_objects(self, segments: List[VideoSegment]) -> List[str]:
        """Identify all objects present in the video."""
        try:
            all_objects = []
            for segment in segments:
                all_objects.extend(segment.dominant_objects)
            
            # Count and rank objects
            object_counts = {}
            for obj in all_objects:
                object_counts[obj] = object_counts.get(obj, 0) + 1
            
            # Return top objects
            sorted_objects = sorted(object_counts.items(), key=lambda x: x[1], reverse=True)
            return [obj for obj, count in sorted_objects[:15]]
            
        except Exception as e:
            logger.error(f"Error identifying objects: {e}")
            return []
    
    async def _identify_scenes(self, segments: List[VideoSegment]) -> List[str]:
        """Identify all scene types present in the video."""
        try:
            scene_types = [segment.scene_type for segment in segments]
            unique_scenes = list(set(scene_types))
            
            # Count scene occurrences
            scene_counts = {}
            for scene in scene_types:
                scene_counts[scene] = scene_counts.get(scene, 0) + 1
            
            # Return scenes sorted by frequency
            sorted_scenes = sorted(scene_counts.items(), key=lambda x: x[1], reverse=True)
            return [scene for scene, count in sorted_scenes]
            
        except Exception as e:
            logger.error(f"Error identifying scenes: {e}")
            return []
    
    async def _generate_content_summary(self, actions: List[str], objects: List[str], scenes: List[str]) -> str:
        """Generate human-readable content summary."""
        try:
            summary_parts = []
            
            if actions:
                top_actions = actions[:3]
                summary_parts.append(f"Features {', '.join(top_actions)}")
            
            if objects:
                top_objects = objects[:5]
                summary_parts.append(f"Contains {', '.join(top_objects)}")
            
            if scenes:
                top_scenes = scenes[:2]
                summary_parts.append(f"Set in {', '.join(top_scenes)} environments")
            
            if summary_parts:
                return ". ".join(summary_parts) + "."
            else:
                return "Content analysis completed."
                
        except Exception as e:
            logger.error(f"Error generating content summary: {e}")
            return "Content analysis completed."
    
    async def _categorize_content(self, actions: List[str], objects: List[str], scenes: List[str]) -> str:
        """Categorize video content based on analysis."""
        try:
            # Define content categories
            categories = {
                'gaming': ['game', 'controller', 'screen', 'player'],
                'cooking': ['food', 'kitchen', 'cook', 'recipe', 'ingredient'],
                'travel': ['landscape', 'nature', 'outdoor', 'scenery', 'mountain'],
                'sports': ['sport', 'ball', 'field', 'athlete', 'movement'],
                'music': ['instrument', 'concert', 'performance', 'dance'],
                'comedy': ['funny', 'laugh', 'joke', 'entertainment'],
                'education': ['book', 'classroom', 'teacher', 'learning'],
                'technology': ['computer', 'device', 'tech', 'innovation']
            }
            
            # Score each category
            category_scores = {}
            all_content = actions + objects + scenes
            
            for category, keywords in categories.items():
                score = 0
                for keyword in keywords:
                    for content in all_content:
                        if keyword.lower() in content.lower():
                            score += 1
                category_scores[category] = score
            
            # Return top category
            if category_scores:
                top_category = max(category_scores.items(), key=lambda x: x[1])
                if top_category[1] > 0:
                    return top_category[0]
            
            return "general"
            
        except Exception as e:
            logger.error(f"Error categorizing content: {e}")
            return "general"
    
    async def _calculate_engagement_potential(self, segments: List[VideoSegment], audio_content: Dict[str, Any]) -> float:
        """Calculate engagement potential of the video."""
        try:
            engagement_score = 0.0
            total_factors = 0
            
            # Visual engagement factors
            if segments:
                # Action variety
                unique_actions = len(set([action for seg in segments for action in seg.dominant_actions]))
                engagement_score += min(unique_actions / 5.0, 1.0)  # Cap at 1.0
                total_factors += 1
                
                # Scene variety
                unique_scenes = len(set([seg.scene_type for seg in segments]))
                engagement_score += min(unique_scenes / 3.0, 1.0)
                total_factors += 1
                
                # Object richness
                unique_objects = len(set([obj for seg in segments for obj in seg.dominant_objects]))
                engagement_score += min(unique_objects / 10.0, 1.0)
                total_factors += 1
            
            # Audio engagement factors
            if audio_content.get('has_speech', False):
                engagement_score += 0.8  # Speech increases engagement
                total_factors += 1
            
            if audio_content.get('tempo', 0) > 120:
                engagement_score += 0.6  # High tempo music
                total_factors += 1
            
            # Normalize score
            if total_factors > 0:
                return engagement_score / total_factors
            else:
                return 0.5
                
        except Exception as e:
            logger.error(f"Error calculating engagement potential: {e}")
            return 0.5
    
    async def _analyze_viral_potential(self, segments: List[VideoSegment], audio_content: Dict[str, Any], metadata: Optional[Dict[str, Any]]) -> Dict[str, float]:
        """Analyze viral potential indicators."""
        try:
            viral_indicators = {}
            
            # Content uniqueness
            if segments:
                unique_actions = len(set([action for seg in segments for action in seg.dominant_actions]))
                viral_indicators['action_uniqueness'] = min(unique_actions / 8.0, 1.0)
                
                unique_objects = len(set([obj for seg in segments for obj in seg.dominant_objects]))
                viral_indicators['object_uniqueness'] = min(unique_objects / 15.0, 1.0)
            
            # Audio appeal
            if audio_content.get('has_speech', False):
                viral_indicators['speech_clarity'] = 0.8
            else:
                viral_indicators['speech_clarity'] = 0.2
            
            # Music appeal
            tempo = audio_content.get('tempo', 0)
            if 90 <= tempo <= 140:  # Optimal tempo range
                viral_indicators['music_appeal'] = 0.9
            elif tempo > 0:
                viral_indicators['music_appeal'] = 0.6
            else:
                viral_indicators['music_appeal'] = 0.3
            
            # Visual appeal
            if segments:
                scene_variety = len(set([seg.scene_type for seg in segments]))
                viral_indicators['visual_variety'] = min(scene_variety / 4.0, 1.0)
            else:
                viral_indicators['visual_variety'] = 0.5
            
            # Overall viral score
            if viral_indicators:
                viral_indicators['overall_viral_score'] = np.mean(list(viral_indicators.values()))
            else:
                viral_indicators['overall_viral_score'] = 0.5
            
            return viral_indicators
            
        except Exception as e:
            logger.error(f"Error analyzing viral potential: {e}")
            return {'overall_viral_score': 0.5}
    
    def close(self):
        """Clean up resources."""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)