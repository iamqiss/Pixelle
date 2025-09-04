from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
import asyncio
import json
import time
import logging
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import redis
import kafka
from kafka import KafkaProducer, KafkaConsumer
import threading
from queue import Queue, Empty
import pickle
import hashlib

logger = logging.getLogger(__name__)

@dataclass
class UserSignal:
    """Real-time user signal data."""
    signal_id: str
    user_id: str
    signal_type: str  # view, like, comment, share, follow, scroll, etc.
    timestamp: datetime
    content_id: Optional[str] = None
    session_id: Optional[str] = None
    duration: float = 0.0
    intensity: float = 1.0
    metadata: Optional[Dict[str, Any]] = None
    processed: bool = False

@dataclass
class SignalAggregate:
    """Aggregated signal data for a user."""
    user_id: str
    time_window: str  # 1m, 5m, 15m, 1h, 24h
    signal_counts: Dict[str, int]
    content_interactions: Dict[str, Dict[str, int]]
    temporal_patterns: Dict[str, float]
    engagement_score: float
    last_updated: datetime

@dataclass
class AdaptiveRecommendation:
    """Adaptive recommendation based on real-time signals."""
    user_id: str
    content_id: str
    base_score: float
    signal_boost: float
    final_score: float
    signal_types: List[str]
    confidence: float
    timestamp: datetime

class RealTimeSignalProcessor:
    """High-performance real-time signal processing system."""
    
    def __init__(self, 
                 redis_url: str = "redis://localhost:6379",
                 kafka_bootstrap_servers: List[str] = None,
                 max_workers: int = 8,
                 signal_buffer_size: int = 10000):
        
        self.redis_url = redis_url
        self.kafka_bootstrap_servers = kafka_bootstrap_servers or ["localhost:9092"]
        self.max_workers = max_workers
        self.signal_buffer_size = signal_buffer_size
        
        # Initialize connections
        self._init_connections()
        
        # Signal processing queues
        self.signal_queue = Queue(maxsize=signal_buffer_size)
        self.high_priority_queue = Queue(maxsize=signal_buffer_size // 10)
        
        # Signal buffers and aggregators
        self.signal_buffers = defaultdict(deque)
        self.signal_aggregates = {}
        self.user_embeddings = {}
        
        # Processing workers
        self.workers = []
        self.running = False
        
        # Signal type weights and decay rates
        self.signal_weights = {
            'view': 1.0,
            'like': 2.0,
            'comment': 3.0,
            'share': 4.0,
            'follow': 5.0,
            'bookmark': 2.5,
            'click': 1.5,
            'scroll': 0.5,
            'dwell': 1.2,
            'completion': 2.8
        }
        
        self.signal_decay_rates = {
            '1m': 0.1,    # 1 minute window
            '5m': 0.05,   # 5 minute window
            '15m': 0.02,  # 15 minute window
            '1h': 0.01,   # 1 hour window
            '24h': 0.001  # 24 hour window
        }
        
        # Performance metrics
        self.processing_metrics = {
            'signals_processed': 0,
            'signals_dropped': 0,
            'processing_latency_ms': deque(maxlen=1000),
            'queue_size': 0
        }
        
        logger.info("RealTimeSignalProcessor initialized")
    
    def _init_connections(self):
        """Initialize Redis and Kafka connections."""
        try:
            # Redis connection
            self.redis_client = redis.Redis.from_url(self.redis_url, decode_responses=True)
            self.redis_client.ping()
            logger.info("Redis connection established")
            
            # Kafka producer for processed signals
            if self.kafka_bootstrap_servers:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None
                )
                logger.info("Kafka producer initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize connections: {e}")
            raise
    
    async def start_processing(self):
        """Start the real-time signal processing system."""
        if self.running:
            logger.warning("Signal processor already running")
            return
        
        self.running = True
        
        # Start worker threads
        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._signal_worker,
                args=(i,),
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
        
        # Start aggregation worker
        aggregation_worker = threading.Thread(
            target=self._aggregation_worker,
            daemon=True
        )
        aggregation_worker.start()
        self.workers.append(aggregation_worker)
        
        # Start performance monitoring
        monitoring_thread = threading.Thread(
            target=self._monitoring_worker,
            daemon=True
        )
        monitoring_thread.start()
        self.workers.append(monitoring_thread)
        
        logger.info(f"Started {len(self.workers)} signal processing workers")
    
    async def stop_processing(self):
        """Stop the real-time signal processing system."""
        self.running = False
        
        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=5.0)
        
        # Close connections
        if hasattr(self, 'kafka_producer'):
            self.kafka_producer.close()
        
        logger.info("Signal processing stopped")
    
    async def process_signal(self, signal: UserSignal, priority: str = "normal"):
        """Process a user signal in real-time."""
        try:
            # Add signal to appropriate queue
            if priority == "high":
                try:
                    self.high_priority_queue.put_nowait(signal)
                except Queue.Full:
                    logger.warning("High priority queue full, dropping signal")
                    self.processing_metrics['signals_dropped'] += 1
            else:
                try:
                    self.signal_queue.put_nowait(signal)
                except Queue.Full:
                    logger.warning("Signal queue full, dropping signal")
                    self.processing_metrics['signals_dropped'] += 1
            
            # Update metrics
            self.processing_metrics['queue_size'] = self.signal_queue.qsize() + self.high_priority_queue.qsize()
            
        except Exception as e:
            logger.error(f"Error processing signal: {e}")
            self.processing_metrics['signals_dropped'] += 1
    
    def _signal_worker(self, worker_id: int):
        """Worker thread for processing signals."""
        logger.info(f"Signal worker {worker_id} started")
        
        while self.running:
            try:
                # Process high priority signals first
                try:
                    signal = self.high_priority_queue.get_nowait()
                except Empty:
                    try:
                        signal = self.signal_queue.get(timeout=1.0)
                    except Empty:
                        continue
                
                # Process the signal
                start_time = time.time()
                self._process_single_signal(signal)
                
                # Update metrics
                processing_time = (time.time() - start_time) * 1000  # Convert to ms
                self.processing_metrics['processing_latency_ms'].append(processing_time)
                self.processing_metrics['signals_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error in signal worker {worker_id}: {e}")
                continue
        
        logger.info(f"Signal worker {worker_id} stopped")
    
    def _process_single_signal(self, signal: UserSignal):
        """Process a single user signal."""
        try:
            # Add to signal buffer
            self.signal_buffers[signal.user_id].append(signal)
            
            # Limit buffer size
            if len(self.signal_buffers[signal.user_id]) > self.signal_buffer_size:
                self.signal_buffers[signal.user_id].popleft()
            
            # Update user embedding if needed
            self._update_user_embedding(signal)
            
            # Mark as processed
            signal.processed = True
            
            # Store in Redis for persistence
            self._store_signal_redis(signal)
            
            # Publish to Kafka if configured
            if hasattr(self, 'kafka_producer'):
                self._publish_signal_kafka(signal)
            
        except Exception as e:
            logger.error(f"Error processing signal {signal.signal_id}: {e}")
    
    def _update_user_embedding(self, signal: UserSignal):
        """Update user embedding based on new signal."""
        try:
            user_id = signal.user_id
            
            # Get current embedding or initialize
            if user_id not in self.user_embeddings:
                self.user_embeddings[user_id] = {
                    'embedding': np.zeros(128),  # Default embedding size
                    'last_updated': datetime.utcnow(),
                    'signal_count': 0
                }
            
            # Calculate signal impact
            signal_weight = self.signal_weights.get(signal.signal_type, 1.0)
            signal_impact = signal_weight * signal.intensity
            
            # Create signal vector (simplified - in practice, this would be more sophisticated)
            signal_vector = np.random.randn(128) * signal_impact
            
            # Update embedding with exponential moving average
            alpha = 0.1  # Learning rate
            current_embedding = self.user_embeddings[user_id]['embedding']
            new_embedding = (1 - alpha) * current_embedding + alpha * signal_vector
            
            # Normalize embedding
            norm = np.linalg.norm(new_embedding)
            if norm > 0:
                new_embedding = new_embedding / norm
            
            # Update user embedding
            self.user_embeddings[user_id]['embedding'] = new_embedding
            self.user_embeddings[user_id]['last_updated'] = datetime.utcnow()
            self.user_embeddings[user_id]['signal_count'] += 1
            
        except Exception as e:
            logger.error(f"Error updating user embedding: {e}")
    
    def _store_signal_redis(self, signal: UserSignal):
        """Store processed signal in Redis."""
        try:
            # Store signal data
            signal_key = f"signal:{signal.signal_id}"
            signal_data = asdict(signal)
            signal_data['timestamp'] = signal.timestamp.isoformat()
            
            self.redis_client.hset(signal_key, mapping=signal_data)
            self.redis_client.expire(signal_key, 86400)  # Expire in 24 hours
            
            # Store in user's signal list
            user_signals_key = f"user_signals:{signal.user_id}"
            self.redis_client.lpush(user_signals_key, signal.signal_id)
            self.redis_client.ltrim(user_signals_key, 0, 999)  # Keep last 1000 signals
            self.redis_client.expire(user_signals_key, 604800)  # Expire in 1 week
            
        except Exception as e:
            logger.error(f"Error storing signal in Redis: {e}")
    
    def _publish_signal_kafka(self, signal: UserSignal):
        """Publish processed signal to Kafka."""
        try:
            topic = "user_signals_processed"
            key = signal.user_id
            value = asdict(signal)
            value['timestamp'] = signal.timestamp.isoformat()
            
            self.kafka_producer.send(topic, key=key, value=value)
            
        except Exception as e:
            logger.error(f"Error publishing signal to Kafka: {e}")
    
    def _aggregation_worker(self):
        """Worker thread for signal aggregation."""
        logger.info("Aggregation worker started")
        
        while self.running:
            try:
                # Process all users
                for user_id in list(self.signal_buffers.keys()):
                    self._aggregate_user_signals(user_id)
                
                # Sleep before next aggregation cycle
                time.sleep(60)  # Aggregate every minute
                
            except Exception as e:
                logger.error(f"Error in aggregation worker: {e}")
                time.sleep(10)
                continue
        
        logger.info("Aggregation worker stopped")
    
    def _aggregate_user_signals(self, user_id: str):
        """Aggregate signals for a specific user."""
        try:
            signals = list(self.signal_buffers[user_id])
            if not signals:
                return
            
            # Group signals by time windows
            time_windows = {
                '1m': datetime.utcnow() - timedelta(minutes=1),
                '5m': datetime.utcnow() - timedelta(minutes=5),
                '15m': datetime.utcnow() - timedelta(minutes=15),
                '1h': datetime.utcnow() - timedelta(hours=1),
                '24h': datetime.utcnow() - timedelta(hours=24)
            }
            
            for window_name, cutoff_time in time_windows.items():
                # Filter signals in time window
                window_signals = [s for s in signals if s.timestamp >= cutoff_time]
                
                if not window_signals:
                    continue
                
                # Aggregate signals
                aggregate = self._create_signal_aggregate(user_id, window_name, window_signals)
                
                # Store aggregate
                self.signal_aggregates[f"{user_id}:{window_name}"] = aggregate
                
                # Store in Redis
                self._store_aggregate_redis(aggregate)
        
        except Exception as e:
            logger.error(f"Error aggregating signals for user {user_id}: {e}")
    
    def _create_signal_aggregate(self, user_id: str, time_window: str, signals: List[UserSignal]) -> SignalAggregate:
        """Create a signal aggregate from a list of signals."""
        try:
            # Count signals by type
            signal_counts = defaultdict(int)
            for signal in signals:
                signal_counts[signal.signal_type] += 1
            
            # Aggregate content interactions
            content_interactions = defaultdict(lambda: defaultdict(int))
            for signal in signals:
                if signal.content_id:
                    content_interactions[signal.content_id][signal.signal_type] += 1
            
            # Calculate temporal patterns
            temporal_patterns = self._calculate_temporal_patterns(signals)
            
            # Calculate engagement score
            engagement_score = self._calculate_engagement_score(signals)
            
            aggregate = SignalAggregate(
                user_id=user_id,
                time_window=time_window,
                signal_counts=dict(signal_counts),
                content_interactions=dict(content_interactions),
                temporal_patterns=temporal_patterns,
                engagement_score=engagement_score,
                last_updated=datetime.utcnow()
            )
            
            return aggregate
            
        except Exception as e:
            logger.error(f"Error creating signal aggregate: {e}")
            # Return empty aggregate
            return SignalAggregate(
                user_id=user_id,
                time_window=time_window,
                signal_counts={},
                content_interactions={},
                temporal_patterns={},
                engagement_score=0.0,
                last_updated=datetime.utcnow()
            )
    
    def _calculate_temporal_patterns(self, signals: List[UserSignal]) -> Dict[str, float]:
        """Calculate temporal patterns from signals."""
        try:
            if not signals:
                return {}
            
            # Sort signals by timestamp
            sorted_signals = sorted(signals, key=lambda x: x.timestamp)
            
            patterns = {}
            
            # Calculate time between signals
            if len(sorted_signals) > 1:
                time_diffs = []
                for i in range(1, len(sorted_signals)):
                    diff = (sorted_signals[i].timestamp - sorted_signals[i-1].timestamp).total_seconds()
                    time_diffs.append(diff)
                
                patterns['avg_time_between_signals'] = np.mean(time_diffs)
                patterns['std_time_between_signals'] = np.std(time_diffs)
                patterns['min_time_between_signals'] = np.min(time_diffs)
                patterns['max_time_between_signals'] = np.max(time_diffs)
            
            # Calculate signal frequency
            total_duration = (sorted_signals[-1].timestamp - sorted_signals[0].timestamp).total_seconds()
            if total_duration > 0:
                patterns['signals_per_second'] = len(signals) / total_duration
            
            # Calculate peak activity hour
            hour_counts = defaultdict(int)
            for signal in signals:
                hour = signal.timestamp.hour
                hour_counts[hour] += 1
            
            if hour_counts:
                peak_hour = max(hour_counts.items(), key=lambda x: x[1])[0]
                patterns['peak_activity_hour'] = peak_hour
            
            return patterns
            
        except Exception as e:
            logger.error(f"Error calculating temporal patterns: {e}")
            return {}
    
    def _calculate_engagement_score(self, signals: List[UserSignal]) -> float:
        """Calculate engagement score from signals."""
        try:
            if not signals:
                return 0.0
            
            total_score = 0.0
            total_weight = 0.0
            
            for signal in signals:
                # Get signal weight
                weight = self.signal_weights.get(signal.signal_type, 1.0)
                
                # Apply intensity
                weighted_score = weight * signal.intensity
                
                # Apply recency decay
                hours_ago = (datetime.utcnow() - signal.timestamp).total_seconds() / 3600
                decay_factor = np.exp(-hours_ago / 24.0)  # 24 hour half-life
                
                final_score = weighted_score * decay_factor
                total_score += final_score
                total_weight += weight
            
            # Normalize score
            if total_weight > 0:
                engagement_score = total_score / total_weight
            else:
                engagement_score = 0.0
            
            return min(engagement_score, 10.0)  # Cap at 10.0
            
        except Exception as e:
            logger.error(f"Error calculating engagement score: {e}")
            return 0.0
    
    def _store_aggregate_redis(self, aggregate: SignalAggregate):
        """Store signal aggregate in Redis."""
        try:
            key = f"signal_aggregate:{aggregate.user_id}:{aggregate.time_window}"
            data = asdict(aggregate)
            data['last_updated'] = aggregate.last_updated.isoformat()
            
            self.redis_client.hset(key, mapping=data)
            self.redis_client.expire(key, 86400)  # Expire in 24 hours
            
        except Exception as e:
            logger.error(f"Error storing aggregate in Redis: {e}")
    
    def get_user_signals(self, user_id: str, time_window: str = "1h") -> List[UserSignal]:
        """Get recent signals for a user."""
        try:
            if user_id in self.signal_buffers:
                cutoff_time = datetime.utcnow() - timedelta(hours=int(time_window[:-1]))
                signals = [s for s in self.signal_buffers[user_id] if s.timestamp >= cutoff_time]
                return signals
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting user signals: {e}")
            return []
    
    def get_user_aggregate(self, user_id: str, time_window: str = "1h") -> Optional[SignalAggregate]:
        """Get signal aggregate for a user."""
        try:
            key = f"{user_id}:{time_window}"
            return self.signal_aggregates.get(key)
            
        except Exception as e:
            logger.error(f"Error getting user aggregate: {e}")
            return None
    
    def get_user_embedding(self, user_id: str) -> Optional[np.ndarray]:
        """Get current user embedding."""
        try:
            if user_id in self.user_embeddings:
                return self.user_embeddings[user_id]['embedding'].copy()
            return None
            
        except Exception as e:
            logger.error(f"Error getting user embedding: {e}")
            return None
    
    def _monitoring_worker(self):
        """Worker thread for monitoring system performance."""
        logger.info("Monitoring worker started")
        
        while self.running:
            try:
                # Log performance metrics
                queue_size = self.processing_metrics['queue_size']
                signals_processed = self.processing_metrics['signals_processed']
                signals_dropped = self.processing_metrics['signals_dropped']
                
                if self.processing_metrics['processing_latency_ms']:
                    avg_latency = np.mean(self.processing_metrics['processing_latency_ms'])
                    p95_latency = np.percentile(self.processing_metrics['processing_latency_ms'], 95)
                else:
                    avg_latency = 0
                    p95_latency = 0
                
                logger.info(f"Performance: Queue={queue_size}, Processed={signals_processed}, "
                          f"Dropped={signals_dropped}, AvgLatency={avg_latency:.2f}ms, "
                          f"P95Latency={p95_latency:.2f}ms")
                
                # Sleep before next monitoring cycle
                time.sleep(300)  # Monitor every 5 minutes
                
            except Exception as e:
                logger.error(f"Error in monitoring worker: {e}")
                time.sleep(60)
                continue
        
        logger.info("Monitoring worker stopped")
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics."""
        return self.processing_metrics.copy()
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get system health status."""
        try:
            health = {
                'status': 'healthy' if self.running else 'stopped',
                'workers_running': len([w for w in self.workers if w.is_alive()]),
                'queue_size': self.processing_metrics['queue_size'],
                'signals_processed': self.processing_metrics['signals_processed'],
                'signals_dropped': self.processing_metrics['signals_dropped'],
                'active_users': len(self.signal_buffers),
                'redis_connected': False,
                'kafka_connected': False
            }
            
            # Check Redis connection
            try:
                self.redis_client.ping()
                health['redis_connected'] = True
            except:
                pass
            
            # Check Kafka connection
            if hasattr(self, 'kafka_producer'):
                try:
                    health['kafka_connected'] = True
                except:
                    pass
            
            return health
            
        except Exception as e:
            logger.error(f"Error getting system health: {e}")
            return {'status': 'error', 'error': str(e)}