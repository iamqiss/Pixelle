from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple, Union
import numpy as np
import pandas as pd
import scipy.sparse as sp
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.decomposition import NMF, TruncatedSVD
from sklearn.preprocessing import StandardScaler
import implicit
import lightfm
from lightfm import LightFM
from lightfm.evaluation import precision_at_k, recall_at_k
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
import asyncio
import json
import pickle
import os

logger = logging.getLogger(__name__)

@dataclass
class UserItemInteraction:
    """User-item interaction data."""
    user_id: str
    item_id: str
    interaction_type: str  # view, like, comment, share, follow, etc.
    timestamp: datetime
    duration: float = 0.0
    intensity: float = 1.0  # Interaction strength
    context: Optional[Dict[str, Any]] = None

@dataclass
class CollaborativeFilteringResult:
    """Result from collaborative filtering."""
    user_id: str
    item_id: str
    score: float
    method: str
    confidence: float
    explanation: str

class AdvancedCollaborativeFiltering:
    """Advanced collaborative filtering system with multiple algorithms."""
    
    def __init__(self, 
                 user_embedding_dim: int = 128,
                 item_embedding_dim: int = 128,
                 n_factors: int = 100,
                 learning_rate: float = 0.01,
                 regularization: float = 0.01):
        
        self.user_embedding_dim = user_embedding_dim
        self.item_embedding_dim = item_embedding_dim
        self.n_factors = n_factors
        self.learning_rate = learning_rate
        self.regularization = regularization
        
        # Data structures
        self.user_item_matrix = None
        self.user_item_interactions = []
        self.user_mapping = {}
        self.item_mapping = {}
        self.reverse_user_mapping = {}
        self.reverse_item_mapping = {}
        
        # Models
        self.matrix_factorization_model = None
        self.neighborhood_model = None
        self.lightfm_model = None
        self.implicit_model = None
        
        # Feature scalers
        self.user_scaler = StandardScaler()
        self.item_scaler = StandardScaler()
        
        # Model performance tracking
        self.model_performance = {}
        
        logger.info("AdvancedCollaborativeFiltering initialized")
    
    def add_interaction(self, interaction: UserItemInteraction):
        """Add a new user-item interaction."""
        self.user_item_interactions.append(interaction)
        
        # Update mappings
        if interaction.user_id not in self.user_mapping:
            user_idx = len(self.user_mapping)
            self.user_mapping[interaction.user_id] = user_idx
            self.reverse_user_mapping[user_idx] = interaction.user_id
        
        if interaction.item_id not in self.item_mapping:
            item_idx = len(self.item_mapping)
            self.item_mapping[interaction.item_id] = item_idx
            self.reverse_item_mapping[item_idx] = interaction.item_id
    
    def _build_interaction_matrix(self) -> sp.csr_matrix:
        """Build sparse interaction matrix from interactions."""
        if not self.user_item_interactions:
            return sp.csr_matrix((0, 0))
        
        # Create interaction weights
        rows, cols, data = [], [], []
        
        for interaction in self.user_item_interactions:
            user_idx = self.user_mapping[interaction.user_id]
            item_idx = self.item_mapping[interaction.item_id]
            
            # Calculate interaction weight based on type and intensity
            weight = self._calculate_interaction_weight(interaction)
            
            rows.append(user_idx)
            cols.append(item_idx)
            data.append(weight)
        
        # Build sparse matrix
        matrix = sp.csr_matrix(
            (data, (rows, cols)),
            shape=(len(self.user_mapping), len(self.item_mapping))
        )
        
        return matrix
    
    def _calculate_interaction_weight(self, interaction: UserItemInteraction) -> float:
        """Calculate weight for an interaction based on type and context."""
        base_weights = {
            'view': 1.0,
            'like': 2.0,
            'comment': 3.0,
            'share': 4.0,
            'follow': 5.0,
            'bookmark': 2.5,
            'click': 1.5
        }
        
        base_weight = base_weights.get(interaction.interaction_type, 1.0)
        
        # Apply duration factor for view interactions
        if interaction.interaction_type == 'view' and interaction.duration > 0:
            duration_factor = min(interaction.duration / 60.0, 3.0)  # Cap at 3x
            base_weight *= (1.0 + duration_factor)
        
        # Apply intensity factor
        base_weight *= interaction.intensity
        
        # Apply recency factor (newer interactions get higher weight)
        hours_ago = (datetime.utcnow() - interaction.timestamp).total_seconds() / 3600
        recency_factor = np.exp(-hours_ago / 168.0)  # 1 week half-life
        base_weight *= (0.5 + 0.5 * recency_factor)
        
        return base_weight
    
    def _train_matrix_factorization(self, interaction_matrix: sp.csr_matrix):
        """Train matrix factorization model."""
        try:
            # Use NMF for non-negative matrix factorization
            self.matrix_factorization_model = NMF(
                n_components=self.n_factors,
                init='random',
                random_state=42,
                max_iter=200,
                alpha=self.regularization
            )
            
            # Fit the model
            user_factors = self.matrix_factorization_model.fit_transform(interaction_matrix)
            item_factors = self.matrix_factorization_model.components_.T
            
            # Store factors
            self.user_factors = user_factors
            self.item_factors = item_factors
            
            logger.info("Matrix factorization model trained successfully")
            
        except Exception as e:
            logger.error(f"Error training matrix factorization: {e}")
    
    def _train_neighborhood_model(self, interaction_matrix: sp.csr_matrix):
        """Train neighborhood-based collaborative filtering."""
        try:
            # Calculate user-user similarity matrix
            user_similarity = cosine_similarity(interaction_matrix)
            
            # Store similarity matrix
            self.user_similarity = user_similarity
            
            # Calculate item-item similarity matrix
            item_similarity = cosine_similarity(interaction_matrix.T)
            self.item_similarity = item_similarity
            
            logger.info("Neighborhood model trained successfully")
            
        except Exception as e:
            logger.error(f"Error training neighborhood model: {e}")
    
    def _train_lightfm_model(self, interaction_matrix: sp.csr_matrix):
        """Train LightFM hybrid model."""
        try:
            # Initialize LightFM model
            self.lightfm_model = LightFM(
                no_components=self.n_factors,
                learning_rate=self.learning_rate,
                loss='warp',  # WARP loss for ranking
                random_state=42
            )
            
            # Train the model
            self.lightfm_model.fit(
                interaction_matrix,
                epochs=30,
                verbose=True
            )
            
            logger.info("LightFM model trained successfully")
            
        except Exception as e:
            logger.error(f"Error training LightFM model: {e}")
    
    def _train_implicit_model(self, interaction_matrix: sp.csr_matrix):
        """Train implicit feedback model."""
        try:
            # Initialize implicit ALS model
            self.implicit_model = implicit.als.AlternatingLeastSquares(
                factors=self.n_factors,
                regularization=self.regularization,
                iterations=50,
                random_state=42
            )
            
            # Train the model
            self.implicit_model.fit(interaction_matrix)
            
            logger.info("Implicit model trained successfully")
            
        except Exception as e:
            logger.error(f"Error training implicit model: {e}")
    
    def train_all_models(self):
        """Train all collaborative filtering models."""
        if not self.user_item_interactions:
            logger.warning("No interactions available for training")
            return
        
        # Build interaction matrix
        self.user_item_matrix = self._build_interaction_matrix()
        
        if self.user_item_matrix.nnz == 0:
            logger.warning("Empty interaction matrix")
            return
        
        logger.info(f"Training models with {self.user_item_matrix.nnz} interactions")
        
        # Train all models
        self._train_matrix_factorization(self.user_item_matrix)
        self._train_neighborhood_model(self.user_item_matrix)
        self._train_lightfm_model(self.user_item_matrix)
        self._train_implicit_model(self.user_item_matrix)
        
        logger.info("All collaborative filtering models trained")
    
    def get_recommendations(self, 
                          user_id: str, 
                          n_recommendations: int = 20,
                          methods: Optional[List[str]] = None) -> List[CollaborativeFilteringResult]:
        """Get recommendations for a user using multiple methods."""
        
        if user_id not in self.user_mapping:
            logger.warning(f"User {user_id} not found in training data")
            return []
        
        if methods is None:
            methods = ['matrix_factorization', 'neighborhood', 'lightfm', 'implicit']
        
        all_recommendations = []
        
        for method in methods:
            try:
                if method == 'matrix_factorization' and self.matrix_factorization_model is not None:
                    recs = self._get_matrix_factorization_recommendations(user_id, n_recommendations)
                    all_recommendations.extend(recs)
                
                elif method == 'neighborhood' and hasattr(self, 'user_similarity'):
                    recs = self._get_neighborhood_recommendations(user_id, n_recommendations)
                    all_recommendations.extend(recs)
                
                elif method == 'lightfm' and self.lightfm_model is not None:
                    recs = self._get_lightfm_recommendations(user_id, n_recommendations)
                    all_recommendations.extend(recs)
                
                elif method == 'implicit' and self.implicit_model is not None:
                    recs = self._get_implicit_recommendations(user_id, n_recommendations)
                    all_recommendations.extend(recs)
                    
            except Exception as e:
                logger.error(f"Error getting recommendations with {method}: {e}")
        
        # Aggregate and rank recommendations
        aggregated_recs = self._aggregate_recommendations(all_recommendations, n_recommendations)
        
        return aggregated_recs
    
    def _get_matrix_factorization_recommendations(self, user_id: str, n_recommendations: int) -> List[CollaborativeFilteringResult]:
        """Get recommendations using matrix factorization."""
        user_idx = self.user_mapping[user_id]
        user_vector = self.user_factors[user_idx]
        
        # Calculate scores for all items
        scores = np.dot(self.item_factors, user_vector)
        
        # Get top items
        top_indices = np.argsort(scores)[::-1][:n_recommendations]
        
        recommendations = []
        for idx in top_indices:
            item_id = self.reverse_item_mapping[idx]
            score = scores[idx]
            
            rec = CollaborativeFilteringResult(
                user_id=user_id,
                item_id=item_id,
                score=float(score),
                method='matrix_factorization',
                confidence=min(abs(score) / 10.0, 1.0),
                explanation=f"Matrix factorization score: {score:.3f}"
            )
            recommendations.append(rec)
        
        return recommendations
    
    def _get_neighborhood_recommendations(self, user_id: str, n_recommendations: int) -> List[CollaborativeFilteringResult]:
        """Get recommendations using neighborhood-based CF."""
        user_idx = self.user_mapping[user_id]
        
        # Find similar users
        user_similarities = self.user_similarity[user_idx]
        similar_users = np.argsort(user_similarities)[::-1][1:11]  # Top 10 similar users
        
        # Get items liked by similar users
        item_scores = {}
        for similar_user_idx in similar_users:
            similarity = user_similarities[similar_user_idx]
            if similarity < 0.1:  # Only consider users with reasonable similarity
                continue
            
            # Get items this user has interacted with
            user_items = self.user_item_matrix[similar_user_idx].nonzero()[1]
            for item_idx in user_items:
                if item_idx not in item_scores:
                    item_scores[item_idx] = 0.0
                item_scores[item_idx] += similarity
        
        # Sort by score and get top items
        sorted_items = sorted(item_scores.items(), key=lambda x: x[1], reverse=True)
        
        recommendations = []
        for item_idx, score in sorted_items[:n_recommendations]:
            item_id = self.reverse_item_mapping[item_idx]
            
            rec = CollaborativeFilteringResult(
                user_id=user_id,
                item_id=item_id,
                score=float(score),
                method='neighborhood',
                confidence=min(score / 10.0, 1.0),
                explanation=f"Neighborhood-based score: {score:.3f}"
            )
            recommendations.append(rec)
        
        return recommendations
    
    def _get_lightfm_recommendations(self, user_id: str, n_recommendations: int) -> List[CollaborativeFilteringResult]:
        """Get recommendations using LightFM."""
        user_idx = self.user_mapping[user_id]
        
        # Get scores for all items
        scores = self.lightfm_model.predict(user_idx, np.arange(len(self.item_mapping)))
        
        # Get top items
        top_indices = np.argsort(scores)[::-1][:n_recommendations]
        
        recommendations = []
        for idx in top_indices:
            item_id = self.reverse_item_mapping[idx]
            score = scores[idx]
            
            rec = CollaborativeFilteringResult(
                user_id=user_id,
                item_id=item_id,
                score=float(score),
                method='lightfm',
                confidence=min(abs(score) / 10.0, 1.0),
                explanation=f"LightFM score: {score:.3f}"
            )
            recommendations.append(rec)
        
        return recommendations
    
    def _get_implicit_recommendations(self, user_id: str, n_recommendations: int) -> List[CollaborativeFilteringResult]:
        """Get recommendations using implicit feedback model."""
        user_idx = self.user_mapping[user_id]
        
        # Get recommendations from implicit model
        item_scores, item_indices = self.implicit_model.recommend(
            user_idx, 
            self.user_item_matrix[user_idx], 
            N=n_recommendations
        )
        
        recommendations = []
        for score, item_idx in zip(item_scores, item_indices):
            item_id = self.reverse_item_mapping[item_idx]
            
            rec = CollaborativeFilteringResult(
                user_id=user_id,
                item_id=item_id,
                score=float(score),
                method='implicit',
                confidence=min(abs(score) / 10.0, 1.0),
                explanation=f"Implicit feedback score: {score:.3f}"
            )
            recommendations.append(rec)
        
        return recommendations
    
    def _aggregate_recommendations(self, 
                                 all_recommendations: List[CollaborativeFilteringResult],
                                 n_recommendations: int) -> List[CollaborativeFilteringResult]:
        """Aggregate recommendations from multiple methods."""
        
        # Group by item
        item_scores = {}
        item_methods = {}
        item_explanations = {}
        
        for rec in all_recommendations:
            if rec.item_id not in item_scores:
                item_scores[rec.item_id] = []
                item_methods[rec.item_id] = []
                item_explanations[rec.item_id] = []
            
            item_scores[rec.item_id].append(rec.score)
            item_methods[rec.item_id].append(rec.method)
            item_explanations[rec.item_id].append(rec.explanation)
        
        # Calculate aggregated scores
        aggregated_recs = []
        for item_id, scores in item_scores.items():
            # Weighted average of scores
            avg_score = np.mean(scores)
            
            # Boost confidence if multiple methods agree
            confidence_boost = min(len(scores) / len(set(item_methods[item_id])), 2.0)
            
            # Combine explanations
            combined_explanation = f"Combined score from {len(scores)} methods: {avg_score:.3f}"
            
            rec = CollaborativeFilteringResult(
                user_id=all_recommendations[0].user_id,
                item_id=item_id,
                score=float(avg_score),
                method='ensemble',
                confidence=min(confidence_boost, 1.0),
                explanation=combined_explanation
            )
            aggregated_recs.append(rec)
        
        # Sort by score and return top N
        aggregated_recs.sort(key=lambda x: x.score, reverse=True)
        return aggregated_recs[:n_recommendations]
    
    def update_model_incremental(self, new_interactions: List[UserItemInteraction]):
        """Update models incrementally with new interactions."""
        # Add new interactions
        for interaction in new_interactions:
            self.add_interaction(interaction)
        
        # Retrain models with updated data
        self.train_all_models()
        
        logger.info(f"Models updated with {len(new_interactions)} new interactions")
    
    def evaluate_models(self, test_interactions: List[UserItemInteraction]) -> Dict[str, float]:
        """Evaluate model performance on test data."""
        if not test_interactions:
            return {}
        
        # Build test matrix
        test_matrix = self._build_test_matrix(test_interactions)
        
        metrics = {}
        
        # Evaluate each model
        if self.matrix_factorization_model is not None:
            metrics['mf_precision'] = self._evaluate_precision(test_matrix, 'matrix_factorization')
            metrics['mf_recall'] = self._evaluate_recall(test_matrix, 'matrix_factorization')
        
        if self.lightfm_model is not None:
            metrics['lightfm_precision'] = self._evaluate_precision(test_matrix, 'lightfm')
            metrics['lightfm_recall'] = self._evaluate_recall(test_matrix, 'lightfm')
        
        self.model_performance = metrics
        return metrics
    
    def _build_test_matrix(self, test_interactions: List[UserItemInteraction]) -> sp.csr_matrix:
        """Build test interaction matrix."""
        # Implementation for test matrix building
        # This would create a separate matrix for evaluation
        pass
    
    def _evaluate_precision(self, test_matrix: sp.csr_matrix, method: str) -> float:
        """Evaluate precision at k for a specific method."""
        # Implementation for precision evaluation
        pass
    
    def _evaluate_recall(self, test_matrix: sp.csr_matrix, method: str) -> float:
        """Evaluate recall at k for a specific method."""
        # Implementation for recall evaluation
        pass
    
    def save_models(self, directory: str):
        """Save trained models to disk."""
        os.makedirs(directory, exist_ok=True)
        
        # Save model states
        if self.matrix_factorization_model is not None:
            with open(os.path.join(directory, 'mf_model.pkl'), 'wb') as f:
                pickle.dump(self.matrix_factorization_model, f)
        
        if self.lightfm_model is not None:
            with open(os.path.join(directory, 'lightfm_model.pkl'), 'wb') as f:
                pickle.dump(self.lightfm_model, f)
        
        if self.implicit_model is not None:
            with open(os.path.join(directory, 'implicit_model.pkl'), 'wb') as f:
                pickle.dump(self.implicit_model, f)
        
        # Save mappings and data
        data = {
            'user_mapping': self.user_mapping,
            'item_mapping': self.item_mapping,
            'reverse_user_mapping': self.reverse_user_mapping,
            'reverse_item_mapping': self.reverse_item_mapping,
            'model_performance': self.model_performance
        }
        
        with open(os.path.join(directory, 'cf_data.pkl'), 'wb') as f:
            pickle.dump(data, f)
        
        logger.info(f"Models saved to {directory}")
    
    def load_models(self, directory: str):
        """Load trained models from disk."""
        try:
            # Load model states
            if os.path.exists(os.path.join(directory, 'mf_model.pkl')):
                with open(os.path.join(directory, 'mf_model.pkl'), 'rb') as f:
                    self.matrix_factorization_model = pickle.load(f)
            
            if os.path.exists(os.path.join(directory, 'lightfm_model.pkl')):
                with open(os.path.join(directory, 'lightfm_model.pkl'), 'rb') as f:
                    self.lightfm_model = pickle.load(f)
            
            if os.path.exists(os.path.join(directory, 'implicit_model.pkl')):
                with open(os.path.join(directory, 'implicit_model.pkl'), 'rb') as f:
                    self.implicit_model = pickle.load(f)
            
            # Load mappings and data
            if os.path.exists(os.path.join(directory, 'cf_data.pkl')):
                with open(os.path.join(directory, 'cf_data.pkl'), 'rb') as f:
                    data = pickle.load(f)
                    self.user_mapping = data['user_mapping']
                    self.item_mapping = data['item_mapping']
                    self.reverse_user_mapping = data['reverse_user_mapping']
                    self.reverse_item_mapping = data['reverse_item_mapping']
                    self.model_performance = data['model_performance']
            
            logger.info(f"Models loaded from {directory}")
            
        except Exception as e:
            logger.error(f"Error loading models: {e}")
            raise