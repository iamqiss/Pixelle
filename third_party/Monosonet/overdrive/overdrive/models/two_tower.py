from __future__ import annotations
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from typing import Dict, List, Tuple, Optional
import json
import os

class UserTower(nn.Module):
	"""User tower for learning user representations."""
	
	def __init__(self, input_dim: int, hidden_dims: List[int], output_dim: int, dropout: float = 0.1):
		super().__init__()
		
		layers = []
		prev_dim = input_dim
		
		for hidden_dim in hidden_dims:
			layers.extend([
				nn.Linear(prev_dim, hidden_dim),
				nn.BatchNorm1d(hidden_dim),
				nn.ReLU(),
				nn.Dropout(dropout)
			])
			prev_dim = hidden_dim
		
		layers.append(nn.Linear(prev_dim, output_dim))
		self.network = nn.Sequential(*layers)
		
		# Initialize weights
		self.apply(self._init_weights)
	
	def _init_weights(self, module):
		if isinstance(module, nn.Linear):
			torch.nn.init.xavier_uniform_(module.weight)
			if module.bias is not None:
				torch.nn.init.zeros_(module.bias)
	
	def forward(self, user_features: torch.Tensor) -> torch.Tensor:
		"""Forward pass through user tower."""
		return self.network(user_features)

class ItemTower(nn.Module):
	"""Item tower for learning item representations."""
	
	def __init__(self, input_dim: int, hidden_dims: List[int], output_dim: int, dropout: float = 0.1):
		super().__init__()
		
		layers = []
		prev_dim = input_dim
		
		for hidden_dim in hidden_dims:
			layers.extend([
				nn.Linear(prev_dim, hidden_dim),
				nn.BatchNorm1d(hidden_dim),
				nn.ReLU(),
				nn.Dropout(dropout)
			])
			prev_dim = hidden_dim
		
		layers.append(nn.Linear(prev_dim, output_dim))
		self.network = nn.Sequential(*layers)
		
		# Initialize weights
		self.apply(self._init_weights)
	
	def _init_weights(self, module):
		if isinstance(module, nn.Linear):
			torch.nn.init.xavier_uniform_(module.weight)
			if module.bias is not None:
				torch.nn.init.zeros_(module.bias)
	
	def forward(self, item_features: torch.Tensor) -> torch.Tensor:
		"""Forward pass through item tower."""
		return self.network(item_features)

class TwoTowerModel(nn.Module):
	"""Two-tower model for user-item matching."""
	
	def __init__(self, 
				 user_input_dim: int,
				 item_input_dim: int,
				 shared_dim: int = 128,
				 user_hidden_dims: List[int] = [256, 128],
				 item_hidden_dims: List[int] = [512, 256],
				 dropout: float = 0.1):
		super().__init__()
		
		self.user_tower = UserTower(user_input_dim, user_hidden_dims, shared_dim, dropout)
		self.item_tower = ItemTower(item_input_dim, item_hidden_dims, shared_dim, dropout)
		
		# Temperature parameter for similarity scaling
		self.temperature = nn.Parameter(torch.ones([]) * 0.1)
		
		# Output dimension
		self.shared_dim = shared_dim
	
	def forward(self, user_features: torch.Tensor, item_features: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
		"""Forward pass through both towers."""
		user_embeddings = self.user_tower(user_features)
		item_embeddings = self.item_tower(item_features)
		
		# L2 normalize embeddings for cosine similarity
		user_embeddings = F.normalize(user_embeddings, p=2, dim=1)
		item_embeddings = F.normalize(item_embeddings, p=2, dim=1)
		
		return user_embeddings, item_embeddings
	
	def compute_similarity(self, user_embeddings: torch.Tensor, item_embeddings: torch.Tensor) -> torch.Tensor:
		"""Compute cosine similarity between user and item embeddings."""
		similarity = torch.mm(user_embeddings, item_embeddings.t())
		return similarity / self.temperature
	
	def get_user_embedding(self, user_features: torch.Tensor) -> torch.Tensor:
		"""Get user embedding only."""
		user_embeddings = self.user_tower(user_features)
		return F.normalize(user_embeddings, p=2, dim=1)
	
	def get_item_embedding(self, item_features: torch.Tensor) -> torch.Tensor:
		"""Get item embedding only."""
		item_embeddings = self.item_tower(item_features)
		return F.normalize(item_embeddings, p=2, dim=1)

class TwoTowerTrainer:
	"""Trainer for two-tower model."""
	
	def __init__(self, model: TwoTowerModel, device: str = "cpu"):
		self.model = model.to(device)
		self.device = device
		
		# Optimizer
		self.optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
		
		# Loss function (in-batch negative sampling)
		self.criterion = nn.CrossEntropyLoss()
		
		# Learning rate scheduler
		self.scheduler = torch.optim.lr_scheduler.StepLR(self.optimizer, step_size=1000, gamma=0.9)
	
	def train_step(self, user_features: torch.Tensor, item_features: torch.Tensor, labels: torch.Tensor) -> float:
		"""Single training step."""
		self.model.train()
		
		# Move to device
		user_features = user_features.to(self.device)
		item_features = item_features.to(self.device)
		labels = labels.to(self.device)
		
		# Forward pass
		user_embeddings, item_embeddings = self.model(user_features, item_features)
		
		# Compute similarity matrix
		similarity = self.model.compute_similarity(user_embeddings, item_embeddings)
		
		# Loss (in-batch negative sampling)
		loss = self.criterion(similarity, labels)
		
		# Backward pass
		self.optimizer.zero_grad()
		loss.backward()
		torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=1.0)
		self.optimizer.step()
		self.scheduler.step()
		
		return loss.item()
	
	def save_model(self, path: str):
		"""Save model to disk."""
		os.makedirs(os.path.dirname(path), exist_ok=True)
		
		# Save model state
		torch.save({
			'model_state_dict': self.model.state_dict(),
			'optimizer_state_dict': self.optimizer.state_dict(),
			'scheduler_state_dict': self.scheduler.state_dict(),
		}, path)
		
		# Save model architecture
		model_config = {
			'user_input_dim': self.model.user_tower.network[0].in_features,
			'item_input_dim': self.model.item_tower.network[0].in_features,
			'shared_dim': self.model.shared_dim,
			'user_hidden_dims': [layer.out_features for layer in self.model.user_tower.network if isinstance(layer, nn.Linear)][:-1],
			'item_hidden_dims': [layer.out_features for layer in self.model.item_tower.network if isinstance(layer, nn.Linear)][:-1],
		}
		
		config_path = path.replace('.pth', '_config.json')
		with open(config_path, 'w') as f:
			json.dump(model_config, f, indent=2)
	
	def load_model(self, path: str):
		"""Load model from disk."""
		checkpoint = torch.load(path, map_location=self.device)
		self.model.load_state_dict(checkpoint['model_state_dict'])
		self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
		self.scheduler.load_state_dict(checkpoint['scheduler_state_dict'])

class FeatureProcessor:
	"""Process raw features into model inputs."""
	
	def __init__(self, user_feature_keys: List[str], item_feature_keys: List[str]):
		self.user_feature_keys = user_feature_keys
		self.item_feature_keys = item_feature_keys
		
		# Feature normalization stats
		self.user_feature_stats = {}
		self.item_feature_stats = {}
	
	def process_user_features(self, features: Dict[str, Any]) -> torch.Tensor:
		"""Process user features into tensor."""
		vector = []
		
		for key in self.user_feature_keys:
			value = features.get(key, 0.0)
			
			# Normalize numerical features
			if isinstance(value, (int, float)):
				if key in self.user_feature_stats:
					mean, std = self.user_feature_stats[key]
					value = (value - mean) / (std + 1e-8)
				vector.append(float(value))
			elif isinstance(value, bool):
				vector.append(1.0 if value else 0.0)
			elif isinstance(value, list):
				vector.append(float(len(value)))
			else:
				vector.append(0.0)
		
		return torch.tensor(vector, dtype=torch.float32)
	
	def process_item_features(self, features: Dict[str, Any]) -> torch.Tensor:
		"""Process item features into tensor."""
		vector = []
		
		for key in self.item_feature_keys:
			value = features.get(key, 0.0)
			
			# Normalize numerical features
			if isinstance(value, (int, float)):
				if key in self.item_feature_stats:
					mean, std = self.item_feature_stats[key]
					value = (value - mean) / (std + 1e-8)
				vector.append(float(value))
			elif isinstance(value, bool):
				vector.append(1.0 if value else 0.0)
			elif isinstance(value, list):
				vector.append(float(len(value)))
			else:
				vector.append(0.0)
		
		return torch.tensor(vector, dtype=torch.float32)
	
	def update_stats(self, user_features_list: List[Dict[str, Any]], item_features_list: List[Dict[str, Any]]):
		"""Update feature normalization statistics."""
		# User features
		for key in self.user_feature_keys:
			values = [f.get(key, 0.0) for f in user_features_list if isinstance(f.get(key), (int, float))]
			if values:
				mean = np.mean(values)
				std = np.std(values)
				self.user_feature_stats[key] = (mean, std)
		
		# Item features
		for key in self.item_feature_keys:
			values = [f.get(key, 0.0) for f in item_features_list if isinstance(f.get(key), (int, float))]
			if values:
				mean = np.mean(values)
				std = np.std(values)
				self.item_feature_stats[key] = (mean, std)