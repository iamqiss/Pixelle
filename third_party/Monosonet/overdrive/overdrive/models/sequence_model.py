import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from typing import Dict, List, Tuple, Optional
import json
import os

class PositionalEncoding(nn.Module):
	"""Positional encoding for Transformer."""
	
	def __init__(self, d_model: int, max_len: int = 5000):
		super().__init__()
		
		pe = torch.zeros(max_len, d_model)
		position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
		div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-np.log(10000.0) / d_model))
		
		pe[:, 0::2] = torch.sin(position * div_term)
		pe[:, 1::2] = torch.cos(position * div_term)
		pe = pe.unsqueeze(0).transpose(0, 1)
		
		self.register_buffer('pe', pe)
	
	def forward(self, x: torch.Tensor) -> torch.Tensor:
		return x + self.pe[:x.size(0), :]

class SequenceModel(nn.Module):
	"""Transformer-based sequence model for next-item prediction."""
	
	def __init__(self, 
				 item_dim: int = 128,
				 hidden_dim: int = 256,
				 num_layers: int = 4,
				 num_heads: int = 8,
				 dropout: float = 0.1,
				 max_seq_len: int = 100):
		super().__init__()
		
		self.item_dim = item_dim
		self.hidden_dim = hidden_dim
		self.max_seq_len = max_seq_len
		
		# Item embedding layer
		self.item_embedding = nn.Linear(item_dim, hidden_dim)
		
		# Positional encoding
		self.pos_encoding = PositionalEncoding(hidden_dim, max_seq_len)
		
		# Transformer encoder
		encoder_layer = nn.TransformerEncoderLayer(
			d_model=hidden_dim,
			nhead=num_heads,
			dim_feedforward=hidden_dim * 4,
			dropout=dropout,
			batch_first=False
		)
		self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
		
		# Output layers
		self.output_projection = nn.Linear(hidden_dim, item_dim)
		self.dropout = nn.Dropout(dropout)
		
		# Initialize weights
		self.apply(self._init_weights)
	
	def _init_weights(self, module):
		if isinstance(module, nn.Linear):
			torch.nn.init.xavier_uniform_(module.weight)
			if module.bias is not None:
				torch.nn.init.zeros_(module.bias)
	
	def forward(self, item_sequence: torch.Tensor, attention_mask: Optional[torch.Tensor] = None) -> torch.Tensor:
		"""
		Forward pass through sequence model.
		
		Args:
			item_sequence: [seq_len, batch_size, item_dim]
			attention_mask: [seq_len, seq_len] or None
		
		Returns:
			Output embeddings: [seq_len, batch_size, item_dim]
		"""
		seq_len, batch_size, _ = item_sequence.shape
		
		# Project items to hidden dimension
		item_embeddings = self.item_embedding(item_sequence)  # [seq_len, batch_size, hidden_dim]
		
		# Add positional encoding
		item_embeddings = self.pos_encoding(item_embeddings)
		
		# Apply dropout
		item_embeddings = self.dropout(item_embeddings)
		
		# Transformer encoding
		if attention_mask is not None:
			# Convert to causal mask for autoregressive generation
			causal_mask = torch.triu(torch.ones(seq_len, seq_len), diagonal=1).bool()
			attention_mask = attention_mask & ~causal_mask
		
		encoded = self.transformer(item_embeddings, src_key_padding_mask=attention_mask)
		
		# Project back to item dimension
		output = self.output_projection(encoded)
		
		return output
	
	def predict_next_item(self, item_sequence: torch.Tensor, candidate_items: torch.Tensor) -> torch.Tensor:
		"""
		Predict next item from sequence and candidate items.
		
		Args:
			item_sequence: [seq_len, batch_size, item_dim]
			candidate_items: [num_candidates, item_dim]
		
		Returns:
			Prediction scores: [batch_size, num_candidates]
		"""
		# Get sequence representation
		sequence_output = self.forward(item_sequence)  # [seq_len, batch_size, item_dim]
		
		# Use last position for prediction
		last_hidden = sequence_output[-1]  # [batch_size, item_dim]
		
		# Compute similarity with candidate items
		# Normalize both vectors for cosine similarity
		last_hidden_norm = F.normalize(last_hidden, p=2, dim=1)  # [batch_size, item_dim]
		candidate_norm = F.normalize(candidate_items, p=2, dim=1)  # [num_candidates, item_dim]
		
		# Compute similarity scores
		scores = torch.mm(last_hidden_norm, candidate_norm.t())  # [batch_size, num_candidates]
		
		return scores
	
	def get_sequence_representation(self, item_sequence: torch.Tensor) -> torch.Tensor:
		"""Get the final sequence representation for a given sequence."""
		with torch.no_grad():
			sequence_output = self.forward(item_sequence)
			return sequence_output[-1]  # Return last position

class SequenceTrainer:
	"""Trainer for sequence model."""
	
	def __init__(self, model: SequenceModel, device: str = "cpu"):
		self.model = model.to(device)
		self.device = device
		
		# Optimizer
		self.optimizer = torch.optim.AdamW(model.parameters(), lr=1e-4, weight_decay=1e-4)
		
		# Loss function
		self.criterion = nn.CrossEntropyLoss()
		
		# Learning rate scheduler
		self.scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(self.optimizer, T_max=1000)
	
	def train_step(self, item_sequences: torch.Tensor, target_items: torch.Tensor, 
				   attention_mask: Optional[torch.Tensor] = None) -> float:
		"""Single training step."""
		self.model.train()
		
		# Move to device
		item_sequences = item_sequences.to(self.device)
		target_items = target_items.to(self.device)
		if attention_mask is not None:
			attention_mask = attention_mask.to(self.device)
		
		# Forward pass
		sequence_output = self.model(item_sequences, attention_mask)
		
		# Shift sequences for next-item prediction
		input_sequence = sequence_output[:-1]  # [seq_len-1, batch_size, item_dim]
		target_sequence = target_items[1:]     # [seq_len-1, batch_size, item_dim]
		
		# Compute loss (predict next item at each position)
		loss = 0.0
		for i in range(input_sequence.size(0)):
			predicted = input_sequence[i]  # [batch_size, item_dim]
			target = target_sequence[i]    # [batch_size, item_dim]
			
			# Convert to classification problem
			# Project predictions to vocabulary size (simplified)
			predicted_proj = F.linear(predicted, target)  # [batch_size, batch_size]
			target_indices = torch.arange(predicted_proj.size(0), device=self.device)
			
			step_loss = self.criterion(predicted_proj, target_indices)
			loss += step_loss
		
		loss = loss / input_sequence.size(0)
		
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
		
		torch.save({
			'model_state_dict': self.model.state_dict(),
			'optimizer_state_dict': self.optimizer.state_dict(),
			'scheduler_state_dict': self.scheduler.state_dict(),
		}, path)
		
		# Save model config
		model_config = {
			'item_dim': self.model.item_dim,
			'hidden_dim': self.model.hidden_dim,
			'num_layers': self.model.transformer.num_layers,
			'num_heads': self.model.transformer.layers[0].self_attn.num_heads,
			'dropout': self.model.dropout.p,
			'max_seq_len': self.model.max_seq_len
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

class SequenceRetriever:
	"""Retriever using trained sequence model."""
	
	def __init__(self, model: SequenceModel, device: str = "cpu"):
		self.model = model.to(device)
		self.device = device
		self.model.eval()
	
	def get_next_item_scores(self, item_sequence: torch.Tensor, 
							candidate_items: torch.Tensor) -> torch.Tensor:
		"""Get scores for next item prediction."""
		with torch.no_grad():
			item_sequence = item_sequence.to(self.device)
			candidate_items = candidate_items.to(self.device)
			
			scores = self.model.predict_next_item(item_sequence, candidate_items)
			return scores
	
	def get_sequence_embedding(self, item_sequence: torch.Tensor) -> torch.Tensor:
		"""Get embedding for a sequence."""
		with torch.no_grad():
			item_sequence = item_sequence.to(self.device)
			embedding = self.model.get_sequence_representation(item_sequence)
			return embedding

class SequenceDataProcessor:
	"""Process data for sequence model training."""
	
	def __init__(self, max_seq_len: int = 50):
		self.max_seq_len = max_seq_len
	
	def create_sequences(self, user_sessions: List[List[str]]) -> Tuple[torch.Tensor, torch.Tensor]:
		"""
		Create training sequences from user sessions.
		
		Args:
			user_sessions: List of user sessions, each session is a list of item IDs
		
		Returns:
			Tuple of (input_sequences, target_sequences)
		"""
		sequences = []
		targets = []
		
		for session in user_sessions:
			if len(session) < 2:
				continue
			
			# Create sliding window sequences
			for i in range(1, len(session)):
				seq = session[max(0, i-self.max_seq_len):i]
				target = session[i]
				
				# Pad sequence to max length
				if len(seq) < self.max_seq_len:
					seq = [0] * (self.max_seq_len - len(seq)) + seq
				
				sequences.append(seq)
				targets.append(target)
		
		return torch.tensor(sequences, dtype=torch.long), torch.tensor(targets, dtype=torch.long)
	
	def create_attention_mask(self, sequences: torch.Tensor) -> torch.Tensor:
		"""Create attention mask for padded sequences."""
		mask = (sequences != 0).float()  # [batch_size, seq_len]
		return mask