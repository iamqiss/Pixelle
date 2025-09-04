#!/usr/bin/env python3
"""
Training script for Overdrive ML models.
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from models.two_tower import TwoTowerModel, TwoTowerTrainer, FeatureProcessor
from models.sequence_model import SequenceModel, SequenceTrainer, SequenceDataProcessor
from features.embeddings import FeatureExtractor
from feature_store import FeatureStore

def train_two_tower_model(data_dir: str, output_dir: str, device: str = "cpu"):
	"""Train the two-tower model for user-item matching."""
	print("Training two-tower model...")
	
	# Initialize feature extractor
	extractor = FeatureExtractor()
	
	# Define feature keys for user and item features
	user_feature_keys = [
		"total_interactions", "avg_session_duration", "avg_engagement_score",
		"activity_frequency", "primary_platform"
	]
	
	item_feature_keys = [
		"text_length", "has_media", "media_count", "has_video", "has_image",
		"engagement_rate", "freshness_score", "quality_score", "author_followers"
	]
	
	# Initialize feature processor
	feature_processor = FeatureProcessor(user_feature_keys, item_feature_keys)
	
	# Create model
	model = TwoTowerModel(
		user_input_dim=len(user_feature_keys),
		item_input_dim=len(item_feature_keys),
		shared_dim=128,
		user_hidden_dims=[256, 128],
		item_hidden_dims=[512, 256],
		dropout=0.1
	)
	
	# Initialize trainer
	trainer = TwoTowerTrainer(model, device=device)
	
	# TODO: Load real training data
	# For now, create dummy data
	print("Creating dummy training data...")
	
	# Dummy user features
	user_features_list = []
	for i in range(1000):
		user_features = {
			"total_interactions": i % 100,
			"avg_session_duration": (i % 60) + 10,
			"avg_engagement_score": (i % 10) / 10.0,
			"activity_frequency": (i % 7) + 1,
			"primary_platform": ["ios", "android", "web"][i % 3]
		}
		user_features_list.append(user_features)
	
	# Dummy item features
	item_features_list = []
	for i in range(2000):
		item_features = {
			"text_length": (i % 280) + 10,
			"has_media": bool(i % 3),
			"media_count": i % 5,
			"has_video": bool(i % 4),
			"has_image": bool(i % 2),
			"engagement_rate": (i % 20) / 100.0,
			"freshness_score": (i % 100) / 100.0,
			"quality_score": (i % 80 + 20) / 100.0,
			"author_followers": (i % 10000) + 100
		}
		item_features_list.append(item_features)
	
	# Update feature statistics
	feature_processor.update_stats(user_features_list, item_features_list)
	
	# Create training pairs (positive examples)
	training_pairs = []
	for i in range(5000):
		user_idx = i % len(user_features_list)
		item_idx = i % len(item_features_list)
		
		user_features = user_features_list[user_idx]
		item_features = item_features_list[item_idx]
		
		user_tensor = feature_processor.process_user_features(user_features)
		item_tensor = feature_processor.process_item_features(item_features)
		
		training_pairs.append((user_tensor, item_tensor))
	
	print(f"Created {len(training_pairs)} training pairs")
	
	# Training loop
	print("Starting training...")
	num_epochs = 10
	batch_size = 32
	
	for epoch in range(num_epochs):
		total_loss = 0.0
		num_batches = 0
		
		# Shuffle training pairs
		import random
		random.shuffle(training_pairs)
		
		# Process in batches
		for i in range(0, len(training_pairs), batch_size):
			batch = training_pairs[i:i + batch_size]
			
			if len(batch) < 2:  # Need at least 2 for in-batch negative sampling
				continue
			
			# Prepare batch
			user_features = torch.stack([pair[0] for pair in batch])
			item_features = torch.stack([pair[1] for pair in batch])
			
			# Create labels for in-batch negative sampling
			labels = torch.arange(len(batch), dtype=torch.long)
			
			# Training step
			loss = trainer.train_step(user_features, item_features, labels)
			total_loss += loss
			num_batches += 1
		
		avg_loss = total_loss / max(1, num_batches)
		print(f"Epoch {epoch + 1}/{num_epochs}, Average Loss: {avg_loss:.4f}")
	
	# Save model
	model_path = os.path.join(output_dir, "two_tower_model.pth")
	trainer.save_model(model_path)
	print(f"Two-tower model saved to {model_path}")

def train_sequence_model(data_dir: str, output_dir: str, device: str = "cpu"):
	"""Train the sequence model for next-item prediction."""
	print("Training sequence model...")
	
	# Create model
	model = SequenceModel(
		item_dim=128,
		hidden_dim=256,
		num_layers=4,
		num_heads=8,
		dropout=0.1,
		max_seq_len=50
	)
	
	# Initialize trainer
	trainer = SequenceTrainer(model, device=device)
	
	# TODO: Load real sequence data
	# For now, create dummy sequences
	print("Creating dummy sequence data...")
	
	# Create dummy user sessions
	user_sessions = []
	for user_id in range(100):
		session = []
		for i in range(random.randint(10, 50)):
			item_id = f"item_{random.randint(0, 999)}"
			session.append(item_id)
		user_sessions.append(session)
	
	# Initialize data processor
	data_processor = SequenceDataProcessor(max_seq_len=50)
	
	# Create training sequences
	sequences, targets = data_processor.create_sequences(user_sessions)
	attention_mask = data_processor.create_attention_mask(sequences)
	
	print(f"Created {len(sequences)} training sequences")
	
	# Training loop
	print("Starting training...")
	num_epochs = 5
	batch_size = 16
	
	for epoch in range(num_epochs):
		total_loss = 0.0
		num_batches = 0
		
		# Process in batches
		for i in range(0, len(sequences), batch_size):
			batch_sequences = sequences[i:i + batch_size]
			batch_targets = targets[i:i + batch_size]
			batch_mask = attention_mask[i:i + batch_size]
			
			if len(batch_sequences) < 2:
				continue
			
			# Convert to item embeddings (simplified)
			# In production, you'd use actual item embeddings
			item_dim = 128
			seq_len = batch_sequences.size(1)
			
			# Create dummy item embeddings
			item_embeddings = torch.randn(seq_len, len(batch_sequences), item_dim)
			target_embeddings = torch.randn(seq_len, len(batch_sequences), item_dim)
			
			# Training step
			loss = trainer.train_step(item_embeddings, target_embeddings, batch_mask)
			total_loss += loss
			num_batches += 1
		
		avg_loss = total_loss / max(1, num_batches)
		print(f"Epoch {epoch + 1}/{num_epochs}, Average Loss: {avg_loss:.4f}")
	
	# Save model
	model_path = os.path.join(output_dir, "sequence_model.pth")
	trainer.save_model(model_path)
	print(f"Sequence model saved to {model_path}")

def main():
	parser = argparse.ArgumentParser(description="Train Overdrive ML models")
	parser.add_argument("--data-dir", required=True, help="Directory containing training data")
	parser.add_argument("--output-dir", required=True, help="Directory to save trained models")
	parser.add_argument("--model", choices=["two-tower", "sequence", "all"], default="all", 
					   help="Which model to train")
	parser.add_argument("--device", default="cpu", help="Device to use for training (cpu/cuda)")
	
	args = parser.parse_args()
	
	# Create output directory
	os.makedirs(args.output_dir, exist_ok=True)
	
	# Check device availability
	if args.device == "cuda" and not torch.cuda.is_available():
		print("CUDA not available, falling back to CPU")
		args.device = "cpu"
	
	print(f"Training on device: {args.device}")
	
	# Train models
	if args.model in ["two-tower", "all"]:
		train_two_tower_model(args.data_dir, args.output_dir, args.device)
	
	if args.model in ["sequence", "all"]:
		train_sequence_model(args.data_dir, args.output_dir, args.device)
	
	print("Training completed!")

if __name__ == "__main__":
	import random
	import torch
	main()