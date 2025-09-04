/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

# Messaging Service Documentation

## Overview

The Messaging Service is the core real-time communication engine for Sonet, providing enterprise-grade messaging capabilities including direct messages, group chats, file attachments, end-to-end encryption, and real-time features like typing indicators and read receipts.

## Features

### 🔐 Security First
- **End-to-End Encryption**: AES-256 and E2E encryption support
- **Message Integrity**: Cryptographic verification
- **Secure Key Exchange**: Industry-standard protocols

### 💬 Messaging Capabilities
- **Direct Messages**: 1-on-1 private conversations
- **Group Chats**: Multi-participant conversations (up to 1000 members)
- **Channels**: Broadcast-style messaging
- **Message Types**: Text, images, videos, audio, files, locations
- **Message Reactions**: Emoji reactions and custom responses
- **Reply Threading**: Quote and reply to specific messages

### 📎 Rich Media Support
- **File Attachments**: Documents, images, videos (up to 100MB)
- **Image Processing**: Automatic thumbnail generation
- **Video Compression**: Optimized streaming
- **Location Sharing**: GPS coordinates with maps

### ⚡ Real-Time Features
- **WebSocket Connections**: Instant message delivery
- **Typing Indicators**: Live typing status
- **Read Receipts**: Delivery and read confirmations
- **Online Presence**: User activity status
- **Push Notifications**: Cross-platform alerts

### 🔍 Advanced Features
- **Message Search**: Full-text search across conversations
- **Message History**: Unlimited message retention
- **Export/Import**: Data portability
- **Message Scheduling**: Send messages at specific times
- **Auto-Delete**: Disappearing messages

## API Endpoints

### Authentication
All endpoints require JWT authentication via `Authorization: Bearer <token>` header.

### Core Endpoints

#### Send Message
```http
NOTE /api/v1/messages
Content-Type: application/json
Authorization: Bearer <token>

{
  "chat_id": "550e8400-e29b-41d4-a716-446655440000",
  "content": "Hello, world!",
  "type": "text",
  "encryption": "e2e",
  "reply_to_message_id": "optional-message-id"
}
```

#### Get Messages
```http
GET /api/v1/chats/{chat_id}/messages?limit=50&before=2025-01-01T00:00:00Z
Authorization: Bearer <token>
```

Note: For encrypted chats, the server returns ciphertext envelopes as-is and does not decrypt on the server. Clients must decrypt using their session keys.

#### Create Chat
```http
NOTE /api/v1/chats
Content-Type: application/json
Authorization: Bearer <token>

{
  "name": "Dev Team Chat",
  "description": "Engineering discussions",
  "type": "group",
  "participant_ids": [
    "user1-uuid",
    "user2-uuid",
    "user3-uuid"
  ]
}
```

#### Upload Attachment
```http
NOTE /api/v1/messages/attachments
Content-Type: multipart/form-data
Authorization: Bearer <token>

file: <binary-data>
```

### Real-Time WebSocket API

#### Connection
```javascript
const ws = new WebSocket('wss://localhost:9096/ws?token=<jwt-token>');
```

#### Message Types
```javascript
// Incoming message
{
  "type": "new_message",
  "data": {
    "message_id": "uuid",
    "chat_id": "uuid", 
    "sender_id": "uuid",
    "content": "Hello!",
    "timestamp": "2025-01-01T00:00:00Z"
  }
}

// Typing indicator
{
  "type": "typing",
  "data": {
    "chat_id": "uuid",
    "user_id": "uuid", 
    "is_typing": true
  }
}

// Read receipt
{
  "type": "read_receipt",
  "data": {
    "message_id": "uuid",
    "user_id": "uuid",
    "read_at": "2025-01-01T00:00:00Z"
  }
}
```

## Database Schema

### Tables
- `chats` - Chat rooms and metadata
- `chat_participants` - User membership in chats
- `messages` - All messages (partitioned by date)
- `message_attachments` - File attachments
- `read_receipts` - Message read status
- `typing_indicators` - Real-time typing status
- `message_reactions` - Emoji reactions

### Performance Optimizations
- **Partitioning**: Messages partitioned by month for query performance
- **Indexing**: Optimized indexes for common query patterns
- **Caching**: Redis caching for active conversations
- **Full-Text Search**: postgresql tsvector for message search

## Configuration

### Environment Variables
```bash
# Database
MESSAGING_DB_HOST=localhost
MESSAGING_DB_PORT=5432
MESSAGING_DB_NAME=messaging_service
MESSAGING_DB_USER=messaging_user
MESSAGING_DB_PASSWORD=secure_password

# Redis Cache
MESSAGING_REDIS_HOST=localhost
MESSAGING_REDIS_PORT=6379
MESSAGING_REDIS_DB=2

# Service Configuration
MESSAGING_SERVICE_PORT=8086
WEBSOCKET_PORT=9096
MAX_MESSAGE_SIZE=10485760  # 10MB
MAX_ATTACHMENT_SIZE=104857600  # 100MB
MESSAGE_RETENTION_DAYS=365
TYPING_TIMEOUT_SECONDS=30

# Encryption
ENCRYPTION_ENABLED=true
ENCRYPTION_KEY_ROTATION_DAYS=30
E2E_ENCRYPTION_ENABLED=true

# Rate Limiting
RATE_LIMIT_MESSAGES_PER_MINUTE=60
RATE_LIMIT_UPLOADS_PER_HOUR=50
```

### Service Configuration (JSON)
```json
{
  "messaging_service": {
    "host": "messaging-service",
    "port": 8086,
    "grpc_port": 9090,
    "websocket_port": 9096,
    "features": {
      "encryption_enabled": true,
      "e2e_encryption": true,
      "file_uploads": true,
      "message_reactions": true,
      "typing_indicators": true,
      "read_receipts": true,
      "message_search": true
    },
    "limits": {
      "max_message_size": 10485760,
      "max_attachment_size": 104857600,
      "max_group_participants": 1000,
      "message_retention_days": 365
    },
    "performance": {
      "connection_pool_size": 50,
      "cache_ttl_seconds": 3600,
      "typing_timeout_seconds": 30
    }
  }
}
```

## Monitoring & Metrics

### Key Metrics
- **Message Throughput**: Messages per second
- **WebSocket Connections**: Active real-time connections
- **Delivery Success Rate**: Message delivery percentage
- **Average Latency**: Message delivery time
- **Storage Usage**: Database and file storage consumption
- **Encryption Performance**: Encryption/decryption overhead

### Health Checks
```http
GET /health
GET /metrics  # Prometheus metrics
GET /debug/connections  # WebSocket connection info
```

### Alerts
- High message delivery latency (>500ms)
- WebSocket connection failures
- Database connection issues
- Storage space warnings
- Encryption key rotation failures

## Security Considerations

### Data Protection
- All messages encrypted at rest
- TLS 1.3 for data in transit
- Regular security audits
- GDPR compliance for data deletion

### Access Control
- JWT-based authentication
- Role-based permissions
- API rate limiting
- WebSocket connection limits

### Encryption Details
- **AES-256-GCM**: Server-side encryption
- **Signal Protocol**: End-to-end encryption
- **Key Rotation**: Automatic 30-day rotation
- **Perfect Forward Secrecy**: Session-based keys

## Development

### Local Setup
```bash
# Start messaging service
cd src/services/messaging_service
make build
make run

# Run tests
make test

# Start with Docker
docker-compose up messaging-service
```

### Testing
```bash
# Unit tests
./build/tests/messaging_service_tests

# Integration tests  
./scripts/test_messaging_integration.sh

# Load tests
./scripts/load_test_messaging.sh
```

## Deployment

### Docker
```dockerfile
# See main Dockerfile for messaging service configuration
```

### Kubernetes
```yaml
# See deployment/kubernetes/messaging-service.yaml
```

### Scaling Considerations
- **Horizontal Scaling**: Multiple service instances behind load balancer
- **Database Sharding**: Partition by chat_id for large deployments  
- **Redis Clustering**: Distributed caching for high availability
- **WebSocket Sticky Sessions**: Consistent connection routing
