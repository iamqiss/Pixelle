# 👻 Server-Side Ghost Reply Implementation

## Overview

This document describes the complete server-side implementation of the Ghost Reply feature for the Sonet platform. The system provides anonymous commenting capabilities with custom ghost avatars and ephemeral IDs, complete with moderation tools, analytics, and real-time updates.

## 🏗️ Architecture

### Service Structure
```
src/services/ghost_reply_service/
├── ghost_reply_service.h          # Core service interface
├── repositories/
│   ├── ghost_reply_repository.h   # Repository interface
│   └── postgres_ghost_reply_repository.h  # PostgreSQL implementation
├── controllers/
│   └── ghost_reply_controller.h   # HTTP API controller
├── validators/
│   └── ghost_reply_validator.h    # Input validation
└── services/
    └── ghost_reply_moderator.h    # Moderation service
```

### Database Schema
- **ghost_replies**: Core ghost reply storage
- **ghost_reply_media**: Media attachments
- **ghost_reply_likes**: Anonymous engagement tracking
- **ghost_reply_replies**: Nested ghost replies
- **ghost_reply_moderation_log**: Moderation audit trail
- **ghost_reply_analytics**: Usage statistics
- **ghost_avatars**: Avatar metadata
- **ghost_reply_threads**: Thread tracking

## 🗄️ Database Setup

### 1. Run Migration
```bash
# Connect to your PostgreSQL database
psql -U your_user -d your_database

# Run the migration
\i database/migrations/002_create_ghost_reply_tables.sql
```

### 2. Verify Tables
```sql
-- Check if tables were created
\dt ghost_*

-- Verify ghost avatars were inserted
SELECT * FROM ghost_avatars LIMIT 5;

-- Check indexes
\di idx_ghost_*
```

### 3. Test Functions
```sql
-- Test ghost ID generation
SELECT generate_unique_ghost_id();

-- Test statistics function
SELECT * FROM get_ghost_reply_stats('your-thread-id'::uuid, 30);
```

## 🚀 API Endpoints

### Core Ghost Reply Operations

#### Create Ghost Reply
```http
POST /api/v1/ghost-replies
Content-Type: application/json

{
  "content": "This is an anonymous ghost reply!",
  "ghost_avatar": "ghost-1",
  "ghost_id": "7A3F",
  "thread_id": "uuid-here",
  "parent_note_id": "uuid-here",
  "language": "en",
  "tags": ["anonymous", "ghost"],
  "media_attachments": []
}
```

**Response:**
```json
{
  "success": true,
  "ghost_reply": {
    "id": "uuid-here",
    "content": "This is an anonymous ghost reply!",
    "ghost_avatar": "ghost-1",
    "ghost_id": "Ghost #7A3F",
    "thread_id": "uuid-here",
    "parent_note_id": "uuid-here",
    "created_at": "2025-01-26T10:30:00Z",
    "moderation_status": "pending"
  }
}
```

#### Get Ghost Replies for Thread
```http
GET /api/v1/threads/{thread_id}/ghost-replies?limit=20&offset=0&sort_by=created_at&sort_order=desc
```

**Response:**
```json
{
  "success": true,
  "ghost_replies": [
    {
      "id": "uuid-here",
      "content": "Ghost reply content",
      "ghost_avatar": "ghost-1",
      "ghost_id": "Ghost #7A3F",
      "created_at": "2025-01-26T10:30:00Z",
      "like_count": 5,
      "reply_count": 2,
      "moderation_status": "approved"
    }
  ],
  "pagination": {
    "total": 42,
    "limit": 20,
    "offset": 0,
    "has_more": true
  }
}
```

#### Search Ghost Replies
```http
GET /api/v1/ghost-replies/search?q=anonymous&limit=20&language=en
```

### Moderation Endpoints

#### Get Pending Moderation
```http
GET /api/v1/moderation/ghost-replies/pending
Authorization: Bearer {moderator_token}
```

#### Moderate Ghost Reply
```http
POST /api/v1/moderation/ghost-replies/{id}/moderate
Authorization: Bearer {moderator_token}
Content-Type: application/json

{
  "action": "approve",
  "reason": "Content meets community guidelines",
  "metadata": {
    "moderator_notes": "Good quality anonymous content"
  }
}
```

### Analytics Endpoints

#### Thread Statistics
```http
GET /api/v1/threads/{thread_id}/ghost-replies/stats?days_back=30
```

**Response:**
```json
{
  "success": true,
  "stats": {
    "total_replies": 42,
    "total_likes": 156,
    "total_views": 1200,
    "avg_spam_score": 0.05,
    "avg_toxicity_score": 0.12,
    "most_active_hour": 14,
    "top_ghost_avatars": ["ghost-7", "ghost-3", "ghost-1"]
  }
}
```

#### Ghost Avatar Statistics
```http
GET /api/v1/ghost-replies/avatars/stats
```

### Engagement Endpoints

#### Like Ghost Reply
```http
POST /api/v1/ghost-replies/{id}/like
Content-Type: application/json

{
  "anonymous_user_hash": "hashed-user-identifier"
}
```

## 🔧 Configuration

### Environment Variables
```bash
# Database
GHOST_REPLY_DB_HOST=localhost
GHOST_REPLY_DB_PORT=5432
GHOST_REPLY_DB_NAME=sonet
GHOST_REPLY_DB_USER=sonet_user
GHOST_REPLY_DB_PASSWORD=secure_password

# Redis (for caching)
GHOST_REPLY_REDIS_URL=redis://localhost:6379

# Rate Limiting
GHOST_REPLY_RATE_LIMIT_PER_IP=10
GHOST_REPLY_RATE_LIMIT_WINDOW=60

# Content Moderation
GHOST_REPLY_SPAM_THRESHOLD=0.8
GHOST_REPLY_TOXICITY_THRESHOLD=0.7
GHOST_REPLY_MAX_CONTENT_LENGTH=300

# Analytics
GHOST_REPLY_ANALYTICS_RETENTION_DAYS=90
GHOST_REPLY_CLEANUP_INTERVAL_HOURS=24
```

### Service Configuration
```cpp
// In your main service configuration
auto ghost_reply_repository = std::make_shared<PostgresGhostReplyRepository>(
    connection_string, 
    connection_pool_size
);

auto ghost_reply_service = std::make_shared<GhostReplyService>(
    ghost_reply_repository,
    validator,
    moderator
);

auto ghost_reply_controller = std::make_shared<GhostReplyController>(
    ghost_reply_repository,
    ghost_reply_service,
    validator,
    moderator,
    redis_client,
    rate_limiter
);

// Register routes
ghost_reply_controller->register_http_routes(http_server);
ghost_reply_controller->register_websocket_handlers(ws_server);
```

## 🛡️ Security & Moderation

### Content Safety
- **Spam Detection**: AI-powered spam scoring
- **Toxicity Analysis**: Content safety evaluation
- **Rate Limiting**: Per-IP request limits
- **Abuse Prevention**: Pattern detection and blocking

### Moderation Workflow
1. **Content Creation**: Ghost replies are marked as "pending"
2. **Automated Review**: AI analysis for spam/toxicity
3. **Manual Review**: Human moderators review flagged content
4. **Action**: Approve, reject, hide, or delete
5. **Audit Trail**: Complete moderation history

### Privacy Features
- **No User Tracking**: Completely anonymous
- **Ephemeral IDs**: Change per thread
- **Avatar Randomization**: Fresh identity each thread
- **Data Isolation**: Separate from user accounts

## 📊 Analytics & Monitoring

### Metrics Tracked
- **Engagement**: Views, likes, replies
- **Content Quality**: Spam scores, toxicity levels
- **Usage Patterns**: Peak hours, popular avatars
- **Geographic Data**: Anonymous location tracking
- **Performance**: Response times, error rates

### Monitoring Dashboard
```bash
# Health check endpoint
GET /api/v1/ghost-replies/health

# Database statistics
GET /api/v1/ghost-replies/admin/db-stats

# Performance metrics
GET /api/v1/ghost-replies/admin/performance
```

## 🚀 Deployment

### 1. Build Service
```bash
cd sonet
mkdir build && cd build
cmake ..
make ghost_reply_service
```

### 2. Database Setup
```bash
# Run migration
psql -U sonet_user -d sonet -f database/migrations/002_create_ghost_reply_tables.sql

# Verify setup
psql -U sonet_user -d sonet -c "SELECT COUNT(*) FROM ghost_avatars;"
```

### 3. Service Configuration
```bash
# Copy configuration
cp config/ghost_reply_service.conf /etc/sonet/

# Set permissions
chown sonet:sonet /etc/sonet/ghost_reply_service.conf
chmod 600 /etc/sonet/ghost_reply_service.conf
```

### 4. Start Service
```bash
# Start ghost reply service
./bin/ghost_reply_service --config /etc/sonet/ghost_reply_service.conf

# Or as systemd service
systemctl enable sonet-ghost-reply
systemctl start sonet-ghost-reply
```

### 5. Load Balancer Configuration
```nginx
# Nginx configuration for ghost reply API
location /api/v1/ghost-replies {
    proxy_pass http://ghost_reply_backend;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    
    # Rate limiting
    limit_req zone=ghost_reply burst=20 nodelay;
}

# Rate limiting zone
limit_req_zone $binary_remote_addr zone=ghost_reply:10m rate=10r/m;
```

## 🧪 Testing

### Unit Tests
```bash
# Run ghost reply service tests
cd sonet/build
make test_ghost_reply_service
./test/test_ghost_reply_service
```

### Integration Tests
```bash
# Test API endpoints
curl -X POST http://localhost:8080/api/v1/ghost-replies \
  -H "Content-Type: application/json" \
  -d '{"content":"Test ghost reply","ghost_avatar":"ghost-1","thread_id":"test-thread"}'

# Test moderation
curl -X GET http://localhost:8080/api/v1/moderation/ghost-replies/pending \
  -H "Authorization: Bearer moderator-token"
```

### Load Testing
```bash
# Using Apache Bench
ab -n 1000 -c 10 -p ghost_reply_payload.json \
   -T application/json \
   http://localhost:8080/api/v1/ghost-replies

# Using wrk
wrk -t12 -c400 -d30s \
    -s ghost_reply_script.lua \
    http://localhost:8080/api/v1/ghost-replies
```

## 🔍 Troubleshooting

### Common Issues

#### Database Connection Errors
```bash
# Check PostgreSQL status
systemctl status postgresql

# Verify connection
psql -U sonet_user -d sonet -c "SELECT 1;"

# Check logs
tail -f /var/log/postgresql/postgresql-*.log
```

#### Rate Limiting Issues
```bash
# Check Redis
redis-cli ping

# Monitor rate limiting
redis-cli monitor | grep ghost_reply
```

#### Performance Issues
```bash
# Check database performance
psql -U sonet_user -d sonet -c "SELECT * FROM pg_stat_activity;"

# Check slow queries
psql -U sonet_user -d sonet -c "SELECT * FROM pg_stat_statements ORDER BY mean_time DESC LIMIT 10;"
```

### Debug Mode
```bash
# Enable debug logging
export GHOST_REPLY_LOG_LEVEL=DEBUG

# Start service with debug
./bin/ghost_reply_service --debug --config config/ghost_reply_service.conf
```

## 📈 Performance Optimization

### Database Optimization
```sql
-- Analyze table statistics
ANALYZE ghost_replies;
ANALYZE ghost_reply_media;
ANALYZE ghost_reply_analytics;

-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE tablename LIKE 'ghost_reply%';
```

### Caching Strategy
- **Redis**: Session data, rate limiting
- **Application Cache**: Frequently accessed ghost replies
- **Database Cache**: Query result caching
- **CDN**: Static ghost avatar images

### Scaling Considerations
- **Horizontal Scaling**: Multiple ghost reply service instances
- **Database Sharding**: Thread-based sharding
- **Load Balancing**: Round-robin or least-connections
- **Microservices**: Separate ghost reply service

## 🔮 Future Enhancements

### Planned Features
- **Ghost Communities**: Anonymous discussion groups
- **Ghost Achievements**: Badges for quality content
- **Advanced Analytics**: Machine learning insights
- **Content Recommendations**: AI-powered suggestions

### Technical Improvements
- **GraphQL API**: Flexible query interface
- **Real-time Streaming**: WebSocket enhancements
- **Mobile Optimization**: Native mobile SDKs
- **Internationalization**: Multi-language support

## 📚 API Documentation

### OpenAPI/Swagger
```bash
# Generate API documentation
swagger-codegen generate -i ghost_reply_api.yaml -l html2 -o docs/

# View documentation
open docs/index.html
```

### Postman Collection
```bash
# Import Postman collection
# File: Ghost_Reply_API.postman_collection.json
```

## 🤝 Contributing

### Development Guidelines
1. **Code Style**: Follow existing C++ patterns
2. **Testing**: Include unit and integration tests
3. **Documentation**: Update API documentation
4. **Security**: Follow security best practices

### Code Review Process
1. **Feature Branch**: Create feature branch
2. **Tests**: Ensure all tests pass
3. **Review**: Submit for code review
4. **Integration**: Merge after approval

---

**👻 Ghost Reply Service Ready!**

The server-side ghost reply implementation is now complete and ready for production deployment. The system provides anonymous commenting with full moderation capabilities, analytics, and real-time updates.

For support or questions, please refer to the development team or create an issue in the project repository.