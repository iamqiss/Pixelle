# SoNet Database Schemas

This directory contains the complete database schemas for all microservices in the SoNet distributed system.

## Overview

The SoNet system uses postgresql as its primary database with separate schemas for each microservice. This approach provides:
- **Service Isolation**: Each service has its own schema and can be deployed independently
- **Data Consistency**: ACID transactions within each service
- **Performance**: Optimized indexes and queries for each service's specific needs
- **Scalability**: Services can be scaled independently based on their data requirements

## Schema Structure

### 1. User Service Schema (`user_schema.sql`)
- **Purpose**: Manages user accounts, authentication, profiles, and sessions
- **Key Tables**: `users`, `user_sessions`, `two_factor_auth`, `password_reset_tokens`, `email_verification_tokens`, `user_settings`, `user_stats`, `user_login_history`
- **Features**: JWT token management, 2FA support, password reset functionality, user activity tracking

### 2. Note Service Schema (`note_schema.sql`)
- **Purpose**: Handles social media notes, content management, and engagement
- **Key Tables**: `notes`, `note_media`, `note_likes`, `note_renotes`, `note_comments`, `note_bookmarks`, `note_hashtags`, `note_mentions`, `note_stats`, `note_visibility`
- **Features**: Full-text search, media attachments, engagement tracking, hashtag management

### 3. Media Service Schema (`media_schema.sql`)
- **Purpose**: Manages file uploads, storage, and media processing
- **Key Tables**: `media_files`, `upload_sessions`, `upload_chunks`, `media_processing_jobs`, `media_access_logs`, `media_tags`, `media_collections`, `user_storage_quotas`
- **Features**: Chunked uploads, processing pipelines, access control, storage quotas

### 4. Follow Service Schema (`follow_schema.sql`)
- **Purpose**: Manages user relationships, follow requests, and social connections
- **Key Tables**: `user_relationships`, `follow_requests`, `user_blocks`, `user_mutes`, `close_friends`, `user_lists`, `social_graph_cache`
- **Features**: Follow/unfollow, blocking, muting, close friends, custom lists

### 5. Notification Service Schema (`notification_schema.sql`)
- **Purpose**: Handles user notifications, delivery, and preferences
- **Key Tables**: `notifications`, `notification_preferences`, `notification_queue`, `notification_templates`, `notification_delivery_logs`, `notification_aggregation_rules`
- **Features**: Push notifications, email notifications, SMS, notification preferences, delivery tracking

### 6. Messaging Service Schema (`messaging_schema.sql`)
- **Purpose**: Manages real-time messaging, chat rooms, and conversations
- **Key Tables**: `chats`, `chat_participants`, `messages` (partitioned), `message_attachments`, `read_receipts`, `typing_indicators`
- **Features**: Real-time messaging, file sharing, read receipts, typing indicators

### 7. Timeline Service Schema (`timeline_schema.sql`)
- **Purpose**: Generates personalized content feeds and timelines
- **Key Tables**: `timeline_entries`, `timeline_preferences`, `curation_rules`, `content_relevance_scores`, `timeline_cache`
- **Features**: Algorithmic timeline generation, content curation, relevance scoring

### 8. Search Service Schema (`search_schema.sql`)
- **Purpose**: Provides full-text search and content discovery
- **Key Tables**: `search_index`, `search_history`, `search_analytics`, `search_suggestions`, `search_cache`, `search_synonyms`
- **Features**: Full-text search, search suggestions, search analytics, result ranking

### 9. Analytics Service Schema (`analytics_schema.sql`)
- **Purpose**: Tracks user behavior, content performance, and business metrics
- **Key Tables**: `user_sessions`, `user_events`, `content_analytics`, `user_engagement_profiles`, `content_performance`, `business_metrics`, `experiments`
- **Features**: User analytics, content performance tracking, A/B testing, business intelligence

## Database Configuration

### Docker Compose Setup
The system uses Docker Compose with the following configuration:
- **postgresql 15**: Main database with multiple schemas
- **Redis 7**: Caching and session storage
- **Multiple Databases**: Each service gets its own database instance

### Environment Variables
Each service connects to its database using:
```bash
DATABASE_URL=postgresql://sonet:sonet_dev_password@postgres:5432/{service_name}
REDIS_URL=redis://redis:6379
```

## Next Steps for Real Mode Transition

### Phase 1: Database Integration (Current)
- ✅ **Complete**: All database schemas created
- ✅ **Complete**: Docker Compose updated with all services
- 🔄 **Next**: Create database connection utilities and repositories

### Phase 2: Service Implementation
- [ ] **Replace stub implementations** with real business logic
- [ ] **Implement database repositories** for each service
- [ ] **Add Redis caching** for frequently accessed data
- [ ] **Implement real gRPC clients** for inter-service communication

### Phase 3: Authentication & Security
- [ ] **JWT verification** in all services
- [ ] **Rate limiting** and security middleware
- [ ] **Input validation** and sanitization
- [ ] **Audit logging** for security events

### Phase 4: Testing & Monitoring
- [ ] **End-to-end tests** through REST gateway → gRPC → Database
- [ ] **Integration tests** for each service
- [ ] **Performance testing** and optimization
- [ ] **Monitoring and alerting** setup

### Phase 5: Production Readiness
- [ ] **Error handling** and recovery
- [ ] **Health checks** and readiness probes
- [ ] **Metrics collection** (Prometheus)
- [ ] **Logging aggregation** (ELK Stack)

## Database Utilities

### Common Functions
Each schema includes common utility functions:
- `update_updated_at_column()`: Automatically updates `updated_at` timestamps
- `cleanup_old_data()`: Removes expired or old data
- Performance monitoring and analytics functions

### Indexes and Performance
- **GIN indexes** for full-text search and JSON fields
- **B-tree indexes** for primary keys and foreign keys
- **Composite indexes** for common query patterns
- **Partial indexes** for active/inactive data filtering

### Triggers
- **Automatic timestamp updates** on record modifications
- **Search vector updates** for full-text search
- **Statistics updates** for engagement metrics
- **Cache invalidation** for related data

## Development Guidelines

### Adding New Tables
1. Use UUID primary keys for distributed systems
2. Include `created_at` and `updated_at` timestamps
3. Add appropriate indexes for query performance
4. Consider partitioning for large tables (e.g., messages, events)

### Data Relationships
- Use foreign keys for referential integrity
- Consider denormalization for read-heavy workloads
- Implement soft deletes where appropriate
- Use JSONB for flexible metadata storage

### Performance Considerations
- Monitor query performance with `EXPLAIN ANALYZE`
- Use connection pooling for database connections
- Implement caching strategies (Redis)
- Consider read replicas for heavy read workloads

## Monitoring and Maintenance

### Regular Tasks
- **Data cleanup**: Remove old logs, expired sessions, etc.
- **Index maintenance**: Rebuild indexes, update statistics
- **Performance monitoring**: Track slow queries and bottlenecks
- **Backup and recovery**: Regular database backups

### Health Checks
- Database connectivity
- Query performance metrics
- Connection pool status
- Disk space and resource usage

## Troubleshooting

### Common Issues
1. **Connection timeouts**: Check network connectivity and connection pool settings
2. **Slow queries**: Analyze query plans and add missing indexes
3. **Memory issues**: Monitor connection count and query memory usage
4. **Lock contention**: Identify and resolve long-running transactions

### Debugging Tools
- postgresql logs and error messages
- Query performance analysis with `pg_stat_statements`
- Connection monitoring with `pg_stat_activity`
- Index usage statistics

## Security Considerations

### Data Protection
- **Encryption at rest** for sensitive data
- **Connection encryption** (TLS/SSL)
- **Access control** with role-based permissions
- **Audit logging** for all data modifications

### Access Management
- **Least privilege principle** for database users
- **Connection pooling** to limit concurrent connections
- **Query timeout** to prevent long-running queries
- **Input validation** to prevent SQL injection

---

This database schema provides a solid foundation for the SoNet distributed system. Each service has its own isolated data while maintaining the ability to share data through well-defined interfaces and cross-service queries when necessary.