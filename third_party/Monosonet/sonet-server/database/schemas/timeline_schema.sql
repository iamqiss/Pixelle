-- Timeline Service Database Schema
-- Handles timeline generation, content curation, and user feed management

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Timeline types
CREATE TYPE timeline_type AS ENUM (
    'home',           -- User's home timeline
    'user',           -- Specific user's timeline
    'hashtag',        -- Hashtag timeline
    'trending',       -- Trending content timeline
    'discover',       -- Discovery timeline
    'bookmarks',      -- User's bookmarked content
    'mentions',       -- Content mentioning the user
    'search_results'  -- Search results timeline
);

-- Content visibility levels
CREATE TYPE content_visibility AS ENUM (
    'public',         -- Visible to everyone
    'followers',      -- Visible to followers only
    'close_friends',  -- Visible to close friends only
    'private',        -- Visible to author only
    'unlisted'        -- Not in public timelines but accessible via direct link
);

-- Timeline generation algorithms
CREATE TYPE timeline_algorithm AS ENUM (
    'chronological',  -- Pure chronological order
    'engagement',     -- Engagement-based ranking
    'relevance',      -- Relevance-based ranking
    'hybrid',         -- Combination of multiple factors
    'personalized'    -- User-specific personalization
);

-- Main timeline entries table
CREATE TABLE timeline_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL, -- user whose timeline this entry is for
    timeline_type timeline_type NOT NULL,
    
    -- Content reference
    content_type VARCHAR(50) NOT NULL, -- 'note', 'media', 'user_profile', etc.
    content_id UUID NOT NULL,
    content_author_id UUID NOT NULL,
    
    -- Timeline positioning
    position INTEGER NOT NULL, -- chronological position in timeline
    relevance_score DECIMAL(5,4), -- algorithmically calculated relevance
    engagement_score DECIMAL(5,4), -- engagement-based score
    
    -- Content metadata
    content_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    content_visibility content_visibility NOT NULL,
    
    -- Timeline generation metadata
    algorithm_used timeline_algorithm NOT NULL,
    generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE, -- when this entry should be removed
    
    -- User interaction tracking
    is_viewed BOOLEAN DEFAULT false,
    viewed_at TIMESTAMP WITH TIME ZONE,
    is_engaged BOOLEAN DEFAULT false, -- liked, reposted, commented, etc.
    engaged_at TIMESTAMP WITH TIME ZONE,
    
    -- Indexes
    CONSTRAINT fk_timeline_entries_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_timeline_entries_content_author FOREIGN KEY (content_author_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Timeline generation rules and preferences
CREATE TABLE timeline_preferences (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL UNIQUE,
    
    -- Algorithm preferences
    preferred_algorithm timeline_algorithm DEFAULT 'hybrid',
    chronological_weight DECIMAL(3,2) DEFAULT 0.3,
    engagement_weight DECIMAL(3,2) DEFAULT 0.4,
    relevance_weight DECIMAL(3,2) DEFAULT 0.3,
    
    -- Content filtering
    include_reposts BOOLEAN DEFAULT true,
    include_quotes BOOLEAN DEFAULT true,
    include_replies BOOLEAN DEFAULT true,
    include_media_only BOOLEAN DEFAULT false,
    
    -- Content sources
    include_following BOOLEAN DEFAULT true,
    include_trending BOOLEAN DEFAULT true,
    include_recommended BOOLEAN DEFAULT true,
    include_local_content BOOLEAN DEFAULT true,
    
    -- Quality filters
    min_engagement_threshold INTEGER DEFAULT 0,
    min_quality_score DECIMAL(3,2) DEFAULT 0.0,
    exclude_low_quality BOOLEAN DEFAULT true,
    
    -- Personalization
    content_diversity_factor DECIMAL(3,2) DEFAULT 0.7, -- 0.0 = same type, 1.0 = diverse
    author_diversity_factor DECIMAL(3,2) DEFAULT 0.8, -- 0.0 = same authors, 1.0 = diverse
    
    -- Timing preferences
    max_content_age_hours INTEGER DEFAULT 168, -- 1 week
    refresh_frequency_minutes INTEGER DEFAULT 15,
    
    -- Advanced settings
    enable_content_curation BOOLEAN DEFAULT true,
    enable_spam_filtering BOOLEAN DEFAULT true,
    enable_nsfw_filtering BOOLEAN DEFAULT true,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_timeline_preferences_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Content curation rules
CREATE TABLE curation_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_name VARCHAR(100) NOT NULL UNIQUE,
    
    -- Rule criteria
    content_type VARCHAR(50),
    content_author_id UUID,
    hashtags TEXT[],
    keywords TEXT[],
    content_quality_min DECIMAL(3,2),
    engagement_threshold INTEGER,
    
    -- Rule actions
    action VARCHAR(20) NOT NULL, -- 'boost', 'suppress', 'remove', 'flag'
    boost_factor DECIMAL(3,2) DEFAULT 1.0,
    suppress_factor DECIMAL(3,2) DEFAULT 0.5,
    
    -- Rule scope
    applies_to_timeline_types timeline_type[],
    applies_to_user_groups TEXT[], -- 'all', 'verified', 'influencers', etc.
    
    -- Rule metadata
    priority INTEGER DEFAULT 0, -- higher priority rules are applied first
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_curation_rules_author FOREIGN KEY (content_author_id) REFERENCES users(id) ON DELETE SET NULL
);

-- Timeline generation history
CREATE TABLE timeline_generation_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    timeline_type timeline_type NOT NULL,
    
    -- Generation details
    algorithm_used timeline_algorithm NOT NULL,
    generation_started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    generation_completed_at TIMESTAMP WITH TIME ZONE,
    generation_duration_ms INTEGER,
    
    -- Results
    entries_generated INTEGER DEFAULT 0,
    entries_filtered INTEGER DEFAULT 0,
    entries_boosted INTEGER DEFAULT 0,
    entries_suppressed INTEGER DEFAULT 0,
    
    -- Performance metrics
    cache_hit BOOLEAN DEFAULT false,
    database_queries_count INTEGER,
    database_query_time_ms INTEGER,
    
    -- Error tracking
    error_occurred BOOLEAN DEFAULT false,
    error_message TEXT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_timeline_generation_logs_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- User content interactions for timeline relevance
CREATE TABLE user_content_interactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    content_type VARCHAR(50) NOT NULL,
    content_id UUID NOT NULL,
    
    -- Interaction details
    interaction_type VARCHAR(50) NOT NULL, -- 'view', 'like', 'repost', 'comment', 'share', 'bookmark', 'report'
    interaction_strength DECIMAL(3,2) DEFAULT 1.0, -- 0.0 to 1.0, higher for more significant interactions
    
    -- Timing
    first_interaction_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_interaction_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    interaction_count INTEGER DEFAULT 1,
    
    -- Context
    session_id UUID,
    device_type VARCHAR(50),
    interaction_source VARCHAR(50), -- 'timeline', 'search', 'profile', 'notification', etc.
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_user_content_interactions_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE(user_id, content_type, content_id, interaction_type)
);

-- Content relevance scores
CREATE TABLE content_relevance_scores (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    content_type VARCHAR(50) NOT NULL,
    content_id UUID NOT NULL,
    
    -- Relevance factors
    content_quality_score DECIMAL(3,2) DEFAULT 0.0,
    engagement_potential_score DECIMAL(3,2) DEFAULT 0.0,
    trending_score DECIMAL(3,2) DEFAULT 0.0,
    personalization_score DECIMAL(3,2) DEFAULT 0.0,
    
    -- Combined scores
    overall_relevance_score DECIMAL(3,2) DEFAULT 0.0,
    relevance_confidence DECIMAL(3,2) DEFAULT 0.0,
    
    -- Score breakdown
    quality_weight DECIMAL(3,2) DEFAULT 0.3,
    engagement_weight DECIMAL(3,2) DEFAULT 0.3,
    trending_weight DECIMAL(3,2) DEFAULT 0.2,
    personalization_weight DECIMAL(3,2) DEFAULT 0.2,
    
    -- Timing
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE, -- when scores need recalculation
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(content_type, content_id)
);

-- Timeline refresh queue
CREATE TABLE timeline_refresh_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    timeline_type timeline_type NOT NULL,
    
    -- Refresh details
    refresh_priority INTEGER DEFAULT 0, -- higher priority = refresh sooner
    refresh_reason VARCHAR(100), -- 'user_request', 'scheduled', 'content_update', 'algorithm_change'
    
    -- Scheduling
    queued_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    scheduled_for TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    
    -- Status
    status VARCHAR(20) DEFAULT 'queued', -- 'queued', 'processing', 'completed', 'failed', 'cancelled'
    attempt_count INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,
    
    -- Error tracking
    error_message TEXT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_timeline_refresh_queue_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Timeline cache for performance
CREATE TABLE timeline_cache (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cache_key VARCHAR(255) NOT NULL UNIQUE,
    
    -- Cache data
    timeline_data JSONB NOT NULL, -- serialized timeline entries
    timeline_metadata JSONB, -- metadata about the cached timeline
    
    -- Cache management
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_accessed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    access_count INTEGER DEFAULT 0,
    
    -- Expiration
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_expired BOOLEAN DEFAULT false,
    
    -- Performance
    cache_size_bytes INTEGER,
    compression_ratio DECIMAL(3,2),
    
    -- Metadata
    cache_version VARCHAR(20) DEFAULT '1.0',
    created_by VARCHAR(100) DEFAULT 'timeline_service'
);

-- Indexes for performance
CREATE INDEX idx_timeline_entries_user_timeline ON timeline_entries(user_id, timeline_type);
CREATE INDEX idx_timeline_entries_position ON timeline_entries(user_id, timeline_type, position);
CREATE INDEX idx_timeline_entries_relevance ON timeline_entries(user_id, timeline_type, relevance_score DESC);
CREATE INDEX idx_timeline_entries_engagement ON timeline_entries(user_id, timeline_type, engagement_score DESC);
CREATE INDEX idx_timeline_entries_content ON timeline_entries(content_type, content_id);
CREATE INDEX idx_timeline_entries_author ON timeline_entries(content_author_id);
CREATE INDEX idx_timeline_entries_generated_at ON timeline_entries(generated_at);
CREATE INDEX idx_timeline_entries_expires_at ON timeline_entries(expires_at);

CREATE INDEX idx_timeline_preferences_user_id ON timeline_preferences(user_id);

CREATE INDEX idx_curation_rules_priority ON curation_rules(priority DESC);
CREATE INDEX idx_curation_rules_content_type ON curation_rules(content_type);
CREATE INDEX idx_curation_rules_is_active ON curation_rules(is_active);

CREATE INDEX idx_timeline_generation_logs_user ON timeline_generation_logs(user_id, timeline_type);
CREATE INDEX idx_timeline_generation_logs_created_at ON timeline_generation_logs(created_at);

CREATE INDEX idx_user_content_interactions_user ON user_content_interactions(user_id);
CREATE INDEX idx_user_content_interactions_content ON user_content_interactions(content_type, content_id);
CREATE INDEX idx_user_content_interactions_type ON user_content_interactions(interaction_type);
CREATE INDEX idx_user_content_interactions_last_interaction ON user_content_interactions(last_interaction_at);

CREATE INDEX idx_content_relevance_scores_content ON content_relevance_scores(content_type, content_id);
CREATE INDEX idx_content_relevance_scores_overall ON content_relevance_scores(overall_relevance_score DESC);
CREATE INDEX idx_content_relevance_scores_calculated_at ON content_relevance_scores(calculated_at);

CREATE INDEX idx_timeline_refresh_queue_user ON timeline_refresh_queue(user_id, timeline_type);
CREATE INDEX idx_timeline_refresh_queue_priority ON timeline_refresh_queue(refresh_priority DESC);
CREATE INDEX idx_timeline_refresh_queue_scheduled_for ON timeline_refresh_queue(scheduled_for);
CREATE INDEX idx_timeline_refresh_queue_status ON timeline_refresh_queue(status);

CREATE INDEX idx_timeline_cache_key ON timeline_cache(cache_key);
CREATE INDEX idx_timeline_cache_expires_at ON timeline_cache(expires_at);
CREATE INDEX idx_timeline_cache_last_accessed ON timeline_cache(last_accessed_at);

-- Full-text search on timeline content
CREATE INDEX idx_timeline_entries_content_search ON timeline_entries USING gin(to_tsvector('english', content_type || ' ' || content_id::text));

-- Functions for timeline management
CREATE OR REPLACE FUNCTION generate_timeline_entries(
    p_user_id UUID,
    p_timeline_type timeline_type,
    p_limit INTEGER DEFAULT 50
)
RETURNS TABLE(
    content_type VARCHAR(50),
    content_id UUID,
    content_author_id UUID,
    position INTEGER,
    relevance_score DECIMAL(5,4),
    engagement_score DECIMAL(5,4)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'note' as content_type,
        n.id as content_id,
        n.author_id as content_author_id,
        ROW_NUMBER() OVER (ORDER BY n.created_at DESC) as position,
        COALESCE(crs.overall_relevance_score, 0.5) as relevance_score,
        COALESCE(ca.engagement_rate, 0.0) as engagement_score
    FROM notes n
    LEFT JOIN content_relevance_scores crs ON crs.content_type = 'note' AND crs.content_id = n.id
    LEFT JOIN content_analytics ca ON ca.content_type = 'note' AND ca.content_id = n.id
    WHERE n.visibility = 'public'
    AND n.author_id IN (
        SELECT followed_user_id 
        FROM user_relationships 
        WHERE follower_user_id = p_user_id 
        AND relationship_type = 'following'
        AND is_active = true
    )
    ORDER BY n.created_at DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Function to update timeline entry relevance scores
CREATE OR REPLACE FUNCTION update_timeline_relevance_scores(
    p_content_type VARCHAR(50),
    p_content_id UUID
)
RETURNS DECIMAL(3,2) AS $$
DECLARE
    overall_score DECIMAL(3,2);
    quality_score DECIMAL(3,2);
    engagement_score DECIMAL(3,2);
    trending_score DECIMAL(3,2);
    personalization_score DECIMAL(3,2);
BEGIN
    -- Calculate quality score (placeholder logic)
    quality_score := 0.7; -- This would be calculated based on content analysis
    
    -- Calculate engagement score
    SELECT COALESCE(engagement_rate, 0.0) INTO engagement_score
    FROM content_analytics
    WHERE content_type = p_content_type AND content_id = p_content_id;
    
    -- Calculate trending score (placeholder logic)
    trending_score := 0.5; -- This would be calculated based on recent activity
    
    -- Calculate personalization score (placeholder logic)
    personalization_score := 0.6; -- This would be calculated based on user preferences
    
    -- Calculate overall score
    overall_score := (
        quality_score * 0.3 +
        engagement_score * 0.3 +
        trending_score * 0.2 +
        personalization_score * 0.2
    );
    
    -- Update or insert relevance scores
    INSERT INTO content_relevance_scores (
        content_type, content_id, content_quality_score, engagement_potential_score,
        trending_score, personalization_score, overall_relevance_score,
        calculated_at, expires_at
    ) VALUES (
        p_content_type, p_content_id, quality_score, engagement_score,
        trending_score, personalization_score, overall_score,
        NOW(), NOW() + INTERVAL '1 hour'
    )
    ON CONFLICT (content_type, content_id) 
    DO UPDATE SET
        content_quality_score = EXCLUDED.content_quality_score,
        engagement_potential_score = EXCLUDED.engagement_potential_score,
        trending_score = EXCLUDED.trending_score,
        personalization_score = EXCLUDED.personalization_score,
        overall_relevance_score = EXCLUDED.overall_relevance_score,
        calculated_at = EXCLUDED.calculated_at,
        expires_at = EXCLUDED.expires_at,
        updated_at = NOW();
    
    RETURN overall_score;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh user timeline
CREATE OR REPLACE FUNCTION refresh_user_timeline(
    p_user_id UUID,
    p_timeline_type timeline_type DEFAULT 'home'
)
RETURNS INTEGER AS $$
DECLARE
    entries_generated INTEGER;
    generation_start TIMESTAMP WITH TIME ZONE;
    generation_duration_ms INTEGER;
BEGIN
    generation_start := NOW();
    
    -- Clear existing timeline entries
    DELETE FROM timeline_entries 
    WHERE user_id = p_user_id AND timeline_type = p_timeline_type;
    
    -- Generate new timeline entries
    INSERT INTO timeline_entries (
        user_id, timeline_type, content_type, content_id, content_author_id,
        position, relevance_score, engagement_score, content_created_at,
        content_visibility, algorithm_used
    )
    SELECT 
        p_user_id, p_timeline_type, te.content_type, te.content_id,
        te.content_author_id, te.position, te.relevance_score,
        te.engagement_score, te.content_created_at, 'public', 'hybrid'
    FROM generate_timeline_entries(p_user_id, p_timeline_type, 100) te;
    
    GET DIAGNOSTICS entries_generated = ROW_COUNT;
    
    -- Calculate generation duration
    generation_duration_ms := EXTRACT(EPOCH FROM (NOW() - generation_start)) * 1000;
    
    -- Log the generation
    INSERT INTO timeline_generation_logs (
        user_id, timeline_type, algorithm_used, generation_started_at,
        generation_completed_at, generation_duration_ms, entries_generated
    ) VALUES (
        p_user_id, p_timeline_type, 'hybrid', generation_start,
        NOW(), generation_duration_ms, entries_generated
    );
    
    RETURN entries_generated;
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup expired timeline entries
CREATE OR REPLACE FUNCTION cleanup_expired_timeline_entries()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM timeline_entries 
    WHERE expires_at IS NOT NULL AND expires_at < NOW();
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup expired cache entries
CREATE OR REPLACE FUNCTION cleanup_expired_timeline_cache()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM timeline_cache 
    WHERE expires_at < NOW() OR is_expired = true;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Views for common timeline queries
CREATE VIEW user_timeline_summary AS
SELECT 
    te.user_id,
    te.timeline_type,
    COUNT(*) as total_entries,
    COUNT(*) FILTER (WHERE te.is_viewed = true) as viewed_entries,
    COUNT(*) FILTER (WHERE te.is_engaged = true) as engaged_entries,
    AVG(te.relevance_score) as avg_relevance_score,
    AVG(te.engagement_score) as avg_engagement_score,
    MAX(te.generated_at) as last_generated_at
FROM timeline_entries te
GROUP BY te.user_id, te.timeline_type;

CREATE VIEW timeline_performance_metrics AS
SELECT 
    tgl.timeline_type,
    COUNT(*) as total_generations,
    AVG(tgl.generation_duration_ms) as avg_generation_time_ms,
    AVG(tgl.entries_generated) as avg_entries_generated,
    COUNT(*) FILTER (WHERE tgl.error_occurred = true) as error_count,
    COUNT(*) FILTER (WHERE tgl.cache_hit = true) as cache_hit_count
FROM timeline_generation_logs tgl
WHERE tgl.created_at >= NOW() - INTERVAL '24 hours'
GROUP BY tgl.timeline_type;

-- Triggers for automatic updates
CREATE OR REPLACE FUNCTION update_timeline_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_timeline_preferences_updated_at
    BEFORE UPDATE ON timeline_preferences
    FOR EACH ROW
    EXECUTE FUNCTION update_timeline_updated_at();

CREATE TRIGGER trigger_curation_rules_updated_at
    BEFORE UPDATE ON curation_rules
    FOR EACH ROW
    EXECUTE FUNCTION update_timeline_updated_at();

CREATE TRIGGER trigger_user_content_interactions_updated_at
    BEFORE UPDATE ON user_content_interactions
    FOR EACH ROW
    EXECUTE FUNCTION update_timeline_updated_at();

CREATE TRIGGER trigger_content_relevance_scores_updated_at
    BEFORE UPDATE ON content_relevance_scores
    FOR EACH ROW
    EXECUTE FUNCTION update_timeline_updated_at();

CREATE TRIGGER trigger_timeline_refresh_queue_updated_at
    BEFORE UPDATE ON timeline_refresh_queue
    FOR EACH ROW
    EXECUTE FUNCTION update_timeline_updated_at();

-- Insert default curation rules
INSERT INTO curation_rules (rule_name, content_type, action, boost_factor, applies_to_timeline_types, priority) VALUES
('boost_high_quality_content', NULL, 'boost', 1.5, ARRAY['home', 'discover'], 10),
('suppress_low_quality_content', NULL, 'suppress', 0.5, ARRAY['home', 'discover'], 5),
('boost_verified_users', NULL, 'boost', 1.2, ARRAY['home', 'discover'], 8),
('boost_trending_content', NULL, 'boost', 1.3, ARRAY['trending', 'discover'], 7),
('suppress_spam_content', NULL, 'suppress', 0.1, ARRAY['home', 'discover'], 15);

-- Comments
COMMENT ON TABLE timeline_entries IS 'Individual timeline entries for users';
COMMENT ON TABLE timeline_preferences IS 'User preferences for timeline generation and display';
COMMENT ON TABLE curation_rules IS 'Rules for content curation and ranking in timelines';
COMMENT ON TABLE timeline_generation_logs IS 'Logs of timeline generation operations';
COMMENT ON TABLE user_content_interactions IS 'User interactions with content for relevance calculation';
COMMENT ON TABLE content_relevance_scores IS 'Calculated relevance scores for content';
COMMENT ON TABLE timeline_refresh_queue IS 'Queue for timeline refresh operations';
COMMENT ON TABLE timeline_cache IS 'Cache for timeline data to improve performance';