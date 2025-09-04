-- Analytics Service Database Schema
-- Handles user behavior tracking, content analytics, and business intelligence

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Event types for tracking
CREATE TYPE analytics_event_type AS ENUM (
    -- User actions
    'user_login',
    'user_logout',
    'user_registration',
    'user_profile_update',
    'user_settings_change',
    
    -- Content interactions
    'note_view',
    'note_like',
    'note_unlike',
    'note_repost',
    'note_unrepost',
    'note_comment',
    'note_share',
    'note_bookmark',
    'note_report',
    
    -- Social interactions
    'follow_user',
    'unfollow_user',
    'follow_request_sent',
    'follow_request_accepted',
    'follow_request_rejected',
    'block_user',
    'unblock_user',
    'mute_user',
    'unmute_user',
    
    -- Media interactions
    'media_upload',
    'media_view',
    'media_like',
    'media_unlike',
    'media_share',
    'media_download',
    
    -- Messaging
    'message_sent',
    'message_received',
    'message_read',
    'message_reaction',
    
    -- Search and discovery
    'search_query',
    'search_result_click',
    'trending_topic_view',
    'hashtag_click',
    'user_discovery',
    
    -- System events
    'notification_sent',
    'notification_delivered',
    'notification_read',
    'error_occurred',
    'performance_metric',
    
    -- Business events
    'ad_impression',
    'ad_click',
    'ad_conversion',
    'subscription_start',
    'subscription_end',
    'payment_success',
    'payment_failed'
);

-- User sessions table
CREATE TABLE user_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    session_token VARCHAR(255) NOT NULL UNIQUE,
    
    -- Session details
    ip_address INET,
    user_agent TEXT,
    device_type VARCHAR(50), -- mobile, tablet, desktop, unknown
    browser VARCHAR(100),
    operating_system VARCHAR(100),
    country_code VARCHAR(2),
    city VARCHAR(100),
    timezone VARCHAR(50),
    
    -- Session timing
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_activity_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ended_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,
    
    -- Session metadata
    is_active BOOLEAN DEFAULT true,
    logout_reason VARCHAR(100), -- user_logout, session_expired, security_concern, etc.
    
    -- Indexes
    CONSTRAINT fk_user_sessions_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- User behavior events
CREATE TABLE user_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID,
    session_id UUID,
    
    -- Event details
    event_type analytics_event_type NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    event_category VARCHAR(50),
    
    -- Event data
    event_data JSONB, -- flexible event-specific data
    event_metadata JSONB, -- technical metadata (timestamp, version, etc.)
    
    -- Context
    page_url TEXT,
    referrer_url TEXT,
    user_agent TEXT,
    ip_address INET,
    
    -- Timing
    occurred_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    client_timestamp TIMESTAMP WITH TIME ZONE,
    
    -- Performance
    load_time_ms INTEGER,
    response_time_ms INTEGER,
    
    -- Business context
    campaign_id VARCHAR(100),
    experiment_id VARCHAR(100),
    variant_id VARCHAR(100),
    
    -- Indexes
    CONSTRAINT fk_user_events_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
    CONSTRAINT fk_user_events_session FOREIGN KEY (session_id) REFERENCES user_sessions(id) ON DELETE SET NULL
);

-- Content analytics
CREATE TABLE content_analytics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    content_type VARCHAR(50) NOT NULL, -- note, media, user_profile, etc.
    content_id UUID NOT NULL,
    
    -- View metrics
    total_views INTEGER DEFAULT 0,
    unique_views INTEGER DEFAULT 0,
    views_today INTEGER DEFAULT 0,
    views_this_week INTEGER DEFAULT 0,
    views_this_month INTEGER DEFAULT 0,
    
    -- Engagement metrics
    total_likes INTEGER DEFAULT 0,
    total_reposts INTEGER DEFAULT 0,
    total_comments INTEGER DEFAULT 0,
    total_shares INTEGER DEFAULT 0,
    total_bookmarks INTEGER DEFAULT 0,
    total_reports INTEGER DEFAULT 0,
    
    -- Time-based metrics
    avg_time_on_content_seconds DECIMAL(10,2),
    bounce_rate DECIMAL(5,4), -- percentage of single-page views
    
    -- Social metrics
    social_reach INTEGER DEFAULT 0, -- estimated reach through social sharing
    viral_coefficient DECIMAL(5,4), -- how viral the content is
    
    -- Quality metrics
    engagement_rate DECIMAL(5,4), -- (likes + reposts + comments) / views
    quality_score DECIMAL(3,2), -- algorithmically calculated quality score
    
    -- Timing
    first_viewed_at TIMESTAMP WITH TIME ZONE,
    last_viewed_at TIMESTAMP WITH TIME ZONE,
    peak_popularity_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes
    UNIQUE(content_type, content_id)
);

-- User engagement profiles
CREATE TABLE user_engagement_profiles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL UNIQUE,
    
    -- Activity metrics
    total_sessions INTEGER DEFAULT 0,
    total_session_time_seconds INTEGER DEFAULT 0,
    avg_session_duration_seconds DECIMAL(10,2),
    
    -- Content creation
    notes_created INTEGER DEFAULT 0,
    media_uploaded INTEGER DEFAULT 0,
    comments_posted INTEGER DEFAULT 0,
    
    -- Social activity
    users_followed INTEGER DEFAULT 0,
    users_following INTEGER DEFAULT 0,
    follow_requests_sent INTEGER DEFAULT 0,
    follow_requests_received INTEGER DEFAULT 0,
    
    -- Engagement patterns
    avg_likes_per_day DECIMAL(5,2),
    avg_reposts_per_day DECIMAL(5,2),
    avg_comments_per_day DECIMAL(5,2),
    avg_shares_per_day DECIMAL(5,2),
    
    -- Time patterns
    peak_activity_hour INTEGER, -- 0-23
    most_active_day_of_week INTEGER, -- 0-6 (Sunday = 0)
    avg_daily_active_minutes INTEGER,
    
    -- Content preferences
    preferred_content_types TEXT[], -- array of content types
    preferred_hashtags TEXT[], -- array of hashtags
    preferred_users UUID[], -- array of user IDs
    
    -- Quality metrics
    content_quality_score DECIMAL(3,2),
    engagement_quality_score DECIMAL(3,2),
    overall_score DECIMAL(3,2),
    
    -- Retention metrics
    days_since_first_activity INTEGER,
    days_since_last_activity INTEGER,
    retention_cohort VARCHAR(20), -- weekly, monthly, etc.
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_user_engagement_profiles_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Content performance tracking
CREATE TABLE content_performance (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    content_type VARCHAR(50) NOT NULL,
    content_id UUID NOT NULL,
    
    -- Performance metrics
    load_time_ms INTEGER,
    render_time_ms INTEGER,
    memory_usage_mb DECIMAL(8,2),
    cpu_usage_percent DECIMAL(5,2),
    
    -- Error tracking
    error_count INTEGER DEFAULT 0,
    error_types TEXT[], -- array of error types
    last_error_at TIMESTAMP WITH TIME ZONE,
    
    -- User experience metrics
    user_satisfaction_score DECIMAL(3,2), -- 1.0-5.0
    user_feedback_count INTEGER DEFAULT 0,
    
    -- Technical metrics
    cache_hit_rate DECIMAL(5,4),
    database_query_count INTEGER,
    database_query_time_ms INTEGER,
    
    -- Timing
    measured_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes
    UNIQUE(content_type, content_id, measured_at)
);

-- Search analytics
CREATE TABLE search_analytics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID,
    session_id UUID,
    
    -- Search details
    query_text TEXT NOT NULL,
    query_type VARCHAR(50), -- text, hashtag, user, advanced, etc.
    
    -- Results
    results_count INTEGER,
    results_returned INTEGER,
    first_result_rank INTEGER,
    
    -- User behavior
    clicked_result_rank INTEGER,
    time_to_first_click_ms INTEGER,
    search_refinement_count INTEGER,
    
    -- Search quality
    query_success BOOLEAN, -- did user find what they were looking for?
    search_satisfaction_score DECIMAL(3,2),
    
    -- Performance
    search_time_ms INTEGER,
    cache_hit BOOLEAN,
    
    -- Context
    search_source VARCHAR(50), -- homepage, trending, hashtag, etc.
    device_type VARCHAR(50),
    
    -- Timing
    searched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes
    CONSTRAINT fk_search_analytics_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
    CONSTRAINT fk_search_analytics_session FOREIGN KEY (session_id) REFERENCES user_sessions(id) ON DELETE SET NULL
);

-- Trending topics and hashtags
CREATE TABLE trending_topics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic_text VARCHAR(255) NOT NULL,
    topic_type VARCHAR(50), -- hashtag, keyword, phrase, etc.
    
    -- Trending metrics
    current_rank INTEGER,
    previous_rank INTEGER,
    rank_change INTEGER,
    
    -- Volume metrics
    mention_count INTEGER DEFAULT 0,
    mention_count_change INTEGER DEFAULT 0,
    unique_users_mentioning INTEGER DEFAULT 0,
    
    -- Engagement metrics
    total_engagement INTEGER DEFAULT 0,
    engagement_change INTEGER DEFAULT 0,
    avg_engagement_per_mention DECIMAL(10,2),
    
    -- Growth metrics
    growth_rate_percent DECIMAL(8,4),
    velocity_score DECIMAL(10,4), -- rate of change
    
    -- Demographics
    top_countries TEXT[], -- array of country codes
    top_languages TEXT[], -- array of language codes
    age_group_distribution JSONB, -- age group breakdown
    
    -- Timing
    first_trending_at TIMESTAMP WITH TIME ZONE,
    last_trending_at TIMESTAMP WITH TIME ZONE,
    trend_duration_hours INTEGER,
    
    -- Metadata
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Business metrics
CREATE TABLE business_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    metric_name VARCHAR(100) NOT NULL,
    metric_category VARCHAR(50) NOT NULL,
    
    -- Metric values
    metric_value DECIMAL(15,4),
    metric_unit VARCHAR(20),
    
    -- Comparison
    previous_value DECIMAL(15,4),
    change_percent DECIMAL(8,4),
    
    -- Targets
    target_value DECIMAL(15,4),
    target_achievement_percent DECIMAL(5,2),
    
    -- Context
    time_period VARCHAR(20), -- daily, weekly, monthly, quarterly, yearly
    period_start_date DATE,
    period_end_date DATE,
    
    -- Dimensions
    dimension_1_name VARCHAR(50),
    dimension_1_value VARCHAR(100),
    dimension_2_name VARCHAR(50),
    dimension_2_value VARCHAR(100),
    
    -- Metadata
    data_source VARCHAR(100),
    calculation_method TEXT,
    last_calculated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes
    UNIQUE(metric_name, metric_category, time_period, period_start_date, dimension_1_name, dimension_1_value, dimension_2_name, dimension_2_value)
);

-- A/B testing and experiments
CREATE TABLE experiments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    experiment_name VARCHAR(100) NOT NULL UNIQUE,
    
    -- Experiment details
    description TEXT,
    hypothesis TEXT,
    success_metrics TEXT[],
    
    -- Configuration
    traffic_percentage DECIMAL(5,2), -- percentage of users in experiment
    start_date DATE NOT NULL,
    end_date DATE,
    
    -- Status
    status VARCHAR(20) DEFAULT 'draft', -- draft, running, paused, completed, cancelled
    is_active BOOLEAN DEFAULT false,
    
    -- Results
    primary_metric_value DECIMAL(15,4),
    confidence_level DECIMAL(5,4),
    p_value DECIMAL(10,8),
    statistical_significance BOOLEAN,
    
    -- Metadata
    created_by UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE experiment_variants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    experiment_id UUID NOT NULL,
    
    -- Variant details
    variant_name VARCHAR(100) NOT NULL,
    variant_type VARCHAR(20) DEFAULT 'control', -- control, treatment
    traffic_percentage DECIMAL(5,2),
    
    -- Configuration
    variant_config JSONB, -- variant-specific configuration
    
    -- Performance
    conversion_rate DECIMAL(5,4),
    avg_order_value DECIMAL(10,2),
    user_satisfaction_score DECIMAL(3,2),
    
    -- Sample size
    users_assigned INTEGER DEFAULT 0,
    users_completed INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_experiment_variants_experiment FOREIGN KEY (experiment_id) REFERENCES experiments(id) ON DELETE CASCADE,
    UNIQUE(experiment_id, variant_name)
);

CREATE TABLE user_experiment_assignments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    experiment_id UUID NOT NULL,
    variant_id UUID NOT NULL,
    
    -- Assignment details
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    exposure_count INTEGER DEFAULT 0,
    last_exposure_at TIMESTAMP WITH TIME ZONE,
    
    -- Conversion tracking
    converted BOOLEAN DEFAULT false,
    converted_at TIMESTAMP WITH TIME ZONE,
    conversion_value DECIMAL(10,2),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_user_experiment_assignments_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_user_experiment_assignments_experiment FOREIGN KEY (experiment_id) REFERENCES experiments(id) ON DELETE CASCADE,
    CONSTRAINT fk_user_experiment_assignments_variant FOREIGN KEY (variant_id) REFERENCES experiment_variants(id) ON DELETE CASCADE,
    UNIQUE(user_id, experiment_id)
);

-- Indexes for performance
CREATE INDEX idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_user_sessions_session_token ON user_sessions(session_token);
CREATE INDEX idx_user_sessions_started_at ON user_sessions(started_at);
CREATE INDEX idx_user_sessions_is_active ON user_sessions(is_active);

CREATE INDEX idx_user_events_user_id ON user_events(user_id);
CREATE INDEX idx_user_events_session_id ON user_events(session_id);
CREATE INDEX idx_user_events_event_type ON user_events(event_type);
CREATE INDEX idx_user_events_occurred_at ON user_events(occurred_at);
CREATE INDEX idx_user_events_event_name ON user_events(event_name);

CREATE INDEX idx_content_analytics_content ON content_analytics(content_type, content_id);
CREATE INDEX idx_content_analytics_views ON content_analytics(total_views);
CREATE INDEX idx_content_analytics_engagement ON content_analytics(engagement_rate);
CREATE INDEX idx_content_analytics_quality ON content_analytics(quality_score);

CREATE INDEX idx_user_engagement_profiles_user_id ON user_engagement_profiles(user_id);
CREATE INDEX idx_user_engagement_profiles_overall_score ON user_engagement_profiles(overall_score);
CREATE INDEX idx_user_engagement_profiles_retention ON user_engagement_profiles(days_since_last_activity);

CREATE INDEX idx_content_performance_content ON content_performance(content_type, content_id);
CREATE INDEX idx_content_performance_measured_at ON content_performance(measured_at);

CREATE INDEX idx_search_analytics_user_id ON search_analytics(user_id);
CREATE INDEX idx_search_analytics_query_text ON search_analytics USING gin(to_tsvector('english', query_text));
CREATE INDEX idx_search_analytics_searched_at ON search_analytics(searched_at);

CREATE INDEX idx_trending_topics_topic_text ON trending_topics(topic_text);
CREATE INDEX idx_trending_topics_current_rank ON trending_topics(current_rank);
CREATE INDEX idx_trending_topics_growth_rate ON trending_topics(growth_rate_percent);
CREATE INDEX idx_trending_topics_is_active ON trending_topics(is_active);

CREATE INDEX idx_business_metrics_name_category ON business_metrics(metric_name, metric_category);
CREATE INDEX idx_business_metrics_time_period ON business_metrics(time_period, period_start_date);

CREATE INDEX idx_experiments_status ON experiments(status);
CREATE INDEX idx_experiments_is_active ON experiments(is_active);

CREATE INDEX idx_experiment_variants_experiment_id ON experiment_variants(experiment_id);
CREATE INDEX idx_experiment_variants_variant_type ON experiment_variants(variant_type);

CREATE INDEX idx_user_experiment_assignments_user_id ON user_experiment_assignments(user_id);
CREATE INDEX idx_user_experiment_assignments_experiment_id ON user_experiment_assignments(experiment_id);

-- Full-text search on event data
CREATE INDEX idx_user_events_data_search ON user_events USING gin(event_data);

-- Functions for analytics
CREATE OR REPLACE FUNCTION update_user_session_duration()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.ended_at IS NOT NULL AND OLD.ended_at IS NULL THEN
        NEW.duration_seconds = EXTRACT(EPOCH FROM (NEW.ended_at - NEW.started_at));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate content engagement rate
CREATE OR REPLACE FUNCTION calculate_content_engagement_rate(
    p_content_type VARCHAR(50),
    p_content_id UUID
)
RETURNS DECIMAL(5,4) AS $$
DECLARE
    engagement_rate DECIMAL(5,4);
BEGIN
    SELECT 
        CASE 
            WHEN total_views > 0 THEN 
                (total_likes + total_reposts + total_comments)::DECIMAL / total_views
            ELSE 0 
        END
    INTO engagement_rate
    FROM content_analytics
    WHERE content_type = p_content_type AND content_id = p_content_id;
    
    RETURN COALESCE(engagement_rate, 0);
END;
$$ LANGUAGE plpgsql;

-- Function to get trending topics
CREATE OR REPLACE FUNCTION get_trending_topics(
    p_limit INTEGER DEFAULT 10,
    p_min_mentions INTEGER DEFAULT 5
)
RETURNS TABLE(
    topic_text VARCHAR(255),
    topic_type VARCHAR(50),
    current_rank INTEGER,
    rank_change INTEGER,
    mention_count INTEGER,
    growth_rate_percent DECIMAL(8,4),
    velocity_score DECIMAL(10,4)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        tt.topic_text,
        tt.topic_type,
        tt.current_rank,
        tt.rank_change,
        tt.mention_count,
        tt.growth_rate_percent,
        tt.velocity_score
    FROM trending_topics tt
    WHERE tt.is_active = true
    AND tt.mention_count >= p_min_mentions
    ORDER BY tt.current_rank ASC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Function to update user engagement profile
CREATE OR REPLACE FUNCTION update_user_engagement_profile(p_user_id UUID)
RETURNS VOID AS $$
BEGIN
    INSERT INTO user_engagement_profiles (
        user_id,
        total_sessions,
        total_session_time_seconds,
        avg_session_duration_seconds,
        notes_created,
        media_uploaded,
        comments_posted,
        users_followed,
        users_following,
        follow_requests_sent,
        follow_requests_received,
        avg_likes_per_day,
        avg_reposts_per_day,
        avg_comments_per_day,
        avg_shares_per_day,
        peak_activity_hour,
        most_active_day_of_week,
        avg_daily_active_minutes,
        days_since_first_activity,
        days_since_last_activity
    )
    SELECT 
        p_user_id,
        COUNT(DISTINCT us.id) as total_sessions,
        COALESCE(SUM(us.duration_seconds), 0) as total_session_time_seconds,
        CASE 
            WHEN COUNT(DISTINCT us.id) > 0 THEN 
                AVG(us.duration_seconds)
            ELSE 0 
        END as avg_session_duration_seconds,
        COUNT(DISTINCT ue.id) FILTER (WHERE ue.event_type = 'note_creation') as notes_created,
        COUNT(DISTINCT ue.id) FILTER (WHERE ue.event_type = 'media_upload') as media_uploaded,
        COUNT(DISTINCT ue.id) FILTER (WHERE ue.event_type = 'note_comment') as comments_posted,
        COUNT(DISTINCT ue.id) FILTER (WHERE ue.event_type = 'follow_user') as users_followed,
        COUNT(DISTINCT ue.id) FILTER (WHERE ue.event_type = 'follow_request_accepted') as users_following,
        COUNT(DISTINCT ue.id) FILTER (WHERE ue.event_type = 'follow_request_sent') as follow_requests_sent,
        COUNT(DISTINCT ue.id) FILTER (WHERE ue.event_type = 'follow_request_received') as follow_requests_received,
        AVG(daily_stats.likes_per_day) as avg_likes_per_day,
        AVG(daily_stats.reposts_per_day) as avg_reposts_per_day,
        AVG(daily_stats.comments_per_day) as avg_comments_per_day,
        AVG(daily_stats.shares_per_day) as avg_shares_per_day,
        EXTRACT(HOUR FROM us.started_at) as peak_activity_hour,
        EXTRACT(DOW FROM us.started_at) as most_active_day_of_week,
        AVG(us.duration_seconds) / 60 as avg_daily_active_minutes,
        EXTRACT(DAY FROM (NOW() - MIN(us.started_at))) as days_since_first_activity,
        EXTRACT(DAY FROM (NOW() - MAX(us.last_activity_at))) as days_since_last_activity
    FROM user_sessions us
    LEFT JOIN user_events ue ON us.id = ue.session_id
    LEFT JOIN (
        SELECT 
            DATE(ue.occurred_at) as event_date,
            COUNT(*) FILTER (WHERE ue.event_type = 'note_like') as likes_per_day,
            COUNT(*) FILTER (WHERE ue.event_type = 'note_repost') as reposts_per_day,
            COUNT(*) FILTER (WHERE ue.event_type = 'note_comment') as comments_per_day,
            COUNT(*) FILTER (WHERE ue.event_type = 'note_share') as shares_per_day
        FROM user_events ue
        WHERE ue.user_id = p_user_id
        GROUP BY DATE(ue.occurred_at)
    ) daily_stats ON true
    WHERE us.user_id = p_user_id
    GROUP BY us.user_id
    
    ON CONFLICT (user_id) 
    DO UPDATE SET
        total_sessions = EXCLUDED.total_sessions,
        total_session_time_seconds = EXCLUDED.total_session_time_seconds,
        avg_session_duration_seconds = EXCLUDED.avg_session_duration_seconds,
        notes_created = EXCLUDED.notes_created,
        media_uploaded = EXCLUDED.media_uploaded,
        comments_posted = EXCLUDED.comments_posted,
        users_followed = EXCLUDED.users_followed,
        users_following = EXCLUDED.users_following,
        follow_requests_sent = EXCLUDED.follow_requests_sent,
        follow_requests_received = EXCLUDED.follow_requests_received,
        avg_likes_per_day = EXCLUDED.avg_likes_per_day,
        avg_reposts_per_day = EXCLUDED.avg_reposts_per_day,
        avg_comments_per_day = EXCLUDED.avg_comments_per_day,
        avg_shares_per_day = EXCLUDED.avg_shares_per_day,
        peak_activity_hour = EXCLUDED.peak_activity_hour,
        most_active_day_of_week = EXCLUDED.most_active_day_of_week,
        avg_daily_active_minutes = EXCLUDED.avg_daily_active_minutes,
        days_since_first_activity = EXCLUDED.days_since_first_activity,
        days_since_last_activity = EXCLUDED.days_since_last_activity,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup old analytics data
CREATE OR REPLACE FUNCTION cleanup_old_analytics_data(
    p_days_old INTEGER DEFAULT 365
)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    -- Cleanup old user events
    DELETE FROM user_events 
    WHERE occurred_at < NOW() - INTERVAL '1 day' * p_days_old;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    -- Cleanup old sessions
    DELETE FROM user_sessions 
    WHERE started_at < NOW() - INTERVAL '1 day' * p_days_old
    AND is_active = false;
    
    -- Cleanup old content performance data
    DELETE FROM content_performance 
    WHERE measured_at < NOW() - INTERVAL '1 day' * p_days_old;
    
    -- Cleanup old search analytics
    DELETE FROM search_analytics 
    WHERE searched_at < NOW() - INTERVAL '1 day' * p_days_old;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Views for common analytics queries
CREATE VIEW daily_user_activity AS
SELECT 
    DATE(ue.occurred_at) as activity_date,
    COUNT(DISTINCT ue.user_id) as active_users,
    COUNT(*) as total_events,
    COUNT(*) FILTER (WHERE ue.event_type = 'note_creation') as notes_created,
    COUNT(*) FILTER (WHERE ue.event_type = 'note_like') as likes_given,
    COUNT(*) FILTER (WHERE ue.event_type = 'note_repost') as reposts_given,
    COUNT(*) FILTER (WHERE ue.event_type = 'note_comment') as comments_posted,
    COUNT(*) FILTER (WHERE ue.event_type = 'follow_user') as follows_given
FROM user_events ue
WHERE ue.occurred_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(ue.occurred_at)
ORDER BY activity_date DESC;

CREATE VIEW content_performance_summary AS
SELECT 
    ca.content_type,
    COUNT(*) as total_content,
    AVG(ca.total_views) as avg_views,
    AVG(ca.engagement_rate) as avg_engagement_rate,
    AVG(ca.quality_score) as avg_quality_score,
    SUM(ca.total_likes) as total_likes,
    SUM(ca.total_reposts) as total_reposts,
    SUM(ca.total_comments) as total_comments
FROM content_analytics ca
GROUP BY ca.content_type
ORDER BY avg_engagement_rate DESC;

CREATE VIEW user_retention_cohorts AS
SELECT 
    DATE_TRUNC('week', ue.first_activity) as cohort_week,
    COUNT(DISTINCT ue.user_id) as cohort_size,
    COUNT(DISTINCT CASE WHEN ue.week_1_active = true THEN ue.user_id END) as week_1_retained,
    COUNT(DISTINCT CASE WHEN ue.week_2_active = true THEN ue.user_id END) as week_2_retained,
    COUNT(DISTINCT CASE WHEN ue.week_4_active = true THEN ue.user_id END) as week_4_retained,
    COUNT(DISTINCT CASE WHEN ue.week_8_active = true THEN ue.user_id END) as week_8_retained
FROM (
    SELECT 
        ue.user_id,
        MIN(ue.occurred_at) as first_activity,
        COUNT(*) FILTER (WHERE ue.occurred_at >= MIN(ue.occurred_at) + INTERVAL '1 week') > 0 as week_1_active,
        COUNT(*) FILTER (WHERE ue.occurred_at >= MIN(ue.occurred_at) + INTERVAL '2 weeks') > 0 as week_2_active,
        COUNT(*) FILTER (WHERE ue.occurred_at >= MIN(ue.occurred_at) + INTERVAL '4 weeks') > 0 as week_4_active,
        COUNT(*) FILTER (WHERE ue.occurred_at >= MIN(ue.occurred_at) + INTERVAL '8 weeks') > 0 as week_8_active
    FROM user_events ue
    GROUP BY ue.user_id
) ue
GROUP BY DATE_TRUNC('week', ue.first_activity)
ORDER BY cohort_week DESC;

-- Triggers for automatic updates
CREATE TRIGGER trigger_user_sessions_duration
    BEFORE UPDATE ON user_sessions
    FOR EACH ROW
    EXECUTE FUNCTION update_user_session_duration();

CREATE OR REPLACE FUNCTION update_analytics_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_user_engagement_profiles_updated_at
    BEFORE UPDATE ON user_engagement_profiles
    FOR EACH ROW
    EXECUTE FUNCTION update_analytics_updated_at();

CREATE TRIGGER trigger_trending_topics_updated_at
    BEFORE UPDATE ON trending_topics
    FOR EACH ROW
    EXECUTE FUNCTION update_analytics_updated_at();

CREATE TRIGGER trigger_business_metrics_updated_at
    BEFORE UPDATE ON business_metrics
    FOR EACH ROW
    EXECUTE FUNCTION update_analytics_updated_at();

CREATE TRIGGER trigger_experiments_updated_at
    BEFORE UPDATE ON experiments
    FOR EACH ROW
    EXECUTE FUNCTION update_analytics_updated_at();

CREATE TRIGGER trigger_experiment_variants_updated_at
    BEFORE UPDATE ON experiment_variants
    FOR EACH ROW
    EXECUTE FUNCTION update_analytics_updated_at();

CREATE TRIGGER trigger_user_experiment_assignments_updated_at
    BEFORE UPDATE ON user_experiment_assignments
    FOR EACH ROW
    EXECUTE FUNCTION update_analytics_updated_at();

-- Comments
COMMENT ON TABLE user_sessions IS 'Tracks user sessions and device information';
COMMENT ON TABLE user_events IS 'Stores all user behavior events for analytics';
COMMENT ON TABLE content_analytics IS 'Aggregated analytics for content performance';
COMMENT ON TABLE user_engagement_profiles IS 'User engagement metrics and behavior patterns';
COMMENT ON TABLE content_performance IS 'Technical performance metrics for content';
COMMENT ON TABLE search_analytics IS 'Search behavior and performance analytics';
COMMENT ON TABLE trending_topics IS 'Trending topics and hashtags with metrics';
COMMENT ON TABLE business_metrics IS 'Key business performance indicators';
COMMENT ON TABLE experiments IS 'A/B testing experiments configuration';
COMMENT ON TABLE experiment_variants IS 'Experiment variants and their performance';
COMMENT ON TABLE user_experiment_assignments IS 'User assignments to experiment variants';