/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

-- Ghost Reply Service Database Schema
-- Handles anonymous ghost replies with custom avatars and ephemeral IDs

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Ghost replies table - stores "anonymous" replies with user tracking for moderation
CREATE TABLE ghost_replies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    content TEXT NOT NULL,
    ghost_avatar VARCHAR(255) NOT NULL,
    ghost_id VARCHAR(10) NOT NULL, -- e.g., "7A3F", "2B9E"
    thread_id UUID NOT NULL, -- References the thread/note being replied to
    parent_note_id UUID REFERENCES notes(note_id) ON DELETE CASCADE,
    
    -- User tracking (hidden from public but available to moderators)
    author_id UUID NOT NULL, -- References users table for accountability
    author_ip_address INET, -- IP address for abuse prevention
    author_user_agent TEXT, -- User agent for pattern detection
    
    -- Content metadata
    language VARCHAR(10) DEFAULT 'en',
    tags TEXT[],
    content_warning VARCHAR(50) DEFAULT 'none',
    
    -- Moderation fields
    is_deleted BOOLEAN DEFAULT FALSE,
    is_hidden BOOLEAN DEFAULT FALSE,
    is_flagged BOOLEAN DEFAULT FALSE,
    spam_score DECIMAL(3,2) DEFAULT 0.0, -- 0.00 to 1.00
    toxicity_score DECIMAL(3,2) DEFAULT 0.0, -- 0.00 to 1.00
    moderation_status VARCHAR(20) DEFAULT 'pending' 
        CHECK (moderation_status IN ('pending', 'approved', 'rejected', 'flagged')),
    
    -- Rate limiting and abuse prevention
    rate_limit_key VARCHAR(255), -- For tracking user rate limits
    abuse_score INTEGER DEFAULT 0, -- Cumulative abuse score
    last_abuse_incident TIMESTAMP WITH TIME ZONE, -- Last abuse detection
    
    -- Engagement tracking (public metrics)
    like_count INTEGER DEFAULT 0,
    reply_count INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE,
    
    -- Search and indexing
    search_vector tsvector,
    
    -- Constraints
    CONSTRAINT ghost_reply_content_length CHECK (char_length(content) <= 300),
    CONSTRAINT ghost_reply_ghost_id_format CHECK (ghost_id ~ '^[0-9A-F]{4}$'),
    CONSTRAINT ghost_reply_spam_score_range CHECK (spam_score >= 0.0 AND spam_score <= 1.0),
    CONSTRAINT ghost_reply_toxicity_score_range CHECK (toxicity_score >= 0.0 AND toxicity_score <= 1.0),
    CONSTRAINT ghost_reply_abuse_score_range CHECK (abuse_score >= 0 AND abuse_score <= 100)
);

-- Ghost reply media attachments table
CREATE TABLE ghost_reply_media (
    media_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ghost_reply_id UUID NOT NULL REFERENCES ghost_replies(id) ON DELETE CASCADE,
    media_type VARCHAR(20) NOT NULL 
        CHECK (media_type IN ('image', 'video', 'audio', 'file', 'gif')),
    url TEXT NOT NULL,
    thumbnail_url TEXT,
    alt_text TEXT,
    caption TEXT,
    file_size BIGINT,
    duration INTEGER, -- for video/audio in seconds
    width INTEGER,
    height INTEGER,
    metadata JSONB DEFAULT '{}',
    order_index INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Ghost reply likes table (anonymous tracking)
CREATE TABLE ghost_reply_likes (
    like_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ghost_reply_id UUID NOT NULL REFERENCES ghost_replies(id) ON DELETE CASCADE,
    anonymous_user_hash VARCHAR(64) NOT NULL, -- Hashed identifier for anonymous users
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(ghost_reply_id, anonymous_user_hash)
);

-- Ghost reply replies table (ghosts replying to other ghosts)
CREATE TABLE ghost_reply_replies (
    reply_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    parent_ghost_reply_id UUID NOT NULL REFERENCES ghost_replies(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    ghost_avatar VARCHAR(255) NOT NULL,
    ghost_id VARCHAR(10) NOT NULL,
    thread_id UUID NOT NULL,
    
    -- Content metadata
    language VARCHAR(10) DEFAULT 'en',
    content_warning VARCHAR(50) DEFAULT 'none',
    
    -- Moderation fields
    is_deleted BOOLEAN DEFAULT FALSE,
    is_hidden BOOLEAN DEFAULT FALSE,
    spam_score DECIMAL(3,2) DEFAULT 0.0,
    toxicity_score DECIMAL(3,2) DEFAULT 0.0,
    moderation_status VARCHAR(20) DEFAULT 'pending',
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT ghost_reply_reply_content_length CHECK (char_length(content) <= 300),
    CONSTRAINT ghost_reply_reply_ghost_id_format CHECK (ghost_id ~ '^[0-9A-F]{4}$')
);

-- Ghost reply moderation log table
CREATE TABLE ghost_reply_moderation_log (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ghost_reply_id UUID NOT NULL REFERENCES ghost_replies(id) ON DELETE CASCADE,
    moderator_id UUID NOT NULL, -- References users table
    action VARCHAR(50) NOT NULL, -- 'approve', 'reject', 'hide', 'flag', 'delete'
    reason TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Ghost reply rate limiting table
CREATE TABLE ghost_reply_rate_limits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL, -- References users table
    ip_address INET NOT NULL,
    rate_limit_key VARCHAR(255) NOT NULL, -- Composite key for rate limiting
    
    -- Rate limiting counters
    ghost_reply_count INTEGER DEFAULT 0, -- Ghost replies in current window
    last_ghost_reply_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Cooldown tracking
    cooldown_until TIMESTAMP WITH TIME ZONE, -- When cooldown expires
    cooldown_reason VARCHAR(100), -- Reason for cooldown
    
    -- Abuse tracking
    warning_count INTEGER DEFAULT 0, -- Number of warnings
    suspension_count INTEGER DEFAULT 0, -- Number of suspensions
    is_suspended BOOLEAN DEFAULT FALSE, -- Current suspension status
    suspended_until TIMESTAMP WITH TIME ZONE, -- When suspension expires
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(user_id, ip_address, rate_limit_key)
);

-- Ghost reply abuse detection table
CREATE TABLE ghost_reply_abuse_patterns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pattern_type VARCHAR(50) NOT NULL, -- 'spam', 'harassment', 'inappropriate', etc.
    pattern_signature TEXT NOT NULL, -- Hash or signature of the pattern
    severity INTEGER DEFAULT 1, -- 1-10 severity scale
    detection_count INTEGER DEFAULT 0, -- How many times this pattern was detected
    
    -- Pattern metadata
    description TEXT,
    keywords TEXT[],
    regex_pattern TEXT,
    
    -- Auto-moderation settings
    auto_flag BOOLEAN DEFAULT FALSE, -- Automatically flag content with this pattern
    auto_hide BOOLEAN DEFAULT FALSE, -- Automatically hide content with this pattern
    auto_suspend BOOLEAN DEFAULT FALSE, -- Automatically suspend users with this pattern
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Ghost reply analytics table
CREATE TABLE ghost_reply_analytics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ghost_reply_id UUID NOT NULL REFERENCES ghost_replies(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    hour INTEGER CHECK (hour >= 0 AND hour <= 23),
    
    -- Daily metrics
    daily_views INTEGER DEFAULT 0,
    daily_likes INTEGER DEFAULT 0,
    daily_replies INTEGER DEFAULT 0,
    
    -- Geographic data (anonymous)
    country_codes TEXT[],
    city_names TEXT[],
    
    -- Device/platform data (anonymous)
    device_types TEXT[],
    platform_types TEXT[],
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(ghost_reply_id, date, hour)
);

-- Ghost avatar metadata table
CREATE TABLE ghost_avatars (
    avatar_id VARCHAR(50) PRIMARY KEY, -- e.g., 'ghost-1', 'ghost-2'
    avatar_name VARCHAR(100) NOT NULL,
    avatar_description TEXT,
    avatar_category VARCHAR(50) DEFAULT 'cute', -- 'cute', 'spooky', 'funny', etc.
    avatar_style VARCHAR(50) DEFAULT 'cartoon',
    is_active BOOLEAN DEFAULT TRUE,
    usage_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Ghost reply thread tracking table
CREATE TABLE ghost_reply_threads (
    thread_id UUID PRIMARY KEY,
    note_id UUID NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
    ghost_reply_count INTEGER DEFAULT 0,
    last_ghost_reply_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_ghost_replies_thread_id ON ghost_replies(thread_id);
CREATE INDEX idx_ghost_replies_parent_note_id ON ghost_replies(parent_note_id);
CREATE INDEX idx_ghost_replies_ghost_id ON ghost_replies(ghost_id);
CREATE INDEX idx_ghost_replies_created_at ON ghost_replies(created_at DESC);
CREATE INDEX idx_ghost_replies_moderation_status ON ghost_replies(moderation_status);
CREATE INDEX idx_ghost_replies_spam_score ON ghost_replies(spam_score DESC);
CREATE INDEX idx_ghost_replies_toxicity_score ON ghost_replies(toxicity_score DESC);

CREATE INDEX idx_ghost_reply_media_reply_id ON ghost_reply_media(ghost_reply_id);
CREATE INDEX idx_ghost_reply_likes_reply_id ON ghost_reply_likes(ghost_reply_id);
CREATE INDEX idx_ghost_reply_replies_parent_id ON ghost_reply_replies(parent_ghost_reply_id);
CREATE INDEX idx_ghost_reply_replies_thread_id ON ghost_reply_replies(thread_id);

CREATE INDEX idx_ghost_reply_moderation_log_reply_id ON ghost_reply_moderation_log(ghost_reply_id);
CREATE INDEX idx_ghost_reply_moderation_log_moderator_id ON ghost_reply_moderation_log(moderator_id);
CREATE INDEX idx_ghost_reply_moderation_log_action ON ghost_reply_moderation_log(action);
CREATE INDEX idx_ghost_reply_moderation_log_created_at ON ghost_reply_moderation_log(created_at DESC);

CREATE INDEX idx_ghost_reply_analytics_reply_id ON ghost_reply_analytics(ghost_reply_id);
CREATE INDEX idx_ghost_reply_analytics_date ON ghost_reply_analytics(date DESC);
CREATE INDEX idx_ghost_reply_analytics_date_hour ON ghost_reply_analytics(date, hour);

CREATE INDEX idx_ghost_reply_threads_note_id ON ghost_reply_threads(note_id);

-- Rate limiting and abuse prevention indexes
CREATE INDEX idx_ghost_reply_rate_limits_user_id ON ghost_reply_rate_limits(user_id);
CREATE INDEX idx_ghost_reply_rate_limits_ip_address ON ghost_reply_rate_limits(ip_address);
CREATE INDEX idx_ghost_reply_rate_limits_rate_limit_key ON ghost_reply_rate_limits(rate_limit_key);
CREATE INDEX idx_ghost_reply_rate_limits_suspended ON ghost_reply_rate_limits(is_suspended, suspended_until);
CREATE INDEX idx_ghost_reply_rate_limits_cooldown ON ghost_reply_rate_limits(cooldown_until);

CREATE INDEX idx_ghost_reply_abuse_patterns_type ON ghost_reply_abuse_patterns(pattern_type);
CREATE INDEX idx_ghost_reply_abuse_patterns_severity ON ghost_reply_abuse_patterns(severity);
CREATE INDEX idx_ghost_reply_abuse_patterns_signature ON ghost_reply_abuse_patterns(pattern_signature);

-- User tracking indexes (for moderation)
CREATE INDEX idx_ghost_replies_author_id ON ghost_replies(author_id);
CREATE INDEX idx_ghost_replies_ip_address ON ghost_replies(author_ip_address);
CREATE INDEX idx_ghost_replies_rate_limit_key ON ghost_replies(rate_limit_key);
CREATE INDEX idx_ghost_replies_abuse_score ON ghost_replies(abuse_score);

-- Full-text search index for ghost reply content
CREATE INDEX idx_ghost_replies_search ON ghost_replies USING gin(search_vector);

-- Trigger to update search vector
CREATE OR REPLACE FUNCTION update_ghost_reply_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('english', COALESCE(NEW.content, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(array_to_string(NEW.tags, ' '), '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(NEW.ghost_id, '')), 'C');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_ghost_reply_search_vector
    BEFORE INSERT OR UPDATE ON ghost_replies
    FOR EACH ROW
    EXECUTE FUNCTION update_ghost_reply_search_vector();

-- Trigger to update thread ghost reply count
CREATE OR REPLACE FUNCTION update_ghost_reply_thread_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO ghost_reply_threads (thread_id, note_id, ghost_reply_count, last_ghost_reply_at)
        VALUES (NEW.thread_id, NEW.parent_note_id, 1, NEW.created_at)
        ON CONFLICT (thread_id) DO UPDATE SET
            ghost_reply_count = ghost_reply_threads.ghost_reply_count + 1,
            last_ghost_reply_at = NEW.created_at,
            updated_at = NOW();
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE ghost_reply_threads 
        SET ghost_reply_count = ghost_reply_count - 1,
            updated_at = NOW()
        WHERE thread_id = OLD.thread_id;
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_ghost_reply_thread_count
    AFTER INSERT OR DELETE ON ghost_replies
    FOR EACH ROW
    EXECUTE FUNCTION update_ghost_reply_thread_count();

-- Function to generate unique ghost ID
CREATE OR REPLACE FUNCTION generate_unique_ghost_id()
RETURNS VARCHAR(10) AS $$
DECLARE
    new_id VARCHAR(10);
    attempts INTEGER := 0;
    max_attempts INTEGER := 100;
BEGIN
    LOOP
        -- Generate random 4-character hex ID
        new_id := 'Ghost #' || lpad(to_hex(floor(random() * 65536)::integer), 4, '0');
        
        -- Check if ID already exists
        IF NOT EXISTS (SELECT 1 FROM ghost_replies WHERE ghost_id = new_id) THEN
            RETURN new_id;
        END IF;
        
        attempts := attempts + 1;
        IF attempts >= max_attempts THEN
            RAISE EXCEPTION 'Unable to generate unique ghost ID after % attempts', max_attempts;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to check ghost reply rate limits
CREATE OR REPLACE FUNCTION check_ghost_reply_rate_limit(
    p_user_id UUID,
    p_ip_address INET,
    p_rate_limit_key VARCHAR(255),
    p_max_replies_per_hour INTEGER DEFAULT 10,
    p_max_replies_per_day INTEGER DEFAULT 50
)
RETURNS TABLE(
    can_post BOOLEAN,
    reason TEXT,
    cooldown_remaining INTEGER,
    next_allowed_at TIMESTAMP WITH TIME ZONE
) AS $$
DECLARE
    v_rate_limit_record RECORD;
    v_hourly_count INTEGER;
    v_daily_count INTEGER;
    v_cooldown_remaining INTEGER;
BEGIN
    -- Check if user is suspended
    SELECT * INTO v_rate_limit_record 
    FROM ghost_reply_rate_limits 
    WHERE user_id = p_user_id AND ip_address = p_ip_address AND rate_limit_key = p_rate_limit_key;
    
    IF v_rate_limit_record.is_suspended AND v_rate_limit_record.suspended_until > NOW() THEN
        RETURN QUERY SELECT 
            FALSE as can_post,
            'User suspended' as reason,
            EXTRACT(EPOCH FROM (v_rate_limit_record.suspended_until - NOW()))::INTEGER as cooldown_remaining,
            v_rate_limit_record.suspended_until as next_allowed_at;
        RETURN;
    END IF;
    
    -- Check cooldown
    IF v_rate_limit_record.cooldown_until > NOW() THEN
        v_cooldown_remaining := EXTRACT(EPOCH FROM (v_rate_limit_record.cooldown_until - NOW()))::INTEGER;
        RETURN QUERY SELECT 
            FALSE as can_post,
            'Cooldown active' as reason,
            v_cooldown_remaining as cooldown_remaining,
            v_rate_limit_record.cooldown_until as next_allowed_at;
        RETURN;
    END IF;
    
    -- Count hourly and daily posts
    SELECT COUNT(*) INTO v_hourly_count
    FROM ghost_replies 
    WHERE author_id = p_user_id 
    AND created_at >= NOW() - INTERVAL '1 hour';
    
    SELECT COUNT(*) INTO v_daily_count
    FROM ghost_replies 
    WHERE author_id = p_user_id 
    AND created_at >= NOW() - INTERVAL '1 day';
    
    -- Check hourly limit
    IF v_hourly_count >= p_max_replies_per_hour THEN
        RETURN QUERY SELECT 
            FALSE as can_post,
            'Hourly limit exceeded' as reason,
            3600 as cooldown_remaining, -- 1 hour cooldown
            NOW() + INTERVAL '1 hour' as next_allowed_at;
        RETURN;
    END IF;
    
    -- Check daily limit
    IF v_daily_count >= p_max_replies_per_day THEN
        RETURN QUERY SELECT 
            FALSE as can_post,
            'Daily limit exceeded' as reason,
            86400 as cooldown_remaining, -- 24 hour cooldown
            NOW() + INTERVAL '1 day' as next_allowed_at;
        RETURN;
    END IF;
    
    -- All checks passed
    RETURN QUERY SELECT 
        TRUE as can_post,
        'OK' as reason,
        0 as cooldown_remaining,
        NOW() as next_allowed_at;
END;
$$ LANGUAGE plpgsql;

-- Function to apply rate limiting penalties
CREATE OR REPLACE FUNCTION apply_ghost_reply_penalty(
    p_user_id UUID,
    p_ip_address INET,
    p_rate_limit_key VARCHAR(255),
    p_penalty_type VARCHAR(50), -- 'warning', 'cooldown', 'suspension'
    p_duration_minutes INTEGER DEFAULT 0,
    p_reason TEXT DEFAULT ''
)
RETURNS BOOLEAN AS $$
DECLARE
    v_rate_limit_record RECORD;
    v_new_warning_count INTEGER;
    v_new_suspension_count INTEGER;
BEGIN
    -- Get or create rate limit record
    SELECT * INTO v_rate_limit_record 
    FROM ghost_reply_rate_limits 
    WHERE user_id = p_user_id AND ip_address = p_ip_address AND rate_limit_key = p_rate_limit_key;
    
    IF NOT FOUND THEN
        INSERT INTO ghost_reply_rate_limits (user_id, ip_address, rate_limit_key)
        VALUES (p_user_id, p_ip_address, p_rate_limit_key);
        v_rate_limit_record.warning_count := 0;
        v_rate_limit_record.suspension_count := 0;
    END IF;
    
    CASE p_penalty_type
        WHEN 'warning' THEN
            v_new_warning_count := v_rate_limit_record.warning_count + 1;
            UPDATE ghost_reply_rate_limits 
            SET warning_count = v_new_warning_count,
                updated_at = NOW()
            WHERE user_id = p_user_id AND ip_address = p_ip_address AND rate_limit_key = p_rate_limit_key;
            
        WHEN 'cooldown' THEN
            UPDATE ghost_reply_rate_limits 
            SET cooldown_until = NOW() + (p_duration_minutes || ' minutes')::INTERVAL,
                cooldown_reason = p_reason,
                updated_at = NOW()
            WHERE user_id = p_user_id AND ip_address = p_ip_address AND rate_limit_key = p_rate_limit_key;
            
        WHEN 'suspension' THEN
            v_new_suspension_count := v_rate_limit_record.suspension_count + 1;
            UPDATE ghost_reply_rate_limits 
            SET is_suspended = TRUE,
                suspended_until = NOW() + (p_duration_minutes || ' minutes')::INTERVAL,
                suspension_count = v_new_suspension_count,
                updated_at = NOW()
            WHERE user_id = p_user_id AND ip_address = p_ip_address AND rate_limit_key = p_rate_limit_key;
            
        ELSE
            RETURN FALSE;
    END CASE;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Function to get ghost reply statistics
CREATE OR REPLACE FUNCTION get_ghost_reply_stats(
    p_thread_id UUID,
    p_days_back INTEGER DEFAULT 30
)
RETURNS TABLE(
    total_replies BIGINT,
    total_likes BIGINT,
    total_views BIGINT,
    avg_spam_score DECIMAL(3,2),
    avg_toxicity_score DECIMAL(3,2),
    most_active_hour INTEGER,
    top_ghost_avatars TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(gr.id)::BIGINT as total_replies,
        COALESCE(SUM(gr.like_count), 0)::BIGINT as total_likes,
        COALESCE(SUM(gr.view_count), 0)::BIGINT as total_views,
        ROUND(AVG(gr.spam_score), 2) as avg_spam_score,
        ROUND(AVG(gr.toxicity_score), 2) as avg_toxicity_score,
        EXTRACT(hour FROM gr.created_at)::INTEGER as most_active_hour,
        ARRAY_AGG(DISTINCT gr.ghost_avatar) as top_ghost_avatars
    FROM ghost_replies gr
    WHERE gr.thread_id = p_thread_id
    AND gr.created_at >= NOW() - INTERVAL '1 day' * p_days_back
    AND gr.is_deleted = FALSE
    GROUP BY EXTRACT(hour FROM gr.created_at)
    ORDER BY COUNT(gr.id) DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Insert default ghost avatars
INSERT INTO ghost_avatars (avatar_id, avatar_name, avatar_description, avatar_category, avatar_style) VALUES
('ghost-1', 'Cute Ghost 1', 'Adorable cartoon ghost with friendly expression', 'cute', 'cartoon'),
('ghost-2', 'Cute Ghost 2', 'Sweet ghost with big eyes and smile', 'cute', 'cartoon'),
('ghost-3', 'Cute Ghost 3', 'Charming ghost with rosy cheeks', 'cute', 'cartoon'),
('ghost-4', 'Cute Ghost 4', 'Lovable ghost with heart-shaped features', 'cute', 'cartoon'),
('ghost-5', 'Cute Ghost 5', 'Gentle ghost with soft colors', 'cute', 'cartoon'),
('ghost-6', 'Cute Ghost 6', 'Friendly ghost with welcoming pose', 'cute', 'cartoon'),
('ghost-7', 'Cute Ghost 7', 'Playful ghost with fun expression', 'cute', 'cartoon'),
('ghost-8', 'Cute Ghost 8', 'Sweet ghost with innocent look', 'cute', 'cartoon'),
('ghost-9', 'Cute Ghost 9', 'Charming ghost with cute accessories', 'cute', 'cartoon'),
('ghost-10', 'Cute Ghost 10', 'Lovable ghost with gentle features', 'cute', 'cartoon'),
('ghost-11', 'Cute Ghost 11', 'Adorable ghost with sweet smile', 'cute', 'cartoon'),
('ghost-12', 'Cute Ghost 12', 'Friendly ghost with warm expression', 'cute', 'cartoon')
ON CONFLICT (avatar_id) DO NOTHING;

-- Grant permissions (adjust based on your database setup)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO sonet_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO sonet_user;