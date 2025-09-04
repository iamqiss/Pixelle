-- Sonet Moderation System Database Migration
-- Version: 1.0.0
-- Author: Sonet Engineering Team
-- Date: 2024-01-01

-- Enable UUID extension for PostgreSQL
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create moderation schema
CREATE SCHEMA IF NOT EXISTS moderation;

-- Enum types for moderation system
CREATE TYPE moderation.user_role AS ENUM (
    'user',
    'moderator', 
    'admin',
    'super_admin',
    'founder'
);

CREATE TYPE moderation.moderation_status AS ENUM (
    'clean',
    'flagged',
    'warned',
    'shadowbanned',
    'suspended',
    'banned'
);

CREATE TYPE moderation.verification_status AS ENUM (
    'unverified',
    'pending',
    'verified',
    'rejected',
    'founder_verified'
);

CREATE TYPE moderation.moderation_action_type AS ENUM (
    'flag',
    'warn',
    'shadowban',
    'suspend',
    'ban',
    'delete_note',
    'remove_flag'
);

CREATE TYPE moderation.moderation_severity AS ENUM (
    'low',
    'medium',
    'high',
    'critical'
);

CREATE TYPE moderation.flag_reason AS ENUM (
    'spam',
    'harassment',
    'inappropriate_content',
    'fake_news',
    'bot_activity',
    'violence',
    'hate_speech',
    'copyright_violation',
    'other'
);

-- Users table with moderation fields
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    display_name VARCHAR(100),
    bio TEXT,
    avatar_url TEXT,
    banner_url TEXT,
    location VARCHAR(100),
    website VARCHAR(255),
    phone_number VARCHAR(20),
    language VARCHAR(10) DEFAULT 'en',
    timezone VARCHAR(50),
    
    -- Moderation fields
    status VARCHAR(20) DEFAULT 'active',
    role moderation.user_role DEFAULT 'user',
    moderation_status moderation.moderation_status DEFAULT 'clean',
    email_verified moderation.verification_status DEFAULT 'unverified',
    phone_verified moderation.verification_status DEFAULT 'unverified',
    
    -- Flag fields
    flagged_at TIMESTAMP WITH TIME ZONE,
    flag_expires_at TIMESTAMP WITH TIME ZONE,
    flag_reason VARCHAR(100),
    flag_warning_message TEXT,
    
    -- Profile settings
    is_public_profile BOOLEAN DEFAULT true,
    allow_direct_messages BOOLEAN DEFAULT true,
    allow_mentions BOOLEAN DEFAULT true,
    
    -- Metadata
    interests TEXT[],
    skills TEXT[],
    social_links TEXT[],
    
    -- Timestamps
    last_active_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE,
    
    -- Audit fields
    created_by UUID,
    updated_by UUID,
    deleted_by UUID,
    
    -- Additional metadata as JSONB for flexibility
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Moderation actions table
CREATE TABLE IF NOT EXISTS moderation.actions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    target_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    target_username VARCHAR(50) NOT NULL,
    moderator_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    moderator_username VARCHAR(50) NOT NULL,
    
    action_type moderation.moderation_action_type NOT NULL,
    severity moderation.moderation_severity DEFAULT 'medium',
    reason VARCHAR(255) NOT NULL,
    details TEXT,
    warning_message TEXT,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Status
    is_active BOOLEAN DEFAULT true,
    is_anonymous BOOLEAN DEFAULT true,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    
    -- Constraints
    CONSTRAINT valid_expiration CHECK (expires_at IS NULL OR expires_at > created_at)
);

-- Account flags table
CREATE TABLE IF NOT EXISTS moderation.account_flags (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    username VARCHAR(50) NOT NULL,
    
    reason moderation.flag_reason NOT NULL,
    warning_message TEXT,
    custom_reason VARCHAR(255),
    
    -- Timestamps
    flagged_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    reviewed_at TIMESTAMP WITH TIME ZONE,
    
    -- Status
    is_active BOOLEAN DEFAULT true,
    is_reviewed BOOLEAN DEFAULT false,
    is_auto_expired BOOLEAN DEFAULT false,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    
    -- Constraints
    CONSTRAINT valid_flag_expiration CHECK (expires_at > flagged_at)
);

-- Moderation queue table
CREATE TABLE IF NOT EXISTS moderation.queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    username VARCHAR(50) NOT NULL,
    
    content_type VARCHAR(50) NOT NULL, -- 'note', 'profile', 'user'
    content_id UUID NOT NULL,
    content_preview TEXT,
    
    flag_reason moderation.flag_reason NOT NULL,
    custom_reason VARCHAR(255),
    
    -- Reporter info (hidden from users)
    reporter_id UUID REFERENCES users(id) ON DELETE SET NULL,
    reporter_username VARCHAR(50),
    
    -- Timestamps
    reported_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() + INTERVAL '7 days'),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    
    -- Status
    is_reviewed BOOLEAN DEFAULT false,
    is_auto_expired BOOLEAN DEFAULT false,
    priority_level VARCHAR(20) DEFAULT 'normal',
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Moderation statistics table
CREATE TABLE IF NOT EXISTS moderation.statistics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    period_start TIMESTAMP WITH TIME ZONE NOT NULL,
    period_end TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Counts
    total_flags INTEGER DEFAULT 0,
    total_warnings INTEGER DEFAULT 0,
    total_shadowbans INTEGER DEFAULT 0,
    total_suspensions INTEGER DEFAULT 0,
    total_bans INTEGER DEFAULT 0,
    total_notes_deleted INTEGER DEFAULT 0,
    
    -- Auto-expired items
    auto_expired_flags INTEGER DEFAULT 0,
    auto_expired_warnings INTEGER DEFAULT 0,
    
    -- Review counts
    manual_reviews INTEGER DEFAULT 0,
    automated_actions INTEGER DEFAULT 0,
    
    -- Response times
    avg_response_time_minutes INTEGER,
    max_response_time_minutes INTEGER,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    
    -- Constraints
    CONSTRAINT valid_period CHECK (period_end > period_start)
);

-- Audit log table
CREATE TABLE IF NOT EXISTS moderation.audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Action details
    action_type VARCHAR(100) NOT NULL,
    target_type VARCHAR(50) NOT NULL, -- 'user', 'note', 'profile'
    target_id UUID NOT NULL,
    target_username VARCHAR(50),
    
    -- Actor details
    actor_id UUID REFERENCES users(id) ON DELETE SET NULL,
    actor_username VARCHAR(50),
    actor_role VARCHAR(50),
    
    -- Action metadata
    action_details JSONB NOT NULL,
    ip_address INET,
    user_agent TEXT,
    session_id VARCHAR(255),
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb
);

-- Create indexes for performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_moderation_status ON users(moderation_status);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_flagged_at ON users(flagged_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_flag_expires_at ON users(flag_expires_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users(email);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_actions_target_user ON moderation.actions(target_user_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_actions_moderator ON moderation.actions(moderator_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_actions_type ON moderation.actions(action_type);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_actions_created_at ON moderation.actions(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_actions_expires_at ON moderation.actions(expires_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_actions_active ON moderation.actions(is_active);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_flags_user ON moderation.account_flags(user_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_flags_reason ON moderation.account_flags(reason);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_flags_flagged_at ON moderation.account_flags(flagged_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_flags_expires_at ON moderation.account_flags(expires_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_flags_active ON moderation.account_flags(is_active);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_queue_user ON moderation.queue(user_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_queue_content ON moderation.queue(content_type, content_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_queue_reported_at ON moderation.queue(reported_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_queue_expires_at ON moderation.queue(expires_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_queue_reviewed ON moderation.queue(is_reviewed);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_stats_period ON moderation.statistics(period_start, period_end);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_audit_target ON moderation.audit_log(target_type, target_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_audit_actor ON moderation.audit_log(actor_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_audit_created_at ON moderation.audit_log(created_at);

-- Create partial indexes for active items
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_actions_active_partial ON moderation.actions(created_at) WHERE is_active = true;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_flags_active_partial ON moderation.account_flags(flagged_at) WHERE is_active = true;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_queue_unreviewed_partial ON moderation.queue(reported_at) WHERE is_reviewed = false;

-- Create GIN indexes for JSONB fields
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_metadata_gin ON users USING GIN (metadata);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_actions_metadata_gin ON moderation.actions USING GIN (metadata);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_flags_metadata_gin ON moderation.account_flags USING GIN (metadata);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_queue_metadata_gin ON moderation.queue USING GIN (metadata);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_stats_metadata_gin ON moderation.statistics USING GIN (metadata);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_audit_metadata_gin ON moderation.audit_log USING GIN (metadata);

-- Create unique constraints
ALTER TABLE moderation.account_flags ADD CONSTRAINT unique_active_user_flag UNIQUE (user_id) WHERE is_active = true;
ALTER TABLE moderation.queue ADD CONSTRAINT unique_content_report UNIQUE (content_type, content_id, reporter_id) WHERE NOT is_reviewed;

-- Create check constraints
ALTER TABLE users ADD CONSTRAINT valid_username CHECK (username ~ '^[a-zA-Z0-9_]{3,50}$');
ALTER TABLE users ADD CONSTRAINT valid_email CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
ALTER TABLE moderation.actions ADD CONSTRAINT valid_severity_reason CHECK (
    (severity = 'critical' AND reason IN ('violence', 'hate_speech', 'copyright_violation')) OR
    (severity = 'high' AND reason IN ('harassment', 'inappropriate_content')) OR
    (severity = 'medium' AND reason IN ('spam', 'fake_news')) OR
    (severity = 'low' AND reason IN ('bot_activity', 'other'))
);

-- Create functions for automatic updates
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for automatic timestamp updates
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_moderation_actions_updated_at BEFORE UPDATE ON moderation.actions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create function to automatically expire flags
CREATE OR REPLACE FUNCTION expire_flags()
RETURNS void AS $$
BEGIN
    UPDATE moderation.account_flags 
    SET is_active = false, is_auto_expired = true 
    WHERE expires_at < NOW() AND is_active = true;
    
    UPDATE users 
    SET moderation_status = 'clean', 
        flagged_at = NULL, 
        flag_expires_at = NULL, 
        flag_reason = NULL, 
        flag_warning_message = NULL
    WHERE flag_expires_at < NOW() AND moderation_status = 'flagged';
END;
$$ LANGUAGE plpgsql;

-- Create function to clean up expired moderation actions
CREATE OR REPLACE FUNCTION cleanup_expired_actions()
RETURNS void AS $$
BEGIN
    UPDATE moderation.actions 
    SET is_active = false 
    WHERE expires_at < NOW() AND is_active = true;
END;
$$ LANGUAGE plpgsql;

-- Create function to update moderation statistics
CREATE OR REPLACE FUNCTION update_moderation_stats()
RETURNS void AS $$
DECLARE
    period_start TIMESTAMP WITH TIME ZONE;
    period_end TIMESTAMP WITH TIME ZONE;
BEGIN
    -- Set period to last 24 hours
    period_start := date_trunc('hour', NOW() - INTERVAL '24 hours');
    period_end := date_trunc('hour', NOW());
    
    -- Insert or update statistics
    INSERT INTO moderation.statistics (
        period_start, period_end,
        total_flags, total_warnings, total_shadowbans, total_suspensions, total_bans,
        total_notes_deleted, auto_expired_flags, manual_reviews
    )
    SELECT 
        period_start, period_end,
        COUNT(CASE WHEN action_type = 'flag' THEN 1 END),
        COUNT(CASE WHEN action_type = 'warn' THEN 1 END),
        COUNT(CASE WHEN action_type = 'shadowban' THEN 1 END),
        COUNT(CASE WHEN action_type = 'suspend' THEN 1 END),
        COUNT(CASE WHEN action_type = 'ban' THEN 1 END),
        COUNT(CASE WHEN action_type = 'delete_note' THEN 1 END),
        COUNT(CASE WHEN is_auto_expired = true THEN 1 END),
        COUNT(CASE WHEN is_reviewed = true THEN 1 END)
    FROM moderation.actions 
    WHERE created_at >= period_start AND created_at < period_end
    ON CONFLICT (period_start, period_end) DO UPDATE SET
        total_flags = EXCLUDED.total_flags,
        total_warnings = EXCLUDED.total_warnings,
        total_shadowbans = EXCLUDED.total_shadowbans,
        total_suspensions = EXCLUDED.total_suspensions,
        total_bans = EXCLUDED.total_bans,
        total_notes_deleted = EXCLUDED.total_notes_deleted,
        auto_expired_flags = EXCLUDED.auto_expired_flags,
        manual_reviews = EXCLUDED.manual_reviews;
END;
$$ LANGUAGE plpgsql;

-- Create scheduled jobs (requires pg_cron extension)
-- SELECT cron.schedule('expire-flags', '0 */6 * * *', 'SELECT expire_flags();');
-- SELECT cron.schedule('cleanup-actions', '0 2 * * *', 'SELECT cleanup_expired_actions();');
-- SELECT cron.schedule('update-stats', '0 * * * *', 'SELECT update_moderation_stats();');

-- Insert default founder account (Neo Qiss)
INSERT INTO users (
    username, 
    email, 
    hashed_password, 
    display_name, 
    role, 
    moderation_status, 
    email_verified,
    is_public_profile,
    created_at
) VALUES (
    'neoqiss',
    'neo@sonet.com',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/RK.s5uOeK', -- Change this in production
    'Neo Qiss',
    'founder',
    'clean',
    'verified',
    true,
    NOW()
) ON CONFLICT (username) DO NOTHING;

-- Create views for common queries
CREATE VIEW moderation.active_flags AS
SELECT 
    af.*,
    u.username as target_username,
    u.email as target_email
FROM moderation.account_flags af
JOIN users u ON af.user_id = u.id
WHERE af.is_active = true AND af.expires_at > NOW();

CREATE VIEW moderation.pending_reviews AS
SELECT 
    q.*,
    u.username as target_username,
    u.email as target_email
FROM moderation.queue q
JOIN users u ON q.user_id = u.id
WHERE q.is_reviewed = false AND q.expires_at > NOW();

CREATE VIEW moderation.recent_actions AS
SELECT 
    ma.*,
    u.username as target_username
FROM moderation.actions ma
JOIN users u ON ma.target_user_id = u.id
WHERE ma.created_at >= NOW() - INTERVAL '24 hours'
ORDER BY ma.created_at DESC;

-- Grant permissions
GRANT USAGE ON SCHEMA moderation TO sonet_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA moderation TO sonet_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA moderation TO sonet_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA moderation TO sonet_app;

-- Create indexes for performance monitoring
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_performance_created_at ON moderation.actions(created_at) WHERE created_at >= NOW() - INTERVAL '30 days';
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_moderation_performance_target_user ON moderation.actions(target_user_id, created_at) WHERE created_at >= NOW() - INTERVAL '30 days';

-- Add comments for documentation
COMMENT ON TABLE users IS 'Main users table with moderation capabilities';
COMMENT ON TABLE moderation.actions IS 'All moderation actions taken by moderators and founders';
COMMENT ON TABLE moderation.account_flags IS 'Account flags with automatic expiration';
COMMENT ON TABLE moderation.queue IS 'Moderation queue for content review';
COMMENT ON TABLE moderation.statistics IS 'Aggregated moderation statistics';
COMMENT ON TABLE moderation.audit_log IS 'Complete audit trail of all moderation actions';

COMMENT ON COLUMN users.role IS 'User role in the system (founder has full privileges)';
COMMENT ON COLUMN users.moderation_status IS 'Current moderation status of the account';
COMMENT ON COLUMN moderation.actions.is_anonymous IS 'Whether the moderator identity is hidden from users';
COMMENT ON COLUMN moderation.account_flags.expires_at IS 'Automatic expiration after 60 days';