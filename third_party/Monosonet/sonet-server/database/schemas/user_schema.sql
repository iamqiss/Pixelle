/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

-- User Service Database Schema
-- Handles user accounts, authentication, and profiles

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Users table
CREATE TABLE users (
    user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    salt VARCHAR(255) NOT NULL,
    display_name VARCHAR(100),
    bio TEXT,
    avatar_url TEXT,
    location VARCHAR(100),
    website VARCHAR(255),
    status VARCHAR(20) NOT NULL DEFAULT 'pending_verification' 
        CHECK (status IN ('pending_verification', 'active', 'suspended', 'deactivated', 'banned')),
    is_verified BOOLEAN DEFAULT FALSE,
    is_private BOOLEAN DEFAULT FALSE,
    email_verified_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_login TIMESTAMP WITH TIME ZONE,
    last_activity TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- User sessions table
CREATE TABLE user_sessions (
    session_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    device_id VARCHAR(255),
    device_name VARCHAR(255),
    ip_address INET,
    user_agent TEXT,
    type VARCHAR(20) NOT NULL DEFAULT 'web' 
        CHECK (type IN ('web', 'mobile', 'api', 'admin')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_activity TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    is_suspicious BOOLEAN DEFAULT FALSE,
    location_info JSONB,
    refresh_token_hash VARCHAR(255),
    access_token_hash VARCHAR(255)
);

-- Two-factor authentication table
CREATE TABLE two_factor_auth (
    user_id UUID PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    is_enabled BOOLEAN DEFAULT FALSE,
    secret_key VARCHAR(255),
    qr_code_url TEXT,
    backup_codes TEXT[], -- Array of hashed backup codes
    setup_at TIMESTAMP WITH TIME ZONE,
    last_used_at TIMESTAMP WITH TIME ZONE
);

-- Password reset tokens table
CREATE TABLE password_reset_tokens (
    token_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    token_hash VARCHAR(255) NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    used_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ip_address INET,
    user_agent TEXT
);

-- Email verification tokens table
CREATE TABLE email_verification_tokens (
    token_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    token_hash VARCHAR(255) NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    used_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- User settings table
CREATE TABLE user_settings (
    user_id UUID PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    settings JSONB DEFAULT '{}',
    privacy_settings JSONB DEFAULT '{}',
    notification_settings JSONB DEFAULT '{}',
    theme_settings JSONB DEFAULT '{}',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- User statistics table (denormalized for performance)
CREATE TABLE user_stats (
    user_id UUID PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    follower_count BIGINT DEFAULT 0,
    following_count BIGINT DEFAULT 0,
    note_count BIGINT DEFAULT 0,
    like_count BIGINT DEFAULT 0,
    repost_count BIGINT DEFAULT 0,
    comment_count BIGINT DEFAULT 0,
    last_stats_update TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- User login history table
CREATE TABLE user_login_history (
    login_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    ip_address INET,
    user_agent TEXT,
    location_info JSONB,
    success BOOLEAN NOT NULL,
    failure_reason VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_status ON users(status);
CREATE INDEX idx_users_created_at ON users(created_at);
CREATE INDEX idx_users_last_activity ON users(last_activity);

CREATE INDEX idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_user_sessions_expires_at ON user_sessions(expires_at);
CREATE INDEX idx_user_sessions_is_active ON user_sessions(is_active);
CREATE INDEX idx_user_sessions_device_id ON user_sessions(device_id);

CREATE INDEX idx_password_reset_tokens_user_id ON password_reset_tokens(user_id);
CREATE INDEX idx_password_reset_tokens_expires_at ON password_reset_tokens(expires_at);
CREATE INDEX idx_password_reset_tokens_token_hash ON password_reset_tokens(token_hash);

CREATE INDEX idx_email_verification_tokens_user_id ON email_verification_tokens(user_id);
CREATE INDEX idx_email_verification_tokens_expires_at ON email_verification_tokens(expires_at);

CREATE INDEX idx_user_login_history_user_id ON user_login_history(user_id);
CREATE INDEX idx_user_login_history_created_at ON user_login_history(created_at);
CREATE INDEX idx_user_login_history_success ON user_login_history(success);

-- Full-text search index on users
CREATE INDEX idx_users_search ON users USING gin(
    to_tsvector('english', 
        COALESCE(username, '') || ' ' || 
        COALESCE(display_name, '') || ' ' || 
        COALESCE(bio, '')
    )
);

-- Functions and triggers

-- Update user last_activity
CREATE OR REPLACE FUNCTION update_user_last_activity()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE users 
    SET last_activity = NOW()
    WHERE user_id = NEW.user_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_user_last_activity_trigger
    AFTER INSERT OR UPDATE ON user_sessions
    FOR EACH ROW
    EXECUTE FUNCTION update_user_last_activity();

-- Update user_stats when user is created
CREATE OR REPLACE FUNCTION initialize_user_stats()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO user_stats (user_id) VALUES (NEW.user_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER initialize_user_stats_trigger
    AFTER INSERT ON users
    FOR EACH ROW
    EXECUTE FUNCTION initialize_user_stats();

-- Clean up expired sessions
CREATE OR REPLACE FUNCTION cleanup_expired_sessions()
RETURNS void AS $$
BEGIN
    DELETE FROM user_sessions 
    WHERE expires_at < NOW() OR NOT is_active;
END;
$$ LANGUAGE plpgsql;

-- Clean up expired tokens
CREATE OR REPLACE FUNCTION cleanup_expired_tokens()
RETURNS void AS $$
BEGIN
    DELETE FROM password_reset_tokens WHERE expires_at < NOW();
    DELETE FROM email_verification_tokens WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;

-- Views for common queries

-- Active user sessions
CREATE VIEW active_user_sessions AS
SELECT 
    us.*,
    u.username,
    u.display_name
FROM user_sessions us
JOIN users u ON us.user_id = u.user_id
WHERE us.is_active AND us.expires_at > NOW();

-- User profile with stats
CREATE VIEW user_profile_with_stats AS
SELECT 
    u.*,
    us.follower_count,
    us.following_count,
    us.note_count,
    us.like_count,
    us.repost_count,
    us.comment_count
FROM users u
LEFT JOIN user_stats us ON u.user_id = us.user_id;

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO user_service_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO user_service_user;