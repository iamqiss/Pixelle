/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

-- Migration: Create Ghost Reply Tables
-- Version: 002
-- Description: Adds ghost reply functionality for anonymous commenting
-- Date: 2025-01-26

-- Begin transaction
BEGIN;

-- Check if migration has already been applied
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'ghost_replies') THEN
        RAISE NOTICE 'Ghost reply tables already exist, skipping migration';
        RETURN;
    END IF;
END $$;

-- Enable necessary extensions if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Create ghost replies table
CREATE TABLE IF NOT EXISTS ghost_replies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    content TEXT NOT NULL,
    ghost_avatar VARCHAR(255) NOT NULL,
    ghost_id VARCHAR(10) NOT NULL,
    thread_id UUID NOT NULL,
    parent_note_id UUID REFERENCES notes(note_id) ON DELETE CASCADE,
    
    -- Content metadata
    language VARCHAR(10) DEFAULT 'en',
    tags TEXT[],
    content_warning VARCHAR(50) DEFAULT 'none',
    
    -- Moderation fields
    is_deleted BOOLEAN DEFAULT FALSE,
    is_hidden BOOLEAN DEFAULT FALSE,
    is_flagged BOOLEAN DEFAULT FALSE,
    spam_score DECIMAL(3,2) DEFAULT 0.0,
    toxicity_score DECIMAL(3,2) DEFAULT 0.0,
    moderation_status VARCHAR(20) DEFAULT 'pending' 
        CHECK (moderation_status IN ('pending', 'approved', 'rejected', 'flagged')),
    
    -- Engagement tracking (anonymous)
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
    CONSTRAINT ghost_reply_toxicity_score_range CHECK (toxicity_score >= 0.0 AND toxicity_score <= 1.0)
);

-- Create ghost reply media attachments table
CREATE TABLE IF NOT EXISTS ghost_reply_media (
    media_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ghost_reply_id UUID NOT NULL REFERENCES ghost_replies(id) ON DELETE CASCADE,
    media_type VARCHAR(20) NOT NULL 
        CHECK (media_type IN ('image', 'video', 'audio', 'file', 'gif')),
    url TEXT NOT NULL,
    thumbnail_url TEXT,
    alt_text TEXT,
    caption TEXT,
    file_size BIGINT,
    duration INTEGER,
    width INTEGER,
    height INTEGER,
    metadata JSONB DEFAULT '{}',
    order_index INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create ghost reply likes table
CREATE TABLE IF NOT EXISTS ghost_reply_likes (
    like_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ghost_reply_id UUID NOT NULL REFERENCES ghost_replies(id) ON DELETE CASCADE,
    anonymous_user_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(ghost_reply_id, anonymous_user_hash)
);

-- Create ghost reply replies table
CREATE TABLE IF NOT EXISTS ghost_reply_replies (
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

-- Create ghost reply moderation log table
CREATE TABLE IF NOT EXISTS ghost_reply_moderation_log (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ghost_reply_id UUID NOT NULL REFERENCES ghost_replies(id) ON DELETE CASCADE,
    moderator_id UUID NOT NULL,
    action VARCHAR(50) NOT NULL,
    reason TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create ghost reply analytics table
CREATE TABLE IF NOT EXISTS ghost_reply_analytics (
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

-- Create ghost avatars table
CREATE TABLE IF NOT EXISTS ghost_avatars (
    avatar_id VARCHAR(50) PRIMARY KEY,
    avatar_name VARCHAR(100) NOT NULL,
    avatar_description TEXT,
    avatar_category VARCHAR(50) DEFAULT 'cute',
    avatar_style VARCHAR(50) DEFAULT 'cartoon',
    is_active BOOLEAN DEFAULT TRUE,
    usage_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create ghost reply thread tracking table
CREATE TABLE IF NOT EXISTS ghost_reply_threads (
    thread_id UUID PRIMARY KEY,
    note_id UUID NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
    ghost_reply_count INTEGER DEFAULT 0,
    last_ghost_reply_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_ghost_replies_thread_id ON ghost_replies(thread_id);
CREATE INDEX IF NOT EXISTS idx_ghost_replies_parent_note_id ON ghost_replies(parent_note_id);
CREATE INDEX IF NOT EXISTS idx_ghost_replies_ghost_id ON ghost_replies(ghost_id);
CREATE INDEX IF NOT EXISTS idx_ghost_replies_created_at ON ghost_replies(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ghost_replies_moderation_status ON ghost_replies(moderation_status);
CREATE INDEX IF NOT EXISTS idx_ghost_replies_spam_score ON ghost_replies(spam_score DESC);
CREATE INDEX IF NOT EXISTS idx_ghost_replies_toxicity_score ON ghost_replies(toxicity_score DESC);

CREATE INDEX IF NOT EXISTS idx_ghost_reply_media_reply_id ON ghost_reply_media(ghost_reply_id);
CREATE INDEX IF NOT EXISTS idx_ghost_reply_likes_reply_id ON ghost_reply_likes(ghost_reply_id);
CREATE INDEX IF NOT EXISTS idx_ghost_reply_replies_parent_id ON ghost_reply_replies(parent_ghost_reply_id);
CREATE INDEX IF NOT EXISTS idx_ghost_reply_replies_thread_id ON ghost_reply_replies(thread_id);

CREATE INDEX IF NOT EXISTS idx_ghost_reply_moderation_log_reply_id ON ghost_reply_moderation_log(ghost_reply_id);
CREATE INDEX IF NOT EXISTS idx_ghost_reply_moderation_log_moderator_id ON ghost_reply_moderation_log(moderator_id);
CREATE INDEX IF NOT EXISTS idx_ghost_reply_moderation_log_action ON ghost_reply_moderation_log(action);
CREATE INDEX IF NOT EXISTS idx_ghost_reply_moderation_log_created_at ON ghost_reply_moderation_log(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ghost_reply_analytics_reply_id ON ghost_reply_analytics(ghost_reply_id);
CREATE INDEX IF NOT EXISTS idx_ghost_reply_analytics_date ON ghost_reply_analytics(date DESC);
CREATE INDEX IF NOT EXISTS idx_ghost_reply_analytics_date_hour ON ghost_reply_analytics(date, hour);

CREATE INDEX IF NOT EXISTS idx_ghost_reply_threads_note_id ON ghost_reply_threads(note_id);

-- Create full-text search index
CREATE INDEX IF NOT EXISTS idx_ghost_replies_search ON ghost_replies USING gin(search_vector);

-- Create or replace functions
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

CREATE OR REPLACE FUNCTION generate_unique_ghost_id()
RETURNS VARCHAR(10) AS $$
DECLARE
    new_id VARCHAR(10);
    attempts INTEGER := 0;
    max_attempts INTEGER := 100;
BEGIN
    LOOP
        new_id := 'Ghost #' || lpad(to_hex(floor(random() * 65536)::integer), 4, '0');
        
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

-- Create triggers
DROP TRIGGER IF EXISTS trigger_ghost_reply_search_vector ON ghost_replies;
CREATE TRIGGER trigger_ghost_reply_search_vector
    BEFORE INSERT OR UPDATE ON ghost_replies
    FOR EACH ROW
    EXECUTE FUNCTION update_ghost_reply_search_vector();

DROP TRIGGER IF EXISTS trigger_ghost_reply_thread_count ON ghost_replies;
CREATE TRIGGER trigger_ghost_reply_thread_count
    AFTER INSERT OR DELETE ON ghost_replies
    FOR EACH ROW
    EXECUTE FUNCTION update_ghost_reply_thread_count();

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

-- Create migration tracking table if it doesn't exist
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(50) PRIMARY KEY,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    description TEXT
);

-- Record this migration
INSERT INTO schema_migrations (version, description) VALUES 
('002_create_ghost_reply_tables', 'Adds ghost reply functionality for anonymous commenting')
ON CONFLICT (version) DO NOTHING;

-- Commit transaction
COMMIT;

-- Log successful migration
DO $$
BEGIN
    RAISE NOTICE 'Migration 002: Ghost reply tables created successfully';
END $$;