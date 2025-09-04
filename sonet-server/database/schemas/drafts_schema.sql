/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

-- Drafts Service Database Schema
-- Handles post drafts and draft management

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Drafts table
CREATE TABLE drafts (
    draft_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    title VARCHAR(255),
    content TEXT NOT NULL,
    reply_to_uri TEXT,
    quote_uri TEXT,
    mention_handle VARCHAR(255),
    image_uris JSONB DEFAULT '[]',
    video_uri JSONB,
    labels JSONB DEFAULT '[]',
    threadgate JSONB,
    interaction_settings JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_auto_saved BOOLEAN DEFAULT FALSE
);

-- Draft images table for better organization
CREATE TABLE draft_images (
    image_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    draft_id UUID NOT NULL REFERENCES drafts(draft_id) ON DELETE CASCADE,
    uri TEXT NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    alt_text TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_drafts_user_id ON drafts(user_id);
CREATE INDEX idx_drafts_created_at ON drafts(created_at);
CREATE INDEX idx_drafts_updated_at ON drafts(updated_at);
CREATE INDEX idx_drafts_is_auto_saved ON drafts(is_auto_saved);

CREATE INDEX idx_draft_images_draft_id ON draft_images(draft_id);

-- Full-text search index on drafts
CREATE INDEX idx_drafts_search ON drafts USING gin(
    to_tsvector('english', 
        COALESCE(title, '') || ' ' || 
        COALESCE(content, '')
    )
);

-- Functions and triggers

-- Update draft updated_at timestamp
CREATE OR REPLACE FUNCTION update_draft_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_draft_updated_at_trigger
    BEFORE UPDATE ON drafts
    FOR EACH ROW
    EXECUTE FUNCTION update_draft_updated_at();

-- Clean up old auto-saved drafts (older than 30 days)
CREATE OR REPLACE FUNCTION cleanup_old_auto_saved_drafts()
RETURNS void AS $$
BEGIN
    DELETE FROM drafts 
    WHERE is_auto_saved = TRUE 
    AND updated_at < NOW() - INTERVAL '30 days';
END;
$$ LANGUAGE plpgsql;

-- Views for common queries

-- User drafts with image count
CREATE VIEW user_drafts_with_image_count AS
SELECT 
    d.*,
    COUNT(di.image_id) as image_count
FROM drafts d
LEFT JOIN draft_images di ON d.draft_id = di.draft_id
GROUP BY d.draft_id, d.user_id, d.title, d.content, d.reply_to_uri, 
         d.quote_uri, d.mention_handle, d.image_uris, d.video_uri, 
         d.labels, d.threadgate, d.interaction_settings, d.created_at, d.updated_at, d.is_auto_saved;

-- Recent drafts for user
CREATE VIEW recent_user_drafts AS
SELECT 
    d.*,
    COUNT(di.image_id) as image_count
FROM drafts d
LEFT JOIN draft_images di ON d.draft_id = di.draft_id
WHERE d.is_auto_saved = FALSE
GROUP BY d.draft_id, d.user_id, d.title, d.content, d.reply_to_uri, 
         d.quote_uri, d.mention_handle, d.image_uris, d.video_uri, 
         d.labels, d.threadgate, d.interaction_settings, d.created_at, d.updated_at, d.is_auto_saved
ORDER BY d.updated_at DESC;

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO drafts_service_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO drafts_service_user;