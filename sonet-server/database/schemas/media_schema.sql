/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

-- Media Service Database Schema
-- Handles media files, uploads, processing, and storage

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Media files table
CREATE TABLE media_files (
    media_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    owner_user_id UUID NOT NULL,
    filename VARCHAR(255) NOT NULL,
    original_filename VARCHAR(255) NOT NULL,
    content_type VARCHAR(100) NOT NULL,
    file_size BIGINT NOT NULL,
    media_type VARCHAR(20) NOT NULL 
        CHECK (media_type IN ('image', 'video', 'audio', 'file', 'gif', 'document')),
    status VARCHAR(20) NOT NULL DEFAULT 'uploading' 
        CHECK (status IN ('uploading', 'processing', 'ready', 'failed', 'deleted')),
    
    -- Storage paths
    original_url TEXT,
    thumbnail_url TEXT,
    hls_url TEXT, -- for video streaming
    webp_url TEXT, -- for web-optimized images
    mp4_url TEXT, -- for video downloads
    
    -- Media metadata
    width INTEGER,
    height INTEGER,
    duration INTEGER, -- for video/audio in seconds
    bitrate INTEGER, -- for video/audio
    fps REAL, -- for video
    color_space VARCHAR(20),
    exif_data JSONB,
    
    -- Processing info
    processing_started_at TIMESTAMP WITH TIME ZONE,
    processing_completed_at TIMESTAMP WITH TIME ZONE,
    processing_error TEXT,
    processing_progress INTEGER DEFAULT 0, -- 0-100
    
    -- Upload info
    upload_session_id UUID,
    chunk_count INTEGER DEFAULT 1,
    uploaded_chunks INTEGER DEFAULT 0,
    
    -- Access control
    is_public BOOLEAN DEFAULT FALSE,
    access_token VARCHAR(255),
    expires_at TIMESTAMP WITH TIME ZONE,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- Upload sessions table
CREATE TABLE upload_sessions (
    session_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    filename VARCHAR(255) NOT NULL,
    content_type VARCHAR(100) NOT NULL,
    file_size BIGINT NOT NULL,
    chunk_size INTEGER NOT NULL DEFAULT 1048576, -- 1MB default
    total_chunks INTEGER NOT NULL,
    uploaded_chunks INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'active' 
        CHECK (status IN ('active', 'completed', 'cancelled', 'expired')),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Upload chunks table
CREATE TABLE upload_chunks (
    chunk_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES upload_sessions(session_id) ON DELETE CASCADE,
    chunk_number INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    chunk_hash VARCHAR(64) NOT NULL, -- SHA-256 hash
    chunk_data BYTEA,
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(session_id, chunk_number)
);

-- Media processing jobs table
CREATE TABLE media_processing_jobs (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    media_id UUID NOT NULL REFERENCES media_files(media_id) ON DELETE CASCADE,
    job_type VARCHAR(50) NOT NULL 
        CHECK (job_type IN ('thumbnail', 'hls', 'webp', 'mp4', 'metadata_extraction')),
    status VARCHAR(20) NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')),
    priority INTEGER DEFAULT 5, -- 1-10, higher is more important
    attempts INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,
    error_message TEXT,
    result_data JSONB,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Media access logs table
CREATE TABLE media_access_logs (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    media_id UUID NOT NULL REFERENCES media_files(media_id) ON DELETE CASCADE,
    user_id UUID, -- NULL for anonymous access
    ip_address INET,
    user_agent TEXT,
    access_type VARCHAR(20) NOT NULL 
        CHECK (access_type IN ('view', 'download', 'stream', 'thumbnail')),
    referrer TEXT,
    success BOOLEAN NOT NULL,
    error_message TEXT,
    response_time_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Media tags table
CREATE TABLE media_tags (
    tag_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    media_id UUID NOT NULL REFERENCES media_files(media_id) ON DELETE CASCADE,
    tag_name VARCHAR(100) NOT NULL,
    tag_value TEXT,
    confidence REAL DEFAULT 1.0, -- for AI-generated tags
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(media_id, tag_name)
);

-- Media collections table
CREATE TABLE media_collections (
    collection_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    owner_user_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    is_public BOOLEAN DEFAULT FALSE,
    cover_media_id UUID REFERENCES media_files(media_id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Media collection items table
CREATE TABLE media_collection_items (
    collection_id UUID NOT NULL REFERENCES media_collections(collection_id) ON DELETE CASCADE,
    media_id UUID NOT NULL REFERENCES media_files(media_id) ON DELETE CASCADE,
    order_index INTEGER DEFAULT 0,
    added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (collection_id, media_id)
);

-- Storage quotas table
CREATE TABLE user_storage_quotas (
    user_id UUID PRIMARY KEY,
    total_quota_bytes BIGINT NOT NULL DEFAULT 1073741824, -- 1GB default
    used_bytes BIGINT DEFAULT 0,
    file_count INTEGER DEFAULT 0,
    last_quota_check TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_media_files_owner_user_id ON media_files(owner_user_id);
CREATE INDEX idx_media_files_status ON media_files(status);
CREATE INDEX idx_media_files_media_type ON media_files(media_type);
CREATE INDEX idx_media_files_created_at ON media_files(created_at DESC);
CREATE INDEX idx_media_files_content_type ON media_files(content_type);
CREATE INDEX idx_media_files_is_public ON media_files(is_public);

CREATE INDEX idx_upload_sessions_user_id ON upload_sessions(user_id);
CREATE INDEX idx_upload_sessions_status ON upload_sessions(status);
CREATE INDEX idx_upload_sessions_expires_at ON upload_sessions(expires_at);

CREATE INDEX idx_upload_chunks_session_id ON upload_chunks(session_id);
CREATE INDEX idx_upload_chunks_chunk_number ON upload_chunks(chunk_number);

CREATE INDEX idx_media_processing_jobs_media_id ON media_processing_jobs(media_id);
CREATE INDEX idx_media_processing_jobs_status ON media_processing_jobs(status);
CREATE INDEX idx_media_processing_jobs_priority ON media_processing_jobs(priority DESC);
CREATE INDEX idx_media_processing_jobs_created_at ON media_processing_jobs(created_at);

CREATE INDEX idx_media_access_logs_media_id ON media_access_logs(media_id);
CREATE INDEX idx_media_access_logs_user_id ON media_access_logs(user_id);
CREATE INDEX idx_media_access_logs_created_at ON media_access_logs(created_at);

CREATE INDEX idx_media_tags_media_id ON media_tags(media_id);
CREATE INDEX idx_media_tags_tag_name ON media_tags(tag_name);

CREATE INDEX idx_media_collections_owner_user_id ON media_collections(owner_user_id);
CREATE INDEX idx_media_collections_is_public ON media_collections(is_public);

CREATE INDEX idx_media_collection_items_collection_id ON media_collection_items(collection_id);
CREATE INDEX idx_media_collection_items_media_id ON media_collection_items(media_id);

-- Full-text search index on media tags
CREATE INDEX idx_media_tags_search ON media_tags USING gin(
    to_tsvector('english', tag_name || ' ' || COALESCE(tag_value, ''))
);

-- Functions and triggers

-- Update media file status when processing job completes
CREATE OR REPLACE FUNCTION update_media_status_from_jobs()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'completed' THEN
        -- Check if all jobs for this media are completed
        IF NOT EXISTS (
            SELECT 1 FROM media_processing_jobs 
            WHERE media_id = NEW.media_id AND status NOT IN ('completed', 'cancelled')
        ) THEN
            UPDATE media_files 
            SET status = 'ready', 
                processing_completed_at = NOW(),
                updated_at = NOW()
            WHERE media_id = NEW.media_id;
        END IF;
    ELSIF NEW.status = 'failed' AND NEW.attempts >= NEW.max_attempts THEN
        UPDATE media_files 
        SET status = 'failed', 
            processing_error = NEW.error_message,
            updated_at = NOW()
        WHERE media_id = NEW.media_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_media_status_from_jobs_trigger
    AFTER UPDATE ON media_processing_jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_media_status_from_jobs();

-- Update upload session progress
CREATE OR REPLACE FUNCTION update_upload_session_progress()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE upload_sessions 
        SET uploaded_chunks = uploaded_chunks + 1,
            updated_at = NOW()
        WHERE session_id = NEW.session_id;
        
        -- Check if upload is complete
        UPDATE upload_sessions 
        SET status = 'completed'
        WHERE session_id = NEW.session_id 
          AND uploaded_chunks >= total_chunks;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE upload_sessions 
        SET uploaded_chunks = GREATEST(uploaded_chunks - 1, 0),
            updated_at = NOW()
        WHERE session_id = OLD.session_id;
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_upload_session_progress_trigger
    AFTER INSERT OR DELETE ON upload_chunks
    FOR EACH ROW
    EXECUTE FUNCTION update_upload_session_progress();

-- Update user storage quota
CREATE OR REPLACE FUNCTION update_user_storage_quota()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO user_storage_quotas (user_id, used_bytes, file_count)
        VALUES (NEW.owner_user_id, NEW.file_size, 1)
        ON CONFLICT (user_id) 
        DO UPDATE SET 
            used_bytes = user_storage_quotas.used_bytes + NEW.file_size,
            file_count = user_storage_quotas.file_count + 1,
            updated_at = NOW();
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE user_storage_quotas 
        SET used_bytes = GREATEST(used_bytes - OLD.file_size, 0),
            file_count = GREATEST(file_count - 1, 0),
            updated_at = NOW()
        WHERE user_id = OLD.owner_user_id;
    ELSIF TG_OP = 'UPDATE' AND OLD.file_size != NEW.file_size THEN
        UPDATE user_storage_quotas 
        SET used_bytes = used_bytes - OLD.file_size + NEW.file_size,
            updated_at = NOW()
        WHERE user_id = NEW.owner_user_id;
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_user_storage_quota_trigger
    AFTER INSERT OR DELETE OR UPDATE ON media_files
    FOR EACH ROW
    EXECUTE FUNCTION update_user_storage_quota();

-- Clean up expired upload sessions
CREATE OR REPLACE FUNCTION cleanup_expired_upload_sessions()
RETURNS void AS $$
BEGIN
    DELETE FROM upload_sessions WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;

-- Clean up old access logs
CREATE OR REPLACE FUNCTION cleanup_old_access_logs()
RETURNS void AS $$
BEGIN
    DELETE FROM media_access_logs WHERE created_at < NOW() - INTERVAL '90 days';
END;
$$ LANGUAGE plpgsql;

-- Views for common queries

-- Media files with processing status
CREATE VIEW media_with_processing_status AS
SELECT 
    mf.*,
    mpj.pending_jobs,
    mpj.processing_jobs,
    mpj.failed_jobs
FROM media_files mf
LEFT JOIN (
    SELECT 
        media_id,
        COUNT(*) FILTER (WHERE status = 'pending') as pending_jobs,
        COUNT(*) FILTER (WHERE status = 'processing') as processing_jobs,
        COUNT(*) FILTER (WHERE status = 'failed') as failed_jobs
    FROM media_processing_jobs
    GROUP BY media_id
) mpj ON mf.media_id = mpj.media_id;

-- User storage usage
CREATE VIEW user_storage_usage AS
SELECT 
    u.user_id,
    u.username,
    usq.total_quota_bytes,
    usq.used_bytes,
    usq.file_count,
    ROUND((usq.used_bytes::float / usq.total_quota_bytes * 100), 2) as usage_percentage
FROM users u
JOIN user_storage_quotas usq ON u.user_id = usq.user_id;

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO media_service_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO media_service_user;