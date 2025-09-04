/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

-- Note Service Database Schema
-- Handles notes, likes, reposts, comments, and media

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Notes table
CREATE TABLE notes (
    note_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    author_id UUID NOT NULL,
    parent_note_id UUID REFERENCES notes(note_id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    content_type VARCHAR(20) DEFAULT 'text' 
        CHECK (content_type IN ('text', 'markdown', 'html')),
    is_public BOOLEAN DEFAULT TRUE,
    is_deleted BOOLEAN DEFAULT FALSE,
    is_edited BOOLEAN DEFAULT FALSE,
    edit_count INTEGER DEFAULT 0,
    last_edit_at TIMESTAMP WITH TIME ZONE,
    language VARCHAR(10) DEFAULT 'en',
    tags TEXT[],
    location_info JSONB,
    search_vector tsvector,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Note media attachments table
CREATE TABLE note_media (
    media_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    note_id UUID NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
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

-- Note likes table
CREATE TABLE note_likes (
    like_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    note_id UUID NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(note_id, user_id)
);

-- Note reposts table
CREATE TABLE note_reposts (
    repost_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    note_id UUID NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    repost_type VARCHAR(20) DEFAULT 'repost' 
        CHECK (repost_type IN ('repost', 'quote', 'retweet')),
    quote_content TEXT, -- for quote reposts
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(note_id, user_id)
);

-- Note comments table
CREATE TABLE note_comments (
    comment_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    note_id UUID NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
    author_id UUID NOT NULL,
    parent_comment_id UUID REFERENCES note_comments(comment_id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    is_edited BOOLEAN DEFAULT FALSE,
    edit_count INTEGER DEFAULT 0,
    last_edit_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Note bookmarks table
CREATE TABLE note_bookmarks (
    bookmark_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    note_id UUID NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    folder VARCHAR(100) DEFAULT 'default',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(note_id, user_id)
);

-- Note hashtags table
CREATE TABLE note_hashtags (
    hashtag_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    note_id UUID NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
    hashtag VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(note_id, hashtag)
);

-- Note mentions table
CREATE TABLE note_mentions (
    mention_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    note_id UUID NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
    mentioned_user_id UUID NOT NULL,
    position_in_content INTEGER, -- position in the note content
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(note_id, mentioned_user_id)
);

-- Note statistics table (denormalized for performance)
CREATE TABLE note_stats (
    note_id UUID PRIMARY KEY REFERENCES notes(note_id) ON DELETE CASCADE,
    like_count BIGINT DEFAULT 0,
    repost_count BIGINT DEFAULT 0,
    comment_count BIGINT DEFAULT 0,
    bookmark_count BIGINT DEFAULT 0,
    view_count BIGINT DEFAULT 0,
    share_count BIGINT DEFAULT 0,
    last_stats_update TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Note visibility table (for private notes and selective sharing)
CREATE TABLE note_visibility (
    visibility_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    note_id UUID NOT NULL REFERENCES notes(note_id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    visibility_type VARCHAR(20) NOT NULL 
        CHECK (visibility_type IN ('public', 'followers', 'private', 'custom')),
    allowed_users UUID[], -- for custom visibility
    blocked_users UUID[], -- for custom visibility
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_notes_author_id ON notes(author_id);
CREATE INDEX idx_notes_parent_note_id ON notes(parent_note_id);
CREATE INDEX idx_notes_created_at ON notes(created_at DESC);
CREATE INDEX idx_notes_is_public ON notes(is_public);
CREATE INDEX idx_notes_is_deleted ON notes(is_deleted);
CREATE INDEX idx_notes_search_vector ON notes USING gin(search_vector);
CREATE INDEX idx_notes_tags ON notes USING gin(tags);
CREATE INDEX idx_notes_language ON notes(language);

CREATE INDEX idx_note_media_note_id ON note_media(note_id);
CREATE INDEX idx_note_media_media_type ON note_media(media_type);

CREATE INDEX idx_note_likes_note_id ON note_likes(note_id);
CREATE INDEX idx_note_likes_user_id ON note_likes(user_id);
CREATE INDEX idx_note_likes_created_at ON note_likes(created_at);

CREATE INDEX idx_note_reposts_note_id ON note_reposts(note_id);
CREATE INDEX idx_note_reposts_user_id ON note_reposts(user_id);
CREATE INDEX idx_note_reposts_repost_type ON note_reposts(repost_type);

CREATE INDEX idx_note_comments_note_id ON note_comments(note_id);
CREATE INDEX idx_note_comments_author_id ON note_comments(author_id);
CREATE INDEX idx_note_comments_parent_comment_id ON note_comments(parent_comment_id);
CREATE INDEX idx_note_comments_created_at ON note_comments(created_at);

CREATE INDEX idx_note_bookmarks_user_id ON note_bookmarks(user_id);
CREATE INDEX idx_note_bookmarks_folder ON note_bookmarks(folder);

CREATE INDEX idx_note_hashtags_hashtag ON note_hashtags(hashtag);
CREATE INDEX idx_note_hashtags_note_id ON note_hashtags(note_id);

CREATE INDEX idx_note_mentions_note_id ON note_mentions(note_id);
CREATE INDEX idx_note_mentions_mentioned_user_id ON note_mentions(mentioned_user_id);

CREATE INDEX idx_note_visibility_note_id ON note_visibility(note_id);
CREATE INDEX idx_note_visibility_user_id ON note_visibility(user_id);

-- Full-text search index on notes
CREATE INDEX idx_notes_content_search ON notes USING gin(
    to_tsvector('english', content)
);

-- Functions and triggers

-- Update search vector for full-text search
CREATE OR REPLACE FUNCTION update_note_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('english', COALESCE(NEW.content, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_note_search_vector_trigger
    BEFORE INSERT OR UPDATE ON notes
    FOR EACH ROW
    EXECUTE FUNCTION update_note_search_vector();

-- Update note statistics
CREATE OR REPLACE FUNCTION update_note_stats()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Update like count
        IF TG_TABLE_NAME = 'note_likes' THEN
            UPDATE note_stats 
            SET like_count = like_count + 1, last_stats_update = NOW()
            WHERE note_id = NEW.note_id;
        -- Update repost count
        ELSIF TG_TABLE_NAME = 'note_reposts' THEN
            UPDATE note_stats 
            SET repost_count = repost_count + 1, last_stats_update = NOW()
            WHERE note_id = NEW.note_id;
        -- Update comment count
        ELSIF TG_TABLE_NAME = 'note_comments' THEN
            UPDATE note_stats 
            SET comment_count = comment_count + 1, last_stats_update = NOW()
            WHERE note_id = NEW.note_id;
        -- Update bookmark count
        ELSIF TG_TABLE_NAME = 'note_bookmarks' THEN
            UPDATE note_stats 
            SET bookmark_count = bookmark_count + 1, last_stats_update = NOW()
            WHERE note_id = NEW.note_id;
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        -- Update like count
        IF TG_TABLE_NAME = 'note_likes' THEN
            UPDATE note_stats 
            SET like_count = GREATEST(like_count - 1, 0), last_stats_update = NOW()
            WHERE note_id = OLD.note_id;
        -- Update repost count
        ELSIF TG_TABLE_NAME = 'note_reposts' THEN
            UPDATE note_stats 
            SET repost_count = GREATEST(repost_count - 1, 0), last_stats_update = NOW()
            WHERE note_id = OLD.note_id;
        -- Update comment count
        ELSIF TG_TABLE_NAME = 'note_comments' THEN
            UPDATE note_stats 
            SET comment_count = GREATEST(comment_count - 1, 0), last_stats_update = NOW()
            WHERE note_id = OLD.note_id;
        -- Update bookmark count
        ELSIF TG_TABLE_NAME = 'note_bookmarks' THEN
            UPDATE note_stats 
            SET bookmark_count = GREATEST(bookmark_count - 1, 0), last_stats_update = NOW()
            WHERE note_id = OLD.note_id;
        END IF;
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Create triggers for each table that affects note stats
CREATE TRIGGER update_note_stats_likes_trigger
    AFTER INSERT OR DELETE ON note_likes
    FOR EACH ROW
    EXECUTE FUNCTION update_note_stats();

CREATE TRIGGER update_note_stats_reposts_trigger
    AFTER INSERT OR DELETE ON note_reposts
    FOR EACH ROW
    EXECUTE FUNCTION update_note_stats();

CREATE TRIGGER update_note_stats_comments_trigger
    AFTER INSERT OR DELETE ON note_comments
    FOR EACH ROW
    EXECUTE FUNCTION update_note_stats();

CREATE TRIGGER update_note_stats_bookmarks_trigger
    AFTER INSERT OR DELETE ON note_bookmarks
    FOR EACH ROW
    EXECUTE FUNCTION update_note_stats();

-- Initialize note stats when note is created
CREATE OR REPLACE FUNCTION initialize_note_stats()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO note_stats (note_id) VALUES (NEW.note_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER initialize_note_stats_trigger
    AFTER INSERT ON notes
    FOR EACH ROW
    EXECUTE FUNCTION initialize_note_stats();

-- Views for common queries

-- Note with media and stats
CREATE VIEW note_with_media_and_stats AS
SELECT 
    n.*,
    nm.media_count,
    ns.like_count,
    ns.repost_count,
    ns.comment_count,
    ns.bookmark_count,
    ns.view_count
FROM notes n
LEFT JOIN (
    SELECT note_id, COUNT(*) as media_count 
    FROM note_media 
    GROUP BY note_id
) nm ON n.note_id = nm.note_id
LEFT JOIN note_stats ns ON n.note_id = ns.note_id
WHERE n.is_deleted = FALSE;

-- User's notes with stats
CREATE VIEW user_notes_with_stats AS
SELECT 
    n.*,
    ns.like_count,
    ns.repost_count,
    ns.comment_count,
    ns.bookmark_count
FROM notes n
LEFT JOIN note_stats ns ON n.note_id = ns.note_id
WHERE n.author_id = $1 AND n.is_deleted = FALSE
ORDER BY n.created_at DESC;

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO note_service_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO note_service_user;