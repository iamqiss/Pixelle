/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

-- Messaging Service Database Schema
-- Optimized for high-throughput messaging with encryption support

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Chats table
CREATE TABLE chats (
    chat_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255),
    description TEXT,
    type VARCHAR(20) NOT NULL CHECK (type IN ('direct', 'group', 'channel')),
    creator_id UUID NOT NULL,
    avatar_url TEXT,
    is_archived BOOLEAN DEFAULT FALSE,
    is_muted BOOLEAN DEFAULT FALSE,
    settings JSONB DEFAULT '{}',
    last_message_id UUID,
    last_activity TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Chat participants table (many-to-many relationship)
CREATE TABLE chat_participants (
    chat_id UUID NOT NULL REFERENCES chats(chat_id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    role VARCHAR(20) DEFAULT 'member' CHECK (role IN ('owner', 'admin', 'member')),
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_read_message_id UUID,
    is_muted BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (chat_id, user_id)
);

-- Messages table (partitioned by created_at for performance)
CREATE TABLE messages (
    message_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chat_id UUID NOT NULL REFERENCES chats(chat_id) ON DELETE CASCADE,
    sender_id UUID NOT NULL,
    content TEXT,
    type VARCHAR(20) NOT NULL DEFAULT 'text' CHECK (type IN ('text', 'image', 'video', 'audio', 'file', 'location', 'system')),
    status VARCHAR(20) NOT NULL DEFAULT 'sent' CHECK (status IN ('sent', 'delivered', 'read', 'failed')),
    encryption_type VARCHAR(20) DEFAULT 'none' CHECK (encryption_type IN ('none', 'aes256', 'e2e')),
    encrypted_content TEXT,
    reply_to_message_id UUID REFERENCES messages(message_id),
    is_edited BOOLEAN DEFAULT FALSE,
    is_deleted BOOLEAN DEFAULT FALSE,
    search_vector tsvector,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    delivered_at TIMESTAMP WITH TIME ZONE,
    read_at TIMESTAMP WITH TIME ZONE
) PARTITION BY RANGE (created_at);

-- Create monthly partitions for messages (example for 2025)
CREATE TABLE messages_2025_01 PARTITION OF messages 
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE messages_2025_02 PARTITION OF messages 
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE messages_2025_03 PARTITION OF messages 
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
-- Add more partitions as needed

-- Message attachments table
CREATE TABLE message_attachments (
    attachment_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    message_id UUID NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    content_type VARCHAR(100) NOT NULL,
    size BIGINT NOT NULL,
    url TEXT NOT NULL,
    thumbnail_url TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Read receipts table
CREATE TABLE read_receipts (
    message_id UUID NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    read_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (message_id, user_id)
);

-- Typing indicators table (volatile data, consider Redis for production)
CREATE TABLE typing_indicators (
    chat_id UUID NOT NULL REFERENCES chats(chat_id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    is_typing BOOLEAN DEFAULT FALSE,
    last_activity TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (chat_id, user_id)
);

-- Message reactions table
CREATE TABLE message_reactions (
    message_id UUID NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    reaction VARCHAR(50) NOT NULL, -- emoji or reaction type
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (message_id, user_id, reaction)
);

-- Indexes for performance

-- Chat indexes
CREATE INDEX idx_chats_creator_id ON chats(creator_id);
CREATE INDEX idx_chats_type ON chats(type);
CREATE INDEX idx_chats_last_activity ON chats(last_activity DESC);

-- Chat participants indexes
CREATE INDEX idx_chat_participants_user_id ON chat_participants(user_id);
CREATE INDEX idx_chat_participants_chat_id_joined ON chat_participants(chat_id, joined_at DESC);

-- Message indexes
CREATE INDEX idx_messages_chat_id_created ON messages(chat_id, created_at DESC);
CREATE INDEX idx_messages_sender_id ON messages(sender_id);
CREATE INDEX idx_messages_reply_to ON messages(reply_to_message_id);
CREATE INDEX idx_messages_search_vector ON messages USING gin(search_vector);
CREATE INDEX idx_messages_status ON messages(status);

-- Attachment indexes
CREATE INDEX idx_message_attachments_message_id ON message_attachments(message_id);

-- Read receipt indexes
CREATE INDEX idx_read_receipts_user_id ON read_receipts(user_id);
CREATE INDEX idx_read_receipts_message_id_read_at ON read_receipts(message_id, read_at DESC);

-- Typing indicator indexes
CREATE INDEX idx_typing_indicators_chat_id ON typing_indicators(chat_id);
CREATE INDEX idx_typing_indicators_last_activity ON typing_indicators(last_activity);

-- Reaction indexes
CREATE INDEX idx_message_reactions_message_id ON message_reactions(message_id);
CREATE INDEX idx_message_reactions_user_id ON message_reactions(user_id);

-- Functions and triggers

-- Update search vector for full-text search
CREATE OR REPLACE FUNCTION update_message_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := to_tsvector('english', COALESCE(NEW.content, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_message_search_vector_trigger
    BEFORE INSERT OR UPDATE ON messages
    FOR EACH ROW
    EXECUTE FUNCTION update_message_search_vector();

-- Update chat last_activity and last_message_id
CREATE OR REPLACE FUNCTION update_chat_activity()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE chats 
    SET last_activity = NEW.created_at,
        last_message_id = NEW.message_id
    WHERE chat_id = NEW.chat_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_chat_activity_trigger
    AFTER INSERT ON messages
    FOR EACH ROW
    EXECUTE FUNCTION update_chat_activity();

-- Auto-cleanup old typing indicators
CREATE OR REPLACE FUNCTION cleanup_old_typing_indicators()
RETURNS void AS $$
BEGIN
    DELETE FROM typing_indicators 
    WHERE last_activity < NOW() - INTERVAL '30 seconds';
END;
$$ LANGUAGE plpgsql;

-- Views for common queries

-- Chat with participant count
CREATE VIEW chat_summary AS
SELECT 
    c.*,
    COUNT(cp.user_id) as participant_count,
    ARRAY_AGG(cp.user_id) as participant_ids
FROM chats c
LEFT JOIN chat_participants cp ON c.chat_id = cp.chat_id
GROUP BY c.chat_id;

-- User's unread message count per chat
CREATE VIEW unread_message_counts AS
SELECT 
    cp.chat_id,
    cp.user_id,
    COUNT(m.message_id) as unread_count
FROM chat_participants cp
LEFT JOIN messages m ON m.chat_id = cp.chat_id 
    AND m.created_at > COALESCE(
        (SELECT read_at FROM read_receipts 
         WHERE message_id = cp.last_read_message_id AND user_id = cp.user_id),
        cp.joined_at
    )
    AND m.sender_id != cp.user_id
    AND m.is_deleted = FALSE
GROUP BY cp.chat_id, cp.user_id;

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO messaging_service_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO messaging_service_user;
