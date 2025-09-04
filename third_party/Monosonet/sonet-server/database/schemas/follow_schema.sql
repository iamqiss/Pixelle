/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

-- Follow Service Database Schema
-- Handles user relationships, follow requests, and social graph

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- User relationships table
CREATE TABLE user_relationships (
    relationship_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    follower_id UUID NOT NULL,
    following_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'accepted', 'rejected', 'blocked', 'muted')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    accepted_at TIMESTAMP WITH TIME ZONE,
    UNIQUE(follower_id, following_id)
);

-- Follow requests table
CREATE TABLE follow_requests (
    request_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    requester_id UUID NOT NULL,
    target_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'accepted', 'rejected', 'cancelled')),
    message TEXT, -- optional message with the request
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    responded_at TIMESTAMP WITH TIME ZONE,
    UNIQUE(requester_id, target_id)
);

-- User blocks table
CREATE TABLE user_blocks (
    block_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    blocker_id UUID NOT NULL,
    blocked_user_id UUID NOT NULL,
    reason VARCHAR(255),
    is_permanent BOOLEAN DEFAULT FALSE,
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(blocker_id, blocked_user_id)
);

-- User mutes table
CREATE TABLE user_mutes (
    mute_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    muter_id UUID NOT NULL,
    muted_user_id UUID NOT NULL,
    mute_type VARCHAR(20) DEFAULT 'all' 
        CHECK (mute_type IN ('all', 'posts', 'stories', 'mentions')),
    reason VARCHAR(255),
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(muter_id, muted_user_id)
);

-- Close friends table
CREATE TABLE close_friends (
    friendship_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    close_friend_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(user_id, close_friend_id)
);

-- User lists table
CREATE TABLE user_lists (
    list_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    owner_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    is_public BOOLEAN DEFAULT FALSE,
    list_type VARCHAR(20) DEFAULT 'custom' 
        CHECK (list_type IN ('custom', 'close_friends', 'family', 'work', 'school')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- User list members table
CREATE TABLE user_list_members (
    list_id UUID NOT NULL REFERENCES user_lists(list_id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    added_by UUID NOT NULL,
    notes TEXT, -- optional notes about this user in the list
    PRIMARY KEY (list_id, user_id)
);

-- Relationship history table (for audit trail)
CREATE TABLE relationship_history (
    history_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    relationship_id UUID NOT NULL REFERENCES user_relationships(relationship_id) ON DELETE CASCADE,
    follower_id UUID NOT NULL,
    following_id UUID NOT NULL,
    old_status VARCHAR(20),
    new_status VARCHAR(20) NOT NULL,
    changed_by UUID NOT NULL, -- who made the change
    reason VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Social graph cache table (for performance)
CREATE TABLE social_graph_cache (
    cache_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    cache_type VARCHAR(20) NOT NULL 
        CHECK (cache_type IN ('followers', 'following', 'mutual', 'suggestions')),
    cached_data JSONB NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_user_relationships_follower_id ON user_relationships(follower_id);
CREATE INDEX idx_user_relationships_following_id ON user_relationships(following_id);
CREATE INDEX idx_user_relationships_status ON user_relationships(status);
CREATE INDEX idx_user_relationships_created_at ON user_relationships(created_at);

CREATE INDEX idx_follow_requests_requester_id ON follow_requests(requester_id);
CREATE INDEX idx_follow_requests_target_id ON follow_requests(target_id);
CREATE INDEX idx_follow_requests_status ON follow_requests(status);
CREATE INDEX idx_follow_requests_created_at ON follow_requests(created_at);

CREATE INDEX idx_user_blocks_blocker_id ON user_blocks(blocker_id);
CREATE INDEX idx_user_blocks_blocked_user_id ON user_blocks(blocked_user_id);
CREATE INDEX idx_user_blocks_expires_at ON user_blocks(expires_at);

CREATE INDEX idx_user_mutes_muter_id ON user_mutes(muter_id);
CREATE INDEX idx_user_mutes_muted_user_id ON user_mutes(muted_user_id);
CREATE INDEX idx_user_mutes_expires_at ON user_mutes(expires_at);

CREATE INDEX idx_close_friends_user_id ON close_friends(user_id);
CREATE INDEX idx_close_friends_close_friend_id ON close_friends(close_friend_id);

CREATE INDEX idx_user_lists_owner_id ON user_lists(owner_id);
CREATE INDEX idx_user_lists_is_public ON user_lists(is_public);
CREATE INDEX idx_user_lists_list_type ON user_lists(list_type);

CREATE INDEX idx_user_list_members_list_id ON user_list_members(list_id);
CREATE INDEX idx_user_list_members_user_id ON user_list_members(user_id);

CREATE INDEX idx_relationship_history_relationship_id ON relationship_history(relationship_id);
CREATE INDEX idx_relationship_history_follower_id ON relationship_history(follower_id);
CREATE INDEX idx_relationship_history_following_id ON relationship_history(following_id);
CREATE INDEX idx_relationship_history_created_at ON relationship_history(created_at);

CREATE INDEX idx_social_graph_cache_user_id ON social_graph_cache(user_id);
CREATE INDEX idx_social_graph_cache_cache_type ON social_graph_cache(cache_type);
CREATE INDEX idx_social_graph_cache_expires_at ON social_graph_cache(expires_at);

-- Composite indexes for common queries
CREATE INDEX idx_user_relationships_follower_status ON user_relationships(follower_id, status);
CREATE INDEX idx_user_relationships_following_status ON user_relationships(following_id, status);
CREATE INDEX idx_user_relationships_bidirectional ON user_relationships(follower_id, following_id, status);

-- Functions and triggers

-- Update relationship history when status changes
CREATE OR REPLACE FUNCTION update_relationship_history()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status != NEW.status THEN
        INSERT INTO relationship_history (
            relationship_id, 
            follower_id, 
            following_id, 
            old_status, 
            new_status, 
            changed_by
        ) VALUES (
            NEW.relationship_id,
            NEW.follower_id,
            NEW.following_id,
            OLD.status,
            NEW.status,
            NEW.follower_id -- assuming the follower initiated the change
        );
        
        -- Update accepted_at timestamp
        IF NEW.status = 'accepted' THEN
            NEW.accepted_at = NOW();
        END IF;
    END IF;
    
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_relationship_history_trigger
    AFTER UPDATE ON user_relationships
    FOR EACH ROW
    EXECUTE FUNCTION update_relationship_history();

-- Update follow request when relationship is created/updated
CREATE OR REPLACE FUNCTION sync_follow_requests()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Create follow request if relationship is pending
        IF NEW.status = 'pending' THEN
            INSERT INTO follow_requests (requester_id, target_id, status)
            VALUES (NEW.follower_id, NEW.following_id, 'pending')
            ON CONFLICT (requester_id, target_id) DO NOTHING;
        END IF;
    ELSIF TG_OP = 'UPDATE' THEN
        -- Update follow request status
        IF NEW.status = 'accepted' THEN
            UPDATE follow_requests 
            SET status = 'accepted', 
                responded_at = NOW(),
                updated_at = NOW()
            WHERE requester_id = NEW.follower_id AND target_id = NEW.following_id;
        ELSIF NEW.status = 'rejected' THEN
            UPDATE follow_requests 
            SET status = 'rejected', 
                responded_at = NOW(),
                updated_at = NOW()
            WHERE requester_id = NEW.follower_id AND target_id = NEW.following_id;
        END IF;
    END IF;
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sync_follow_requests_trigger
    AFTER INSERT OR UPDATE ON user_relationships
    FOR EACH ROW
    EXECUTE FUNCTION sync_follow_requests();

-- Invalidate social graph cache when relationships change
CREATE OR REPLACE FUNCTION invalidate_social_graph_cache()
RETURNS TRIGGER AS $$
BEGIN
    -- Invalidate cache for both users involved
    DELETE FROM social_graph_cache 
    WHERE user_id IN (NEW.follower_id, NEW.following_id);
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER invalidate_social_graph_cache_trigger
    AFTER INSERT OR UPDATE OR DELETE ON user_relationships
    FOR EACH ROW
    EXECUTE FUNCTION invalidate_social_graph_cache();

-- Clean up expired blocks and mutes
CREATE OR REPLACE FUNCTION cleanup_expired_blocks_and_mutes()
RETURNS void AS $$
BEGIN
    DELETE FROM user_blocks WHERE expires_at < NOW() AND NOT is_permanent;
    DELETE FROM user_mutes WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;

-- Clean up old relationship history
CREATE OR REPLACE FUNCTION cleanup_old_relationship_history()
RETURNS void AS $$
BEGIN
    DELETE FROM relationship_history WHERE created_at < NOW() - INTERVAL '2 years';
END;
$$ LANGUAGE plpgsql;

-- Clean up expired social graph cache
CREATE OR REPLACE FUNCTION cleanup_expired_social_graph_cache()
RETURNS void AS $$
BEGIN
    DELETE FROM social_graph_cache WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;

-- Views for common queries

-- User's followers
CREATE VIEW user_followers AS
SELECT 
    ur.follower_id,
    u.username,
    u.display_name,
    u.avatar_url,
    ur.created_at as followed_at,
    ur.status
FROM user_relationships ur
JOIN users u ON ur.follower_id = u.user_id
WHERE ur.following_id = $1 AND ur.status = 'accepted';

-- User's following
CREATE VIEW user_following AS
SELECT 
    ur.following_id,
    u.username,
    u.display_name,
    u.avatar_url,
    ur.created_at as followed_at,
    ur.status
FROM user_relationships ur
JOIN users u ON ur.following_id = u.user_id
WHERE ur.follower_id = $1 AND ur.status = 'accepted';

-- Mutual followers
CREATE VIEW mutual_followers AS
SELECT 
    u1.user_id as user1_id,
    u1.username as user1_username,
    u2.user_id as user2_id,
    u2.username as user2_username
FROM user_relationships ur1
JOIN user_relationships ur2 ON ur1.follower_id = ur2.following_id AND ur1.following_id = ur2.follower_id
JOIN users u1 ON ur1.follower_id = u1.user_id
JOIN users u2 ON ur1.following_id = u2.user_id
WHERE ur1.status = 'accepted' AND ur2.status = 'accepted';

-- Pending follow requests
CREATE VIEW pending_follow_requests AS
SELECT 
    fr.*,
    u.username as requester_username,
    u.display_name as requester_display_name,
    u.avatar_url as requester_avatar_url
FROM follow_requests fr
JOIN users u ON fr.requester_id = u.user_id
WHERE fr.status = 'pending';

-- User's blocked users
CREATE VIEW user_blocked_users AS
SELECT 
    ub.blocked_user_id,
    u.username,
    u.display_name,
    u.avatar_url,
    ub.reason,
    ub.is_permanent,
    ub.expires_at,
    ub.created_at
FROM user_blocks ub
JOIN users u ON ub.blocked_user_id = u.user_id
WHERE ub.blocker_id = $1;

-- User's muted users
CREATE VIEW user_muted_users AS
SELECT 
    um.muted_user_id,
    u.username,
    u.display_name,
    u.avatar_url,
    um.mute_type,
    um.reason,
    um.expires_at,
    um.created_at
FROM user_mutes um
JOIN users u ON um.muted_user_id = u.user_id
WHERE um.muter_id = $1;

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO follow_service_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO follow_service_user;