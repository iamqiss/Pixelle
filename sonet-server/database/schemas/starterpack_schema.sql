-- Starterpacks schema for Sonet
-- This schema defines the tables for managing user starterpacks

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Starterpacks table
CREATE TABLE starterpacks (
    starterpack_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    creator_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    avatar_url TEXT,
    is_public BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Starterpack items table
CREATE TABLE starterpack_items (
    item_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    starterpack_id UUID NOT NULL REFERENCES starterpacks(starterpack_id) ON DELETE CASCADE,
    item_type VARCHAR(20) NOT NULL CHECK (item_type IN ('profile', 'feed')),
    item_uri TEXT NOT NULL,
    item_order INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_starterpacks_creator_id ON starterpacks(creator_id);
CREATE INDEX idx_starterpacks_is_public ON starterpacks(is_public);
CREATE INDEX idx_starterpacks_created_at ON starterpacks(created_at);

CREATE INDEX idx_starterpack_items_starterpack_id ON starterpack_items(starterpack_id);
CREATE INDEX idx_starterpack_items_item_type ON starterpack_items(item_type);
CREATE INDEX idx_starterpack_items_item_order ON starterpack_items(item_order);

-- Composite indexes for common queries
CREATE INDEX idx_starterpacks_creator_public ON starterpacks(creator_id, is_public);
CREATE INDEX idx_starterpack_items_starterpack_order ON starterpack_items(starterpack_id, item_order);

-- Functions and triggers
CREATE OR REPLACE FUNCTION update_starterpack_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_starterpack_updated_at_trigger
    BEFORE UPDATE ON starterpacks
    FOR EACH ROW
    EXECUTE FUNCTION update_starterpack_updated_at();

-- Function to get starterpack with item count
CREATE OR REPLACE FUNCTION get_starterpack_with_count(starterpack_uuid UUID)
RETURNS TABLE (
    starterpack_id UUID,
    creator_id UUID,
    name VARCHAR(255),
    description TEXT,
    avatar_url TEXT,
    is_public BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    item_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.starterpack_id,
        s.creator_id,
        s.name,
        s.description,
        s.avatar_url,
        s.is_public,
        s.created_at,
        s.updated_at,
        COUNT(si.item_id) as item_count
    FROM starterpacks s
    LEFT JOIN starterpack_items si ON s.starterpack_id = si.starterpack_id
    WHERE s.starterpack_id = starterpack_uuid
    GROUP BY s.starterpack_id, s.creator_id, s.name, s.description, s.avatar_url, s.is_public, s.created_at, s.updated_at;
END;
$$ LANGUAGE plpgsql;

-- Function to get user starterpacks with item counts
CREATE OR REPLACE FUNCTION get_user_starterpacks_with_counts(user_uuid UUID, limit_count INTEGER DEFAULT 20, offset_count INTEGER DEFAULT 0)
RETURNS TABLE (
    starterpack_id UUID,
    creator_id UUID,
    name VARCHAR(255),
    description TEXT,
    avatar_url TEXT,
    is_public BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    item_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.starterpack_id,
        s.creator_id,
        s.name,
        s.description,
        s.avatar_url,
        s.is_public,
        s.created_at,
        s.updated_at,
        COUNT(si.item_id) as item_count
    FROM starterpacks s
    LEFT JOIN starterpack_items si ON s.starterpack_id = si.starterpack_id
    WHERE s.creator_id = user_uuid
    GROUP BY s.starterpack_id, s.creator_id, s.name, s.description, s.avatar_url, s.is_public, s.created_at, s.updated_at
    ORDER BY s.created_at DESC
    LIMIT limit_count OFFSET offset_count;
END;
$$ LANGUAGE plpgsql;

-- Function to get public starterpacks for discovery
CREATE OR REPLACE FUNCTION get_public_starterpacks_for_discovery(limit_count INTEGER DEFAULT 20, offset_count INTEGER DEFAULT 0)
RETURNS TABLE (
    starterpack_id UUID,
    creator_id UUID,
    name VARCHAR(255),
    description TEXT,
    avatar_url TEXT,
    is_public BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    item_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.starterpack_id,
        s.creator_id,
        s.name,
        s.description,
        s.avatar_url,
        s.is_public,
        s.created_at,
        s.updated_at,
        COUNT(si.item_id) as item_count
    FROM starterpacks s
    LEFT JOIN starterpack_items si ON s.starterpack_id = si.starterpack_id
    WHERE s.is_public = TRUE
    GROUP BY s.starterpack_id, s.creator_id, s.name, s.description, s.avatar_url, s.is_public, s.created_at, s.updated_at
    ORDER BY s.created_at DESC
    LIMIT limit_count OFFSET offset_count;
END;
$$ LANGUAGE plpgsql;

-- Comments
COMMENT ON TABLE starterpacks IS 'Stores user-created starterpacks for onboarding and discovery';
COMMENT ON TABLE starterpack_items IS 'Stores items (profiles/feeds) within starterpacks';
COMMENT ON COLUMN starterpacks.creator_id IS 'UUID of the user who created this starterpack';
COMMENT ON COLUMN starterpacks.is_public IS 'Whether this starterpack is visible to all users';
COMMENT ON COLUMN starterpack_items.item_type IS 'Type of item: profile or feed';
COMMENT ON COLUMN starterpack_items.item_uri IS 'URI of the profile or feed';
COMMENT ON COLUMN starterpack_items.item_order IS 'Order of items within the starterpack';