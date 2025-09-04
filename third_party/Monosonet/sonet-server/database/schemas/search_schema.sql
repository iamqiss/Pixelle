-- Search Service Database Schema
-- Handles full-text search, search history, search analytics, and search optimization

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "unaccent";

-- Create search_schema
CREATE SCHEMA IF NOT EXISTS search_schema;

-- Set search path
SET search_path TO search_schema, public;

-- Search index for different content types
CREATE TABLE search_index (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    content_type VARCHAR(50) NOT NULL, -- 'note', 'user', 'media', 'hashtag'
    content_id UUID NOT NULL,
    title TEXT,
    content TEXT,
    tags TEXT[],
    metadata JSONB,
    search_vector tsvector,
    relevance_score DECIMAL(5,4) DEFAULT 1.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_indexed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Search history for users
CREATE TABLE search_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    query TEXT NOT NULL,
    search_type VARCHAR(50) DEFAULT 'general', -- 'general', 'users', 'notes', 'media', 'hashtags'
    filters JSONB,
    results_count INTEGER,
    execution_time_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Search analytics and metrics
CREATE TABLE search_analytics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query TEXT NOT NULL,
    search_count INTEGER DEFAULT 1,
    avg_results_count DECIMAL(10,2),
    avg_execution_time_ms DECIMAL(10,2),
    success_rate DECIMAL(5,4),
    user_satisfaction_score DECIMAL(3,2), -- 1-5 scale
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Search suggestions and autocomplete
CREATE TABLE search_suggestions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    suggestion TEXT NOT NULL,
    suggestion_type VARCHAR(50) NOT NULL, -- 'query', 'user', 'hashtag', 'trending'
    frequency INTEGER DEFAULT 1,
    relevance_score DECIMAL(5,4) DEFAULT 1.0,
    last_used_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Search filters and preferences
CREATE TABLE search_filters (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    filter_name VARCHAR(100) NOT NULL,
    filter_type VARCHAR(50) NOT NULL, -- 'content_type', 'date_range', 'language', 'custom'
    filter_value JSONB NOT NULL,
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Search result rankings and personalization
CREATE TABLE search_rankings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID,
    content_id UUID NOT NULL,
    content_type VARCHAR(50) NOT NULL,
    ranking_factors JSONB, -- 'relevance', 'popularity', 'recency', 'user_preference'
    personalization_score DECIMAL(5,4),
    global_score DECIMAL(5,4),
    final_score DECIMAL(5,4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Search performance monitoring
CREATE TABLE search_performance (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_pattern VARCHAR(200),
    avg_execution_time_ms DECIMAL(10,2),
    query_count INTEGER DEFAULT 1,
    cache_hit_rate DECIMAL(5,4),
    index_usage_stats JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Search cache for frequently requested queries
CREATE TABLE search_cache (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cache_key VARCHAR(500) NOT NULL UNIQUE,
    query_hash VARCHAR(64) NOT NULL,
    query_params JSONB NOT NULL,
    results JSONB,
    result_count INTEGER,
    cache_ttl_seconds INTEGER DEFAULT 3600, -- 1 hour default
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    access_count INTEGER DEFAULT 1,
    last_accessed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Search synonyms and related terms
CREATE TABLE search_synonyms (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    term TEXT NOT NULL,
    synonyms TEXT[] NOT NULL,
    language VARCHAR(10) DEFAULT 'en',
    confidence_score DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Search blacklist for inappropriate terms
CREATE TABLE search_blacklist (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    term TEXT NOT NULL UNIQUE,
    reason VARCHAR(200),
    blacklisted_by UUID,
    blacklisted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Create indexes for performance
CREATE INDEX idx_search_index_content_type ON search_index(content_type);
CREATE INDEX idx_search_index_content_id ON search_index(content_id);
CREATE INDEX idx_search_index_search_vector ON search_index USING GIN(search_vector);
CREATE INDEX idx_search_index_relevance_score ON search_index(relevance_score);
CREATE INDEX idx_search_index_created_at ON search_index(created_at);
CREATE INDEX idx_search_index_last_indexed ON search_index(last_indexed_at);

CREATE INDEX idx_search_history_user_id ON search_history(user_id);
CREATE INDEX idx_search_history_query ON search_history USING GIN(to_tsvector('english', query));
CREATE INDEX idx_search_history_created_at ON search_history(created_at);

CREATE INDEX idx_search_analytics_query ON search_analytics USING GIN(to_tsvector('english', query));
CREATE INDEX idx_search_analytics_search_count ON search_analytics(search_count);
CREATE INDEX idx_search_analytics_updated_at ON search_analytics(updated_at);

CREATE INDEX idx_search_suggestions_suggestion ON search_suggestions USING GIN(to_tsvector('english', suggestion));
CREATE INDEX idx_search_suggestions_type ON search_suggestions(suggestion_type);
CREATE INDEX idx_search_suggestions_frequency ON search_suggestions(frequency);

CREATE INDEX idx_search_filters_user_id ON search_filters(user_id);
CREATE INDEX idx_search_filters_type ON search_filters(filter_type);

CREATE INDEX idx_search_rankings_user_content ON search_rankings(user_id, content_id);
CREATE INDEX idx_search_rankings_content_type ON search_rankings(content_type);
CREATE INDEX idx_search_rankings_final_score ON search_rankings(final_score);

CREATE INDEX idx_search_cache_query_hash ON search_cache(query_hash);
CREATE INDEX idx_search_cache_expires_at ON search_cache(expires_at);
CREATE INDEX idx_search_cache_last_accessed ON search_cache(last_accessed_at);

CREATE INDEX idx_search_synonyms_term ON search_synonyms USING GIN(to_tsvector('english', term));
CREATE INDEX idx_search_synonyms_language ON search_synonyms(language);

-- Create full-text search index
CREATE INDEX idx_search_index_fulltext ON search_index USING GIN(search_vector);

-- Create functions for search operations
CREATE OR REPLACE FUNCTION update_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := 
        setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.content, '')), 'B') ||
        setweight(to_tsvector('english', array_to_string(COALESCE(NEW.tags, ARRAY[]::TEXT[]), ' ')), 'C');
    
    NEW.last_indexed_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate search relevance score
CREATE OR REPLACE FUNCTION calculate_search_relevance(
    p_content_type VARCHAR(50),
    p_content_id UUID,
    p_user_id UUID DEFAULT NULL
)
RETURNS DECIMAL(5,4) AS $$
DECLARE
    relevance_score DECIMAL(5,4) := 1.0;
    popularity_factor DECIMAL(3,2) := 1.0;
    recency_factor DECIMAL(3,2) := 1.0;
    personalization_factor DECIMAL(3,2) := 1.0;
BEGIN
    -- Base relevance score
    relevance_score := 1.0;
    
    -- Popularity factor (based on engagement)
    SELECT COALESCE(engagement_score, 1.0) INTO popularity_factor
    FROM analytics_schema.user_engagement_profiles
    WHERE user_id = p_content_id AND p_content_type = 'user';
    
    -- Recency factor (newer content gets higher score)
    SELECT CASE 
        WHEN EXTRACT(EPOCH FROM (NOW() - created_at)) < 86400 THEN 1.2  -- 1 day
        WHEN EXTRACT(EPOCH FROM (NOW() - created_at)) < 604800 THEN 1.1 -- 1 week
        ELSE 1.0
    END INTO recency_factor
    FROM search_index
    WHERE content_id = p_content_id;
    
    -- Personalization factor (user preferences)
    IF p_user_id IS NOT NULL THEN
        SELECT COALESCE(personalization_score, 1.0) INTO personalization_factor
        FROM search_rankings
        WHERE user_id = p_user_id AND content_id = p_content_id;
    END IF;
    
    -- Calculate final relevance score
    relevance_score := relevance_score * popularity_factor * recency_factor * personalization_factor;
    
    -- Ensure score is within bounds
    RETURN GREATEST(0.1, LEAST(5.0, relevance_score));
END;
$$ LANGUAGE plpgsql;

-- Function to get search suggestions
CREATE OR REPLACE FUNCTION get_search_suggestions(
    p_query TEXT,
    p_limit INTEGER DEFAULT 10,
    p_user_id UUID DEFAULT NULL
)
RETURNS TABLE (
    suggestion TEXT,
    suggestion_type VARCHAR(50),
    relevance_score DECIMAL(5,4)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.suggestion,
        s.suggestion_type,
        s.relevance_score
    FROM search_suggestions s
    WHERE s.suggestion ILIKE p_query || '%'
        OR s.suggestion ILIKE '%' || p_query || '%'
    ORDER BY 
        CASE WHEN s.suggestion ILIKE p_query || '%' THEN 1 ELSE 2 END,
        s.frequency DESC,
        s.relevance_score DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Function to update search analytics
CREATE OR REPLACE FUNCTION update_search_analytics(
    p_query TEXT,
    p_results_count INTEGER,
    p_execution_time_ms INTEGER
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO search_analytics (query, results_count, execution_time_ms)
    VALUES (p_query, p_results_count, p_execution_time_ms)
    ON CONFLICT (query) DO UPDATE SET
        search_count = search_analytics.search_count + 1,
        avg_results_count = (search_analytics.avg_results_count * search_analytics.search_count + p_results_count) / (search_analytics.search_count + 1),
        avg_execution_time_ms = (search_analytics.avg_execution_time_ms * search_analytics.search_count + p_execution_time_ms) / (search_analytics.search_count + 1),
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup old search data
CREATE OR REPLACE FUNCTION cleanup_old_search_data()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER := 0;
BEGIN
    -- Cleanup old search history (older than 90 days)
    DELETE FROM search_history 
    WHERE created_at < NOW() - INTERVAL '90 days';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    -- Cleanup expired search cache
    DELETE FROM search_cache 
    WHERE expires_at < NOW();
    
    -- Cleanup old search analytics (older than 1 year)
    DELETE FROM search_analytics 
    WHERE updated_at < NOW() - INTERVAL '1 year';
    
    -- Cleanup old search performance data (older than 6 months)
    DELETE FROM search_performance 
    WHERE updated_at < NOW() - INTERVAL '6 months';
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Create triggers
CREATE TRIGGER trigger_search_index_vector
    BEFORE INSERT OR UPDATE ON search_index
    FOR EACH ROW
    EXECUTE FUNCTION update_search_vector();

CREATE TRIGGER trigger_search_index_updated_at
    BEFORE UPDATE ON search_index
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_search_analytics_updated_at
    BEFORE UPDATE ON search_analytics
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_search_suggestions_updated_at
    BEFORE UPDATE ON search_suggestions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_search_filters_updated_at
    BEFORE UPDATE ON search_filters
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_search_rankings_updated_at
    BEFORE UPDATE ON search_rankings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_search_performance_updated_at
    BEFORE UPDATE ON search_performance
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create views for common queries
CREATE VIEW search_performance_summary AS
SELECT 
    DATE_TRUNC('day', created_at) as date,
    COUNT(*) as total_searches,
    AVG(execution_time_ms) as avg_execution_time,
    AVG(results_count) as avg_results_count
FROM search_analytics
GROUP BY DATE_TRUNC('day', created_at)
ORDER BY date DESC;

CREATE VIEW popular_search_queries AS
SELECT 
    query,
    search_count,
    avg_results_count,
    avg_execution_time_ms,
    success_rate
FROM search_analytics
WHERE search_count > 5
ORDER BY search_count DESC, avg_execution_time_ms ASC;

CREATE VIEW search_suggestions_summary AS
SELECT 
    suggestion_type,
    COUNT(*) as suggestion_count,
    AVG(frequency) as avg_frequency,
    AVG(relevance_score) as avg_relevance
FROM search_suggestions
GROUP BY suggestion_type
ORDER BY suggestion_count DESC;

-- Insert default search suggestions
INSERT INTO search_suggestions (suggestion, suggestion_type, frequency, relevance_score) VALUES
('technology', 'query', 100, 1.0),
('programming', 'query', 95, 1.0),
('ai', 'query', 90, 1.0),
('machine learning', 'query', 85, 1.0),
('web development', 'query', 80, 1.0),
('data science', 'query', 75, 1.0),
('cybersecurity', 'query', 70, 1.0),
('blockchain', 'query', 65, 1.0),
('cloud computing', 'query', 60, 1.0),
('mobile development', 'query', 55, 1.0);

-- Insert default search synonyms
INSERT INTO search_synonyms (term, synonyms, language, confidence_score) VALUES
('ai', ARRAY['artificial intelligence', 'machine learning', 'ML'], 'en', 0.9),
('programming', ARRAY['coding', 'development', 'software engineering'], 'en', 0.8),
('tech', ARRAY['technology', 'technical', 'engineering'], 'en', 0.7),
('dev', ARRAY['developer', 'development', 'programming'], 'en', 0.8),
('data', ARRAY['information', 'analytics', 'statistics'], 'en', 0.6);

-- Create search cache cleanup job (runs every hour)
SELECT cron.schedule(
    'cleanup-search-cache',
    '0 * * * *', -- Every hour
    'SELECT search_schema.cleanup_old_search_data();'
);

-- Grant permissions
GRANT USAGE ON SCHEMA search_schema TO sonet;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA search_schema TO sonet;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA search_schema TO sonet;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA search_schema TO sonet;