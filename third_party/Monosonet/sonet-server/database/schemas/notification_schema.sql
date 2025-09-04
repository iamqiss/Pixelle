-- Notification Service Database Schema
-- Handles user notifications, preferences, and delivery

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Notification types enum
CREATE TYPE notification_type AS ENUM (
    'note_like',
    'note_repost',
    'note_comment',
    'note_mention',
    'follow_request',
    'follow_accepted',
    'follow_new',
    'message_received',
    'message_reaction',
    'media_tagged',
    'media_liked',
    'system_announcement',
    'security_alert',
    'achievement_unlocked'
);

-- Notification priority levels
CREATE TYPE notification_priority AS ENUM (
    'low',
    'normal',
    'high',
    'urgent'
);

-- Notification delivery status
CREATE TYPE notification_status AS ENUM (
    'pending',
    'sent',
    'delivered',
    'read',
    'failed',
    'cancelled'
);

-- Main notifications table
CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL, -- recipient
    actor_id UUID, -- user who triggered the notification (NULL for system)
    notification_type notification_type NOT NULL,
    priority notification_priority DEFAULT 'normal',
    status notification_status DEFAULT 'pending',
    
    -- Content fields
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    rich_content JSONB, -- structured content for rich notifications
    
    -- Reference fields (optional, for context)
    reference_type VARCHAR(50), -- 'note', 'message', 'media', etc.
    reference_id UUID, -- ID of the referenced entity
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    scheduled_for TIMESTAMP WITH TIME ZONE, -- for scheduled notifications
    expires_at TIMESTAMP WITH TIME ZONE, -- for expiring notifications
    
    -- Delivery tracking
    sent_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    read_at TIMESTAMP WITH TIME ZONE,
    
    -- Failure tracking
    failure_reason TEXT,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    
    -- Indexes
    CONSTRAINT fk_notifications_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_notifications_actor FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE SET NULL
);

-- Notification preferences per user
CREATE TABLE notification_preferences (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL UNIQUE,
    
    -- Channel preferences
    email_enabled BOOLEAN DEFAULT true,
    push_enabled BOOLEAN DEFAULT true,
    sms_enabled BOOLEAN DEFAULT false,
    in_app_enabled BOOLEAN DEFAULT true,
    
    -- Type-specific preferences
    note_like_enabled BOOLEAN DEFAULT true,
    note_repost_enabled BOOLEAN DEFAULT true,
    note_comment_enabled BOOLEAN DEFAULT true,
    note_mention_enabled BOOLEAN DEFAULT true,
    follow_request_enabled BOOLEAN DEFAULT true,
    follow_accepted_enabled BOOLEAN DEFAULT true,
    follow_new_enabled BOOLEAN DEFAULT true,
    message_received_enabled BOOLEAN DEFAULT true,
    message_reaction_enabled BOOLEAN DEFAULT true,
    media_tagged_enabled BOOLEAN DEFAULT true,
    media_liked_enabled BOOLEAN DEFAULT true,
    system_announcement_enabled BOOLEAN DEFAULT true,
    security_alert_enabled BOOLEAN DEFAULT true,
    achievement_unlocked_enabled BOOLEAN DEFAULT true,
    
    -- Frequency preferences
    digest_frequency VARCHAR(20) DEFAULT 'immediate', -- immediate, hourly, daily, weekly
    quiet_hours_start TIME DEFAULT '22:00:00',
    quiet_hours_end TIME DEFAULT '08:00:00',
    quiet_hours_enabled BOOLEAN DEFAULT true,
    
    -- Advanced preferences
    max_notifications_per_day INTEGER DEFAULT 100,
    group_similar_notifications BOOLEAN DEFAULT true,
    show_preview_content BOOLEAN DEFAULT true,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_notification_preferences_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Notification delivery queue
CREATE TABLE notification_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id UUID NOT NULL,
    user_id UUID NOT NULL,
    
    -- Delivery details
    channel VARCHAR(20) NOT NULL, -- email, push, sms, in_app
    priority notification_priority DEFAULT 'normal',
    status notification_status DEFAULT 'pending',
    
    -- Scheduling
    scheduled_for TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    retry_after TIMESTAMP WITH TIME ZONE,
    
    -- Attempt tracking
    attempt_count INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    
    -- Delivery metadata
    delivery_data JSONB, -- channel-specific delivery data
    failure_reason TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_notification_queue_notification FOREIGN KEY (notification_id) REFERENCES notifications(id) ON DELETE CASCADE,
    CONSTRAINT fk_notification_queue_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Notification templates
CREATE TABLE notification_templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    template_key VARCHAR(100) NOT NULL UNIQUE,
    notification_type notification_type NOT NULL,
    
    -- Template content
    title_template TEXT NOT NULL,
    message_template TEXT NOT NULL,
    rich_content_template JSONB,
    
    -- Localization
    language VARCHAR(10) DEFAULT 'en',
    locale VARCHAR(20) DEFAULT 'en-US',
    
    -- Metadata
    is_active BOOLEAN DEFAULT true,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Notification delivery logs
CREATE TABLE notification_delivery_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id UUID NOT NULL,
    queue_id UUID NOT NULL,
    user_id UUID NOT NULL,
    
    -- Delivery details
    channel VARCHAR(20) NOT NULL,
    status notification_status NOT NULL,
    
    -- Timing
    queued_at TIMESTAMP WITH TIME ZONE NOT NULL,
    sent_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    delivery_data JSONB,
    failure_reason TEXT,
    response_data JSONB,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_delivery_logs_notification FOREIGN KEY (notification_id) REFERENCES notifications(id) ON DELETE CASCADE,
    CONSTRAINT fk_delivery_logs_queue FOREIGN KEY (queue_id) REFERENCES notification_queue(id) ON DELETE CASCADE,
    CONSTRAINT fk_delivery_logs_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Notification aggregation rules
CREATE TABLE notification_aggregation_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_name VARCHAR(100) NOT NULL UNIQUE,
    
    -- Aggregation criteria
    notification_type notification_type NOT NULL,
    actor_id UUID, -- NULL means any actor
    reference_type VARCHAR(50),
    reference_id UUID,
    
    -- Aggregation settings
    time_window_minutes INTEGER DEFAULT 60,
    max_notifications INTEGER DEFAULT 10,
    group_by_actor BOOLEAN DEFAULT true,
    group_by_reference BOOLEAN DEFAULT true,
    
    -- Output settings
    aggregated_title_template TEXT NOT NULL,
    aggregated_message_template TEXT NOT NULL,
    
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_aggregation_rules_actor FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Notification metrics
CREATE TABLE notification_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    date DATE NOT NULL,
    
    -- Counts
    total_received INTEGER DEFAULT 0,
    total_read INTEGER DEFAULT 0,
    total_clicked INTEGER DEFAULT 0,
    total_dismissed INTEGER DEFAULT 0,
    
    -- Channel breakdown
    email_sent INTEGER DEFAULT 0,
    push_sent INTEGER DEFAULT 0,
    sms_sent INTEGER DEFAULT 0,
    in_app_sent INTEGER DEFAULT 0,
    
    -- Type breakdown
    note_like_count INTEGER DEFAULT 0,
    note_repost_count INTEGER DEFAULT 0,
    note_comment_count INTEGER DEFAULT 0,
    note_mention_count INTEGER DEFAULT 0,
    follow_request_count INTEGER DEFAULT 0,
    follow_accepted_count INTEGER DEFAULT 0,
    follow_new_count INTEGER DEFAULT 0,
    message_received_count INTEGER DEFAULT 0,
    message_reaction_count INTEGER DEFAULT 0,
    media_tagged_count INTEGER DEFAULT 0,
    media_liked_count INTEGER DEFAULT 0,
    system_announcement_count INTEGER DEFAULT 0,
    security_alert_count INTEGER DEFAULT 0,
    achievement_unlocked_count INTEGER DEFAULT 0,
    
    -- Engagement metrics
    avg_time_to_read INTERVAL,
    avg_time_to_click INTERVAL,
    engagement_rate DECIMAL(5,4), -- (read + clicked) / received
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_notification_metrics_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE(user_id, date)
);

-- Indexes for performance
CREATE INDEX idx_notifications_user_id ON notifications(user_id);
CREATE INDEX idx_notifications_actor_id ON notifications(actor_id);
CREATE INDEX idx_notifications_type ON notifications(notification_type);
CREATE INDEX idx_notifications_status ON notifications(status);
CREATE INDEX idx_notifications_created_at ON notifications(created_at);
CREATE INDEX idx_notifications_scheduled_for ON notifications(scheduled_for);
CREATE INDEX idx_notifications_reference ON notifications(reference_type, reference_id);

CREATE INDEX idx_notification_queue_user_id ON notification_queue(user_id);
CREATE INDEX idx_notification_queue_status ON notification_queue(status);
CREATE INDEX idx_notification_queue_scheduled_for ON notification_queue(scheduled_for);
CREATE INDEX idx_notification_queue_channel ON notification_queue(channel);

CREATE INDEX idx_notification_delivery_logs_notification_id ON notification_delivery_logs(notification_id);
CREATE INDEX idx_notification_delivery_logs_user_id ON notification_delivery_logs(user_id);
CREATE INDEX idx_notification_delivery_logs_created_at ON notification_delivery_logs(created_at);

CREATE INDEX idx_notification_metrics_user_date ON notification_metrics(user_id, date);

-- Full-text search on notification content
CREATE INDEX idx_notifications_content_search ON notifications USING gin(to_tsvector('english', title || ' ' || message));

-- Functions for notification management
CREATE OR REPLACE FUNCTION update_notification_status(
    p_notification_id UUID,
    p_status notification_status,
    p_user_id UUID DEFAULT NULL
)
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE notifications 
    SET status = p_status,
        updated_at = NOW(),
        sent_at = CASE WHEN p_status = 'sent' THEN NOW() ELSE sent_at END,
        delivered_at = CASE WHEN p_status = 'delivered' THEN NOW() ELSE delivered_at END,
        read_at = CASE WHEN p_status = 'read' THEN NOW() ELSE read_at END
    WHERE id = p_notification_id 
    AND (p_user_id IS NULL OR user_id = p_user_id);
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

-- Function to get unread notification count
CREATE OR REPLACE FUNCTION get_unread_notification_count(p_user_id UUID)
RETURNS INTEGER AS $$
BEGIN
    RETURN (
        SELECT COUNT(*) 
        FROM notifications 
        WHERE user_id = p_user_id 
        AND status IN ('pending', 'sent', 'delivered')
    );
END;
$$ LANGUAGE plpgsql;

-- Function to mark notifications as read
CREATE OR REPLACE FUNCTION mark_notifications_read(
    p_user_id UUID,
    p_notification_ids UUID[] DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    IF p_notification_ids IS NULL THEN
        -- Mark all unread notifications as read
        UPDATE notifications 
        SET status = 'read',
            read_at = NOW(),
            updated_at = NOW()
        WHERE user_id = p_user_id 
        AND status IN ('pending', 'sent', 'delivered');
        
        GET DIAGNOSTICS updated_count = ROW_COUNT;
    ELSE
        -- Mark specific notifications as read
        UPDATE notifications 
        SET status = 'read',
            read_at = NOW(),
            updated_at = NOW()
        WHERE user_id = p_user_id 
        AND id = ANY(p_notification_ids)
        AND status IN ('pending', 'sent', 'delivered');
        
        GET DIAGNOSTICS updated_count = ROW_COUNT;
    END IF;
    
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup old notifications
CREATE OR REPLACE FUNCTION cleanup_old_notifications(p_days_old INTEGER DEFAULT 90)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM notifications 
    WHERE created_at < NOW() - INTERVAL '1 day' * p_days_old
    AND status IN ('read', 'cancelled');
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to update notification metrics
CREATE OR REPLACE FUNCTION update_notification_metrics(
    p_user_id UUID,
    p_date DATE DEFAULT CURRENT_DATE
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO notification_metrics (
        user_id, date, total_received, total_read, total_clicked,
        note_like_count, note_repost_count, note_comment_count, note_mention_count,
        follow_request_count, follow_accepted_count, follow_new_count,
        message_received_count, message_reaction_count, media_tagged_count,
        media_liked_count, system_announcement_count, security_alert_count,
        achievement_unlocked_count
    )
    SELECT 
        user_id,
        p_date,
        COUNT(*) as total_received,
        COUNT(*) FILTER (WHERE status = 'read') as total_read,
        COUNT(*) FILTER (WHERE status = 'clicked') as total_clicked,
        COUNT(*) FILTER (WHERE notification_type = 'note_like') as note_like_count,
        COUNT(*) FILTER (WHERE notification_type = 'note_repost') as note_repost_count,
        COUNT(*) FILTER (WHERE notification_type = 'note_comment') as note_comment_count,
        COUNT(*) FILTER (WHERE notification_type = 'note_mention') as note_mention_count,
        COUNT(*) FILTER (WHERE notification_type = 'follow_request') as follow_request_count,
        COUNT(*) FILTER (WHERE notification_type = 'follow_accepted') as follow_accepted_count,
        COUNT(*) FILTER (WHERE notification_type = 'follow_new') as follow_new_count,
        COUNT(*) FILTER (WHERE notification_type = 'message_received') as message_received_count,
        COUNT(*) FILTER (WHERE notification_type = 'message_reaction') as message_reaction_count,
        COUNT(*) FILTER (WHERE notification_type = 'media_tagged') as media_tagged_count,
        COUNT(*) FILTER (WHERE notification_type = 'media_liked') as media_liked_count,
        COUNT(*) FILTER (WHERE notification_type = 'system_announcement') as system_announcement_count,
        COUNT(*) FILTER (WHERE notification_type = 'security_alert') as security_alert_count,
        COUNT(*) FILTER (WHERE notification_type = 'achievement_unlocked') as achievement_unlocked_count
    FROM notifications 
    WHERE user_id = p_user_id 
    AND DATE(created_at) = p_date
    GROUP BY user_id
    
    ON CONFLICT (user_id, date) 
    DO UPDATE SET
        total_received = EXCLUDED.total_received,
        total_read = EXCLUDED.total_read,
        total_clicked = EXCLUDED.total_clicked,
        note_like_count = EXCLUDED.note_like_count,
        note_repost_count = EXCLUDED.note_repost_count,
        note_comment_count = EXCLUDED.note_comment_count,
        note_mention_count = EXCLUDED.note_mention_count,
        follow_request_count = EXCLUDED.follow_request_count,
        follow_accepted_count = EXCLUDED.follow_accepted_count,
        follow_new_count = EXCLUDED.follow_new_count,
        message_received_count = EXCLUDED.message_received_count,
        message_reaction_count = EXCLUDED.message_reaction_count,
        media_tagged_count = EXCLUDED.media_tagged_count,
        media_liked_count = EXCLUDED.media_liked_count,
        system_announcement_count = EXCLUDED.system_announcement_count,
        security_alert_count = EXCLUDED.security_alert_count,
        achievement_unlocked_count = EXCLUDED.achievement_unlocked_count,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Views for common queries
CREATE VIEW user_notifications_summary AS
SELECT 
    n.user_id,
    u.username,
    COUNT(*) as total_notifications,
    COUNT(*) FILTER (WHERE n.status = 'pending') as pending_count,
    COUNT(*) FILTER (WHERE n.status = 'sent') as sent_count,
    COUNT(*) FILTER (WHERE n.status = 'delivered') as delivered_count,
    COUNT(*) FILTER (WHERE n.status = 'read') as read_count,
    COUNT(*) FILTER (WHERE n.status = 'failed') as failed_count,
    MAX(n.created_at) as last_notification_at
FROM notifications n
JOIN users u ON n.user_id = u.id
GROUP BY n.user_id, u.username;

CREATE VIEW notification_type_summary AS
SELECT 
    notification_type,
    COUNT(*) as total_count,
    COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
    COUNT(*) FILTER (WHERE status = 'delivered') as delivered_count,
    COUNT(*) FILTER (WHERE status = 'read') as read_count,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
    AVG(EXTRACT(EPOCH FROM (read_at - created_at))) as avg_time_to_read_seconds
FROM notifications
WHERE read_at IS NOT NULL
GROUP BY notification_type;

-- Triggers for automatic updates
CREATE OR REPLACE FUNCTION update_notification_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_notifications_updated_at
    BEFORE UPDATE ON notifications
    FOR EACH ROW
    EXECUTE FUNCTION update_notification_updated_at();

CREATE TRIGGER trigger_notification_preferences_updated_at
    BEFORE UPDATE ON notification_preferences
    FOR EACH ROW
    EXECUTE FUNCTION update_notification_updated_at();

CREATE TRIGGER trigger_notification_queue_updated_at
    BEFORE UPDATE ON notification_queue
    FOR EACH ROW
    EXECUTE FUNCTION update_notification_updated_at();

CREATE TRIGGER trigger_notification_templates_updated_at
    BEFORE UPDATE ON notification_templates
    FOR EACH ROW
    EXECUTE FUNCTION update_notification_updated_at();

CREATE TRIGGER trigger_notification_metrics_updated_at
    BEFORE UPDATE ON notification_metrics
    FOR EACH ROW
    EXECUTE FUNCTION update_notification_updated_at();

-- Insert default notification templates
INSERT INTO notification_templates (template_key, notification_type, title_template, message_template) VALUES
('note_like_default', 'note_like', '@{actor_username} liked your note', '@{actor_username} liked your note "{note_preview}"'),
('note_repost_default', 'note_repost', '@{actor_username} reposted your note', '@{actor_username} reposted your note "{note_preview}"'),
('note_comment_default', 'note_comment', '@{actor_username} commented on your note', '@{actor_username} commented: "{comment_preview}"'),
('note_mention_default', 'note_mention', '@{actor_username} mentioned you in a note', '@{actor_username} mentioned you: "{note_preview}"'),
('follow_request_default', 'follow_request', 'New follow request from @{actor_username}', '@{actor_username} wants to follow you'),
('follow_accepted_default', 'follow_accepted', '@{actor_username} accepted your follow request', '@{actor_username} accepted your follow request'),
('follow_new_default', 'follow_new', '@{actor_username} started following you', '@{actor_username} started following you'),
('message_received_default', 'message_received', 'New message from @{actor_username}', '@{actor_username} sent you a message'),
('message_reaction_default', 'message_reaction', '@{actor_username} reacted to your message', '@{actor_username} reacted with {reaction} to your message'),
('media_tagged_default', 'media_tagged', '@{actor_username} tagged you in media', '@{actor_username} tagged you in media'),
('media_liked_default', 'media_liked', '@{actor_username} liked your media', '@{actor_username} liked your media'),
('system_announcement_default', 'system_announcement', 'System Announcement', '{announcement_title}'),
('security_alert_default', 'security_alert', 'Security Alert', '{security_message}'),
('achievement_unlocked_default', 'achievement_unlocked', 'Achievement Unlocked!', 'You unlocked the "{achievement_name}" achievement!');

-- Comments
COMMENT ON TABLE notifications IS 'Main table storing all user notifications';
COMMENT ON TABLE notification_preferences IS 'User preferences for notification delivery and types';
COMMENT ON TABLE notification_queue IS 'Queue for processing notification delivery across different channels';
COMMENT ON TABLE notification_templates IS 'Templates for generating notification content';
COMMENT ON TABLE notification_delivery_logs IS 'Audit log of all notification delivery attempts';
COMMENT ON TABLE notification_aggregation_rules IS 'Rules for grouping similar notifications together';
COMMENT ON TABLE notification_metrics IS 'Daily aggregated metrics for notification engagement';