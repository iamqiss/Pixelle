
/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "include/message_threading.hpp"
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include <regex>

namespace sonet::messaging::threading {

// ThreadMetadata implementation
Json::Value ThreadMetadata::to_json() const {
    Json::Value json;
    json["thread_id"] = thread_id;
    json["chat_id"] = chat_id;
    json["parent_message_id"] = parent_message_id;
    json["title"] = title;
    json["description"] = description;
    json["visibility"] = static_cast<int>(visibility);
    json["status"] = static_cast<int>(status);
    json["creator_id"] = creator_id;
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["updated_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        updated_at.time_since_epoch()).count();
    json["last_activity"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        last_activity.time_since_epoch()).count();
    
    json["message_count"] = message_count;
    json["participant_count"] = participant_count;
    json["view_count"] = view_count;
    
    json["allow_reactions"] = allow_reactions;
    json["allow_replies"] = allow_replies;
    json["auto_archive"] = auto_archive;
    json["auto_archive_duration"] = static_cast<int64_t>(auto_archive_duration.count());
    json["max_participants"] = max_participants;
    
    Json::Value tags_json(Json::arrayValue);
    for (const auto& tag : tags) {
        tags_json.append(tag);
    }
    json["tags"] = tags_json;
    
    json["category"] = category;
    json["priority"] = priority;
    
    return json;
}

ThreadMetadata ThreadMetadata::from_json(const Json::Value& json) {
    ThreadMetadata metadata;
    metadata.thread_id = json["thread_id"].asString();
    metadata.chat_id = json["chat_id"].asString();
    metadata.parent_message_id = json["parent_message_id"].asString();
    metadata.title = json["title"].asString();
    metadata.description = json["description"].asString();
    metadata.visibility = static_cast<ThreadVisibility>(json["visibility"].asInt());
    metadata.status = static_cast<ThreadStatus>(json["status"].asInt());
    metadata.creator_id = json["creator_id"].asString();
    
    auto created_ms = json["created_at"].asInt64();
    metadata.created_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(created_ms));
    
    auto updated_ms = json["updated_at"].asInt64();
    metadata.updated_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(updated_ms));
    
    auto activity_ms = json["last_activity"].asInt64();
    metadata.last_activity = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(activity_ms));
    
    metadata.message_count = json["message_count"].asUInt();
    metadata.participant_count = json["participant_count"].asUInt();
    metadata.view_count = json["view_count"].asUInt();
    
    metadata.allow_reactions = json["allow_reactions"].asBool();
    metadata.allow_replies = json["allow_replies"].asBool();
    metadata.auto_archive = json["auto_archive"].asBool();
    metadata.auto_archive_duration = std::chrono::hours(json["auto_archive_duration"].asInt64());
    metadata.max_participants = json["max_participants"].asUInt();
    
    const auto& tags_json = json["tags"];
    for (const auto& tag : tags_json) {
        metadata.tags.push_back(tag.asString());
    }
    
    metadata.category = json["category"].asString();
    metadata.priority = json["priority"].asUInt();
    
    return metadata;
}

// ThreadParticipant implementation
Json::Value ThreadParticipant::to_json() const {
    Json::Value json;
    json["user_id"] = user_id;
    json["thread_id"] = thread_id;
    json["level"] = static_cast<int>(level);
    json["joined_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        joined_at.time_since_epoch()).count();
    json["last_read"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        last_read.time_since_epoch()).count();
    json["notifications_enabled"] = notifications_enabled;
    json["is_muted"] = is_muted;
    json["unread_count"] = unread_count;
    json["messages_sent"] = messages_sent;
    json["reactions_given"] = reactions_given;
    json["last_active"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        last_active.time_since_epoch()).count();
    return json;
}

ThreadParticipant ThreadParticipant::from_json(const Json::Value& json) {
    ThreadParticipant participant;
    participant.user_id = json["user_id"].asString();
    participant.thread_id = json["thread_id"].asString();
    participant.level = static_cast<ParticipationLevel>(json["level"].asInt());
    
    auto joined_ms = json["joined_at"].asInt64();
    participant.joined_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(joined_ms));
    
    auto read_ms = json["last_read"].asInt64();
    participant.last_read = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(read_ms));
    
    auto active_ms = json["last_active"].asInt64();
    participant.last_active = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(active_ms));
    
    participant.notifications_enabled = json["notifications_enabled"].asBool();
    participant.is_muted = json["is_muted"].asBool();
    participant.unread_count = json["unread_count"].asUInt();
    participant.messages_sent = json["messages_sent"].asUInt();
    participant.reactions_given = json["reactions_given"].asUInt();
    
    return participant;
}

// MessageReply implementation
Json::Value MessageReply::to_json() const {
    Json::Value json;
    json["reply_id"] = reply_id;
    json["parent_message_id"] = parent_message_id;
    json["replying_message_id"] = replying_message_id;
    json["user_id"] = user_id;
    json["quoted_text"] = quoted_text;
    json["created_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_at.time_since_epoch()).count();
    json["is_thread_starter"] = is_thread_starter;
    json["depth_level"] = depth_level;
    return json;
}

MessageReply MessageReply::from_json(const Json::Value& json) {
    MessageReply reply;
    reply.reply_id = json["reply_id"].asString();
    reply.parent_message_id = json["parent_message_id"].asString();
    reply.replying_message_id = json["replying_message_id"].asString();
    reply.user_id = json["user_id"].asString();
    reply.quoted_text = json["quoted_text"].asString();
    
    auto created_ms = json["created_at"].asInt64();
    reply.created_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(created_ms));
    
    reply.is_thread_starter = json["is_thread_starter"].asBool();
    reply.depth_level = json["depth_level"].asUInt();
    
    return reply;
}

// ThreadAnalytics implementation
Json::Value ThreadAnalytics::to_json() const {
    Json::Value json;
    json["thread_id"] = thread_id;
    json["period_start"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        period_start.time_since_epoch()).count();
    json["period_end"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        period_end.time_since_epoch()).count();
    
    json["total_messages"] = total_messages;
    json["messages_per_hour"] = messages_per_hour;
    json["average_message_length"] = average_message_length;
    json["peak_concurrent_users"] = peak_concurrent_users;
    
    json["unique_participants"] = unique_participants;
    json["active_participants"] = active_participants;
    json["participation_rate"] = participation_rate;
    
    Json::Value user_counts(Json::objectValue);
    for (const auto& [user_id, count] : user_message_counts) {
        user_counts[user_id] = count;
    }
    json["user_message_counts"] = user_counts;
    
    Json::Value reactions(Json::objectValue);
    for (const auto& [reaction, count] : popular_reactions) {
        reactions[reaction] = count;
    }
    json["popular_reactions"] = reactions;
    
    Json::Value topics(Json::arrayValue);
    for (const auto& topic : trending_topics) {
        topics.append(topic);
    }
    json["trending_topics"] = topics;
    
    json["media_shares"] = media_shares;
    json["link_shares"] = link_shares;
    
    return json;
}

void ThreadAnalytics::reset() {
    total_messages = 0;
    messages_per_hour = 0;
    average_message_length = 0.0;
    peak_concurrent_users = 0;
    unique_participants = 0;
    active_participants = 0;
    participation_rate = 0.0;
    user_message_counts.clear();
    popular_reactions.clear();
    trending_topics.clear();
    media_shares = 0;
    link_shares = 0;
}

// ThreadSearchQuery implementation
Json::Value ThreadSearchQuery::to_json() const {
    Json::Value json;
    json["query_text"] = query_text;
    json["chat_id"] = chat_id;
    
    Json::Value tags_json(Json::arrayValue);
    for (const auto& tag : tags) {
        tags_json.append(tag);
    }
    json["tags"] = tags_json;
    
    json["category"] = category;
    json["status"] = static_cast<int>(status);
    json["visibility"] = static_cast<int>(visibility);
    json["created_after"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_after.time_since_epoch()).count();
    json["created_before"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        created_before.time_since_epoch()).count();
    json["min_participants"] = min_participants;
    json["max_participants"] = max_participants;
    json["creator_id"] = creator_id;
    json["include_archived"] = include_archived;
    json["limit"] = limit;
    json["offset"] = offset;
    json["sort_by"] = static_cast<int>(sort_by);
    json["ascending"] = ascending;
    
    return json;
}

ThreadSearchQuery ThreadSearchQuery::from_json(const Json::Value& json) {
    ThreadSearchQuery query;
    query.query_text = json["query_text"].asString();
    query.chat_id = json["chat_id"].asString();
    
    const auto& tags_json = json["tags"];
    for (const auto& tag : tags_json) {
        query.tags.push_back(tag.asString());
    }
    
    query.category = json["category"].asString();
    query.status = static_cast<ThreadStatus>(json["status"].asInt());
    query.visibility = static_cast<ThreadVisibility>(json["visibility"].asInt());
    
    auto after_ms = json["created_after"].asInt64();
    query.created_after = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(after_ms));
    
    auto before_ms = json["created_before"].asInt64();
    query.created_before = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(before_ms));
    
    query.min_participants = json["min_participants"].asUInt();
    query.max_participants = json["max_participants"].asUInt();
    query.creator_id = json["creator_id"].asString();
    query.include_archived = json["include_archived"].asBool();
    query.limit = json["limit"].asUInt();
    query.offset = json["offset"].asUInt();
    query.sort_by = static_cast<ThreadSearchQuery::SortBy>(json["sort_by"].asInt());
    query.ascending = json["ascending"].asBool();
    
    return query;
}

// MessageThreadManager implementation
MessageThreadManager::MessageThreadManager()
    : auto_archive_enabled_(true), max_thread_depth_(50), analytics_enabled_(true), background_running_(true) {
    
    // Start background threads
    analytics_thread_ = std::thread([this]() { run_analytics_loop(); });
    cleanup_thread_ = std::thread([this]() { run_cleanup_loop(); });
}

MessageThreadManager::~MessageThreadManager() {
    background_running_ = false;
    
    if (analytics_thread_.joinable()) {
        analytics_thread_.join();
    }
    
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
}

std::future<ThreadMetadata> MessageThreadManager::create_thread(const std::string& chat_id,
                                                               const std::string& parent_message_id,
                                                               const std::string& creator_id,
                                                               const std::string& title,
                                                               const std::string& description) {
    return std::async(std::launch::async, [this, chat_id, parent_message_id, creator_id, title, description]() -> ThreadMetadata {
        ThreadMetadata metadata;
        metadata.thread_id = generate_thread_id();
        metadata.chat_id = chat_id;
        metadata.parent_message_id = parent_message_id;
        metadata.title = title.empty() ? "Thread" : title;
        metadata.description = description;
        metadata.visibility = ThreadVisibility::PUBLIC;
        metadata.status = ThreadStatus::ACTIVE;
        metadata.creator_id = creator_id;
        metadata.created_at = std::chrono::system_clock::now();
        metadata.updated_at = metadata.created_at;
        metadata.last_activity = metadata.created_at;
        
        metadata.message_count = 0;
        metadata.participant_count = 1;
        metadata.view_count = 0;
        
        metadata.allow_reactions = true;
        metadata.allow_replies = true;
        metadata.auto_archive = true;
        metadata.auto_archive_duration = std::chrono::hours(24 * 7); // 1 week
        metadata.max_participants = 1000;
        
        metadata.category = "general";
        metadata.priority = 1;
        
        {
            std::unique_lock<std::shared_mutex> lock(threads_mutex_);
            threads_[metadata.thread_id] = std::make_shared<ThreadMetadata>(metadata);
            chat_threads_[chat_id].insert(metadata.thread_id);
            user_threads_[creator_id].insert(metadata.thread_id);
            parent_message_threads_[parent_message_id].insert(metadata.thread_id);
        }
        
        // Add creator as admin participant
        ThreadParticipant creator_participant;
        creator_participant.user_id = creator_id;
        creator_participant.thread_id = metadata.thread_id;
        creator_participant.level = ParticipationLevel::ADMIN;
        creator_participant.joined_at = metadata.created_at;
        creator_participant.last_read = metadata.created_at;
        creator_participant.notifications_enabled = true;
        creator_participant.is_muted = false;
        creator_participant.unread_count = 0;
        creator_participant.messages_sent = 0;
        creator_participant.reactions_given = 0;
        creator_participant.last_active = metadata.created_at;
        
        {
            std::unique_lock<std::shared_mutex> lock(participants_mutex_);
            thread_participants_[metadata.thread_id].push_back(creator_participant);
        }
        
        // Initialize analytics
        if (analytics_enabled_.load()) {
            ThreadAnalytics analytics;
            analytics.thread_id = metadata.thread_id;
            analytics.period_start = metadata.created_at;
            analytics.period_end = metadata.created_at + std::chrono::hours(24);
            analytics.reset();
            thread_analytics_[metadata.thread_id] = analytics;
        }
        
        // Notify subscribers
        if (thread_created_callback_) {
            thread_created_callback_(metadata);
        }
        
        log_info("Created thread: " + metadata.thread_id + " in chat: " + chat_id);
        
        return metadata;
    });
}

std::future<bool> MessageThreadManager::add_participant(const std::string& thread_id,
                                                       const std::string& user_id,
                                                       ParticipationLevel level) {
    return std::async(std::launch::async, [this, thread_id, user_id, level]() -> bool {
        // Check if thread exists
        std::shared_lock<std::shared_mutex> thread_lock(threads_mutex_);
        auto thread_it = threads_.find(thread_id);
        if (thread_it == threads_.end()) {
            return false;
        }
        
        auto thread_metadata = thread_it->second;
        thread_lock.unlock();
        
        // Check if user is already a participant
        std::unique_lock<std::shared_mutex> participants_lock(participants_mutex_);
        auto& participants = thread_participants_[thread_id];
        
        auto existing = std::find_if(participants.begin(), participants.end(),
            [&user_id](const ThreadParticipant& p) { return p.user_id == user_id; });
        
        if (existing != participants.end()) {
            return false; // Already a participant
        }
        
        // Check participant limit
        if (participants.size() >= thread_metadata->max_participants) {
            return false;
        }
        
        // Create new participant
        ThreadParticipant participant;
        participant.user_id = user_id;
        participant.thread_id = thread_id;
        participant.level = level;
        participant.joined_at = std::chrono::system_clock::now();
        participant.last_read = participant.joined_at;
        participant.notifications_enabled = true;
        participant.is_muted = false;
        participant.unread_count = 0;
        participant.messages_sent = 0;
        participant.reactions_given = 0;
        participant.last_active = participant.joined_at;
        
        participants.push_back(participant);
        participants_lock.unlock();
        
        // Update thread metadata
        {
            std::unique_lock<std::shared_mutex> lock(threads_mutex_);
            thread_metadata->participant_count++;
            thread_metadata->updated_at = std::chrono::system_clock::now();
            user_threads_[user_id].insert(thread_id);
        }
        
        // Notify subscribers
        Json::Value event_data;
        event_data["type"] = "participant_joined";
        event_data["thread_id"] = thread_id;
        event_data["participant"] = participant.to_json();
        notify_thread_subscribers(thread_id, event_data);
        
        if (participant_joined_callback_) {
            participant_joined_callback_(participant);
        }
        
        return true;
    });
}

std::future<MessageReply> MessageThreadManager::create_reply(const std::string& parent_message_id,
                                                            const std::string& replying_message_id,
                                                            const std::string& user_id,
                                                            const std::string& quoted_text) {
    return std::async(std::launch::async, [this, parent_message_id, replying_message_id, user_id, quoted_text]() -> MessageReply {
        MessageReply reply;
        reply.reply_id = generate_reply_id();
        reply.parent_message_id = parent_message_id;
        reply.replying_message_id = replying_message_id;
        reply.user_id = user_id;
        reply.quoted_text = quoted_text;
        reply.created_at = std::chrono::system_clock::now();
        reply.is_thread_starter = false;
        
        // Calculate depth level
        std::shared_lock<std::shared_mutex> lock(replies_mutex_);
        reply.depth_level = ThreadUtils::calculate_thread_depth(parent_message_id, 
            [this](const std::string& msg_id) -> std::optional<MessageReply> {
                auto it = message_replies_.find(msg_id);
                if (it != message_replies_.end() && !it->second.empty()) {
                    return it->second[0]; // Return first reply as parent
                }
                return std::nullopt;
            });
        lock.unlock();
        
        // Check depth limit
        if (reply.depth_level > max_thread_depth_.load()) {
            log_warning("Reply depth exceeded limit for message: " + parent_message_id);
            reply.depth_level = max_thread_depth_.load();
        }
        
        // Store the reply
        {
            std::unique_lock<std::shared_mutex> lock(replies_mutex_);
            message_replies_[parent_message_id].push_back(reply);
        }
        
        // Find and update associated thread
        std::shared_lock<std::shared_mutex> thread_lock(threads_mutex_);
        for (const auto& [thread_id, participants] : thread_participants_) {
            auto thread_it = threads_.find(thread_id);
            if (thread_it != threads_.end()) {
                auto& thread_meta = thread_it->second;
                if (thread_meta->parent_message_id == parent_message_id || 
                    message_replies_.count(parent_message_id)) {
                    
                    // Update thread activity
                    thread_meta->last_activity = reply.created_at;
                    thread_meta->message_count++;
                    
                    // Update analytics
                    update_thread_analytics(thread_id, "message_replied");
                    
                    // Notify thread subscribers
                    Json::Value event_data;
                    event_data["type"] = "message_replied";
                    event_data["thread_id"] = thread_id;
                    event_data["reply"] = reply.to_json();
                    notify_thread_subscribers(thread_id, event_data);
                    
                    break;
                }
            }
        }
        thread_lock.unlock();
        
        return reply;
    });
}

std::future<std::vector<ThreadMetadata>> MessageThreadManager::search_threads(const ThreadSearchQuery& query) {
    return std::async(std::launch::async, [this, query]() -> std::vector<ThreadMetadata> {
        std::vector<ThreadMetadata> results;
        
        std::shared_lock<std::shared_mutex> lock(threads_mutex_);
        
        for (const auto& [thread_id, thread_ptr] : threads_) {
            const auto& thread = *thread_ptr;
            
            // Apply filters
            if (!query.chat_id.empty() && thread.chat_id != query.chat_id) {
                continue;
            }
            
            if (!query.creator_id.empty() && thread.creator_id != query.creator_id) {
                continue;
            }
            
            if (thread.status != query.status && query.status != ThreadStatus::ACTIVE) {
                continue;
            }
            
            if (thread.visibility != query.visibility && query.visibility != ThreadVisibility::PUBLIC) {
                continue;
            }
            
            if (!query.include_archived && thread.status == ThreadStatus::ARCHIVED) {
                continue;
            }
            
            if (thread.created_at < query.created_after || thread.created_at > query.created_before) {
                continue;
            }
            
            if (thread.participant_count < query.min_participants || 
                thread.participant_count > query.max_participants) {
                continue;
            }
            
            if (!query.category.empty() && thread.category != query.category) {
                continue;
            }
            
            // Tag filtering
            if (!query.tags.empty()) {
                bool has_tag = false;
                for (const auto& query_tag : query.tags) {
                    if (std::find(thread.tags.begin(), thread.tags.end(), query_tag) != thread.tags.end()) {
                        has_tag = true;
                        break;
                    }
                }
                if (!has_tag) {
                    continue;
                }
            }
            
            // Text search
            if (!query.query_text.empty()) {
                if (!ThreadUtils::matches_search_query(thread, query)) {
                    continue;
                }
            }
            
            results.push_back(thread);
        }
        
        // Sort results
        std::sort(results.begin(), results.end(), [&query](const ThreadMetadata& a, const ThreadMetadata& b) {
            switch (query.sort_by) {
                case ThreadSearchQuery::SortBy::CREATED_AT:
                    return query.ascending ? a.created_at < b.created_at : a.created_at > b.created_at;
                case ThreadSearchQuery::SortBy::UPDATED_AT:
                    return query.ascending ? a.updated_at < b.updated_at : a.updated_at > b.updated_at;
                case ThreadSearchQuery::SortBy::LAST_ACTIVITY:
                    return query.ascending ? a.last_activity < b.last_activity : a.last_activity > b.last_activity;
                case ThreadSearchQuery::SortBy::MESSAGE_COUNT:
                    return query.ascending ? a.message_count < b.message_count : a.message_count > b.message_count;
                case ThreadSearchQuery::SortBy::PARTICIPANT_COUNT:
                    return query.ascending ? a.participant_count < b.participant_count : a.participant_count > b.participant_count;
                case ThreadSearchQuery::SortBy::RELEVANCE:
                    if (!query.query_text.empty()) {
                        double score_a = ThreadUtils::calculate_relevance_score(a, query.query_text);
                        double score_b = ThreadUtils::calculate_relevance_score(b, query.query_text);
                        return query.ascending ? score_a < score_b : score_a > score_b;
                    }
                    return a.created_at > b.created_at; // Default to newest first
                default:
                    return a.created_at > b.created_at;
            }
        });
        
        // Apply pagination
        if (query.offset >= results.size()) {
            return {};
        }
        
        auto start = results.begin() + query.offset;
        auto end = (query.limit > 0 && query.offset + query.limit < results.size()) 
                   ? start + query.limit 
                   : results.end();
        
        return std::vector<ThreadMetadata>(start, end);
    });
}

bool MessageThreadManager::can_view_thread(const std::string& thread_id, const std::string& user_id) {
    std::shared_lock<std::shared_mutex> thread_lock(threads_mutex_);
    auto thread_it = threads_.find(thread_id);
    if (thread_it == threads_.end()) {
        return false;
    }
    
    const auto& thread = *thread_it->second;
    
    // Public threads are viewable by anyone in the chat
    if (thread.visibility == ThreadVisibility::PUBLIC) {
        return true;
    }
    
    // For private/restricted threads, check participation
    return has_permission(thread_id, user_id, ParticipationLevel::OBSERVER);
}

bool MessageThreadManager::can_participate_in_thread(const std::string& thread_id, const std::string& user_id) {
    return has_permission(thread_id, user_id, ParticipationLevel::PARTICIPANT);
}

bool MessageThreadManager::can_moderate_thread(const std::string& thread_id, const std::string& user_id) {
    return has_permission(thread_id, user_id, ParticipationLevel::MODERATOR);
}

void MessageThreadManager::notify_thread_subscribers(const std::string& thread_id, const Json::Value& event) {
    std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
    
    auto thread_subs = subscriptions_.find(thread_id);
    if (thread_subs != subscriptions_.end()) {
        for (const auto& [user_id, callback] : thread_subs->second) {
            try {
                callback(event);
            } catch (const std::exception& e) {
                log_error("Error notifying subscriber " + user_id + ": " + e.what());
            }
        }
    }
}

void MessageThreadManager::update_thread_analytics(const std::string& thread_id, const std::string& event_type) {
    if (!analytics_enabled_.load()) {
        return;
    }
    
    auto analytics_it = thread_analytics_.find(thread_id);
    if (analytics_it != thread_analytics_.end()) {
        auto& analytics = analytics_it->second;
        
        if (event_type == "message_replied") {
            analytics.total_messages++;
        }
        // Add more event type handling as needed
    }
}

std::string MessageThreadManager::generate_thread_id() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    
    uint64_t high = dis(gen);
    uint64_t low = dis(gen);
    
    std::stringstream ss;
    ss << "thread_" << std::hex << high << low;
    return ss.str();
}

std::string MessageThreadManager::generate_reply_id() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    
    uint64_t high = dis(gen);
    uint64_t low = dis(gen);
    
    std::stringstream ss;
    ss << "reply_" << std::hex << high << low;
    return ss.str();
}

bool MessageThreadManager::has_permission(const std::string& thread_id, const std::string& user_id, ParticipationLevel required_level) {
    std::shared_lock<std::shared_mutex> lock(participants_mutex_);
    
    auto participants_it = thread_participants_.find(thread_id);
    if (participants_it == thread_participants_.end()) {
        return false;
    }
    
    for (const auto& participant : participants_it->second) {
        if (participant.user_id == user_id) {
            return static_cast<int>(participant.level) >= static_cast<int>(required_level);
        }
    }
    
    return false;
}

void MessageThreadManager::run_analytics_loop() {
    while (background_running_.load()) {
        try {
            if (analytics_enabled_.load()) {
                calculate_trending_scores();
            }
            
            std::this_thread::sleep_for(std::chrono::minutes(15));
            
        } catch (const std::exception& e) {
            log_error("Analytics loop error: " + std::string(e.what()));
            std::this_thread::sleep_for(std::chrono::minutes(5));
        }
    }
}

void MessageThreadManager::run_cleanup_loop() {
    while (background_running_.load()) {
        try {
            if (auto_archive_enabled_.load()) {
                archive_inactive_threads();
            }
            
            std::this_thread::sleep_for(std::chrono::hours(1));
            
        } catch (const std::exception& e) {
            log_error("Cleanup loop error: " + std::string(e.what()));
            std::this_thread::sleep_for(std::chrono::minutes(30));
        }
    }
}

void MessageThreadManager::archive_inactive_threads() {
    auto now = std::chrono::system_clock::now();
    
    std::unique_lock<std::shared_mutex> lock(threads_mutex_);
    
    for (auto& [thread_id, thread_ptr] : threads_) {
        auto& thread = *thread_ptr;
        
        if (thread.status == ThreadStatus::ACTIVE && thread.auto_archive) {
            auto inactive_duration = now - thread.last_activity;
            
            if (inactive_duration > thread.auto_archive_duration) {
                thread.status = ThreadStatus::ARCHIVED;
                thread.updated_at = now;
                
                log_info("Auto-archived inactive thread: " + thread_id);
                
                // Notify subscribers
                Json::Value event_data;
                event_data["type"] = "thread_archived";
                event_data["thread_id"] = thread_id;
                event_data["reason"] = "auto_archive";
                notify_thread_subscribers(thread_id, event_data);
            }
        }
    }
}

void MessageThreadManager::calculate_trending_scores() {
    // Implementation for calculating trending thread scores
    // This would analyze recent activity, engagement, etc.
}

void MessageThreadManager::log_info(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    
    std::cout << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") 
              << "] [Threading] [INFO] " << message << std::endl;
}

void MessageThreadManager::log_warning(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    
    std::cout << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") 
              << "] [Threading] [WARN] " << message << std::endl;
}

void MessageThreadManager::log_error(const std::string& message) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    
    std::cerr << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") 
              << "] [Threading] [ERROR] " << message << std::endl;
}

// ThreadUtils implementation
uint32_t ThreadUtils::calculate_thread_depth(const std::string& message_id,
                                            std::function<std::optional<MessageReply>(const std::string&)> get_parent) {
    uint32_t depth = 0;
    std::string current_id = message_id;
    std::unordered_set<std::string> visited; // Prevent infinite loops
    
    while (!current_id.empty() && visited.find(current_id) == visited.end()) {
        visited.insert(current_id);
        
        auto parent = get_parent(current_id);
        if (parent) {
            current_id = parent->parent_message_id;
            depth++;
        } else {
            break;
        }
        
        // Safety limit
        if (depth > 100) {
            break;
        }
    }
    
    return depth;
}

bool ThreadUtils::matches_search_query(const ThreadMetadata& thread, const ThreadSearchQuery& query) {
    std::string search_text = query.query_text;
    std::transform(search_text.begin(), search_text.end(), search_text.begin(), ::tolower);
    
    // Search in title
    std::string title = thread.title;
    std::transform(title.begin(), title.end(), title.begin(), ::tolower);
    if (title.find(search_text) != std::string::npos) {
        return true;
    }
    
    // Search in description
    std::string description = thread.description;
    std::transform(description.begin(), description.end(), description.begin(), ::tolower);
    if (description.find(search_text) != std::string::npos) {
        return true;
    }
    
    // Search in tags
    for (const auto& tag : thread.tags) {
        std::string tag_lower = tag;
        std::transform(tag_lower.begin(), tag_lower.end(), tag_lower.begin(), ::tolower);
        if (tag_lower.find(search_text) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

double ThreadUtils::calculate_relevance_score(const ThreadMetadata& thread, const std::string& query) {
    double score = 0.0;
    
    std::string query_lower = query;
    std::transform(query_lower.begin(), query_lower.end(), query_lower.begin(), ::tolower);
    
    // Title match (highest weight)
    std::string title_lower = thread.title;
    std::transform(title_lower.begin(), title_lower.end(), title_lower.begin(), ::tolower);
    if (title_lower.find(query_lower) != std::string::npos) {
        score += 10.0;
    }
    
    // Description match
    std::string desc_lower = thread.description;
    std::transform(desc_lower.begin(), desc_lower.end(), desc_lower.begin(), ::tolower);
    if (desc_lower.find(query_lower) != std::string::npos) {
        score += 5.0;
    }
    
    // Tag match
    for (const auto& tag : thread.tags) {
        std::string tag_lower = tag;
        std::transform(tag_lower.begin(), tag_lower.end(), tag_lower.begin(), ::tolower);
        if (tag_lower.find(query_lower) != std::string::npos) {
            score += 3.0;
        }
    }
    
    // Activity boost
    auto now = std::chrono::system_clock::now();
    auto time_since_activity = now - thread.last_activity;
    auto hours_since = std::chrono::duration_cast<std::chrono::hours>(time_since_activity).count();
    
    if (hours_since < 24) {
        score += 2.0;
    } else if (hours_since < 168) { // 1 week
        score += 1.0;
    }
    
    // Engagement boost
    score += thread.message_count * 0.1;
    score += thread.participant_count * 0.2;
    
    return score;
}

bool ThreadUtils::validate_thread_title(const std::string& title) {
    return !title.empty() && title.length() <= 100;
}

bool ThreadUtils::validate_thread_description(const std::string& description) {
    return description.length() <= 1000;
}

bool ThreadUtils::validate_participation_level(ParticipationLevel level, ParticipationLevel required) {
    return static_cast<int>(level) >= static_cast<int>(required);
}

} // namespace sonet::messaging::threading
