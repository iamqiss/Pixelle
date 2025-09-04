/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "enhanced_typing_manager.hpp"
#include <algorithm>
#include <random>
#include <sstream>
#include <iomanip>
#include <cmath>

namespace sonet::messaging::typing {

// EnhancedTypingIndicator implementation
Json::Value EnhancedTypingIndicator::to_json() const {
    Json::Value json;
    json["typing_id"] = typing_id;
    json["user_id"] = user_id;
    json["chat_id"] = chat_id;
    json["thread_id"] = thread_id;
    json["reply_to_message_id"] = reply_to_message_id;
    json["activity"] = static_cast<int>(activity);
    json["context"] = static_cast<int>(context);
    
    auto to_timestamp = [](const std::chrono::system_clock::time_point& tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    };
    
    json["started_at"] = static_cast<int64_t>(to_timestamp(started_at));
    json["last_update"] = static_cast<int64_t>(to_timestamp(last_update));
    json["expires_at"] = static_cast<int64_t>(to_timestamp(expires_at));
    
    json["device_type"] = device_type;
    json["platform"] = platform;
    json["is_dictating"] = is_dictating;
    json["estimated_length"] = estimated_length;
    json["typing_speed_wpm"] = typing_speed_wpm;
    json["is_draft_saved"] = is_draft_saved;
    json["in_foreground"] = in_foreground;
    json["has_focus"] = has_focus;
    json["is_mobile_keyboard"] = is_mobile_keyboard;
    
    return json;
}

EnhancedTypingIndicator EnhancedTypingIndicator::from_json(const Json::Value& json) {
    EnhancedTypingIndicator indicator;
    indicator.typing_id = json["typing_id"].asString();
    indicator.user_id = json["user_id"].asString();
    indicator.chat_id = json["chat_id"].asString();
    indicator.thread_id = json["thread_id"].asString();
    indicator.reply_to_message_id = json["reply_to_message_id"].asString();
    indicator.activity = static_cast<TypingActivity>(json["activity"].asInt());
    indicator.context = static_cast<TypingContext>(json["context"].asInt());
    
    auto from_timestamp = [](int64_t ms) {
        return std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
    };
    
    indicator.started_at = from_timestamp(json["started_at"].asInt64());
    indicator.last_update = from_timestamp(json["last_update"].asInt64());
    indicator.expires_at = from_timestamp(json["expires_at"].asInt64());
    
    indicator.device_type = json["device_type"].asString();
    indicator.platform = json["platform"].asString();
    indicator.is_dictating = json["is_dictating"].asBool();
    indicator.estimated_length = json["estimated_length"].asUInt();
    indicator.typing_speed_wpm = json["typing_speed_wpm"].asDouble();
    indicator.is_draft_saved = json["is_draft_saved"].asBool();
    indicator.in_foreground = json["in_foreground"].asBool();
    indicator.has_focus = json["has_focus"].asBool();
    indicator.is_mobile_keyboard = json["is_mobile_keyboard"].asBool();
    
    return indicator;
}

bool EnhancedTypingIndicator::is_expired() const {
    return std::chrono::system_clock::now() > expires_at;
}

std::chrono::milliseconds EnhancedTypingIndicator::time_since_start() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now() - started_at);
}

std::chrono::milliseconds EnhancedTypingIndicator::time_since_update() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now() - last_update);
}

// TypingPatterns implementation
Json::Value TypingPatterns::to_json() const {
    Json::Value json;
    json["user_id"] = user_id;
    
    auto to_timestamp = [](const std::chrono::system_clock::time_point& tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    };
    
    json["analysis_period_start"] = static_cast<int64_t>(to_timestamp(analysis_period_start));
    json["analysis_period_end"] = static_cast<int64_t>(to_timestamp(analysis_period_end));
    
    json["average_typing_speed_wpm"] = average_typing_speed_wpm;
    json["peak_typing_speed_wpm"] = peak_typing_speed_wpm;
    json["typing_consistency_score"] = typing_consistency_score;
    json["average_thinking_pause"] = static_cast<int64_t>(average_thinking_pause.count());
    json["longest_thinking_pause"] = static_cast<int64_t>(longest_thinking_pause.count());
    json["backspace_frequency"] = backspace_frequency;
    json["autocorrect_usage"] = autocorrect_usage;
    
    Json::Value activity_json;
    for (const auto& [activity, count] : activity_counts) {
        activity_json[std::to_string(static_cast<int>(activity))] = count;
    }
    json["activity_counts"] = activity_json;
    
    Json::Value device_json;
    for (const auto& [device, count] : device_usage) {
        device_json[device] = count;
    }
    json["device_usage"] = device_json;
    
    Json::Value hourly_json;
    for (const auto& [hour, count] : hourly_activity) {
        hourly_json[std::to_string(hour)] = count;
    }
    json["hourly_activity"] = hourly_json;
    
    json["average_message_length"] = average_message_length;
    json["draft_save_frequency"] = draft_save_frequency;
    json["completion_rate"] = completion_rate;
    
    return json;
}

void TypingPatterns::reset() {
    average_typing_speed_wpm = 0.0;
    peak_typing_speed_wpm = 0.0;
    typing_consistency_score = 0.0;
    average_thinking_pause = std::chrono::milliseconds::zero();
    longest_thinking_pause = std::chrono::milliseconds::zero();
    backspace_frequency = 0;
    autocorrect_usage = 0;
    activity_counts.clear();
    device_usage.clear();
    hourly_activity.clear();
    average_message_length = 0.0;
    draft_save_frequency = 0;
    completion_rate = 0.0;
}

// TypingSession implementation
Json::Value TypingSession::to_json() const {
    Json::Value json;
    json["session_id"] = session_id;
    json["user_id"] = user_id;
    json["chat_id"] = chat_id;
    json["thread_id"] = thread_id;
    json["completed_message"] = completed_message;
    
    auto to_timestamp = [](const std::chrono::system_clock::time_point& tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    };
    
    json["started_at"] = static_cast<int64_t>(to_timestamp(started_at));
    json["ended_at"] = static_cast<int64_t>(to_timestamp(ended_at));
    
    json["total_typing_time"] = static_cast<int64_t>(total_typing_time.count());
    json["total_pause_time"] = static_cast<int64_t>(total_pause_time.count());
    json["keystroke_count"] = keystroke_count;
    json["backspace_count"] = backspace_count;
    json["word_count"] = word_count;
    json["character_count"] = character_count;
    json["focus_changes"] = focus_changes;
    json["app_switches"] = app_switches;
    
    Json::Value timeline_json(Json::arrayValue);
    for (const auto& [timestamp, activity] : activity_timeline) {
        Json::Value entry;
        entry["timestamp"] = static_cast<int64_t>(to_timestamp(timestamp));
        entry["activity"] = static_cast<int>(activity);
        timeline_json.append(entry);
    }
    json["activity_timeline"] = timeline_json;
    
    return json;
}

void TypingSession::add_activity(TypingActivity activity) {
    activity_timeline.emplace_back(std::chrono::system_clock::now(), activity);
}

std::chrono::milliseconds TypingSession::get_session_duration() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(ended_at - started_at);
}

double TypingSession::calculate_effective_typing_speed() const {
    if (total_typing_time.count() == 0) return 0.0;
    
    double minutes = static_cast<double>(total_typing_time.count()) / 60000.0; // Convert to minutes
    return word_count / minutes;
}

// ChatTypingState implementation
Json::Value ChatTypingState::to_json() const {
    Json::Value json;
    json["chat_id"] = chat_id;
    json["total_active_typers"] = total_active_typers;
    json["typing_text_count"] = typing_text_count;
    json["recording_audio_count"] = recording_audio_count;
    json["uploading_file_count"] = uploading_file_count;
    
    auto to_timestamp = [](const std::chrono::system_clock::time_point& tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    };
    
    json["last_update"] = static_cast<int64_t>(to_timestamp(last_update));
    
    Json::Value typers_json;
    for (const auto& [user_id, indicator] : active_typers) {
        typers_json[user_id] = indicator.to_json();
    }
    json["active_typers"] = typers_json;
    
    Json::Value groups_json;
    for (const auto& [activity, users] : activity_groups) {
        Json::Value user_array(Json::arrayValue);
        for (const auto& user : users) {
            user_array.append(user);
        }
        groups_json[std::to_string(static_cast<int>(activity))] = user_array;
    }
    json["activity_groups"] = groups_json;
    
    return json;
}

void ChatTypingState::add_typer(const EnhancedTypingIndicator& indicator) {
    active_typers[indicator.user_id] = indicator;
    activity_groups[indicator.activity].insert(indicator.user_id);
    last_update = std::chrono::system_clock::now();
    
    // Update counters
    total_active_typers = static_cast<uint32_t>(active_typers.size());
    typing_text_count = static_cast<uint32_t>(activity_groups[TypingActivity::TYPING].size());
    recording_audio_count = static_cast<uint32_t>(activity_groups[TypingActivity::RECORDING_AUDIO].size());
    uploading_file_count = static_cast<uint32_t>(activity_groups[TypingActivity::UPLOADING_FILE].size());
}

void ChatTypingState::remove_typer(const std::string& user_id) {
    auto it = active_typers.find(user_id);
    if (it != active_typers.end()) {
        TypingActivity activity = it->second.activity;
        active_typers.erase(it);
        activity_groups[activity].erase(user_id);
        last_update = std::chrono::system_clock::now();
        
        // Update counters
        total_active_typers = static_cast<uint32_t>(active_typers.size());
        typing_text_count = static_cast<uint32_t>(activity_groups[TypingActivity::TYPING].size());
        recording_audio_count = static_cast<uint32_t>(activity_groups[TypingActivity::RECORDING_AUDIO].size());
        uploading_file_count = static_cast<uint32_t>(activity_groups[TypingActivity::UPLOADING_FILE].size());
    }
}

void ChatTypingState::cleanup_expired(const std::chrono::system_clock::time_point& now) {
    std::vector<std::string> expired_users;
    
    for (const auto& [user_id, indicator] : active_typers) {
        if (indicator.expires_at <= now) {
            expired_users.push_back(user_id);
        }
    }
    
    for (const auto& user_id : expired_users) {
        remove_typer(user_id);
    }
}

bool ChatTypingState::has_activity() const {
    return total_active_typers > 0;
}

// TypingNotificationConfig implementation
Json::Value TypingNotificationConfig::to_json() const {
    Json::Value json;
    json["user_id"] = user_id;
    json["enabled"] = enabled;
    json["show_detailed_activity"] = show_detailed_activity;
    json["show_typing_speed"] = show_typing_speed;
    json["show_device_type"] = show_device_type;
    json["group_similar_activities"] = group_similar_activities;
    json["notification_delay"] = static_cast<int64_t>(notification_delay.count());
    json["min_duration"] = static_cast<int64_t>(min_duration.count());
    
    Json::Value activities_json(Json::arrayValue);
    for (const auto& activity : visible_activities) {
        activities_json.append(static_cast<int>(activity));
    }
    json["visible_activities"] = activities_json;
    
    return json;
}

TypingNotificationConfig TypingNotificationConfig::from_json(const Json::Value& json) {
    TypingNotificationConfig config;
    config.user_id = json["user_id"].asString();
    config.enabled = json["enabled"].asBool();
    config.show_detailed_activity = json["show_detailed_activity"].asBool();
    config.show_typing_speed = json["show_typing_speed"].asBool();
    config.show_device_type = json["show_device_type"].asBool();
    config.group_similar_activities = json["group_similar_activities"].asBool();
    config.notification_delay = std::chrono::milliseconds(json["notification_delay"].asInt64());
    config.min_duration = std::chrono::milliseconds(json["min_duration"].asInt64());
    
    for (const auto& activity_json : json["visible_activities"]) {
        config.visible_activities.insert(static_cast<TypingActivity>(activity_json.asInt()));
    }
    
    return config;
}

TypingNotificationConfig TypingNotificationConfig::default_config() {
    TypingNotificationConfig config;
    config.enabled = true;
    config.show_detailed_activity = true;
    config.show_typing_speed = false;
    config.show_device_type = false;
    config.group_similar_activities = true;
    config.notification_delay = std::chrono::milliseconds(500);
    config.min_duration = std::chrono::milliseconds(1000);
    
    // Default visible activities
    config.visible_activities = {
        TypingActivity::TYPING,
        TypingActivity::RECORDING_AUDIO,
        TypingActivity::RECORDING_VIDEO,
        TypingActivity::UPLOADING_FILE
    };
    
    return config;
}

// EnhancedTypingManager implementation
EnhancedTypingManager::EnhancedTypingManager()
    : default_timeout_(std::chrono::seconds(10))
    , analytics_enabled_(true)
    , draft_auto_save_(true)
    , background_running_(true) {
    
    // Start background threads
    cleanup_thread_ = std::thread(&EnhancedTypingManager::run_cleanup_loop, this);
    analytics_thread_ = std::thread(&EnhancedTypingManager::run_analytics_loop, this);
    
    log_info("Enhanced typing manager initialized");
}

EnhancedTypingManager::~EnhancedTypingManager() {
    background_running_ = false;
    
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    if (analytics_thread_.joinable()) {
        analytics_thread_.join();
    }
    
    log_info("Enhanced typing manager destroyed");
}

std::future<bool> EnhancedTypingManager::start_typing(const std::string& user_id,
                                                     const std::string& chat_id,
                                                     TypingActivity activity,
                                                     TypingContext context,
                                                     const std::string& thread_id,
                                                     const std::string& reply_to_message_id) {
    return std::async(std::launch::async, [=]() -> bool {
        try {
            std::unique_lock<std::shared_mutex> lock(typers_mutex_);
            
            auto now = std::chrono::system_clock::now();
            auto expires_at = now + default_timeout_.load();
            
            EnhancedTypingIndicator indicator;
            indicator.typing_id = generate_typing_id();
            indicator.user_id = user_id;
            indicator.chat_id = chat_id;
            indicator.thread_id = thread_id;
            indicator.reply_to_message_id = reply_to_message_id;
            indicator.activity = activity;
            indicator.context = context;
            indicator.started_at = now;
            indicator.last_update = now;
            indicator.expires_at = expires_at;
            
            // Default device context
            indicator.device_type = "unknown";
            indicator.platform = "unknown";
            indicator.is_dictating = false;
            indicator.estimated_length = 0;
            indicator.typing_speed_wpm = 0.0;
            indicator.is_draft_saved = false;
            indicator.in_foreground = true;
            indicator.has_focus = true;
            indicator.is_mobile_keyboard = false;
            
            // Store indicator
            chat_typers_[chat_id][user_id] = indicator;
            
            // Update chat state
            if (chat_states_.find(chat_id) == chat_states_.end()) {
                chat_states_[chat_id].chat_id = chat_id;
            }
            chat_states_[chat_id].add_typer(indicator);
            
            lock.unlock();
            
            // Notify subscribers
            notify_chat_subscribers(chat_id, chat_states_[chat_id]);
            notify_user_subscribers(user_id, indicator);
            
            // Trigger callback
            if (typing_started_callback_) {
                typing_started_callback_(indicator);
            }
            
            log_info("Started typing for user " + user_id + " in chat " + chat_id);
            return true;
            
        } catch (const std::exception& e) {
            log_error("Failed to start typing: " + std::string(e.what()));
            return false;
        }
    });
}

std::future<bool> EnhancedTypingManager::update_typing(const std::string& user_id,
                                                      const std::string& chat_id,
                                                      TypingActivity activity,
                                                      uint32_t estimated_length,
                                                      double typing_speed) {
    return std::async(std::launch::async, [=]() -> bool {
        try {
            std::unique_lock<std::shared_mutex> lock(typers_mutex_);
            
            auto chat_it = chat_typers_.find(chat_id);
            if (chat_it == chat_typers_.end()) {
                return false;
            }
            
            auto user_it = chat_it->second.find(user_id);
            if (user_it == chat_it->second.end()) {
                return false;
            }
            
            auto now = std::chrono::system_clock::now();
            auto& indicator = user_it->second;
            
            // Update activity if changed
            bool activity_changed = (indicator.activity != activity);
            if (activity_changed) {
                // Remove from old activity group
                chat_states_[chat_id].activity_groups[indicator.activity].erase(user_id);
                indicator.activity = activity;
                // Add to new activity group
                chat_states_[chat_id].activity_groups[activity].insert(user_id);
            }
            
            indicator.last_update = now;
            indicator.expires_at = now + default_timeout_.load();
            indicator.estimated_length = estimated_length;
            indicator.typing_speed_wpm = typing_speed;
            
            // Update chat state
            chat_states_[chat_id].add_typer(indicator);
            
            lock.unlock();
            
            // Notify subscribers
            notify_chat_subscribers(chat_id, chat_states_[chat_id]);
            if (activity_changed) {
                notify_user_subscribers(user_id, indicator);
                if (activity_changed_callback_) {
                    activity_changed_callback_(indicator);
                }
            }
            
            return true;
            
        } catch (const std::exception& e) {
            log_error("Failed to update typing: " + std::string(e.what()));
            return false;
        }
    });
}

std::future<bool> EnhancedTypingManager::stop_typing(const std::string& user_id,
                                                     const std::string& chat_id,
                                                     bool message_sent) {
    return std::async(std::launch::async, [=]() -> bool {
        try {
            std::unique_lock<std::shared_mutex> lock(typers_mutex_);
            
            auto chat_it = chat_typers_.find(chat_id);
            if (chat_it == chat_typers_.end()) {
                return false;
            }
            
            auto user_it = chat_it->second.find(user_id);
            if (user_it == chat_it->second.end()) {
                return false;
            }
            
            // Remove from chat typers
            chat_it->second.erase(user_it);
            
            // Update chat state
            chat_states_[chat_id].remove_typer(user_id);
            
            lock.unlock();
            
            // Notify subscribers
            notify_chat_subscribers(chat_id, chat_states_[chat_id]);
            
            // Trigger callback
            if (typing_stopped_callback_) {
                typing_stopped_callback_(user_id, chat_id);
            }
            
            log_info("Stopped typing for user " + user_id + " in chat " + chat_id);
            return true;
            
        } catch (const std::exception& e) {
            log_error("Failed to stop typing: " + std::string(e.what()));
            return false;
        }
    });
}

std::future<bool> EnhancedTypingManager::pause_typing(const std::string& user_id,
                                                      const std::string& chat_id,
                                                      std::chrono::milliseconds pause_duration) {
    return std::async(std::launch::async, [=]() -> bool {
        try {
            return update_typing(user_id, chat_id, TypingActivity::THINKING).get();
        } catch (const std::exception& e) {
            log_error("Failed to pause typing: " + std::string(e.what()));
            return false;
        }
    });
}

std::future<bool> EnhancedTypingManager::start_thread_typing(const std::string& user_id,
                                                            const std::string& chat_id,
                                                            const std::string& thread_id,
                                                            TypingActivity activity) {
    return start_typing(user_id, chat_id, activity, TypingContext::THREAD, thread_id);
}

std::future<bool> EnhancedTypingManager::start_reply_typing(const std::string& user_id,
                                                           const std::string& chat_id,
                                                           const std::string& reply_to_message_id,
                                                           TypingActivity activity) {
    return start_typing(user_id, chat_id, activity, TypingContext::REPLY, "", reply_to_message_id);
}

std::future<std::vector<EnhancedTypingIndicator>> EnhancedTypingManager::get_chat_typers(const std::string& chat_id) {
    return std::async(std::launch::async, [=]() -> std::vector<EnhancedTypingIndicator> {
        std::shared_lock<std::shared_mutex> lock(typers_mutex_);
        
        std::vector<EnhancedTypingIndicator> indicators;
        auto chat_it = chat_typers_.find(chat_id);
        if (chat_it != chat_typers_.end()) {
            for (const auto& [user_id, indicator] : chat_it->second) {
                if (!indicator.is_expired()) {
                    indicators.push_back(indicator);
                }
            }
        }
        
        return indicators;
    });
}

std::future<std::vector<EnhancedTypingIndicator>> EnhancedTypingManager::get_thread_typers(const std::string& thread_id) {
    return std::async(std::launch::async, [=]() -> std::vector<EnhancedTypingIndicator> {
        std::shared_lock<std::shared_mutex> lock(typers_mutex_);
        
        std::vector<EnhancedTypingIndicator> indicators;
        for (const auto& [chat_id, typers] : chat_typers_) {
            for (const auto& [user_id, indicator] : typers) {
                if (indicator.thread_id == thread_id && !indicator.is_expired()) {
                    indicators.push_back(indicator);
                }
            }
        }
        
        return indicators;
    });
}

std::future<ChatTypingState> EnhancedTypingManager::get_chat_typing_state(const std::string& chat_id) {
    return std::async(std::launch::async, [=]() -> ChatTypingState {
        std::shared_lock<std::shared_mutex> lock(typers_mutex_);
        
        auto it = chat_states_.find(chat_id);
        if (it != chat_states_.end()) {
            auto state = it->second;
            state.cleanup_expired(std::chrono::system_clock::now());
            return state;
        }
        
        ChatTypingState empty_state;
        empty_state.chat_id = chat_id;
        return empty_state;
    });
}

std::future<std::optional<EnhancedTypingIndicator>> EnhancedTypingManager::get_user_typing_state(const std::string& user_id,
                                                                                                 const std::string& chat_id) {
    return std::async(std::launch::async, [=]() -> std::optional<EnhancedTypingIndicator> {
        std::shared_lock<std::shared_mutex> lock(typers_mutex_);
        
        auto chat_it = chat_typers_.find(chat_id);
        if (chat_it != chat_typers_.end()) {
            auto user_it = chat_it->second.find(user_id);
            if (user_it != chat_it->second.end() && !user_it->second.is_expired()) {
                return user_it->second;
            }
        }
        
        return std::nullopt;
    });
}

void EnhancedTypingManager::subscribe_to_chat_typing(const std::string& chat_id,
                                                    const std::string& subscriber_id,
                                                    std::function<void(const ChatTypingState&)> callback) {
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    chat_subscriptions_[chat_id][subscriber_id] = std::move(callback);
    log_info("Subscribed " + subscriber_id + " to chat typing for " + chat_id);
}

void EnhancedTypingManager::subscribe_to_user_typing(const std::string& user_id,
                                                    const std::string& subscriber_id,
                                                    std::function<void(const EnhancedTypingIndicator&)> callback) {
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    user_subscriptions_[user_id][subscriber_id] = std::move(callback);
    log_info("Subscribed " + subscriber_id + " to user typing for " + user_id);
}

void EnhancedTypingManager::unsubscribe_from_chat_typing(const std::string& chat_id,
                                                        const std::string& subscriber_id) {
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    auto chat_it = chat_subscriptions_.find(chat_id);
    if (chat_it != chat_subscriptions_.end()) {
        chat_it->second.erase(subscriber_id);
        if (chat_it->second.empty()) {
            chat_subscriptions_.erase(chat_it);
        }
    }
    log_info("Unsubscribed " + subscriber_id + " from chat typing for " + chat_id);
}

void EnhancedTypingManager::unsubscribe_from_user_typing(const std::string& user_id,
                                                        const std::string& subscriber_id) {
    std::unique_lock<std::shared_mutex> lock(subscriptions_mutex_);
    auto user_it = user_subscriptions_.find(user_id);
    if (user_it != user_subscriptions_.end()) {
        user_it->second.erase(subscriber_id);
        if (user_it->second.empty()) {
            user_subscriptions_.erase(user_it);
        }
    }
    log_info("Unsubscribed " + subscriber_id + " from user typing for " + user_id);
}

// Additional implementation methods continue...
std::string EnhancedTypingManager::generate_typing_id() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    
    std::stringstream ss;
    ss << "typ_";
    for (int i = 0; i < 32; ++i) {
        ss << std::hex << dis(gen);
    }
    return ss.str();
}

std::string EnhancedTypingManager::generate_session_id() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    
    std::stringstream ss;
    ss << "ses_";
    for (int i = 0; i < 32; ++i) {
        ss << std::hex << dis(gen);
    }
    return ss.str();
}

void EnhancedTypingManager::notify_chat_subscribers(const std::string& chat_id, const ChatTypingState& state) {
    std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
    auto it = chat_subscriptions_.find(chat_id);
    if (it != chat_subscriptions_.end()) {
        for (const auto& [subscriber_id, callback] : it->second) {
            try {
                callback(state);
            } catch (const std::exception& e) {
                log_error("Error notifying chat subscriber " + subscriber_id + ": " + e.what());
            }
        }
    }
}

void EnhancedTypingManager::notify_user_subscribers(const std::string& user_id, const EnhancedTypingIndicator& indicator) {
    std::shared_lock<std::shared_mutex> lock(subscriptions_mutex_);
    auto it = user_subscriptions_.find(user_id);
    if (it != user_subscriptions_.end()) {
        for (const auto& [subscriber_id, callback] : it->second) {
            try {
                if (should_notify_subscriber(subscriber_id, indicator)) {
                    callback(indicator);
                }
            } catch (const std::exception& e) {
                log_error("Error notifying user subscriber " + subscriber_id + ": " + e.what());
            }
        }
    }
}

bool EnhancedTypingManager::should_notify_subscriber(const std::string& subscriber_id,
                                                    const EnhancedTypingIndicator& indicator) {
    // Check notification config for subscriber
    auto config_it = notification_configs_.find(subscriber_id);
    if (config_it == notification_configs_.end()) {
        return true; // Default to notify
    }
    
    const auto& config = config_it->second;
    if (!config.enabled) {
        return false;
    }
    
    // Check if activity is visible
    if (config.visible_activities.find(indicator.activity) == config.visible_activities.end()) {
        return false;
    }
    
    // Check notification delay
    if (indicator.time_since_start() < config.notification_delay) {
        return false;
    }
    
    return true;
}

void EnhancedTypingManager::cleanup_expired_indicators() {
    std::unique_lock<std::shared_mutex> lock(typers_mutex_);
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::pair<std::string, std::string>> expired_typers;
    
    for (auto& [chat_id, typers] : chat_typers_) {
        for (auto& [user_id, indicator] : typers) {
            if (indicator.expires_at <= now) {
                expired_typers.emplace_back(chat_id, user_id);
            }
        }
    }
    
    for (const auto& [chat_id, user_id] : expired_typers) {
        chat_typers_[chat_id].erase(user_id);
        chat_states_[chat_id].remove_typer(user_id);
        
        // Notify subscribers
        notify_chat_subscribers(chat_id, chat_states_[chat_id]);
        if (typing_stopped_callback_) {
            typing_stopped_callback_(user_id, chat_id);
        }
    }
    
    // Clean up empty chat states
    for (auto it = chat_states_.begin(); it != chat_states_.end();) {
        if (!it->second.has_activity()) {
            it = chat_states_.erase(it);
        } else {
            ++it;
        }
    }
}

void EnhancedTypingManager::run_cleanup_loop() {
    while (background_running_) {
        try {
            cleanup_expired_indicators();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        } catch (const std::exception& e) {
            log_error("Error in cleanup loop: " + std::string(e.what()));
        }
    }
}

void EnhancedTypingManager::run_analytics_loop() {
    while (background_running_) {
        try {
            if (analytics_enabled_) {
                // Update typing patterns periodically
                // This would integrate with analytics collection
            }
            std::this_thread::sleep_for(std::chrono::seconds(30));
        } catch (const std::exception& e) {
            log_error("Error in analytics loop: " + std::string(e.what()));
        }
    }
}

void EnhancedTypingManager::log_info(const std::string& message) {
    // Integrate with logging system
    std::cout << "[INFO] EnhancedTypingManager: " << message << std::endl;
}

void EnhancedTypingManager::log_warning(const std::string& message) {
    // Integrate with logging system
    std::cout << "[WARN] EnhancedTypingManager: " << message << std::endl;
}

void EnhancedTypingManager::log_error(const std::string& message) {
    // Integrate with logging system
    std::cerr << "[ERROR] EnhancedTypingManager: " << message << std::endl;
}

// TypingEvent implementation
Json::Value TypingEvent::to_json() const {
    Json::Value json;
    json["type"] = static_cast<int>(type);
    json["user_id"] = user_id;
    json["chat_id"] = chat_id;
    json["thread_id"] = thread_id;
    json["activity"] = static_cast<int>(activity);
    json["data"] = data;
    json["event_id"] = event_id;
    
    auto to_timestamp = [](const std::chrono::system_clock::time_point& tp) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    };
    
    json["timestamp"] = static_cast<int64_t>(to_timestamp(timestamp));
    
    return json;
}

TypingEvent TypingEvent::from_json(const Json::Value& json) {
    TypingEvent event;
    event.type = static_cast<TypingEventType>(json["type"].asInt());
    event.user_id = json["user_id"].asString();
    event.chat_id = json["chat_id"].asString();
    event.thread_id = json["thread_id"].asString();
    event.activity = static_cast<TypingActivity>(json["activity"].asInt());
    event.data = json["data"];
    event.event_id = json["event_id"].asString();
    
    auto from_timestamp = [](int64_t ms) {
        return std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
    };
    
    event.timestamp = from_timestamp(json["timestamp"].asInt64());
    
    return event;
}

// TypingUtils implementation
TypingActivity TypingUtils::detect_activity_from_input(const std::string& input_type) {
    if (input_type == "voice" || input_type == "audio") return TypingActivity::RECORDING_AUDIO;
    if (input_type == "video") return TypingActivity::RECORDING_VIDEO;
    if (input_type == "file" || input_type == "upload") return TypingActivity::UPLOADING_FILE;
    if (input_type == "edit") return TypingActivity::EDITING;
    return TypingActivity::TYPING;
}

bool TypingUtils::is_voice_activity(TypingActivity activity) {
    return activity == TypingActivity::RECORDING_AUDIO || activity == TypingActivity::RECORDING_VIDEO;
}

bool TypingUtils::is_file_activity(TypingActivity activity) {
    return activity == TypingActivity::UPLOADING_FILE;
}

double TypingUtils::calculate_typing_speed_wpm(uint32_t characters, std::chrono::milliseconds duration) {
    if (duration.count() == 0) return 0.0;
    
    double minutes = static_cast<double>(duration.count()) / 60000.0;
    double words = static_cast<double>(characters) / 5.0; // Average 5 characters per word
    return words / minutes;
}

std::string TypingUtils::get_speed_description(double wpm) {
    if (wpm < 20) return "slow";
    if (wpm < 40) return "normal";
    if (wpm < 60) return "fast";
    return "very fast";
}

std::string TypingUtils::format_typing_notification(const std::vector<EnhancedTypingIndicator>& indicators,
                                                   const TypingNotificationConfig& config) {
    if (indicators.empty()) return "";
    
    if (config.group_similar_activities) {
        std::unordered_map<TypingActivity, std::vector<std::string>> grouped;
        for (const auto& indicator : indicators) {
            grouped[indicator.activity].push_back(indicator.user_id);
        }
        
        std::vector<std::string> parts;
        for (const auto& [activity, users] : grouped) {
            std::string activity_desc = get_activity_description(activity);
            if (users.size() == 1) {
                parts.push_back(users[0] + " is " + activity_desc);
            } else {
                parts.push_back(std::to_string(users.size()) + " people are " + activity_desc);
            }
        }
        
        if (parts.size() == 1) return parts[0];
        if (parts.size() == 2) return parts[0] + " and " + parts[1];
        
        std::string result = parts[0];
        for (size_t i = 1; i < parts.size() - 1; ++i) {
            result += ", " + parts[i];
        }
        result += ", and " + parts.back();
        return result;
    }
    
    // Individual notifications
    if (indicators.size() == 1) {
        const auto& indicator = indicators[0];
        std::string msg = indicator.user_id + " is " + get_activity_description(indicator.activity);
        
        if (config.show_typing_speed && indicator.typing_speed_wpm > 0) {
            msg += " (" + get_speed_description(indicator.typing_speed_wpm) + ")";
        }
        
        if (config.show_device_type && !indicator.device_type.empty()) {
            msg += " " + get_device_icon(indicator.device_type);
        }
        
        return msg;
    }
    
    return std::to_string(indicators.size()) + " people are active";
}

std::string TypingUtils::get_activity_description(TypingActivity activity) {
    switch (activity) {
        case TypingActivity::TYPING: return "typing";
        case TypingActivity::RECORDING_AUDIO: return "recording audio";
        case TypingActivity::RECORDING_VIDEO: return "recording video";
        case TypingActivity::UPLOADING_FILE: return "uploading file";
        case TypingActivity::THINKING: return "thinking";
        case TypingActivity::EDITING: return "editing";
        default: return "active";
    }
}

std::string TypingUtils::get_device_icon(const std::string& device_type) {
    if (device_type == "mobile") return "📱";
    if (device_type == "desktop") return "💻";
    if (device_type == "tablet") return "📟";
    return "🖥️";
}

bool TypingUtils::validate_typing_context(TypingContext context, 
                                         const std::string& thread_id, 
                                         const std::string& reply_id) {
    switch (context) {
        case TypingContext::THREAD:
            return !thread_id.empty();
        case TypingContext::REPLY:
            return !reply_id.empty();
        case TypingContext::MAIN_CHAT:
        case TypingContext::DIRECT_MESSAGE:
            return true;
        default:
            return false;
    }
}

bool TypingUtils::is_reasonable_typing_speed(double wpm) {
    return wpm >= 0.0 && wpm <= 200.0; // Reasonable human typing speed range
}

} // namespace sonet::messaging::typing
