/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include "user_types.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>

namespace sonet::user {

/**
 * Session Manager - Keeping track of who's online and what they're doing
 * 
 * I built this to handle millions of concurrent sessions efficiently.
 * In production, the actual storage goes to Redis for scalability,
 * but I want the interface to be clean and simple.
 */
class SessionManager {
public:
    SessionManager();
    ~SessionManager() = default;

    // Core session operations
    std::string create_session(const User& user, const std::string& device_info, const std::string& ip_address);
    bool validate_session(const std::string& session_id);
    bool update_session_activity(const std::string& session_id);
    bool terminate_session(const std::string& session_id);
    
    // Multi-session management - because people have multiple devices
    std::vector<UserSession> get_user_sessions(const std::string& user_id);
    bool terminate_all_user_sessions(const std::string& user_id);
    bool terminate_other_sessions(const std::string& user_id, const std::string& current_session_id);
    
    // Session information and validation
    std::optional<UserSession> get_session(const std::string& session_id);
    std::optional<std::string> get_user_id_from_session(const std::string& session_id);
    bool is_session_expired(const std::string& session_id);
    
    // Security features
    bool mark_session_suspicious(const std::string& session_id, const std::string& reason);
    std::vector<UserSession> get_suspicious_sessions();
    bool is_device_trusted(const std::string& user_id, const std::string& device_fingerprint);
    void mark_device_as_trusted(const std::string& user_id, const std::string& device_fingerprint);
    
    // Session analytics and monitoring
    size_t get_active_session_count();
    size_t get_user_session_count(const std::string& user_id);
    std::vector<UserSession> get_sessions_by_ip(const std::string& ip_address);
    
    // Session configuration
    void set_session_timeout(std::chrono::seconds timeout);
    void set_max_sessions_per_user(size_t max_sessions);
    
    // Cleanup and maintenance
    void cleanup_expired_sessions();
    void force_cleanup_user_sessions(const std::string& user_id);

private:
    // Session storage - in production this would be Redis
    std::unordered_map<std::string, UserSession> sessions_;
    std::unordered_map<std::string, std::vector<std::string>> user_sessions_; // user_id -> session_ids
    std::unordered_map<std::string, std::vector<std::string>> ip_sessions_;   // ip -> session_ids
    
    // Trusted devices per user
    std::unordered_map<std::string, std::set<std::string>> trusted_devices_; // user_id -> device_fingerprints
    
    // Configuration
    std::chrono::seconds session_timeout_{std::chrono::hours(24)}; // 24 hours default
    size_t max_sessions_per_user_{10}; // Maximum concurrent sessions per user
    
    // Thread safety - because concurrent access is real
    mutable std::shared_mutex sessions_mutex_;
    mutable std::shared_mutex user_sessions_mutex_;
    mutable std::shared_mutex trusted_devices_mutex_;
    
    // Helper methods
    std::string generate_session_id();
    std::string generate_device_fingerprint(const std::string& user_agent, const std::string& ip_address);
    SessionType detect_session_type(const std::string& user_agent);
    std::string extract_device_name(const std::string& user_agent);
    
    // Session lifecycle helpers
    void add_session_to_indices(const UserSession& session);
    void remove_session_from_indices(const std::string& session_id);
    void enforce_session_limit(const std::string& user_id);
    
    // Security helpers
    bool is_location_suspicious(const std::string& user_id, const std::string& ip_address);
    std::string get_location_info(const std::string& ip_address);
    
    // Maintenance helpers
    bool should_cleanup_session(const UserSession& session);
    void remove_expired_session(const std::string& session_id);
};

} // namespace sonet::user
