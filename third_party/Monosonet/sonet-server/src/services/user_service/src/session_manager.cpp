/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "session_manager.h"
#include "security_utils.h"
#include <random>
#include <algorithm>
#include <spdlog/spdlog.h>
#include <openssl/rand.h>

namespace sonet::user {

SessionManager::SessionManager() {
    spdlog::info("Session manager initialized - ready to track user sessions like a security expert");
}

std::string SessionManager::create_session(const User& user, const std::string& device_info, const std::string& ip_address) {
    std::unique_lock<std::shared_mutex> sessions_lock(sessions_mutex_);
    std::unique_lock<std::shared_mutex> user_sessions_lock(user_sessions_mutex_);
    
    // Check if user has too many active sessions
    auto user_session_it = user_sessions_.find(user.user_id);
    if (user_session_it != user_sessions_.end() && 
        user_session_it->second.size() >= max_sessions_per_user_) {
        // Remove the oldest session to make room
        enforce_session_limit(user.user_id);
    }
    
    // Create new session
    UserSession session;
    session.session_id = generate_session_id();
    session.user_id = user.user_id;
    session.device_id = generate_device_fingerprint(device_info, ip_address);
    session.device_name = extract_device_name(device_info);
    session.ip_address = ip_address;
    session.user_agent = device_info;
    session.type = detect_session_type(device_info);
    session.created_at = std::chrono::system_clock::now();
    session.last_activity = session.created_at;
    session.expires_at = session.created_at + session_timeout_;
    session.is_active = true;
    session.location_info = get_location_info(ip_address);
    
    // Security checks - I'm paranoid about suspicious activity
    session.is_suspicious = is_location_suspicious(user.user_id, ip_address) ||
                           !is_device_trusted(user.user_id, session.device_id);
    
    // Store the session
    sessions_[session.session_id] = session;
    add_session_to_indices(session);
    
    if (session.is_suspicious) {
        spdlog::warn("Created suspicious session for user {}: {}", user.user_id, session.session_id);
    } else {
        spdlog::info("Created session for user {}: {}", user.user_id, session.session_id);
    }
    
    return session.session_id;
}

bool SessionManager::validate_session(const std::string& session_id) {
    std::shared_lock<std::shared_mutex> lock(sessions_mutex_);
    
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) {
        return false;
    }
    
    const auto& session = it->second;
    
    // Check if session is expired
    if (!session.is_active || std::chrono::system_clock::now() > session.expires_at) {
        // Mark as expired (we'll clean it up later)
        return false;
    }
    
    return true;
}

bool SessionManager::update_session_activity(const std::string& session_id) {
    std::unique_lock<std::shared_mutex> lock(sessions_mutex_);
    
    auto it = sessions_.find(session_id);
    if (it == sessions_.end() || !it->second.is_active) {
        return false;
    }
    
    // Update last activity and extend expiration
    it->second.last_activity = std::chrono::system_clock::now();
    it->second.expires_at = it->second.last_activity + session_timeout_;
    
    return true;
}

bool SessionManager::terminate_session(const std::string& session_id) {
    std::unique_lock<std::shared_mutex> sessions_lock(sessions_mutex_);
    std::unique_lock<std::shared_mutex> user_sessions_lock(user_sessions_mutex_);
    
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) {
        return false;
    }
    
    // Mark as inactive and remove from indices
    it->second.is_active = false;
    remove_session_from_indices(session_id);
    
    spdlog::info("Terminated session: {}", session_id);
    return true;
}

std::vector<UserSession> SessionManager::get_user_sessions(const std::string& user_id) {
    std::shared_lock<std::shared_mutex> sessions_lock(sessions_mutex_);
    std::shared_lock<std::shared_mutex> user_sessions_lock(user_sessions_mutex_);
    
    std::vector<UserSession> user_sessions;
    
    auto user_it = user_sessions_.find(user_id);
    if (user_it != user_sessions_.end()) {
        for (const auto& session_id : user_it->second) {
            auto session_it = sessions_.find(session_id);
            if (session_it != sessions_.end() && session_it->second.is_active) {
                user_sessions.push_back(session_it->second);
            }
        }
    }
    
    return user_sessions;
}

bool SessionManager::terminate_all_user_sessions(const std::string& user_id) {
    std::unique_lock<std::shared_mutex> sessions_lock(sessions_mutex_);
    std::unique_lock<std::shared_mutex> user_sessions_lock(user_sessions_mutex_);
    
    auto user_it = user_sessions_.find(user_id);
    if (user_it == user_sessions_.end()) {
        return false;
    }
    
    // Terminate all sessions for this user
    for (const auto& session_id : user_it->second) {
        auto session_it = sessions_.find(session_id);
        if (session_it != sessions_.end()) {
            session_it->second.is_active = false;
        }
    }
    
    // Clear the user's session list
    user_it->second.clear();
    
    spdlog::info("Terminated all sessions for user: {}", user_id);
    return true;
}

std::optional<UserSession> SessionManager::get_session(const std::string& session_id) {
    std::shared_lock<std::shared_mutex> lock(sessions_mutex_);
    
    auto it = sessions_.find(session_id);
    if (it != sessions_.end() && it->second.is_active) {
        return it->second;
    }
    
    return std::nullopt;
}

std::optional<std::string> SessionManager::get_user_id_from_session(const std::string& session_id) {
    std::shared_lock<std::shared_mutex> lock(sessions_mutex_);
    
    auto it = sessions_.find(session_id);
    if (it != sessions_.end() && it->second.is_active) {
        return it->second.user_id;
    }
    
    return std::nullopt;
}

bool SessionManager::mark_session_suspicious(const std::string& session_id, const std::string& reason) {
    std::unique_lock<std::shared_mutex> lock(sessions_mutex_);
    
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) {
        return false;
    }
    
    it->second.is_suspicious = true;
    spdlog::warn("Marked session {} as suspicious: {}", session_id, reason);
    
    return true;
}

std::vector<UserSession> SessionManager::get_suspicious_sessions() {
    std::shared_lock<std::shared_mutex> lock(sessions_mutex_);
    
    std::vector<UserSession> suspicious_sessions;
    
    for (const auto& [session_id, session] : sessions_) {
        if (session.is_active && session.is_suspicious) {
            suspicious_sessions.push_back(session);
        }
    }
    
    return suspicious_sessions;
}

bool SessionManager::is_device_trusted(const std::string& user_id, const std::string& device_fingerprint) {
    std::shared_lock<std::shared_mutex> lock(trusted_devices_mutex_);
    
    auto user_it = trusted_devices_.find(user_id);
    if (user_it != trusted_devices_.end()) {
        return user_it->second.count(device_fingerprint) > 0;
    }
    
    return false;
}

void SessionManager::mark_device_as_trusted(const std::string& user_id, const std::string& device_fingerprint) {
    std::unique_lock<std::shared_mutex> lock(trusted_devices_mutex_);
    
    trusted_devices_[user_id].insert(device_fingerprint);
    spdlog::info("Marked device as trusted for user {}", user_id);
}

void SessionManager::cleanup_expired_sessions() {
    std::unique_lock<std::shared_mutex> sessions_lock(sessions_mutex_);
    std::unique_lock<std::shared_mutex> user_sessions_lock(user_sessions_mutex_);
    
    auto now = std::chrono::system_clock::now();
    std::vector<std::string> expired_sessions;
    
    // Find expired sessions
    for (const auto& [session_id, session] : sessions_) {
        if (should_cleanup_session(session)) {
            expired_sessions.push_back(session_id);
        }
    }
    
    // Remove expired sessions
    for (const auto& session_id : expired_sessions) {
        remove_expired_session(session_id);
    }
    
    if (!expired_sessions.empty()) {
        spdlog::info("Cleaned up {} expired sessions", expired_sessions.size());
    }
}

// Private helper methods

std::string SessionManager::generate_session_id() {
    // Generate a cryptographically secure session ID
    return SecurityUtils::generate_secure_random_string(32);
}

std::string SessionManager::generate_device_fingerprint(const std::string& user_agent, const std::string& ip_address) {
    // Create a device fingerprint from user agent and other factors
    return SecurityUtils::hash_string(user_agent + ip_address);
}

SessionType SessionManager::detect_session_type(const std::string& user_agent) {
    // Basic user agent detection - in production this would be more sophisticated
    std::string lower_ua = user_agent;
    std::transform(lower_ua.begin(), lower_ua.end(), lower_ua.begin(), ::tolower);
    
    if (lower_ua.find("mobile") != std::string::npos || 
        lower_ua.find("android") != std::string::npos ||
        lower_ua.find("iphone") != std::string::npos) {
        return SessionType::MOBILE;
    }
    
    if (lower_ua.find("api") != std::string::npos || 
        lower_ua.find("curl") != std::string::npos ||
        lower_ua.find("bot") != std::string::npos) {
        return SessionType::API;
    }
    
    return SessionType::WEB;
}

std::string SessionManager::extract_device_name(const std::string& user_agent) {
    // Extract device name from user agent - simplified implementation
    if (user_agent.find("iPhone") != std::string::npos) return "iPhone";
    if (user_agent.find("Android") != std::string::npos) return "Android Device";
    if (user_agent.find("Chrome") != std::string::npos) return "Chrome Browser";
    if (user_agent.find("Firefox") != std::string::npos) return "Firefox Browser";
    if (user_agent.find("Safari") != std::string::npos) return "Safari Browser";
    
    return "Unknown Device";
}

void SessionManager::add_session_to_indices(const UserSession& session) {
    // Add to user sessions index
    user_sessions_[session.user_id].push_back(session.session_id);
    
    // Add to IP sessions index
    ip_sessions_[session.ip_address].push_back(session.session_id);
}

void SessionManager::remove_session_from_indices(const std::string& session_id) {
    auto session_it = sessions_.find(session_id);
    if (session_it == sessions_.end()) return;
    
    const auto& session = session_it->second;
    
    // Remove from user sessions
    auto user_it = user_sessions_.find(session.user_id);
    if (user_it != user_sessions_.end()) {
        auto& session_list = user_it->second;
        session_list.erase(
            std::remove(session_list.begin(), session_list.end(), session_id),
            session_list.end()
        );
    }
    
    // Remove from IP sessions
    auto ip_it = ip_sessions_.find(session.ip_address);
    if (ip_it != ip_sessions_.end()) {
        auto& session_list = ip_it->second;
        session_list.erase(
            std::remove(session_list.begin(), session_list.end(), session_id),
            session_list.end()
        );
    }
}

void SessionManager::enforce_session_limit(const std::string& user_id) {
    auto user_it = user_sessions_.find(user_id);
    if (user_it == user_sessions_.end()) return;
    
    auto& session_ids = user_it->second;
    
    // Sort sessions by creation time (oldest first)
    std::sort(session_ids.begin(), session_ids.end(), 
        [this](const std::string& a, const std::string& b) {
            auto session_a = sessions_.find(a);
            auto session_b = sessions_.find(b);
            
            if (session_a == sessions_.end()) return true;
            if (session_b == sessions_.end()) return false;
            
            return session_a->second.created_at < session_b->second.created_at;
        });
    
    // Remove oldest sessions until we're under the limit
    while (session_ids.size() >= max_sessions_per_user_) {
        std::string oldest_session = session_ids.front();
        session_ids.erase(session_ids.begin());
        
        auto session_it = sessions_.find(oldest_session);
        if (session_it != sessions_.end()) {
            session_it->second.is_active = false;
            spdlog::info("Terminated oldest session {} for user {} due to session limit", 
                        oldest_session, user_id);
        }
    }
}

bool SessionManager::is_location_suspicious(const std::string& user_id, const std::string& ip_address) {
    // In production, this would check for impossible travel scenarios
    // For now, just check if it's a known bad IP range
    
    // Block obviously suspicious IPs
    if (ip_address.starts_with("10.0.0.") || 
        ip_address.starts_with("192.168.") ||
        ip_address == "127.0.0.1") {
        return false; // These are internal/local IPs
    }
    
    // In production: check against geo-location databases and user's recent locations
    return false;
}

std::string SessionManager::get_location_info(const std::string& ip_address) {
    // In production, this would use a geo-location service
    // For now, return a placeholder
    if (ip_address.starts_with("192.168.") || ip_address == "127.0.0.1") {
        return "Local Network";
    }
    
    return "Unknown Location";
}

bool SessionManager::should_cleanup_session(const UserSession& session) {
    auto now = std::chrono::system_clock::now();
    return !session.is_active || now > session.expires_at;
}

void SessionManager::remove_expired_session(const std::string& session_id) {
    remove_session_from_indices(session_id);
    sessions_.erase(session_id);
}

size_t SessionManager::get_active_session_count() {
    std::shared_lock<std::shared_mutex> lock(sessions_mutex_);
    
    size_t count = 0;
    for (const auto& [session_id, session] : sessions_) {
        if (session.is_active && std::chrono::system_clock::now() <= session.expires_at) {
            count++;
        }
    }
    
    return count;
}

} // namespace sonet::user
