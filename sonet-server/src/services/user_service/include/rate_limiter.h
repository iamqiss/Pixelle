/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <chrono>
#include <unordered_map>
#include <memory>

namespace sonet::user {

/**
 * Rate Limiter - Because attackers are relentless
 * 
 * I implement multiple algorithms here because different scenarios need
 * different approaches. Token bucket for smooth rate limiting, sliding
 * window for precise control, and fixed window for simplicity.
 */
class RateLimiter {
public:
    enum class Algorithm {
        TOKEN_BUCKET,    // Smooth rate limiting with bursts
        SLIDING_WINDOW,  // Precise rate limiting
        FIXED_WINDOW     // Simple and efficient
    };
    
    RateLimiter(Algorithm algorithm = Algorithm::SLIDING_WINDOW);
    ~RateLimiter() = default;
    
    // Core rate limiting methods
    bool is_allowed(const std::string& key, int max_requests, std::chrono::seconds window);
    bool is_allowed_with_burst(const std::string& key, int max_requests, std::chrono::seconds window, int burst_size);
    
    // Specialized rate limits for authentication
    bool check_login_attempts(const std::string& identifier);
    bool check_registration_attempts(const std::string& ip_address);
    bool check_password_reset_attempts(const std::string& identifier);
    bool check_verification_attempts(const std::string& identifier);
    
    // Rate limit information
    struct RateLimitInfo {
        int requests_made;
        int requests_remaining;
        std::chrono::system_clock::time_point reset_time;
        bool is_blocked;
    };
    
    RateLimitInfo get_rate_limit_info(const std::string& key, int max_requests, std::chrono::seconds window);
    
    // Blocking and unblocking
    void block_key(const std::string& key, std::chrono::seconds duration);
    void unblock_key(const std::string& key);
    bool is_blocked(const std::string& key);
    
    // Cleanup and maintenance
    void cleanup_expired_entries();
    void reset_counter(const std::string& key);
    void clear_all_counters();

private:
    Algorithm algorithm_;
    
    // Token bucket implementation
    struct TokenBucket {
        int tokens;
        int capacity;
        int refill_rate;
        std::chrono::system_clock::time_point last_refill;
    };
    
    // Sliding window implementation
    struct SlidingWindow {
        std::vector<std::chrono::system_clock::time_point> requests;
        std::chrono::seconds window_size;
    };
    
    // Fixed window implementation  
    struct FixedWindow {
        int count;
        std::chrono::system_clock::time_point window_start;
        std::chrono::seconds window_size;
    };
    
    // Storage for different algorithms
    std::unordered_map<std::string, TokenBucket> token_buckets_;
    std::unordered_map<std::string, SlidingWindow> sliding_windows_;
    std::unordered_map<std::string, FixedWindow> fixed_windows_;
    
    // Blocked keys with expiration
    std::unordered_map<std::string, std::chrono::system_clock::time_point> blocked_keys_;
    
    // Algorithm-specific implementations
    bool check_token_bucket(const std::string& key, int max_requests, std::chrono::seconds window, int burst_size);
    bool check_sliding_window(const std::string& key, int max_requests, std::chrono::seconds window);
    bool check_fixed_window(const std::string& key, int max_requests, std::chrono::seconds window);
    
    // Helper methods
    void refill_bucket(TokenBucket& bucket, int refill_rate);
    void cleanup_sliding_window(SlidingWindow& window);
    bool is_window_expired(const FixedWindow& window);
    
    // Configuration for auth-specific limits
    struct AuthRateLimits {
        int login_max_attempts = 10;
        std::chrono::seconds login_window{std::chrono::hours(1)};
        
        int registration_max_attempts = 5;
        std::chrono::seconds registration_window{std::chrono::hours(1)};
        
        int password_reset_max_attempts = 3;
        std::chrono::seconds password_reset_window{std::chrono::hours(1)};
        
        int verification_max_attempts = 10;
        std::chrono::seconds verification_window{std::chrono::hours(1)};
    } auth_limits_;
};

} // namespace sonet::user
