/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <vector>
#include <optional>
#include <ctime>
#include <memory>

namespace sonet::user::models {

enum class SessionStatus {
    ACTIVE,
    EXPIRED,
    REVOKED,
    SUSPENDED
};

enum class DeviceType {
    DESKTOP,
    MOBILE,
    TABLET,
    TV,
    WATCH,
    OTHER
};

enum class SessionType {
    LOGIN,          // Regular login session
    API,            // API access session
    OAUTH,          // OAuth session
    TEMPORARY,      // Temporary session (password reset, etc.)
    REMEMBER_ME     // Long-term remember me session
};

struct DeviceInfo {
    std::string device_id;
    std::string device_name;        // User-friendly name
    DeviceType device_type;
    std::string operating_system;   // iOS, Android, Windows, macOS, Linux
    std::string browser;           // Chrome, Safari, Firefox, etc.
    std::string browser_version;
    std::string user_agent;
    std::string screen_resolution;
    std::string timezone;
    bool is_trusted;               // User has marked this device as trusted
    std::time_t first_seen;
    std::time_t last_seen;
    
    DeviceInfo() = default;
    DeviceInfo(const std::string& device_id, const std::string& device_name, DeviceType type);
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
    std::string to_json() const;
    void from_json(const std::string& json);
    std::string get_display_name() const;
    bool is_mobile_device() const;
    bool is_desktop_device() const;
};

struct LocationInfo {
    std::string ip_address;
    std::string country;
    std::string country_code;
    std::string region;
    std::string city;
    std::string noteal_code;
    double latitude;
    double longitude;
    std::string timezone;
    std::string isp;
    bool is_vpn;
    bool is_proxy;
    bool is_tor;
    std::time_t resolved_at;
    
    LocationInfo() = default;
    LocationInfo(const std::string& ip_address);
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
    std::string to_json() const;
    void from_json(const std::string& json);
    std::string get_display_location() const;
    bool is_suspicious() const;
    double distance_from(const LocationInfo& other) const;
};

struct SecurityFlags {
    bool requires_2fa;
    bool force_password_change;
    bool suspicious_activity;
    bool new_device_login;
    bool unusual_location;
    bool concurrent_sessions_exceeded;
    bool password_compromised;
    std::vector<std::string> security_alerts;
    std::time_t last_security_check;
    
    SecurityFlags() = default;
    
    void add_alert(const std::string& alert);
    void clear_alert(const std::string& alert);
    bool has_alerts() const;
    bool is_secure() const;
    std::string to_json() const;
    void from_json(const std::string& json);
};

class Session {
public:
    // Core session identifiers
    std::string session_id;
    std::string user_id;
    std::string access_token;
    std::string refresh_token;
    
    // Session metadata
    SessionType session_type;
    SessionStatus status;
    std::string session_name;       // User-provided name for the session
    
    // Device and location information
    DeviceInfo device_info;
    LocationInfo location_info;
    
    // Security information
    SecurityFlags security_flags;
    std::string csrf_token;
    std::vector<std::string> scopes;        // OAuth scopes or permissions
    std::vector<std::string> permissions;   // Additional permissions
    
    // Session timing
    std::time_t created_at;
    std::time_t updated_at;
    std::time_t expires_at;
    std::time_t last_activity_at;
    std::time_t last_token_refresh;
    
    // Activity tracking
    int request_count;
    int failed_request_count;
    std::string last_endpoint;
    std::vector<std::string> recent_endpoints;  // Last 10 endpoints accessed
    
    // Login context
    std::string login_method;       // password, oauth, sso, biometric
    std::string login_provider;     // For OAuth: google, facebook, etc.
    std::string referrer_url;       // URL that initiated login
    bool is_first_login;           // First time logging in from this device
    
    // Session limits and policies
    int max_idle_minutes;          // Auto-logout after inactivity
    int max_session_duration_hours; // Maximum session length
    bool allow_concurrent_sessions;
    bool remember_me;
    
    // Constructors
    Session();
    Session(const std::string& user_id, const DeviceInfo& device, const LocationInfo& location);
    Session(const std::string& user_id, SessionType type, const std::string& access_token);
    
    // Session lifecycle
    void activate();
    void suspend(const std::string& reason);
    void revoke(const std::string& reason);
    void expire();
    void refresh_tokens(const std::string& new_access_token, const std::string& new_refresh_token);
    void extend_expiration(int additional_hours);
    
    // Activity tracking
    void record_activity(const std::string& endpoint);
    void record_failed_request();
    void update_last_activity();
    bool is_activity_recent(int minutes = 30) const;
    
    // Security checks
    bool is_expired() const;
    bool is_active() const;
    bool is_idle(int max_idle_minutes = 30) const;
    bool should_expire() const;
    bool is_token_valid() const;
    bool requires_refresh() const;
    bool is_secure_session() const;
    
    // Device and location validation
    bool is_trusted_device() const;
    bool is_new_location() const;
    bool is_suspicious_location() const;
    void mark_device_as_trusted();
    void mark_location_as_suspicious();
    
    // Session comparison and analysis
    bool is_same_device(const DeviceInfo& other_device) const;
    bool is_same_location(const LocationInfo& other_location) const;
    double calculate_risk_score() const;
    std::vector<std::string> get_security_warnings() const;
    
    // Permission management
    void add_scope(const std::string& scope);
    void remove_scope(const std::string& scope);
    bool has_scope(const std::string& scope) const;
    void add_permission(const std::string& permission);
    void remove_permission(const std::string& permission);
    bool has_permission(const std::string& permission) const;
    
    // Session limits
    void set_idle_timeout(int minutes);
    void set_max_duration(int hours);
    void enable_remember_me();
    void disable_remember_me();
    bool should_auto_logout() const;
    
    // Session views for different contexts
    Session get_public_view() const;         // Minimal info for APIs
    Session get_security_view() const;       // For security dashboards
    Session get_admin_view() const;          // Full info for admins
    Session get_user_view() const;           // Safe info for user to see
    
    // Validation
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
    
    // Serialization
    std::string to_json() const;
    void from_json(const std::string& json);
    
    // Comparison operators
    bool operator==(const Session& other) const;
    bool operator!=(const Session& other) const;
    
    // Session analytics
    struct SessionMetrics {
        int total_requests;
        int failed_requests;
        double session_duration_hours;
        int unique_endpoints_accessed;
        double average_request_interval_seconds;
        std::time_t peak_activity_time;
        
        std::string to_json() const;
        void from_json(const std::string& json);
    };
    
    SessionMetrics get_metrics() const;
};

// Session management requests
struct SessionCreateRequest {
    std::string user_id;
    std::string login_method;
    std::string login_provider;
    DeviceInfo device_info;
    LocationInfo location_info;
    std::vector<std::string> requested_scopes;
    bool remember_me;
    std::string referrer_url;
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
};

struct SessionUpdateRequest {
    std::string session_id;
    std::optional<std::string> session_name;
    std::optional<int> max_idle_minutes;
    std::optional<int> max_session_duration_hours;
    std::optional<bool> allow_concurrent_sessions;
    std::optional<SecurityFlags> security_flags;
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
    std::vector<std::string> get_updated_fields() const;
};

struct SessionSearchRequest {
    std::optional<std::string> user_id;
    std::optional<SessionStatus> status;
    std::optional<SessionType> type;
    std::optional<DeviceType> device_type;
    std::optional<std::string> ip_address;
    std::optional<std::time_t> created_after;
    std::optional<std::time_t> created_before;
    std::optional<bool> active_only;
    std::optional<bool> suspicious_only;
    int limit = 50;
    int offset = 0;
    
    bool validate() const;
    std::vector<std::string> get_validation_errors() const;
};

// Session utilities
class SessionManager {
public:
    static std::string generate_session_id();
    static std::string generate_access_token();
    static std::string generate_refresh_token();
    static std::string generate_csrf_token();
    
    static bool is_valid_session_id(const std::string& session_id);
    static bool is_valid_token(const std::string& token);
    
    static std::time_t calculate_expiration(SessionType type, bool remember_me = false);
    static int get_max_concurrent_sessions(const std::string& user_id);
    
    static SessionType detect_session_type(const std::string& user_agent, 
                                          const std::vector<std::string>& scopes);
    static DeviceType detect_device_type(const std::string& user_agent);
    static std::string parse_browser_info(const std::string& user_agent);
    static std::string parse_os_info(const std::string& user_agent);
};

// Utility functions
std::string session_status_to_string(SessionStatus status);
SessionStatus string_to_session_status(const std::string& status);
std::string session_type_to_string(SessionType type);
SessionType string_to_session_type(const std::string& type);
std::string device_type_to_string(DeviceType type);
DeviceType string_to_device_type(const std::string& type);

} // namespace sonet::user::models