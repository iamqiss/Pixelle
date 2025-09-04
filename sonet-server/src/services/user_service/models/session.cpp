/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "session.h"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <regex>
#include <algorithm>
#include <random>
#include <chrono>
#include <cmath>

namespace sonet::user::models {

// DeviceInfo implementation
DeviceInfo::DeviceInfo(const std::string& device_id, const std::string& device_name, DeviceType type)
    : device_id(device_id), device_name(device_name), device_type(type), is_trusted(false) {
    auto now = std::time(nullptr);
    first_seen = now;
    last_seen = now;
}

bool DeviceInfo::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> DeviceInfo::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (device_id.empty()) {
        errors.push_back("Device ID is required");
    }
    
    if (device_name.empty()) {
        errors.push_back("Device name is required");
    }
    
    if (user_agent.empty()) {
        errors.push_back("User agent is required");
    }
    
    return errors;
}

std::string DeviceInfo::to_json() const {
    nlohmann::json j;
    j["device_id"] = device_id;
    j["device_name"] = device_name;
    j["device_type"] = static_cast<int>(device_type);
    j["operating_system"] = operating_system;
    j["browser"] = browser;
    j["browser_version"] = browser_version;
    j["user_agent"] = user_agent;
    j["screen_resolution"] = screen_resolution;
    j["timezone"] = timezone;
    j["is_trusted"] = is_trusted;
    j["first_seen"] = first_seen;
    j["last_seen"] = last_seen;
    return j.dump();
}

void DeviceInfo::from_json(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        device_id = j.value("device_id", "");
        device_name = j.value("device_name", "");
        device_type = static_cast<DeviceType>(j.value("device_type", 0));
        operating_system = j.value("operating_system", "");
        browser = j.value("browser", "");
        browser_version = j.value("browser_version", "");
        user_agent = j.value("user_agent", "");
        screen_resolution = j.value("screen_resolution", "");
        timezone = j.value("timezone", "");
        is_trusted = j.value("is_trusted", false);
        first_seen = j.value("first_seen", 0);
        last_seen = j.value("last_seen", 0);
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse device info JSON: {}", e.what());
    }
}

std::string DeviceInfo::get_display_name() const {
    if (!device_name.empty()) {
        return device_name;
    }
    
    std::string display = browser;
    if (!browser_version.empty()) {
        display += " " + browser_version;
    }
    
    if (!operating_system.empty()) {
        display += " on " + operating_system;
    }
    
    return display.empty() ? "Unknown Device" : display;
}

bool DeviceInfo::is_mobile_device() const {
    return device_type == DeviceType::MOBILE || device_type == DeviceType::TABLET;
}

bool DeviceInfo::is_desktop_device() const {
    return device_type == DeviceType::DESKTOP;
}

// LocationInfo implementation
LocationInfo::LocationInfo(const std::string& ip_address)
    : ip_address(ip_address), latitude(0.0), longitude(0.0), 
      is_vpn(false), is_proxy(false), is_tor(false) {
    resolved_at = std::time(nullptr);
}

bool LocationInfo::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> LocationInfo::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (ip_address.empty()) {
        errors.push_back("IP address is required");
    } else {
        // Basic IP validation (IPv4 and IPv6)
        std::regex ipv4_regex(R"(^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$)");
        std::regex ipv6_regex(R"(^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$)");
        
        if (!std::regex_match(ip_address, ipv4_regex) && !std::regex_match(ip_address, ipv6_regex)) {
            errors.push_back("Invalid IP address format");
        }
    }
    
    return errors;
}

std::string LocationInfo::to_json() const {
    nlohmann::json j;
    j["ip_address"] = ip_address;
    j["country"] = country;
    j["country_code"] = country_code;
    j["region"] = region;
    j["city"] = city;
    j["noteal_code"] = noteal_code;
    j["latitude"] = latitude;
    j["longitude"] = longitude;
    j["timezone"] = timezone;
    j["isp"] = isp;
    j["is_vpn"] = is_vpn;
    j["is_proxy"] = is_proxy;
    j["is_tor"] = is_tor;
    j["resolved_at"] = resolved_at;
    return j.dump();
}

void LocationInfo::from_json(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        ip_address = j.value("ip_address", "");
        country = j.value("country", "");
        country_code = j.value("country_code", "");
        region = j.value("region", "");
        city = j.value("city", "");
        noteal_code = j.value("noteal_code", "");
        latitude = j.value("latitude", 0.0);
        longitude = j.value("longitude", 0.0);
        timezone = j.value("timezone", "");
        isp = j.value("isp", "");
        is_vpn = j.value("is_vpn", false);
        is_proxy = j.value("is_proxy", false);
        is_tor = j.value("is_tor", false);
        resolved_at = j.value("resolved_at", 0);
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse location info JSON: {}", e.what());
    }
}

std::string LocationInfo::get_display_location() const {
    std::string location;
    
    if (!city.empty()) {
        location = city;
    }
    
    if (!region.empty()) {
        if (!location.empty()) location += ", ";
        location += region;
    }
    
    if (!country.empty()) {
        if (!location.empty()) location += ", ";
        location += country;
    }
    
    return location.empty() ? "Unknown Location" : location;
}

bool LocationInfo::is_suspicious() const {
    return is_vpn || is_proxy || is_tor;
}

double LocationInfo::distance_from(const LocationInfo& other) const {
    // Haversine formula for calculating distance between two points on Earth
    const double R = 6371.0; // Earth's radius in kilometers
    
    double lat1_rad = latitude * M_PI / 180.0;
    double lon1_rad = longitude * M_PI / 180.0;
    double lat2_rad = other.latitude * M_PI / 180.0;
    double lon2_rad = other.longitude * M_PI / 180.0;
    
    double dlat = lat2_rad - lat1_rad;
    double dlon = lon2_rad - lon1_rad;
    
    double a = std::sin(dlat / 2.0) * std::sin(dlat / 2.0) +
               std::cos(lat1_rad) * std::cos(lat2_rad) *
               std::sin(dlon / 2.0) * std::sin(dlon / 2.0);
    
    double c = 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
    
    return R * c;
}

// SecurityFlags implementation
void SecurityFlags::add_alert(const std::string& alert) {
    auto it = std::find(security_alerts.begin(), security_alerts.end(), alert);
    if (it == security_alerts.end()) {
        security_alerts.push_back(alert);
    }
    last_security_check = std::time(nullptr);
}

void SecurityFlags::clear_alert(const std::string& alert) {
    auto it = std::remove(security_alerts.begin(), security_alerts.end(), alert);
    security_alerts.erase(it, security_alerts.end());
    last_security_check = std::time(nullptr);
}

bool SecurityFlags::has_alerts() const {
    return !security_alerts.empty();
}

bool SecurityFlags::is_secure() const {
    return !requires_2fa && !force_password_change && !suspicious_activity &&
           !password_compromised && security_alerts.empty();
}

std::string SecurityFlags::to_json() const {
    nlohmann::json j;
    j["requires_2fa"] = requires_2fa;
    j["force_password_change"] = force_password_change;
    j["suspicious_activity"] = suspicious_activity;
    j["new_device_login"] = new_device_login;
    j["unusual_location"] = unusual_location;
    j["concurrent_sessions_exceeded"] = concurrent_sessions_exceeded;
    j["password_compromised"] = password_compromised;
    j["security_alerts"] = security_alerts;
    j["last_security_check"] = last_security_check;
    return j.dump();
}

void SecurityFlags::from_json(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        requires_2fa = j.value("requires_2fa", false);
        force_password_change = j.value("force_password_change", false);
        suspicious_activity = j.value("suspicious_activity", false);
        new_device_login = j.value("new_device_login", false);
        unusual_location = j.value("unusual_location", false);
        concurrent_sessions_exceeded = j.value("concurrent_sessions_exceeded", false);
        password_compromised = j.value("password_compromised", false);
        
        if (j.contains("security_alerts")) {
            security_alerts = j["security_alerts"].get<std::vector<std::string>>();
        }
        
        last_security_check = j.value("last_security_check", 0);
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse security flags JSON: {}", e.what());
    }
}

// Session implementation
Session::Session() {
    auto now = std::time(nullptr);
    created_at = now;
    updated_at = now;
    last_activity_at = now;
    last_token_refresh = now;
    
    session_type = SessionType::LOGIN;
    status = SessionStatus::ACTIVE;
    request_count = 0;
    failed_request_count = 0;
    max_idle_minutes = 30;
    max_session_duration_hours = 24;
    allow_concurrent_sessions = true;
    remember_me = false;
    is_first_login = false;
    
    // Default expiration: 24 hours
    expires_at = now + (24 * 3600);
}

Session::Session(const std::string& user_id, const DeviceInfo& device, const LocationInfo& location)
    : Session() {
    this->user_id = user_id;
    this->device_info = device;
    this->location_info = location;
    
    session_id = SessionManager::generate_session_id();
    access_token = SessionManager::generate_access_token();
    refresh_token = SessionManager::generate_refresh_token();
    csrf_token = SessionManager::generate_csrf_token();
}

Session::Session(const std::string& user_id, SessionType type, const std::string& access_token)
    : Session() {
    this->user_id = user_id;
    this->session_type = type;
    this->access_token = access_token;
    
    session_id = SessionManager::generate_session_id();
    refresh_token = SessionManager::generate_refresh_token();
    csrf_token = SessionManager::generate_csrf_token();
    
    expires_at = SessionManager::calculate_expiration(type);
}

void Session::activate() {
    status = SessionStatus::ACTIVE;
    updated_at = std::time(nullptr);
    last_activity_at = updated_at;
}

void Session::suspend(const std::string& reason) {
    status = SessionStatus::SUSPENDED;
    updated_at = std::time(nullptr);
    security_flags.add_alert("Session suspended: " + reason);
}

void Session::revoke(const std::string& reason) {
    status = SessionStatus::REVOKED;
    updated_at = std::time(nullptr);
    access_token = "";
    refresh_token = "";
    security_flags.add_alert("Session revoked: " + reason);
}

void Session::expire() {
    status = SessionStatus::EXPIRED;
    updated_at = std::time(nullptr);
    expires_at = updated_at;
}

void Session::refresh_tokens(const std::string& new_access_token, const std::string& new_refresh_token) {
    access_token = new_access_token;
    refresh_token = new_refresh_token;
    last_token_refresh = std::time(nullptr);
    updated_at = last_token_refresh;
    
    // Extend expiration
    if (session_type == SessionType::LOGIN && remember_me) {
        expires_at = last_token_refresh + (30 * 24 * 3600); // 30 days
    } else {
        expires_at = last_token_refresh + (24 * 3600); // 24 hours
    }
}

void Session::extend_expiration(int additional_hours) {
    expires_at += (additional_hours * 3600);
    updated_at = std::time(nullptr);
}

void Session::record_activity(const std::string& endpoint) {
    request_count++;
    last_activity_at = std::time(nullptr);
    updated_at = last_activity_at;
    last_endpoint = endpoint;
    
    // Keep only last 10 endpoints
    recent_endpoints.insert(recent_endpoints.begin(), endpoint);
    if (recent_endpoints.size() > 10) {
        recent_endpoints.pop_back();
    }
}

void Session::record_failed_request() {
    failed_request_count++;
    updated_at = std::time(nullptr);
    
    // Add security alert if too many failed requests
    if (failed_request_count > 10) {
        security_flags.add_alert("Excessive failed requests detected");
        security_flags.suspicious_activity = true;
    }
}

void Session::update_last_activity() {
    last_activity_at = std::time(nullptr);
    updated_at = last_activity_at;
}

bool Session::is_activity_recent(int minutes) const {
    auto now = std::time(nullptr);
    return (now - last_activity_at) <= (minutes * 60);
}

bool Session::is_expired() const {
    return std::time(nullptr) >= expires_at || status == SessionStatus::EXPIRED;
}

bool Session::is_active() const {
    return status == SessionStatus::ACTIVE && !is_expired();
}

bool Session::is_idle(int max_idle_minutes) const {
    auto now = std::time(nullptr);
    return (now - last_activity_at) >= (max_idle_minutes * 60);
}

bool Session::should_expire() const {
    return is_expired() || is_idle(max_idle_minutes) || 
           (max_session_duration_hours > 0 && 
            (std::time(nullptr) - created_at) >= (max_session_duration_hours * 3600));
}

bool Session::is_token_valid() const {
    return !access_token.empty() && is_active();
}

bool Session::requires_refresh() const {
    auto now = std::time(nullptr);
    auto time_until_expiry = expires_at - now;
    
    // Refresh if less than 1 hour until expiry
    return time_until_expiry < 3600 && !refresh_token.empty();
}

bool Session::is_secure_session() const {
    return security_flags.is_secure() && is_active() && !is_suspicious_location();
}

bool Session::is_trusted_device() const {
    return device_info.is_trusted;
}

bool Session::is_new_location() const {
    return security_flags.unusual_location;
}

bool Session::is_suspicious_location() const {
    return location_info.is_suspicious() || security_flags.unusual_location;
}

void Session::mark_device_as_trusted() {
    device_info.is_trusted = true;
    security_flags.new_device_login = false;
    security_flags.clear_alert("New device login");
    updated_at = std::time(nullptr);
}

void Session::mark_location_as_suspicious() {
    security_flags.unusual_location = true;
    security_flags.add_alert("Unusual location detected");
    updated_at = std::time(nullptr);
}

bool Session::is_same_device(const DeviceInfo& other_device) const {
    return device_info.device_id == other_device.device_id;
}

bool Session::is_same_location(const LocationInfo& other_location) const {
    double distance = location_info.distance_from(other_location);
    return distance < 50.0; // Within 50km considered same location
}

double Session::calculate_risk_score() const {
    double risk_score = 0.0;
    
    // Device-based risk
    if (!is_trusted_device()) risk_score += 0.3;
    if (security_flags.new_device_login) risk_score += 0.2;
    
    // Location-based risk
    if (is_suspicious_location()) risk_score += 0.4;
    if (security_flags.unusual_location) risk_score += 0.3;
    
    // Activity-based risk
    if (failed_request_count > 5) risk_score += 0.2;
    if (failed_request_count > 10) risk_score += 0.3;
    
    // Security flags
    if (security_flags.suspicious_activity) risk_score += 0.5;
    if (security_flags.password_compromised) risk_score += 0.8;
    if (security_flags.concurrent_sessions_exceeded) risk_score += 0.2;
    
    return std::min(risk_score, 1.0);
}

std::vector<std::string> Session::get_security_warnings() const {
    std::vector<std::string> warnings;
    
    if (!is_trusted_device()) {
        warnings.push_back("Untrusted device");
    }
    
    if (is_suspicious_location()) {
        warnings.push_back("Suspicious location detected");
    }
    
    if (failed_request_count > 5) {
        warnings.push_back("Multiple failed requests");
    }
    
    if (security_flags.suspicious_activity) {
        warnings.push_back("Suspicious activity detected");
    }
    
    if (requires_refresh()) {
        warnings.push_back("Session token requires refresh");
    }
    
    if (is_idle(max_idle_minutes)) {
        warnings.push_back("Session has been idle");
    }
    
    // Add custom security alerts
    warnings.insert(warnings.end(), security_flags.security_alerts.begin(), 
                   security_flags.security_alerts.end());
    
    return warnings;
}

void Session::add_scope(const std::string& scope) {
    auto it = std::find(scopes.begin(), scopes.end(), scope);
    if (it == scopes.end()) {
        scopes.push_back(scope);
        updated_at = std::time(nullptr);
    }
}

void Session::remove_scope(const std::string& scope) {
    auto it = std::remove(scopes.begin(), scopes.end(), scope);
    if (it != scopes.end()) {
        scopes.erase(it, scopes.end());
        updated_at = std::time(nullptr);
    }
}

bool Session::has_scope(const std::string& scope) const {
    return std::find(scopes.begin(), scopes.end(), scope) != scopes.end();
}

void Session::add_permission(const std::string& permission) {
    auto it = std::find(permissions.begin(), permissions.end(), permission);
    if (it == permissions.end()) {
        permissions.push_back(permission);
        updated_at = std::time(nullptr);
    }
}

void Session::remove_permission(const std::string& permission) {
    auto it = std::remove(permissions.begin(), permissions.end(), permission);
    if (it != permissions.end()) {
        permissions.erase(it, permissions.end());
        updated_at = std::time(nullptr);
    }
}

bool Session::has_permission(const std::string& permission) const {
    return std::find(permissions.begin(), permissions.end(), permission) != permissions.end();
}

void Session::set_idle_timeout(int minutes) {
    max_idle_minutes = minutes;
    updated_at = std::time(nullptr);
}

void Session::set_max_duration(int hours) {
    max_session_duration_hours = hours;
    updated_at = std::time(nullptr);
}

void Session::enable_remember_me() {
    remember_me = true;
    expires_at = created_at + (30 * 24 * 3600); // 30 days
    updated_at = std::time(nullptr);
}

void Session::disable_remember_me() {
    remember_me = false;
    expires_at = created_at + (24 * 3600); // 24 hours
    updated_at = std::time(nullptr);
}

bool Session::should_auto_logout() const {
    return should_expire() || security_flags.force_password_change ||
           (security_flags.requires_2fa && session_type != SessionType::TEMPORARY);
}

Session Session::get_public_view() const {
    Session public_session = *this;
    
    // Clear sensitive information
    public_session.access_token = "";
    public_session.refresh_token = "";
    public_session.csrf_token = "";
    public_session.location_info.ip_address = "";
    public_session.device_info.user_agent = "";
    public_session.security_flags = SecurityFlags();
    public_session.recent_endpoints.clear();
    
    return public_session;
}

Session Session::get_security_view() const {
    Session security_session = *this;
    
    // Show security info but hide tokens
    security_session.access_token = access_token.substr(0, 8) + "...";
    security_session.refresh_token = "";
    security_session.csrf_token = "";
    
    return security_session;
}

Session Session::get_admin_view() const {
    return *this; // Admins see everything
}

Session Session::get_user_view() const {
    Session user_session = *this;
    
    // Hide tokens but show basic session info
    user_session.access_token = "";
    user_session.refresh_token = "";
    user_session.csrf_token = "";
    
    return user_session;
}

bool Session::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> Session::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (session_id.empty()) {
        errors.push_back("Session ID is required");
    }
    
    if (user_id.empty()) {
        errors.push_back("User ID is required");
    }
    
    if (access_token.empty()) {
        errors.push_back("Access token is required");
    }
    
    if (created_at <= 0) {
        errors.push_back("Invalid creation timestamp");
    }
    
    if (expires_at <= created_at) {
        errors.push_back("Expiration time must be after creation time");
    }
    
    // Validate device info
    auto device_errors = device_info.get_validation_errors();
    errors.insert(errors.end(), device_errors.begin(), device_errors.end());
    
    // Validate location info
    auto location_errors = location_info.get_validation_errors();
    errors.insert(errors.end(), location_errors.begin(), location_errors.end());
    
    return errors;
}

std::string Session::to_json() const {
    nlohmann::json j;
    
    j["session_id"] = session_id;
    j["user_id"] = user_id;
    j["access_token"] = access_token;
    j["refresh_token"] = refresh_token;
    j["session_type"] = static_cast<int>(session_type);
    j["status"] = static_cast<int>(status);
    j["session_name"] = session_name;
    j["csrf_token"] = csrf_token;
    j["scopes"] = scopes;
    j["permissions"] = permissions;
    j["created_at"] = created_at;
    j["updated_at"] = updated_at;
    j["expires_at"] = expires_at;
    j["last_activity_at"] = last_activity_at;
    j["last_token_refresh"] = last_token_refresh;
    j["request_count"] = request_count;
    j["failed_request_count"] = failed_request_count;
    j["last_endpoint"] = last_endpoint;
    j["recent_endpoints"] = recent_endpoints;
    j["login_method"] = login_method;
    j["login_provider"] = login_provider;
    j["referrer_url"] = referrer_url;
    j["is_first_login"] = is_first_login;
    j["max_idle_minutes"] = max_idle_minutes;
    j["max_session_duration_hours"] = max_session_duration_hours;
    j["allow_concurrent_sessions"] = allow_concurrent_sessions;
    j["remember_me"] = remember_me;
    
    // Serialize nested objects
    j["device_info"] = nlohmann::json::parse(device_info.to_json());
    j["location_info"] = nlohmann::json::parse(location_info.to_json());
    j["security_flags"] = nlohmann::json::parse(security_flags.to_json());
    
    return j.dump();
}

void Session::from_json(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        
        session_id = j.value("session_id", "");
        user_id = j.value("user_id", "");
        access_token = j.value("access_token", "");
        refresh_token = j.value("refresh_token", "");
        session_type = static_cast<SessionType>(j.value("session_type", 0));
        status = static_cast<SessionStatus>(j.value("status", 0));
        session_name = j.value("session_name", "");
        csrf_token = j.value("csrf_token", "");
        
        if (j.contains("scopes")) {
            scopes = j["scopes"].get<std::vector<std::string>>();
        }
        if (j.contains("permissions")) {
            permissions = j["permissions"].get<std::vector<std::string>>();
        }
        
        created_at = j.value("created_at", 0);
        updated_at = j.value("updated_at", 0);
        expires_at = j.value("expires_at", 0);
        last_activity_at = j.value("last_activity_at", 0);
        last_token_refresh = j.value("last_token_refresh", 0);
        request_count = j.value("request_count", 0);
        failed_request_count = j.value("failed_request_count", 0);
        last_endpoint = j.value("last_endpoint", "");
        
        if (j.contains("recent_endpoints")) {
            recent_endpoints = j["recent_endpoints"].get<std::vector<std::string>>();
        }
        
        login_method = j.value("login_method", "");
        login_provider = j.value("login_provider", "");
        referrer_url = j.value("referrer_url", "");
        is_first_login = j.value("is_first_login", false);
        max_idle_minutes = j.value("max_idle_minutes", 30);
        max_session_duration_hours = j.value("max_session_duration_hours", 24);
        allow_concurrent_sessions = j.value("allow_concurrent_sessions", true);
        remember_me = j.value("remember_me", false);
        
        // Deserialize nested objects
        if (j.contains("device_info")) {
            device_info.from_json(j["device_info"].dump());
        }
        if (j.contains("location_info")) {
            location_info.from_json(j["location_info"].dump());
        }
        if (j.contains("security_flags")) {
            security_flags.from_json(j["security_flags"].dump());
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse session JSON: {}", e.what());
    }
}

bool Session::operator==(const Session& other) const {
    return session_id == other.session_id;
}

bool Session::operator!=(const Session& other) const {
    return !(*this == other);
}

// SessionMetrics implementation
std::string Session::SessionMetrics::to_json() const {
    nlohmann::json j;
    j["total_requests"] = total_requests;
    j["failed_requests"] = failed_requests;
    j["session_duration_hours"] = session_duration_hours;
    j["unique_endpoints_accessed"] = unique_endpoints_accessed;
    j["average_request_interval_seconds"] = average_request_interval_seconds;
    j["peak_activity_time"] = peak_activity_time;
    return j.dump();
}

void Session::SessionMetrics::from_json(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        total_requests = j.value("total_requests", 0);
        failed_requests = j.value("failed_requests", 0);
        session_duration_hours = j.value("session_duration_hours", 0.0);
        unique_endpoints_accessed = j.value("unique_endpoints_accessed", 0);
        average_request_interval_seconds = j.value("average_request_interval_seconds", 0.0);
        peak_activity_time = j.value("peak_activity_time", 0);
    } catch (const std::exception& e) {
        spdlog::error("Failed to parse session metrics JSON: {}", e.what());
    }
}

Session::SessionMetrics Session::get_metrics() const {
    SessionMetrics metrics;
    metrics.total_requests = request_count;
    metrics.failed_requests = failed_request_count;
    
    auto now = std::time(nullptr);
    metrics.session_duration_hours = static_cast<double>(now - created_at) / 3600.0;
    
    // Count unique endpoints
    std::set<std::string> unique_endpoints(recent_endpoints.begin(), recent_endpoints.end());
    metrics.unique_endpoints_accessed = static_cast<int>(unique_endpoints.size());
    
    // Calculate average request interval
    if (request_count > 1 && metrics.session_duration_hours > 0) {
        metrics.average_request_interval_seconds = 
            (metrics.session_duration_hours * 3600.0) / request_count;
    }
    
    metrics.peak_activity_time = last_activity_at;
    
    return metrics;
}

// Request implementations
bool SessionCreateRequest::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> SessionCreateRequest::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (user_id.empty()) {
        errors.push_back("User ID is required");
    }
    
    if (login_method.empty()) {
        errors.push_back("Login method is required");
    }
    
    auto device_errors = device_info.get_validation_errors();
    errors.insert(errors.end(), device_errors.begin(), device_errors.end());
    
    auto location_errors = location_info.get_validation_errors();
    errors.insert(errors.end(), location_errors.begin(), location_errors.end());
    
    return errors;
}

bool SessionUpdateRequest::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> SessionUpdateRequest::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (session_id.empty()) {
        errors.push_back("Session ID is required");
    }
    
    if (max_idle_minutes.has_value() && *max_idle_minutes < 1) {
        errors.push_back("Max idle minutes must be at least 1");
    }
    
    if (max_session_duration_hours.has_value() && *max_session_duration_hours < 1) {
        errors.push_back("Max session duration must be at least 1 hour");
    }
    
    return errors;
}

std::vector<std::string> SessionUpdateRequest::get_updated_fields() const {
    std::vector<std::string> fields;
    
    if (session_name.has_value()) fields.push_back("session_name");
    if (max_idle_minutes.has_value()) fields.push_back("max_idle_minutes");
    if (max_session_duration_hours.has_value()) fields.push_back("max_session_duration_hours");
    if (allow_concurrent_sessions.has_value()) fields.push_back("allow_concurrent_sessions");
    if (security_flags.has_value()) fields.push_back("security_flags");
    
    return fields;
}

bool SessionSearchRequest::validate() const {
    return get_validation_errors().empty();
}

std::vector<std::string> SessionSearchRequest::get_validation_errors() const {
    std::vector<std::string> errors;
    
    if (limit < 1 || limit > 1000) {
        errors.push_back("Limit must be between 1 and 1000");
    }
    
    if (offset < 0) {
        errors.push_back("Offset must be non-negative");
    }
    
    if (created_after.has_value() && created_before.has_value() &&
        *created_after >= *created_before) {
        errors.push_back("Created after must be before created before");
    }
    
    return errors;
}

// SessionManager implementation
std::string SessionManager::generate_session_id() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);
    
    std::string session_id = "sess_";
    for (int i = 0; i < 32; ++i) {
        session_id += "0123456789abcdef"[dis(gen)];
    }
    
    return session_id;
}

std::string SessionManager::generate_access_token() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 61);
    
    const std::string chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string token = "at_";
    
    for (int i = 0; i < 64; ++i) {
        token += chars[dis(gen)];
    }
    
    return token;
}

std::string SessionManager::generate_refresh_token() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 61);
    
    const std::string chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string token = "rt_";
    
    for (int i = 0; i < 64; ++i) {
        token += chars[dis(gen)];
    }
    
    return token;
}

std::string SessionManager::generate_csrf_token() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 61);
    
    const std::string chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string token = "csrf_";
    
    for (int i = 0; i < 32; ++i) {
        token += chars[dis(gen)];
    }
    
    return token;
}

bool SessionManager::is_valid_session_id(const std::string& session_id) {
    return session_id.length() == 37 && session_id.substr(0, 5) == "sess_";
}

bool SessionManager::is_valid_token(const std::string& token) {
    return (token.length() == 67 && (token.substr(0, 3) == "at_" || token.substr(0, 3) == "rt_")) ||
           (token.length() == 37 && token.substr(0, 5) == "csrf_");
}

std::time_t SessionManager::calculate_expiration(SessionType type, bool remember_me) {
    auto now = std::time(nullptr);
    
    switch (type) {
        case SessionType::LOGIN:
            return now + (remember_me ? 30 * 24 * 3600 : 24 * 3600); // 30 days or 24 hours
        case SessionType::API:
            return now + (7 * 24 * 3600); // 7 days
        case SessionType::OAUTH:
            return now + (2 * 3600); // 2 hours
        case SessionType::TEMPORARY:
            return now + (30 * 60); // 30 minutes
        case SessionType::REMEMBER_ME:
            return now + (90 * 24 * 3600); // 90 days
        default:
            return now + (24 * 3600); // 24 hours
    }
}

int SessionManager::get_max_concurrent_sessions(const std::string& user_id) {
    // This could be configurable per user or account type
    return 10; // Default max concurrent sessions
}

SessionType SessionManager::detect_session_type(const std::string& user_agent, 
                                                const std::vector<std::string>& scopes) {
    // Check for API access patterns
    if (user_agent.find("API") != std::string::npos || 
        user_agent.find("Bot") != std::string::npos ||
        user_agent.find("curl") != std::string::npos) {
        return SessionType::API;
    }
    
    // Check for OAuth scopes
    for (const auto& scope : scopes) {
        if (scope.find("oauth") != std::string::npos) {
            return SessionType::OAUTH;
        }
    }
    
    return SessionType::LOGIN;
}

DeviceType SessionManager::detect_device_type(const std::string& user_agent) {
    std::string ua_lower = user_agent;
    std::transform(ua_lower.begin(), ua_lower.end(), ua_lower.begin(), ::tolower);
    
    if (ua_lower.find("mobile") != std::string::npos || 
        ua_lower.find("android") != std::string::npos ||
        ua_lower.find("iphone") != std::string::npos) {
        return DeviceType::MOBILE;
    }
    
    if (ua_lower.find("tablet") != std::string::npos || 
        ua_lower.find("ipad") != std::string::npos) {
        return DeviceType::TABLET;
    }
    
    if (ua_lower.find("tv") != std::string::npos ||
        ua_lower.find("roku") != std::string::npos ||
        ua_lower.find("appletv") != std::string::npos) {
        return DeviceType::TV;
    }
    
    if (ua_lower.find("watch") != std::string::npos) {
        return DeviceType::WATCH;
    }
    
    return DeviceType::DESKTOP;
}

std::string SessionManager::parse_browser_info(const std::string& user_agent) {
    if (user_agent.find("Chrome") != std::string::npos) {
        return "Chrome";
    } else if (user_agent.find("Firefox") != std::string::npos) {
        return "Firefox";
    } else if (user_agent.find("Safari") != std::string::npos) {
        return "Safari";
    } else if (user_agent.find("Edge") != std::string::npos) {
        return "Edge";
    } else if (user_agent.find("Opera") != std::string::npos) {
        return "Opera";
    }
    
    return "Unknown";
}

std::string SessionManager::parse_os_info(const std::string& user_agent) {
    if (user_agent.find("Windows") != std::string::npos) {
        return "Windows";
    } else if (user_agent.find("Mac OS") != std::string::npos) {
        return "macOS";
    } else if (user_agent.find("Linux") != std::string::npos) {
        return "Linux";
    } else if (user_agent.find("Android") != std::string::npos) {
        return "Android";
    } else if (user_agent.find("iOS") != std::string::npos) {
        return "iOS";
    }
    
    return "Unknown";
}

// Utility functions
std::string session_status_to_string(SessionStatus status) {
    switch (status) {
        case SessionStatus::ACTIVE: return "active";
        case SessionStatus::EXPIRED: return "expired";
        case SessionStatus::REVOKED: return "revoked";
        case SessionStatus::SUSPENDED: return "suspended";
        default: return "unknown";
    }
}

SessionStatus string_to_session_status(const std::string& status) {
    if (status == "active") return SessionStatus::ACTIVE;
    if (status == "expired") return SessionStatus::EXPIRED;
    if (status == "revoked") return SessionStatus::REVOKED;
    if (status == "suspended") return SessionStatus::SUSPENDED;
    return SessionStatus::ACTIVE;
}

std::string session_type_to_string(SessionType type) {
    switch (type) {
        case SessionType::LOGIN: return "login";
        case SessionType::API: return "api";
        case SessionType::OAUTH: return "oauth";
        case SessionType::TEMPORARY: return "temporary";
        case SessionType::REMEMBER_ME: return "remember_me";
        default: return "login";
    }
}

SessionType string_to_session_type(const std::string& type) {
    if (type == "login") return SessionType::LOGIN;
    if (type == "api") return SessionType::API;
    if (type == "oauth") return SessionType::OAUTH;
    if (type == "temporary") return SessionType::TEMPORARY;
    if (type == "remember_me") return SessionType::REMEMBER_ME;
    return SessionType::LOGIN;
}

std::string device_type_to_string(DeviceType type) {
    switch (type) {
        case DeviceType::DESKTOP: return "desktop";
        case DeviceType::MOBILE: return "mobile";
        case DeviceType::TABLET: return "tablet";
        case DeviceType::TV: return "tv";
        case DeviceType::WATCH: return "watch";
        case DeviceType::OTHER: return "other";
        default: return "desktop";
    }
}

DeviceType string_to_device_type(const std::string& type) {
    if (type == "desktop") return DeviceType::DESKTOP;
    if (type == "mobile") return DeviceType::MOBILE;
    if (type == "tablet") return DeviceType::TABLET;
    if (type == "tv") return DeviceType::TV;
    if (type == "watch") return DeviceType::WATCH;
    if (type == "other") return DeviceType::OTHER;
    return DeviceType::DESKTOP;
}

} // namespace sonet::user::models