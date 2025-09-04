//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//
#pragma once

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

namespace sonet::media_service {

class Logger {
public:
    enum Level { INFO, WARN, ERROR };
    static Logger& Instance() { static Logger inst; return inst; }
    Logger() {
        const char* lvl = std::getenv("SONET_MEDIA_LOG_LEVEL");
        if (lvl) {
            std::string v = lvl; for (auto& c: v) c = static_cast<char>(std::tolower(c));
            if (v=="error") min_level_ = ERROR; else if (v=="warn") min_level_ = WARN; else min_level_ = INFO;
        }
    }
    void Log(Level lvl, const std::string& msg, const std::unordered_map<std::string,std::string>& fields = {}) {
        if (lvl < min_level_) return;
        std::lock_guard<std::mutex> lock(mu_);
        auto now = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(now);
        std::tm tm{};
#if defined(_WIN32)
        gmtime_s(&tm, &t);
#else
        gmtime_r(&t, &tm);
#endif
        std::ostringstream ts; ts << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
        auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count() % 1000000;
        ts << "." << std::setfill('0') << std::setw(6) << micros << "Z";
        std::ostringstream out;
        out << "{";
        out << "\"ts\":\"" << ts.str() << "\"";
        out << ",\"level\":\"" << LevelName(lvl) << "\"";
        out << ",\"msg\":\"" << Escape(msg) << "\"";
        out << ",\"tid\":\"" << ThreadId() << "\"";
        for (const auto& kv : fields) {
            out << ",\"" << Escape(kv.first) << "\":\"" << Escape(kv.second) << "\"";
        }
        out << "}";
        std::ostream& os = (lvl==ERROR)? std::cerr : std::cout;
        os << out.str() << std::endl;
    }
private:
    std::mutex mu_;
    Level min_level_ = INFO;
    static const char* LevelName(Level l) { switch(l){case INFO:return "info";case WARN:return "warn";default:return "error";} }
    static std::string Escape(const std::string& s) {
        std::string o; o.reserve(s.size()+8);
        for(char c: s){ switch(c){ case '"': o+="\\\""; break; case '\\': o+="\\\\"; break; case '\n': o+="\\n"; break; case '\r': o+="\\r"; break; case '\t': o+="\\t"; break; default: o+=c; } }
        return o;
    }
    static std::string ThreadId() { std::ostringstream oss; oss << std::this_thread::get_id(); return oss.str(); }
};

#define LOG_INFO(msg, fields) ::sonet::media_service::Logger::Instance().Log(::sonet::media_service::Logger::INFO, (msg), (fields))
#define LOG_WARN(msg, fields) ::sonet::media_service::Logger::Instance().Log(::sonet::media_service::Logger::WARN, (msg), (fields))
#define LOG_ERROR(msg, fields) ::sonet::media_service::Logger::Instance().Log(::sonet::media_service::Logger::ERROR, (msg), (fields))

} // namespace sonet::media_service
