#pragma once

#include <string>
#include <memory>
#include <iostream>

namespace spdlog {

namespace sinks {
    class stdout_color_sink_mt {
    public:
        stdout_color_sink_mt() = default;
    };
}

namespace level {
    enum level_enum {
        trace = 0,
        debug = 1,
        info = 2,
        warn = 3,
        err = 4,
        critical = 5,
        off = 6
    };
    
    inline std::string_view to_string_view(level_enum l) {
        switch (l) {
            case trace: return "trace";
            case debug: return "debug";
            case info: return "info";
            case warn: return "warn";
            case err: return "error";
            case critical: return "critical";
            case off: return "off";
            default: return "unknown";
        }
    }
}

class logger {
public:
    logger(const std::string& name) : name_(name) {}
    logger(const std::string& name, std::shared_ptr<sinks::stdout_color_sink_mt> sink) : name_(name) {}
    
    template<typename... Args>
    void trace(const Args&... args) {}
    
    template<typename... Args>
    void debug(const Args&... args) {}
    
    template<typename... Args>
    void info(const Args&... args) {}
    
    template<typename... Args>
    void warn(const Args&... args) {}
    
    template<typename... Args>
    void error(const Args&... args) {}
    
    template<typename... Args>
    void critical(const Args&... args) {}
    
    void set_level(level::level_enum level) {}
    void set_pattern(const std::string& pattern) {}

private:
    std::string name_;
};

inline void set_default_logger(std::shared_ptr<logger> logger) {}
inline void set_level(level::level_enum level) {}
inline void set_pattern(const std::string& pattern) {}

// Global logging functions
template<typename... Args>
void trace(const Args&... args) {}

template<typename... Args>
void debug(const Args&... args) {}

template<typename... Args>
void info(const Args&... args) {}

template<typename... Args>
void warn(const Args&... args) {}

template<typename... Args>
void error(const Args&... args) {}

template<typename... Args>
void critical(const Args&... args) {}

} // namespace spdlog