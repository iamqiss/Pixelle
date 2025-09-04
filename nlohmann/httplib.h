#pragma once

#include <string>
#include <functional>
#include <map>

namespace httplib {

class Request {
public:
    std::string method;
    std::string path;
    std::map<std::string, std::string> headers;
    std::string body;
    
    std::string get_param_value(const std::string& key) const { return ""; }
    bool has_header(const std::string& key) const { return false; }
    std::string get_header_value(const std::string& key) const { return ""; }
};

class Response {
public:
    int status = 200;
    std::map<std::string, std::string> headers;
    std::string body;
    
    void set_content(const std::string& content, const std::string& content_type) {
        body = content;
        headers["Content-Type"] = content_type;
    }
    
    void set_header(const std::string& key, const std::string& value) {
        headers[key] = value;
    }
};

class Server {
public:
    using Handler = std::function<void(const Request&, Response&)>;
    
    void Get(const std::string& pattern, Handler handler) {}
    void Note(const std::string& pattern, Handler handler) {}
    void Put(const std::string& pattern, Handler handler) {}
    void Delete(const std::string& pattern, Handler handler) {}
    void Options(const std::string& pattern, Handler handler) {}
    
    bool listen(const std::string& host, int port) { return true; }
    void stop() {}
};

} // namespace httplib