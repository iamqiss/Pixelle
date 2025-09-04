#pragma once

#include <string>
#include <string_view>
#include <map>
#include <vector>
#include <initializer_list>
#include <type_traits> // For std::is_same_v

namespace nlohmann {

class json {
public:
    // Basic constructors
    json() = default;
    json(const std::string& s) : type_(Type::String), string_value_(s) {}
    json(int i) : type_(Type::Number), number_value_(i) {}
    json(double d) : type_(Type::Number), number_value_(d) {}
    json(bool b) : type_(Type::Boolean), bool_value_(b) {}
    
    // Initializer list constructor for objects
    json(std::initializer_list<std::pair<std::string, json>> init) : type_(Type::Object) {
        for (const auto& pair : init) {
            object_values_[pair.first] = pair.second;
        }
    }
    
    // Copy constructor
    json(const json& other) = default;
    
    // Assignment
    json& operator=(const json& other) = default;
    
    // Assignment from other types
    json& operator=(const std::string& s) { 
        type_ = Type::String; 
        string_value_ = s; 
        return *this; 
    }
    json& operator=(int i) { 
        type_ = Type::Number; 
        number_value_ = i; 
        return *this; 
    }
    json& operator=(double d) { 
        type_ = Type::Number; 
        number_value_ = d; 
        return *this; 
    }
    json& operator=(bool b) { 
        type_ = Type::Boolean; 
        bool_value_ = b; 
        return *this; 
    }
    
    // Support for string_view
    json& operator=(const std::string_view& sv) { 
        type_ = Type::String; 
        string_value_ = std::string(sv); 
        return *this; 
    }
    
    // Basic operations
    std::string dump(int indent = -1) const {
        switch (type_) {
            case Type::String: return "\"" + string_value_ + "\"";
            case Type::Number: return std::to_string(number_value_);
            case Type::Boolean: return bool_value_ ? "true" : "false";
            case Type::Null: return "null";
            case Type::Object: return "{}";
            case Type::Array: return "[]";
            default: return "null";
        }
    }
    
    // Type checking
    bool is_string() const { return type_ == Type::String; }
    bool is_number() const { return type_ == Type::Number; }
    bool is_boolean() const { return type_ == Type::Boolean; }
    bool is_null() const { return type_ == Type::Null; }
    bool is_object() const { return type_ == Type::Object; }
    bool is_array() const { return type_ == Type::Array; }
    
    // Value access
    std::string get_string() const { return string_value_; }
    double get_number() const { return number_value_; }
    bool get_boolean() const { return bool_value_; }
    
    // Operator[] for object access
    json& operator[](const std::string& key) {
        if (type_ != Type::Object) {
            type_ = Type::Object;
        }
        return object_values_[key];
    }
    
    const json& operator[](const std::string& key) const {
        static json null_json;
        if (type_ != Type::Object) return null_json;
        auto it = object_values_.find(key);
        if (it != object_values_.end()) return it->second;
        return null_json;
    }
    
    // Iterator support for object
    auto begin() { return object_values_.begin(); }
    auto end() { return object_values_.end(); }
    auto begin() const { return object_values_.begin(); }
    auto end() const { return object_values_.end(); }
    
    // Contains method for object keys
    bool contains(const std::string& key) const {
        if (type_ != Type::Object) return false;
        return object_values_.find(key) != object_values_.end();
    }
    
    // Array support
    void push_back(const json& item) {
        if (type_ != Type::Array) {
            type_ = Type::Array;
            array_values_.clear();
        }
        array_values_.push_back(item);
    }
    
    // Value method for safe access with default
    template<typename T>
    T value(const std::string& key, const T& default_value) const {
        if (type_ != Type::Object) return default_value;
        auto it = object_values_.find(key);
        if (it != object_values_.end()) {
            if constexpr (std::is_same_v<T, std::string>) return it->second.get_string();
            if constexpr (std::is_same_v<T, int>) return static_cast<int>(it->second.get_number());
            if constexpr (std::is_same_v<T, long>) return static_cast<long>(it->second.get_number());
            if constexpr (std::is_same_v<T, const char*>) return it->second.get_string().c_str();
            if constexpr (std::is_same_v<T, char*>) return const_cast<char*>(it->second.get_string().c_str());
        }
        return default_value;
    }
    
    // Specialized value method for string literals
    std::string value(const std::string& key, const char* default_value) const {
        if (type_ != Type::Object) return std::string(default_value);
        auto it = object_values_.find(key);
        if (it != object_values_.end()) {
            return it->second.get_string();
        }
        return std::string(default_value);
    }
    
    // Parse method
    static json parse(const std::string& s) {
        // Simple stub - just return an empty object
        return json();
    }
    
    // Static constructors
    static json object() { return json(Type::Object); }
    static json array() { return json(Type::Array); }
    static json null() { return json(Type::Null); }

private:
    enum class Type { Null, String, Number, Boolean, Object, Array };
    Type type_ = Type::Null;
    std::string string_value_;
    double number_value_ = 0.0;
    bool bool_value_ = false;
    std::map<std::string, json> object_values_;
    std::vector<json> array_values_;
    
    explicit json(Type t) : type_(t) {}
};

} // namespace nlohmann