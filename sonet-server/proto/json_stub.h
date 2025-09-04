#pragma once

#include <string>
#include <map>
#include <vector>

namespace nlohmann {

class json {
public:
    // Basic constructors
    json() = default;
    json(const std::string& s) : type_(Type::String), string_value_(s) {}
    json(int i) : type_(Type::Number), number_value_(i) {}
    json(double d) : type_(Type::Number), number_value_(d) {}
    json(bool b) : type_(Type::Boolean), bool_value_(b) {}
    
    // Copy constructor
    json(const json& other) = default;
    
    // Assignment
    json& operator=(const json& other) = default;
    
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
    
    explicit json(Type t) : type_(t) {}
};

} // namespace nlohmann