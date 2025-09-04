/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "password_manager.h"
#include "security_utils.h"
#include <argon2.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <algorithm>
#include <regex>
#include <set>
#include <random>
#include <spdlog/spdlog.h>
#include <sstream>

namespace sonet::user {

// I hate weak passwords more than anything else in security
const std::vector<std::string> PasswordPolicy::FORBIDDEN_PASSWORDS = {
    "password", "123456", "123456789", "qwerty", "abc123", "111111",
    "password123", "admin", "welcome", "login", "root", "toor",
    "pass", "test", "guest", "user", "letmein", "monkey", "dragon"
};

// Common phrases that are too predictable for passphrases
const std::vector<std::string> PasswordPolicy::FORBIDDEN_PHRASES = {
    "correct horse battery staple", "the quick brown fox", "lorem ipsum dolor sit",
    "twinkle twinkle little star", "mary had a little lamb", "happy birthday to you",
    "row row row your boat", "old macdonald had a farm", "itsy bitsy spider",
    "the wheels on the bus", "if you're happy and you know it", "head shoulders knees and toes",
    "baa baa black sheep", "humpty dumpty sat on a wall", "jack and jill went up the hill",
    "little miss muffet sat on a tuffet", "peter piper picked a peck", "sally sells seashells",
    "how much wood could a woodchuck", "she sells seashells by the seashore"
};

PasswordManager::PasswordManager() {
    spdlog::info("Passphrase manager initialized with Argon2id - modern security through memorable strength");
}

std::string PasswordManager::hash_password(const std::string& passphrase) {
    // Generate a random salt - this is crucial for security
    std::string salt = generate_salt();
    
    // Prepare output buffer for the hash
    std::vector<uint8_t> hash(argon2_config_.hash_length);
    
    // Use Argon2id - the gold standard for password hashing
    int result = argon2id_hash_raw(
        argon2_config_.time_cost,
        argon2_config_.memory_cost,
        argon2_config_.parallelism,
        passphrase.c_str(),
        passphrase.length(),
        salt.c_str(),
        salt.length(),
        hash.data(),
        hash.size()
    );
    
    if (result != ARGON2_OK) {
        spdlog::error("Argon2 hashing failed: {}", argon2_error_message(result));
        throw std::runtime_error("Passphrase hashing failed");
    }
    
    // Format: salt + hash (both base64 encoded)
    std::string encoded_salt = SecurityUtils::base64_encode(salt);
    std::string encoded_hash = SecurityUtils::base64_encode(std::string(hash.begin(), hash.end()));
    
    return encoded_salt + "$" + encoded_hash;
}

bool PasswordManager::verify_password(const std::string& passphrase, const std::string& stored_hash) {
    // Parse stored hash: salt$hash
    size_t delimiter = stored_hash.find('$');
    if (delimiter == std::string::npos) {
        spdlog::warn("Invalid hash format encountered");
        return false;
    }
    
    std::string encoded_salt = stored_hash.substr(0, delimiter);
    std::string encoded_hash = stored_hash.substr(delimiter + 1);
    
    // Decode the stored components
    std::string salt = SecurityUtils::base64_decode(encoded_salt);
    std::string expected_hash = SecurityUtils::base64_decode(encoded_hash);
    
    // Hash the provided passphrase with the same salt
    std::vector<uint8_t> computed_hash(argon2_config_.hash_length);
    
    int result = argon2id_hash_raw(
        argon2_config_.time_cost,
        argon2_config_.memory_cost,
        argon2_config_.parallelism,
        passphrase.c_str(),
        passphrase.length(),
        salt.c_str(),
        salt.length(),
        computed_hash.data(),
        computed_hash.size()
    );
    
    if (result != ARGON2_OK) {
        spdlog::error("Passphrase verification failed: {}", argon2_error_message(result));
        return false;
    }
    
    // Constant-time comparison to prevent timing attacks
    std::string computed_hash_str(computed_hash.begin(), computed_hash.end());
    return SecurityUtils::secure_compare(computed_hash_str, expected_hash);
}

bool PasswordManager::is_password_strong(const std::string& passphrase) const {
    // Length check - passphrases should be longer than traditional passwords
    if (passphrase.length() < PasswordPolicy::MIN_LENGTH || 
        passphrase.length() > PasswordPolicy::MAX_LENGTH) {
        return false;
    }
    
    // Word count check - passphrases should have multiple words
    if (!has_minimum_word_count(passphrase)) {
        return false;
    }
    
    // Entropy check - I want passphrases that are actually memorable but secure
    if (!has_sufficient_entropy(passphrase)) {
        return false;
    }
    
    // Common passphrase checks
    if (is_in_common_passwords(passphrase)) return false;
    if (is_common_phrase(passphrase)) return false;
    if (is_keyboard_pattern(passphrase)) return false;
    if (is_repeated_pattern(passphrase)) return false;
    
    return true;
}

std::vector<std::string> PasswordManager::get_password_requirements() const {
    return {
        "At least 20 characters long",
        "Contains at least 4 words",
        "Not a common phrase or song lyric",
        "Not a common password",
        "No obvious patterns or repetition"
    };
}

bool PasswordManager::is_password_compromised(const std::string& passphrase) const {
    // In production, this would check against HaveIBeenPwned API
    // For now, just check against our known bad passwords and phrases
    std::string lower_passphrase = passphrase;
    std::transform(lower_passphrase.begin(), lower_passphrase.end(), 
                   lower_passphrase.begin(), ::tolower);
    
    return is_in_common_passwords(lower_passphrase) || is_common_phrase(lower_passphrase);
}

std::string PasswordManager::generate_secure_password(size_t length) const {
    // Character sets for password generation
    const std::string uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const std::string lowercase = "abcdefghijklmnopqrstuvwxyz";
    const std::string digits = "0123456789";
    const std::string special = "!@#$%^&*()_+-=[]{}|;:,.<>?";
    const std::string all_chars = uppercase + lowercase + digits + special;
    
    std::string password;
    password.reserve(length);
    
    // Ensure we have at least one character from each required set
    password += get_random_char(uppercase);
    password += get_random_char(lowercase);
    password += get_random_char(digits);
    password += get_random_char(special);
    
    // Fill the rest randomly
    for (size_t i = 4; i < length; ++i) {
        password += get_random_char(all_chars);
    }
    
    // Shuffle to avoid predictable patterns
    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::shuffle(password.begin(), password.end(), gen);
    }
    
    return password;
}

std::string PasswordManager::generate_secure_passphrase(size_t word_count) const {
    // Common English words that are easy to remember but not predictable
    const std::vector<std::string> word_list = {
        "apple", "beach", "castle", "dragon", "eagle", "forest", "garden", "house",
        "island", "jungle", "kitchen", "lighthouse", "mountain", "ocean", "palace",
        "queen", "river", "sunset", "tiger", "umbrella", "village", "waterfall",
        "xylophone", "yellow", "zebra", "butterfly", "chocolate", "diamond",
        "elephant", "fireworks", "giraffe", "hamburger", "icecream", "jellyfish",
        "kangaroo", "lemonade", "marshmallow", "notebook", "octopus", "penguin",
        "rainbow", "strawberry", "turtle", "unicorn", "volcano", "watermelon"
    };
    
    std::string passphrase;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, word_list.size() - 1);
    
    for (size_t i = 0; i < word_count; ++i) {
        if (i > 0) passphrase += " ";
        passphrase += word_list[dis(gen)];
    }
    
    return passphrase;
}

std::string PasswordManager::generate_reset_token(const std::string& user_id) {
    std::string token = generate_secure_token(32);
    auto expires_at = std::chrono::system_clock::now() + std::chrono::hours(1);
    
    // Store token with expiration - in production this goes to Redis
    reset_tokens_[token] = {user_id, expires_at};
    
    spdlog::info("Generated password reset token for user {}", user_id);
    return token;
}

bool PasswordManager::verify_reset_token(const std::string& token, const std::string& user_id) {
    auto it = reset_tokens_.find(token);
    if (it == reset_tokens_.end()) {
        return false;
    }
    
    const auto& [stored_user_id, expires_at] = it->second;
    
    // Check if token has expired
    if (std::chrono::system_clock::now() > expires_at) {
        reset_tokens_.erase(it);
        return false;
    }
    
    // Check if token belongs to the right user
    return stored_user_id == user_id;
}

void PasswordManager::invalidate_reset_token(const std::string& token) {
    reset_tokens_.erase(token);
}

// Private helper methods

bool PasswordManager::has_uppercase(const std::string& passphrase) const {
    return std::any_of(passphrase.begin(), passphrase.end(), ::isupper);
}

bool PasswordManager::has_lowercase(const std::string& passphrase) const {
    return std::any_of(passphrase.begin(), passphrase.end(), ::islower);
}

bool PasswordManager::has_digit(const std::string& passphrase) const {
    return std::any_of(passphrase.begin(), passphrase.end(), ::isdigit);
}

bool PasswordManager::has_special_char(const std::string& passphrase) const {
    const std::string special_chars = "!@#$%^&*()_+-=[]{}|;:,.<>?";
    return std::any_of(passphrase.begin(), passphrase.end(), 
                       [&special_chars](char c) {
                           return special_chars.find(c) != std::string::npos;
                       });
}

bool PasswordManager::has_sufficient_entropy(const std::string& passphrase) const {
    // Count unique characters - I want diversity
    std::set<char> unique_chars(passphrase.begin(), passphrase.end());
    return unique_chars.size() >= PasswordPolicy::MIN_UNIQUE_CHARS;
}

bool PasswordManager::is_in_common_passwords(const std::string& passphrase) const {
    std::string lower_passphrase = passphrase;
    std::transform(lower_passphrase.begin(), lower_passphrase.end(), 
                   lower_passphrase.begin(), ::tolower);
    
    return std::find(PasswordPolicy::FORBIDDEN_PASSWORDS.begin(),
                     PasswordPolicy::FORBIDDEN_PASSWORDS.end(),
                     lower_passphrase) != PasswordPolicy::FORBIDDEN_PASSWORDS.end();
}

bool PasswordManager::is_common_phrase(const std::string& passphrase) const {
    std::string lower_passphrase = passphrase;
    std::transform(lower_passphrase.begin(), lower_passphrase.end(), 
                   lower_passphrase.begin(), ::tolower);
    
    return std::find(PasswordPolicy::FORBIDDEN_PHRASES.begin(),
                     PasswordPolicy::FORBIDDEN_PHRASES.end(),
                     lower_passphrase) != PasswordPolicy::FORBIDDEN_PHRASES.end();
}

bool PasswordManager::is_keyboard_pattern(const std::string& passphrase) const {
    // Check for obvious keyboard patterns like "qwerty", "asdf", "123456"
    const std::vector<std::string> patterns = {
        "qwerty", "asdf", "zxcv", "123456", "abcdef", "qwertyuiop"
    };
    
    std::string lower_passphrase = passphrase;
    std::transform(lower_passphrase.begin(), lower_passphrase.end(), 
                   lower_passphrase.begin(), ::tolower);
    
    for (const auto& pattern : patterns) {
        if (lower_passphrase.find(pattern) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

bool PasswordManager::is_repeated_pattern(const std::string& passphrase) const {
    // Check for repeated characters or simple patterns
    if (passphrase.length() < 3) return false;
    
    // Check for repeated sequences
    for (size_t len = 1; len <= passphrase.length() / 2; ++len) {
        std::string pattern = passphrase.substr(0, len);
        std::string repeated = pattern + pattern;
        
        if (passphrase.find(repeated) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

bool PasswordManager::has_minimum_word_count(const std::string& passphrase) const {
    std::istringstream iss(passphrase);
    std::string word;
    size_t word_count = 0;
    
    while (iss >> word) {
        // Only count words that are at least 2 characters long
        if (word.length() >= 2) {
            word_count++;
        }
    }
    
    return word_count >= PasswordPolicy::MIN_WORD_COUNT;
}

std::string PasswordManager::generate_salt() const {
    return generate_secure_token(argon2_config_.salt_length);
}

std::string PasswordManager::generate_secure_token(size_t length) const {
    std::vector<uint8_t> random_bytes(length);
    
    if (RAND_bytes(random_bytes.data(), length) != 1) {
        throw std::runtime_error("Failed to generate secure random bytes");
    }
    
    return std::string(random_bytes.begin(), random_bytes.end());
}

} // namespace sonet::user
