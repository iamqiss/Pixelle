/*
 * Copyright (c) 2025 Neo Qiss
 * 
 * This file is part of Sonet - a social media platform built for real connections.
 * 
 * Implementation of the Elasticsearch engine for Twitter-scale search operations.
 * This is where the magic happens - sub-second search across billions of documents
 * with intelligent ranking and real-time updates.
 */

#include "elasticsearch_engine.h"
#ifdef HAVE_CURL
#include <curl/curl.h>
#endif
#include <thread>
#include <queue>
#include <condition_variable>
#include <regex>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <random>
#include <future>
#include <stdexcept>

namespace sonet {
namespace search_service {
namespace engines {

// PIMPL implementation
struct ElasticsearchEngine::Impl {
    ElasticsearchConfig config;
#ifdef HAVE_CURL
    CURL* curl_handle;
#endif
    std::atomic<bool> initialized{false};
    std::atomic<bool> shutdown_requested{false};
    std::atomic<bool> debug_mode{false};
    
    // Metrics
    mutable std::mutex metrics_mutex;
    SearchMetrics metrics;
    
    // Bulk operations queue
    std::queue<BulkOperation> bulk_queue;
    std::mutex bulk_queue_mutex;
    std::condition_variable bulk_queue_cv;
    std::thread bulk_processor_thread;
    
    // Cache for search results
    struct CacheEntry {
        nlohmann::json data;
        std::chrono::system_clock::time_point expiry;
    };
    std::unordered_map<std::string, CacheEntry> search_cache;
    mutable std::mutex cache_mutex;
    
    // Slow query log
    std::vector<nlohmann::json> slow_queries;
    mutable std::mutex slow_queries_mutex;
    static constexpr size_t MAX_SLOW_QUERIES = 100;
    
    Impl(const ElasticsearchConfig& cfg) : config(cfg)
#ifdef HAVE_CURL
    , curl_handle(nullptr)
#endif
    {
#ifdef HAVE_CURL
        curl_handle = curl_easy_init();
        if (curl_handle) {
            curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L);
            curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, static_cast<long>(config.request_timeout.count()));
            curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, static_cast<long>(config.connection_timeout.count()));
            curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYPEER, config.verify_ssl ? 1L : 0L);
        }
#endif
        // Start bulk processor thread
        bulk_processor_thread = std::thread([this] { bulk_processor_loop(); });
    }
    
    ~Impl() {
        shutdown_requested = true;
        bulk_queue_cv.notify_all();
        if (bulk_processor_thread.joinable()) {
            bulk_processor_thread.join();
        }
#ifdef HAVE_CURL
        if (curl_handle) {
            curl_easy_cleanup(curl_handle);
            curl_handle = nullptr;
        }
#endif
    }
    
    void bulk_processor_loop();
#ifdef HAVE_CURL
    static size_t write_callback(void* contents, size_t size, size_t nmemb, void* userp) {
        size_t total = size * nmemb;
        std::string* s = static_cast<std::string*>(userp);
        s->append(static_cast<char*>(contents), total);
        return total;
    }
#endif
    std::string execute_http_request(const std::string& method,
                                     const std::string& url,
                                     const std::string& body,
                                     const std::unordered_map<std::string, std::string>& headers) {
#ifdef HAVE_CURL
        if (!curl_handle) {
            throw std::runtime_error("CURL handle not initialized");
        }
        std::string response;
        struct curl_slist* header_list = nullptr;
        for (const auto& [k, v] : headers) {
            std::string h = k + ": " + v;
            header_list = curl_slist_append(header_list, h.c_str());
        }
        curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, header_list);
        curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, &Impl::write_callback);
        curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &response);

        if (method == "GET") {
            curl_easy_setopt(curl_handle, CURLOPT_HTTPGET, 1L);
        } else if (method == "NOTE") {
            curl_easy_setopt(curl_handle, CURLOPT_POST, 1L);
            curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, body.c_str());
        } else if (method == "PUT") {
            curl_easy_setopt(curl_handle, CURLOPT_CUSTOMREQUEST, "PUT");
            curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, body.c_str());
        } else if (method == "DELETE") {
            curl_easy_setopt(curl_handle, CURLOPT_CUSTOMREQUEST, "DELETE");
        } else {
            curl_easy_setopt(curl_handle, CURLOPT_CUSTOMREQUEST, method.c_str());
            if (!body.empty()) {
                curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, body.c_str());
            }
        }

        CURLcode res = curl_easy_perform(curl_handle);
        long http_code = 0;
        curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &http_code);
        if (header_list) curl_slist_free_all(header_list);
        if (res != CURLE_OK) {
            throw std::runtime_error(std::string("CURL error: ") + curl_easy_strerror(res));
        }
        if (http_code >= 400) {
            // Return response anyway; caller will inspect and handle
        }
        return response;
#else
        (void)method; (void)url; (void)body; (void)headers;
        throw std::runtime_error("CURL not available (HAVE_CURL is off)");
#endif
    }
};

/**
 * ElasticsearchConfig implementation
 */
bool ElasticsearchConfig::is_valid() const {
    return !hosts.empty() && 
           connection_timeout.count() > 0 &&
           request_timeout.count() > 0 &&
           max_connections > 0 &&
           !notes_index.empty() &&
           !users_index.empty();
}

ElasticsearchConfig ElasticsearchConfig::production_config() {
    ElasticsearchConfig config;
    config.hosts = {"https://es-cluster-1:9200", "https://es-cluster-2:9200", "https://es-cluster-3:9200"};
    config.use_ssl = true;
    config.verify_ssl = true;
    config.connection_timeout = std::chrono::seconds{10};
    config.request_timeout = std::chrono::seconds{30};
    config.max_connections = 200;
    config.max_connections_per_host = 50;
    config.bulk_index_size = 5000;
    config.bulk_flush_interval = std::chrono::milliseconds{2000};
    config.enable_request_cache = true;
    config.cache_ttl = std::chrono::minutes{10};
    config.slow_query_threshold = std::chrono::milliseconds{500};
    return config;
}

ElasticsearchConfig ElasticsearchConfig::development_config() {
    ElasticsearchConfig config;
    config.hosts = {"http://localhost:9200"};
    config.use_ssl = false;
    config.verify_ssl = false;
    config.connection_timeout = std::chrono::seconds{30};
    config.request_timeout = std::chrono::seconds{60};
    config.bulk_index_size = 100;
    config.bulk_flush_interval = std::chrono::milliseconds{5000};
    config.slow_query_threshold = std::chrono::milliseconds{2000};
    return config;
}

ElasticsearchConfig ElasticsearchConfig::from_environment() {
    ElasticsearchConfig config;
    
    // Parse hosts from environment
    const char* hosts_env = std::getenv("ELASTICSEARCH_HOSTS");
    if (hosts_env) {
        std::string hosts_str(hosts_env);
        std::istringstream ss(hosts_str);
        std::string host;
        config.hosts.clear();
        while (std::getline(ss, host, ',')) {
            config.hosts.push_back(host);
        }
    }
    
    // Authentication
    const char* username = std::getenv("ELASTICSEARCH_USERNAME");
    if (username) config.username = username;
    
    const char* password = std::getenv("ELASTICSEARCH_PASSWORD");
    if (password) config.password = password;
    
    const char* api_key = std::getenv("ELASTICSEARCH_API_KEY");
    if (api_key) config.api_key = api_key;
    
    // SSL settings
    const char* use_ssl = std::getenv("ELASTICSEARCH_USE_SSL");
    if (use_ssl) config.use_ssl = (std::string(use_ssl) == "true");
    
    return config;
}

/**
 * IndexMappings implementation
 */
nlohmann::json IndexMappings::get_notes_mapping() {
    return nlohmann::json{
        {"mappings", {
            {"properties", {
                {"id", {{"type", "keyword"}}},
                {"user_id", {{"type", "keyword"}}},
                {"username", {{"type", "keyword"}}},
                {"display_name", {
                    {"type", "text"},
                    {"analyzer", "standard"},
                    {"fields", {
                        {"keyword", {{"type", "keyword"}}}
                    }}
                }},
                {"content", {
                    {"type", "text"},
                    {"analyzer", "sonet_text_analyzer"},
                    {"search_analyzer", "sonet_search_analyzer"},
                    {"fields", {
                        {"raw", {{"type", "keyword"}}},
                        {"stemmed", {{"type", "text"}, {"analyzer", "stemmed_analyzer"}}}
                    }}
                }},
                {"hashtags", {
                    {"type", "keyword"},
                    {"normalizer", "lowercase_normalizer"}
                }},
                {"mentions", {{"type", "keyword"}}},
                {"media_urls", {{"type", "keyword"}}},
                {"language", {{"type", "keyword"}}},
                {"created_at", {{"type", "date"}}},
                {"updated_at", {{"type", "date"}}},
                {"location", {{"type", "geo_point"}}},
                {"place_name", {
                    {"type", "text"},
                    {"analyzer", "standard"}
                }},
                {"is_reply", {{"type", "boolean"}}},
                {"reply_to_id", {{"type", "keyword"}}},
                {"is_renote", {{"type", "boolean"}}},
                {"renote_of_id", {{"type", "keyword"}}},
                {"thread_id", {{"type", "keyword"}}},
                {"visibility", {{"type", "keyword"}}},
                {"nsfw", {{"type", "boolean"}}},
                {"sensitive", {{"type", "boolean"}}},
                {"metrics", {
                    {"properties", {
                        {"likes_count", {{"type", "integer"}}},
                        {"renotes_count", {{"type", "integer"}}},
                        {"replies_count", {{"type", "integer"}}},
                        {"views_count", {{"type", "long"}}},
                        {"engagement_score", {{"type", "float"}}},
                        {"virality_score", {{"type", "float"}}},
                        {"trending_score", {{"type", "float"}}}
                    }}
                }},
                {"user_metrics", {
                    {"properties", {
                        {"followers_count", {{"type", "integer"}}},
                        {"following_count", {{"type", "integer"}}},
                        {"reputation_score", {{"type", "float"}}},
                        {"verification_level", {{"type", "keyword"}}}
                    }}
                }},
                {"boost_factors", {
                    {"properties", {
                        {"recency_boost", {{"type", "float"}}},
                        {"engagement_boost", {{"type", "float"}}},
                        {"author_boost", {{"type", "float"}}},
                        {"content_quality_boost", {{"type", "float"}}}
                    }}
                }},
                {"indexing_metadata", {
                    {"properties", {
                        {"indexed_at", {{"type", "date"}}},
                        {"version", {{"type", "integer"}}},
                        {"source", {{"type", "keyword"}}}
                    }}
                }}
            }}
        }}
    };
}

nlohmann::json IndexMappings::get_users_mapping() {
    return nlohmann::json{
        {"mappings", {
            {"properties", {
                {"id", {{"type", "keyword"}}},
                {"username", {
                    {"type", "text"},
                    {"analyzer", "username_analyzer"},
                    {"fields", {
                        {"keyword", {{"type", "keyword"}}},
                        {"suggest", {{"type", "completion"}}}
                    }}
                }},
                {"display_name", {
                    {"type", "text"},
                    {"analyzer", "standard"},
                    {"fields", {
                        {"keyword", {{"type", "keyword"}}},
                        {"suggest", {{"type", "completion"}}}
                    }}
                }},
                {"bio", {
                    {"type", "text"},
                    {"analyzer", "sonet_text_analyzer"}
                }},
                {"location", {{"type", "geo_point"}}},
                {"location_name", {
                    {"type", "text"},
                    {"analyzer", "standard"}
                }},
                {"website", {{"type", "keyword"}}},
                {"created_at", {{"type", "date"}}},
                {"updated_at", {{"type", "date"}}},
                {"last_active_at", {{"type", "date"}}},
                {"verification", {
                    {"properties", {
                        {"is_verified", {{"type", "boolean"}}},
                        {"verification_type", {{"type", "keyword"}}},
                        {"verified_at", {{"type", "date"}}}
                    }}
                }},
                {"metrics", {
                    {"properties", {
                        {"followers_count", {{"type", "integer"}}},
                        {"following_count", {{"type", "integer"}}},
                        {"notes_count", {{"type", "integer"}}},
                        {"likes_given", {{"type", "long"}}},
                        {"likes_received", {{"type", "long"}}},
                        {"reputation_score", {{"type", "float"}}},
                        {"influence_score", {{"type", "float"}}},
                        {"engagement_rate", {{"type", "float"}}}
                    }}
                }},
                {"interests", {{"type", "keyword"}}},
                {"languages", {{"type", "keyword"}}},
                {"timezone", {{"type", "keyword"}}},
                {"privacy", {
                    {"properties", {
                        {"is_private", {{"type", "boolean"}}},
                        {"searchable", {{"type", "boolean"}}},
                        {"indexable", {{"type", "boolean"}}}
                    }}
                }},
                {"activity_score", {{"type", "float"}}},
                {"content_quality_score", {{"type", "float"}}},
                {"spam_score", {{"type", "float"}}},
                {"indexing_metadata", {
                    {"properties", {
                        {"indexed_at", {{"type", "date"}}},
                        {"version", {{"type", "integer"}}},
                        {"source", {{"type", "keyword"}}}
                    }}
                }}
            }}
        }}
    };
}

nlohmann::json IndexMappings::get_hashtags_mapping() {
    return nlohmann::json{
        {"mappings", {
            {"properties", {
                {"tag", {
                    {"type", "keyword"},
                    {"normalizer", "lowercase_normalizer"}
                }},
                {"normalized_tag", {{"type", "keyword"}}},
                {"category", {{"type", "keyword"}}},
                {"language", {{"type", "keyword"}}},
                {"first_used_at", {{"type", "date"}}},
                {"last_used_at", {{"type", "date"}}},
                {"usage_stats", {
                    {"properties", {
                        {"total_uses", {{"type", "long"}}},
                        {"unique_users", {{"type", "integer"}}},
                        {"daily_uses", {{"type", "integer"}}},
                        {"weekly_uses", {{"type", "integer"}}},
                        {"monthly_uses", {{"type", "integer"}}}
                    }}
                }},
                {"trending_metrics", {
                    {"properties", {
                        {"trending_score", {{"type", "float"}}},
                        {"velocity", {{"type", "float"}}},
                        {"momentum", {{"type", "float"}}},
                        {"peak_rank", {{"type", "integer"}}},
                        {"current_rank", {{"type", "integer"}}}
                    }}
                }},
                {"related_tags", {{"type", "keyword"}}},
                {"sentiment_score", {{"type", "float"}}},
                {"spam_score", {{"type", "float"}}},
                {"nsfw_score", {{"type", "float"}}}
            }}
        }}
    };
}

nlohmann::json IndexMappings::get_suggestions_mapping() {
    return nlohmann::json{
        {"mappings", {
            {"properties", {
                {"suggest", {
                    {"type", "completion"},
                    {"analyzer", "simple"},
                    {"preserve_separators", true},
                    {"preserve_position_increments", true},
                    {"max_input_length", 50}
                }},
                {"text", {{"type", "keyword"}}},
                {"type", {{"type", "keyword"}}},  // user, hashtag, note
                {"weight", {{"type", "integer"}}},
                {"payload", {{"enabled", false}}},
                {"context", {
                    {"properties", {
                        {"language", {{"type", "keyword"}}},
                        {"category", {{"type", "keyword"}}},
                        {"popularity", {{"type", "integer"}}}
                    }}
                }}
            }}
        }}
    };
}

nlohmann::json IndexMappings::get_index_settings() {
    return nlohmann::json{
        {"settings", {
            {"number_of_shards", 5},
            {"number_of_replicas", 1},
            {"refresh_interval", "1s"},
            {"max_result_window", 50000},
            {"analysis", {
                {"analyzer", {
                    {"sonet_text_analyzer", {
                        {"type", "custom"},
                        {"tokenizer", "standard"},
                        {"filter", {
                            "lowercase",
                            "stop",
                            "sonet_hashtag_filter",
                            "sonet_mention_filter",
                            "sonet_url_filter",
                            "asciifolding"
                        }}
                    }},
                    {"sonet_search_analyzer", {
                        {"type", "custom"},
                        {"tokenizer", "standard"},
                        {"filter", {
                            "lowercase",
                            "stop",
                            "asciifolding"
                        }}
                    }},
                    {"username_analyzer", {
                        {"type", "custom"},
                        {"tokenizer", "keyword"},
                        {"filter", {"lowercase"}}
                    }},
                    {"stemmed_analyzer", {
                        {"type", "custom"},
                        {"tokenizer", "standard"},
                        {"filter", {
                            "lowercase",
                            "stop",
                            "porter_stem"
                        }}
                    }}
                }},
                {"normalizer", {
                    {"lowercase_normalizer", {
                        {"type", "custom"},
                        {"filter", {"lowercase"}}
                    }}
                }},
                {"filter", {
                    {"sonet_hashtag_filter", {
                        {"type", "pattern_capture"},
                        {"preserve_original", true},
                        {"patterns", {"#(\\w+)"}}
                    }},
                    {"sonet_mention_filter", {
                        {"type", "pattern_capture"},
                        {"preserve_original", true},
                        {"patterns", {"@(\\w+)"}}
                    }},
                    {"sonet_url_filter", {
                        {"type", "pattern_replace"},
                        {"pattern", "https?://[^\\s]+"},
                        {"replacement", ""}
                    }}
                }}
            }},
            {"similarity", {
                {"sonet_similarity", {
                    {"type", "BM25"},
                    {"k1", 1.2},
                    {"b", 0.75}
                }}
            }}
        }}
    };
}

/**
 * SearchMetrics implementation
 */
nlohmann::json SearchMetrics::to_json() const {
    auto now = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - last_reset).count();
    
    return nlohmann::json{
        {"total_searches", total_searches.load()},
        {"successful_searches", successful_searches.load()},
        {"failed_searches", failed_searches.load()},
        {"cached_searches", cached_searches.load()},
        {"slow_searches", slow_searches.load()},
        {"total_query_time_ms", total_query_time_ms.load()},
        {"total_elasticsearch_time_ms", total_elasticsearch_time_ms.load()},
        {"total_cache_time_ms", total_cache_time_ms.load()},
        {"total_documents_searched", total_documents_searched.load()},
        {"total_results_returned", total_results_returned.load()},
        {"average_query_time_ms", get_average_query_time_ms()},
        {"success_rate", get_success_rate()},
        {"cache_hit_rate", get_cache_hit_rate()},
        {"uptime_seconds", duration}
    };
}

void SearchMetrics::reset() {
    total_searches = 0;
    successful_searches = 0;
    failed_searches = 0;
    cached_searches = 0;
    slow_searches = 0;
    total_query_time_ms = 0;
    total_elasticsearch_time_ms = 0;
    total_cache_time_ms = 0;
    total_documents_searched = 0;
    total_results_returned = 0;
    last_reset = std::chrono::system_clock::now();
}

double SearchMetrics::get_average_query_time_ms() const {
    long total = total_searches.load();
    return total > 0 ? static_cast<double>(total_query_time_ms.load()) / total : 0.0;
}

double SearchMetrics::get_success_rate() const {
    long total = total_searches.load();
    return total > 0 ? static_cast<double>(successful_searches.load()) / total : 0.0;
}

double SearchMetrics::get_cache_hit_rate() const {
    long total = total_searches.load();
    return total > 0 ? static_cast<double>(cached_searches.load()) / total : 0.0;
}

/**
 * BulkOperation implementation
 */
std::string BulkOperation::to_bulk_format() const {
    std::string result;
    
    switch (operation_type) {
        case Type::INDEX:
            result += nlohmann::json{
                {"index", {
                    {"_index", index_name},
                    {"_id", document_id}
                }}
            }.dump() + "\n";
            result += document.dump() + "\n";
            break;
            
        case Type::UPDATE:
            result += nlohmann::json{
                {"update", {
                    {"_index", index_name},
                    {"_id", document_id}
                }}
            }.dump() + "\n";
            result += nlohmann::json{{"doc", document}}.dump() + "\n";
            break;
            
        case Type::DELETE:
            result += nlohmann::json{
                {"delete", {
                    {"_index", index_name},
                    {"_id", document_id}
                }}
            }.dump() + "\n";
            break;
    }
    
    return result;
}

/**
 * ElasticsearchEngine implementation
 */
ElasticsearchEngine::ElasticsearchEngine(const ElasticsearchConfig& config)
    : pimpl_(std::make_unique<Impl>(config)) {
    if (!config.is_valid()) {
        throw std::invalid_argument("Invalid Elasticsearch configuration");
    }
}

ElasticsearchEngine::~ElasticsearchEngine() = default;

std::future<bool> ElasticsearchEngine::initialize() {
    return std::async(std::launch::async, [this]() {
        try {
            // Test connection
            auto health_future = get_cluster_health();
            auto health = health_future.get();
            
            if (health["status"] == "red") {
                return false;
            }
            
            // Create indices and templates
            if (!create_indices().get()) {
                return false;
            }
            
            if (!create_templates().get()) {
                return false;
            }
            
            pimpl_->initialized = true;
            return true;
        } catch (const std::exception& e) {
            return false;
        }
    });
}

void ElasticsearchEngine::shutdown() {
    pimpl_->shutdown_requested = true;
    
    // Flush any pending bulk operations
    flush_bulk_queue().get();
}

bool ElasticsearchEngine::is_ready() const {
    return pimpl_->initialized.load() && !pimpl_->shutdown_requested.load();
}

std::future<nlohmann::json> ElasticsearchEngine::get_cluster_health() {
    return execute_request("GET", "/_cluster/health");
}

std::string ElasticsearchEngine::build_url(
    const std::string& path,
    const std::unordered_map<std::string, std::string>& params) const {
    if (pimpl_->config.hosts.empty()) {
        throw std::runtime_error("Elasticsearch hosts not configured");
    }
    // Use first host for now; production should round-robin
    std::ostringstream oss;
    std::string base = pimpl_->config.hosts.front();
    if (base.rfind("http://", 0) != 0 && base.rfind("https://", 0) != 0) {
        base = std::string(pimpl_->config.use_ssl ? "https://" : "http://") + base;
    }
    if (!path.empty() && path[0] != '/') {
        oss << base << "/" << path;
    } else {
        oss << base << path;
    }
    if (!params.empty()) {
        bool first = true;
        oss << "?";
        for (const auto& [k, v] : params) {
            if (!first) oss << "&";
            first = false;
            oss << k << "=" << v; // TODO: URL-encode if needed
        }
    }
    return oss.str();
}

std::future<nlohmann::json> ElasticsearchEngine::execute_request(
    const std::string& method,
    const std::string& path,
    const nlohmann::json& body,
    const std::unordered_map<std::string, std::string>& params) {
    return std::async(std::launch::async, [this, method, path, body, params]() {
        // Build URL
        const std::string url = build_url(path, params);

        // Prepare headers
        std::unordered_map<std::string, std::string> headers;
        headers["Content-Type"] = "application/json";
        if (!pimpl_->config.api_key.empty()) {
            headers["Authorization"] = std::string("ApiKey ") + pimpl_->config.api_key;
        } else if (!pimpl_->config.username.empty()) {
            // Basic auth via libcurl option; still set header if needed by proxies
        }

        // Serialize body
        const std::string body_str = body.is_null() || body.empty() ? std::string() : body.dump();

        // Use Impl::execute_http_request to actually send
        const std::string response_str = pimpl_->execute_http_request(method, url, body_str, headers);
        if (response_str.empty()) {
            throw std::runtime_error("Empty response from Elasticsearch");
        }
        nlohmann::json json_resp = nlohmann::json::parse(response_str, nullptr, true);

        // Handle ES errors
        if (json_resp.contains("error")) {
            // Update metrics as failure
            handle_error(json_resp);
        }
        return json_resp;
    });
}

std::future<models::SearchResult> ElasticsearchEngine::search(const models::SearchQuery& query) {
    return std::async(std::launch::async, [this, query]() {
        auto start_time = std::chrono::steady_clock::now();
        
        try {
            // Check cache first
            std::string cache_key = query.get_cache_key();
            {
                std::lock_guard<std::mutex> lock(pimpl_->cache_mutex);
                auto it = pimpl_->search_cache.find(cache_key);
                if (it != pimpl_->search_cache.end() && 
                    it->second.expiry > std::chrono::system_clock::now()) {
                    
                    auto end_time = std::chrono::steady_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
                    
                    pimpl_->update_metrics(true, duration, std::chrono::milliseconds{0}, true, 0, 0);
                    
                    return models::SearchResult::from_json(it->second.data);
                }
            }
            
            // Build Elasticsearch query
            nlohmann::json es_query = query.to_elasticsearch_query();
            
            // Determine target indices
            auto indices = elasticsearch_utils::get_target_indices(query.get_search_type(), pimpl_->config);
            std::string index_list = "";
            for (size_t i = 0; i < indices.size(); ++i) {
                if (i > 0) index_list += ",";
                index_list += indices[i];
            }
            
            // Execute search
            auto es_start = std::chrono::steady_clock::now();
            auto response = execute_request("POST", "/" + index_list + "/_search", es_query).get();
            auto es_end = std::chrono::steady_clock::now();
            
            // Parse results
            models::SearchResult result = models::SearchResult::from_elasticsearch_response(response);
            
            // Cache results
            if (pimpl_->config.enable_request_cache) {
                std::lock_guard<std::mutex> lock(pimpl_->cache_mutex);
                pimpl_->search_cache[cache_key] = {
                    result.to_json(),
                    std::chrono::system_clock::now() + pimpl_->config.cache_ttl
                };
            }
            
            auto end_time = std::chrono::steady_clock::now();
            auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            auto es_duration = std::chrono::duration_cast<std::chrono::milliseconds>(es_end - es_start);
            
            // Update metrics
            long docs_searched = response.value("hits", nlohmann::json{}).value("total", nlohmann::json{}).value("value", 0);
            long results_returned = result.get_note_results().size() + result.get_user_results().size();
            
            pimpl_->update_metrics(true, total_duration, es_duration, false, docs_searched, results_returned);
            
            // Check for slow query
            if (total_duration >= pimpl_->config.slow_query_threshold) {
                pimpl_->log_slow_query(query, total_duration);
            }
            
            return result;
            
        } catch (const std::exception& e) {
            auto end_time = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            pimpl_->update_metrics(false, duration, std::chrono::milliseconds{0}, false, 0, 0);
            
            throw;
        }
    });
}

std::future<bool> ElasticsearchEngine::index_note(const std::string& note_id, const nlohmann::json& note_document) {
    return std::async(std::launch::async, [this, note_id, note_document]() {
        try {
            auto response = execute_request(
                "PUT", 
                "/" + pimpl_->config.notes_index + "/_doc/" + note_id,
                note_document
            ).get();
            
            return response.value("result", "") == "created" || response.value("result", "") == "updated";
        } catch (const std::exception& e) {
            return false;
        }
    });
}

std::future<bool> ElasticsearchEngine::create_indices() {
    return std::async(std::launch::async, [this]() {
        try {
            // Create notes index
            nlohmann::json notes_mapping = IndexMappings::get_notes_mapping();
            notes_mapping.merge_patch(IndexMappings::get_index_settings());
            
            auto notes_response = execute_request(
                "PUT",
                "/" + pimpl_->config.notes_index,
                notes_mapping
            ).get();
            
            // Create users index
            nlohmann::json users_mapping = IndexMappings::get_users_mapping();
            users_mapping.merge_patch(IndexMappings::get_index_settings());
            
            auto users_response = execute_request(
                "PUT",
                "/" + pimpl_->config.users_index,
                users_mapping
            ).get();
            
            // Create hashtags index
            nlohmann::json hashtags_mapping = IndexMappings::get_hashtags_mapping();
            hashtags_mapping.merge_patch(IndexMappings::get_index_settings());
            
            auto hashtags_response = execute_request(
                "PUT",
                "/" + pimpl_->config.hashtags_index,
                hashtags_mapping
            ).get();
            
            return true;
        } catch (const std::exception& e) {
            return false;
        }
    });
}

// Continue with more implementation...
void ElasticsearchEngine::Impl::bulk_processor_loop() {
    while (!shutdown_requested) {
        std::unique_lock<std::mutex> lock(bulk_queue_mutex);
        
        // Wait for operations or shutdown
        bulk_queue_cv.wait_for(lock, config.bulk_flush_interval, [this] {
            return !bulk_queue.empty() || shutdown_requested;
        });
        
        if (bulk_queue.empty()) {
            continue;
        }
        
        // Collect operations to process
        std::vector<BulkOperation> operations_to_process;
        while (!bulk_queue.empty() && operations_to_process.size() < config.bulk_index_size) {
            operations_to_process.push_back(bulk_queue.front());
            bulk_queue.pop();
        }
        
        lock.unlock();
        
        // Process bulk operations
        if (!operations_to_process.empty()) {
            try {
                std::string bulk_body;
                for (const auto& op : operations_to_process) {
                    bulk_body += op.to_bulk_format();
                }
                
                // Execute bulk request
                // This would use the HTTP client to send to /_bulk endpoint
                // Implementation details omitted for brevity
            } catch (const std::exception& e) {
                // Log error and continue
            }
        }
    }
}

void ElasticsearchEngine::Impl::update_metrics(bool success, std::chrono::milliseconds query_time, 
                                               std::chrono::milliseconds es_time, bool from_cache,
                                               long documents_searched, long results_returned) {
    std::lock_guard<std::mutex> lock(metrics_mutex);
    
    metrics.total_searches++;
    if (success) {
        metrics.successful_searches++;
    } else {
        metrics.failed_searches++;
    }
    
    if (from_cache) {
        metrics.cached_searches++;
        metrics.total_cache_time_ms += query_time.count();
    } else {
        metrics.total_elasticsearch_time_ms += es_time.count();
    }
    
    metrics.total_query_time_ms += query_time.count();
    metrics.total_documents_searched += documents_searched;
    metrics.total_results_returned += results_returned;
    
    // Check if it's a slow query
    if (query_time >= config.slow_query_threshold) {
        metrics.slow_searches++;
    }
}

/**
 * Utility functions
 */
namespace elasticsearch_utils {

std::string generate_time_based_index_name(const std::string& base_name,
                                          const std::chrono::system_clock::time_point& time,
                                          const std::string& format) {
    auto time_t = std::chrono::system_clock::to_time_t(time);
    std::tm* tm = std::gmtime(&time_t);
    
    std::ostringstream oss;
    oss << base_name << "_";
    
    if (format == "yyyy.MM.dd") {
        oss << std::put_time(tm, "%Y.%m.%d");
    } else if (format == "yyyy.MM") {
        oss << std::put_time(tm, "%Y.%m");
    } else if (format == "yyyy") {
        oss << std::put_time(tm, "%Y");
    }
    
    return oss.str();
}

std::vector<std::string> get_target_indices(models::SearchType search_type, const ElasticsearchConfig& config) {
    switch (search_type) {
        case models::SearchType::NOTES:
            return {config.notes_index};
        case models::SearchType::USERS:
            return {config.users_index};
        case models::SearchType::HASHTAGS:
            return {config.hashtags_index};
        case models::SearchType::ALL:
            return {config.notes_index, config.users_index, config.hashtags_index};
        default:
            return {config.notes_index};
    }
}

std::string escape_query_string(const std::string& query) {
    std::string escaped = query;
    std::vector<char> special_chars = {'+', '-', '=', '&', '|', '>', '<', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\\', '/'};
    
    for (char c : special_chars) {
        std::string from(1, c);
        std::string to = "\\" + from;
        size_t pos = 0;
        while ((pos = escaped.find(from, pos)) != std::string::npos) {
            escaped.replace(pos, 1, to);
            pos += 2;
        }
    }
    
    return escaped;
}

nlohmann::json build_date_range_filter(const std::string& field,
                                       const std::optional<std::chrono::system_clock::time_point>& from,
                                       const std::optional<std::chrono::system_clock::time_point>& to) {
    nlohmann::json range_filter = {
        {"range", {
            {field, nlohmann::json::object()}
        }}
    };
    
    if (from.has_value()) {
        auto time_t = std::chrono::system_clock::to_time_t(from.value());
        range_filter["range"][field]["gte"] = time_t;
    }
    
    if (to.has_value()) {
        auto time_t = std::chrono::system_clock::to_time_t(to.value());
        range_filter["range"][field]["lte"] = time_t;
    }
    
    return range_filter;
}

} // namespace elasticsearch_utils

} // namespace engines
} // namespace search_service
} // namespace sonet