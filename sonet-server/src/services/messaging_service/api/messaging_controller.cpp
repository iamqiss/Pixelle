/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "include/messaging_controller.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/json.hpp>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <sodium.h>
#include "../../user_service/include/jwt_manager.h"

namespace sonet::messaging::api {

// AttachmentMetadata implementation
Json::Value AttachmentMetadata::to_json() const {
    Json::Value json;
    json["attachment_id"] = attachment_id;
    json["filename"] = filename;
    json["mime_type"] = mime_type;
    json["file_size"] = static_cast<Json::UInt64>(file_size);
    json["encryption_key"] = encryption_key;
    json["checksum"] = checksum;
    json["storage_path"] = storage_path;
    json["thumbnail_path"] = thumbnail_path;
    json["uploaded_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        uploaded_at.time_since_epoch()).count();
    json["expires_at"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        expires_at.time_since_epoch()).count();
    json["access_count"] = access_count;
    json["is_encrypted"] = is_encrypted;
    return json;
}

AttachmentMetadata AttachmentMetadata::from_json(const Json::Value& json) {
    AttachmentMetadata metadata;
    metadata.attachment_id = json["attachment_id"].asString();
    metadata.filename = json["filename"].asString();
    metadata.mime_type = json["mime_type"].asString();
    metadata.file_size = json["file_size"].asUInt64();
    metadata.encryption_key = json["encryption_key"].asString();
    metadata.checksum = json["checksum"].asString();
    metadata.storage_path = json["storage_path"].asString();
    metadata.thumbnail_path = json["thumbnail_path"].asString();
    
    auto uploaded_ms = json["uploaded_at"].asInt64();
    metadata.uploaded_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(uploaded_ms));
    
    auto expires_ms = json["expires_at"].asInt64();
    metadata.expires_at = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(expires_ms));
    
    metadata.access_count = json["access_count"].asUInt();
    metadata.is_encrypted = json["is_encrypted"].asBool();
    
    return metadata;
}

bool AttachmentMetadata::is_expired() const {
    return std::chrono::system_clock::now() > expires_at;
}

void AttachmentMetadata::increment_access() {
    access_count++;
}

// APIResponse implementation
Json::Value APIResponse::to_json() const {
    Json::Value json;
    json["success"] = success;
    json["message"] = message;
    json["error_code"] = error_code;
    json["data"] = data;
    json["timestamp"] = std::chrono::duration_cast<std::chrono::milliseconds>(
        timestamp.time_since_epoch()).count();
    json["request_id"] = request_id;
    return json;
}

APIResponse APIResponse::success(const std::string& message, const Json::Value& data) {
    APIResponse response;
    response.success = true;
    response.message = message;
    response.data = data;
    response.timestamp = std::chrono::system_clock::now();
    response.request_id = generate_request_id();
    return response;
}

APIResponse APIResponse::error(const std::string& message, const std::string& error_code) {
    APIResponse response;
    response.success = false;
    response.message = message;
    response.error_code = error_code;
    response.timestamp = std::chrono::system_clock::now();
    response.request_id = generate_request_id();
    return response;
}

std::string APIResponse::generate_request_id() {
    if (sodium_init() < 0) {
        // Fallback: timestamp-based
        std::stringstream ss; ss << "req_" << std::hex << std::time(nullptr);
        return ss.str();
    }
    unsigned char buf[16];
    randombytes_buf(buf, sizeof(buf));
    std::stringstream ss;
    ss << "req_";
    for (unsigned char b : buf) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(b);
    }
    return ss.str();
}

// MessagingController implementation
MessagingController::MessagingController(uint16_t http_port, uint16_t websocket_port)
    : http_port_(http_port), websocket_port_(websocket_port),
      max_file_size_(100 * 1024 * 1024), // 100MB
      max_concurrent_uploads_(10),
      running_(false) {
    
    // Initialize components
    message_service_ = std::make_unique<core::MessageService>();
    chat_service_ = std::make_unique<core::ChatService>();
    crypto_engine_ = std::make_unique<crypto::CryptoEngine>();
    websocket_manager_ = std::make_unique<realtime::WebSocketManager>(websocket_port);
    encryption_manager_ = std::make_unique<encryption::EncryptionManager>();

    // JWT manager for token validation (read signing key from env)
    const char* jwt_secret = std::getenv("SONET_JWT_SECRET");
    jwt_manager_ = std::make_unique<sonet::user::JWTManager>(jwt_secret ? jwt_secret : std::string("dev_secret"));

    // Set up WebSocket authentication callback
    websocket_manager_->set_authentication_callback(
        [this](const std::string& user_id, const std::string& token) {
            if (!jwt_manager_) return false;
            auto claims = jwt_manager_->verify_token(token);
            if (!claims.has_value()) return false;
            // Optional: compare user id in token
            auto token_user = jwt_manager_->get_user_id_from_token(token);
            return token_user.has_value() && token_user.value() == user_id && !jwt_manager_->is_token_blacklisted(token);
        }
    );
    
    // Initialize HTTP server
    init_http_server();
    
    // Set up supported MIME types for attachments
    supported_mime_types_ = {
        "image/jpeg", "image/png", "image/gif", "image/webp",
        "video/mp4", "video/webm", "video/mov", "video/avi",
        "audio/mp3", "audio/wav", "audio/ogg", "audio/m4a",
        "application/pdf", "application/doc", "application/docx",
        "application/zip", "application/rar", "text/plain"
    };
}

MessagingController::~MessagingController() {
    stop();
}

bool MessagingController::start() {
    try {
        // Start WebSocket server
        if (!websocket_manager_->start()) {
            return false;
        }
        
        // Start HTTP server
        if (!start_http_server()) {
            websocket_manager_->stop();
            return false;
        }
        
        running_ = true;
        
        // Start background cleanup thread
        cleanup_thread_ = std::thread([this]() {
            while (running_.load()) {
                cleanup_expired_attachments();
                std::this_thread::sleep_for(std::chrono::minutes(30));
            }
        });
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

void MessagingController::stop() {
    running_ = false;
    
    if (websocket_manager_) {
        websocket_manager_->stop();
    }
    
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    
    // Stop HTTP server
    stop_http_server();
}

bool MessagingController::is_running() const {
    return running_.load();
}

void MessagingController::init_http_server() {
    // Initialize routes
    routes_["/api/v1/messages"] = [this](const auto& req) { return handle_messages_endpoint(req); };
    routes_["/api/v1/chats"] = [this](const auto& req) { return handle_chats_endpoint(req); };
    routes_["/api/v1/attachments/upload"] = [this](const auto& req) { return handle_attachment_upload(req); };
    routes_["/api/v1/attachments/download"] = [this](const auto& req) { return handle_attachment_download(req); };
    routes_["/api/v1/health"] = [this](const auto& req) { return handle_health_check(req); };
    routes_["/api/v1/metrics"] = [this](const auto& req) { return handle_metrics(req); };
}

bool MessagingController::start_http_server() {
    try {
        // Create HTTP server thread
        http_server_thread_ = std::thread([this]() {
            run_http_server();
        });
        
        return true;
        
    } catch (const std::exception& e) {
        return false;
    }
}

void MessagingController::stop_http_server() {
    if (http_server_thread_.joinable()) {
        http_server_thread_.join();
    }
}

void MessagingController::run_http_server() {
    namespace beast = boost::beast;
    namespace http = beast::http;
    namespace net = boost::asio;
    using tcp = net::ip::tcp;
    
    try {
        net::io_context ioc;
        tcp::acceptor acceptor{ioc, {tcp::v4(), http_port_}};
        
        while (running_.load()) {
            tcp::socket socket{ioc};
            acceptor.accept(socket);
            
            // Handle request in a separate thread
            std::thread([this, socket = std::move(socket)]() mutable {
                handle_http_connection(std::move(socket));
            }).detach();
        }
        
    } catch (const std::exception& e) {
        // Log error
        running_ = false;
    }
}

void MessagingController::handle_http_connection(boost::asio::ip::tcp::socket socket) {
    namespace beast = boost::beast;
    namespace http = beast::http;
    
    try {
        beast::flat_buffer buffer;
        http::request<http::string_body> req;
        
        // Read the request
        http::read(socket, buffer, req);
        
        // Find and execute route handler
        std::string path = std::string(req.target());
        
        // Remove query parameters for route matching
        size_t query_pos = path.find('?');
        if (query_pos != std::string::npos) {
            path = path.substr(0, query_pos);
        }
        
        auto route_it = routes_.find(path);
        if (route_it != routes_.end()) {
            auto response = route_it->second(req);
            send_http_response(socket, response);
        } else {
            // Send 404 Not Found
            auto response = create_http_response(404, "Not Found", 
                APIResponse::error("Endpoint not found", "NOT_FOUND"));
            send_http_response(socket, response);
        }
        
    } catch (const std::exception& e) {
        // Send 500 Internal Server Error
        auto response = create_http_response(500, "Internal Server Error",
            APIResponse::error("Internal server error", "INTERNAL_ERROR"));
        send_http_response(socket, response);
    }
}

boost::beast::http::response<boost::beast::http::string_body> 
MessagingController::handle_messages_endpoint(const boost::beast::http::request<boost::beast::http::string_body>& req) {
    
    if (req.method() == boost::beast::http::verb::note || req.method() == boost::beast::http::verb::post) {
        return handle_send_message(req);
    } else if (req.method() == boost::beast::http::verb::get) {
        return handle_get_messages(req);
    } else if (req.method() == boost::beast::http::verb::put) {
        return handle_update_message(req);
    } else if (req.method() == boost::beast::http::verb::delete_) {
        return handle_delete_message(req);
    } else {
        return create_http_response(405, "Method Not Allowed",
            APIResponse::error("Method not allowed", "METHOD_NOT_ALLOWED"));
    }
}

boost::beast::http::response<boost::beast::http::string_body> 
MessagingController::handle_chats_endpoint(const boost::beast::http::request<boost::beast::http::string_body>& req) {
	// Validate authentication
	std::string user_id = extract_user_id(req);
	if (user_id.empty()) {
		return create_http_response(401, "Unauthorized",
			APIResponse::error("Authentication required", "UNAUTHORIZED"));
	}

	try {
		if (req.method() == boost::beast::http::verb::get) {
			// List chats for user
			auto chats = chat_service_->get_chats_for_user(user_id);
			Json::Value chats_json(Json::arrayValue);
			for (const auto& chat : chats) {
				chats_json.append(chat->to_json());
			}
			Json::Value response_data;
			response_data["chats"] = chats_json;
			return create_http_response(200, "OK",
				APIResponse::success("Chats retrieved successfully", response_data));
		}

		if (req.method() == boost::beast::http::verb::post || req.method() == boost::beast::http::verb::note) {
			Json::Value request_json;
			Json::Reader reader;
			if (!reader.parse(req.body(), request_json)) {
				return create_http_response(400, "Bad Request",
					APIResponse::error("Invalid JSON", "INVALID_JSON"));
			}
			if (!request_json.isMember("type") || !request_json.isMember("participantIds")) {
				return create_http_response(400, "Bad Request",
					APIResponse::error("type and participantIds are required", "MISSING_FIELDS"));
			}
			std::string type = request_json["type"].asString();
			auto participants_json = request_json["participantIds"];
			std::vector<std::string> participants;
			for (const auto& p : participants_json) participants.push_back(p.asString());
			// Ensure creator is part of chat
			if (std::find(participants.begin(), participants.end(), user_id) == participants.end()) {
				participants.push_back(user_id);
			}
			std::shared_ptr<core::Chat> chat;
			if (type == "direct" && participants.size() == 2) {
				chat = chat_service_->create_direct_chat(participants[0], participants[1]);
			} else {
				std::string name = request_json.get("name", "").asString();
				chat = chat_service_->create_group_chat(name, user_id, participants);
			}
			if (!chat) {
				return create_http_response(500, "Internal Server Error",
					APIResponse::error("Failed to create chat", "CREATE_FAILED"));
			}
			Json::Value response_data;
			response_data["chat"] = chat->to_json();
			return create_http_response(201, "Created",
				APIResponse::success("Chat created", response_data));
		}

		return create_http_response(405, "Method Not Allowed",
			APIResponse::error("Method not allowed", "METHOD_NOT_ALLOWED"));

	} catch (const std::exception& e) {
		return create_http_response(500, "Internal Server Error",
			APIResponse::error("Internal server error", "INTERNAL_ERROR"));
	}
}

boost::beast::http::response<boost::beast::http::string_body> 
MessagingController::handle_send_message(const boost::beast::http::request<boost::beast::http::string_body>& req) {
    
    // Validate authentication
    std::string user_id = extract_user_id(req);
    if (user_id.empty()) {
        return create_http_response(401, "Unauthorized",
            APIResponse::error("Authentication required", "UNAUTHORIZED"));
    }

    auto is_base64 = [](const std::string& s) -> bool {
        if (s.empty()) return false;
        for (char c : s) {
            if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '+' || c == '/' || c == '=' || c == '-' || c == '_')) {
                return false;
            }
        }
        return true;
    };

    auto validate_encryption_envelope = [&](const Json::Value& enc, std::string& err) -> bool {
        if (!enc.isObject()) { err = "invalid_envelope"; return false; }
        if (!enc.isMember("alg") || !enc.isMember("keyId") || !enc.isMember("iv") || !enc.isMember("tag")) { err = "missing_fields"; return false; }
        if (!enc["alg"].isString() && !enc["alg"].isInt()) { err = "bad_alg"; return false; }
        if (!enc["keyId"].isString()) { err = "bad_keyId"; return false; }
        if (!enc["iv"].isString() || !enc["tag"].isString()) { err = "bad_iv_tag"; return false; }
        std::string iv = enc["iv"].asString();
        std::string tag = enc["tag"].asString();
        if (!is_base64(iv) || !is_base64(tag)) { err = "iv_tag_not_base64"; return false; }
        if (iv.size() < 12 || tag.size() < 16) { err = "iv_tag_length"; return false; }
        // Enforce presence of AAD field for integrity binding
        if (!enc.isMember("aad") || !enc["aad"].isString()) { err = "missing_aad"; return false; }
        return true;
    };

    try {
        Json::Value request_json;
        Json::Reader reader;
        if (!reader.parse(req.body(), request_json)) {
            return create_http_response(400, "Bad Request",
                APIResponse::error("Invalid JSON", "INVALID_JSON"));
        }

        if (!request_json.isMember("chatId") || !request_json.isMember("content")) {
            return create_http_response(400, "Bad Request",
                APIResponse::error("chatId and content are required", "MISSING_FIELDS"));
        }

        std::string chat_id = request_json["chatId"].asString();
        std::string content = request_json["content"].asString();

        // Check if user is member of chat
        if (!chat_service_->is_member(chat_id, user_id)) {
            return create_http_response(403, "Forbidden",
                APIResponse::error("Access denied", "ACCESS_DENIED"));
        }

        // Build message
        auto message = std::make_shared<core::Message>();
        message->message_id = generate_message_id();
        message->chat_id = chat_id;
        message->sender_id = user_id;
        message->content = content;
        message->type = core::MessageType::TEXT;
        message->status = core::MessageStatus::SENT;
        message->timestamp = std::chrono::system_clock::now();

        // Attachments
        if (request_json.isMember("attachments")) {
            const auto& attachments_json = request_json["attachments"];
            for (const auto& attachment_json : attachments_json) {
                std::string attachment_id = attachment_json["attachment_id"].asString();
                auto attachment = get_attachment_metadata(attachment_id);
                if (attachment && attachment->attachment_id == attachment_id) {
                    message->attachments.push_back(attachment_id);
                    message->type = core::MessageType::ATTACHMENT;
                }
            }
        }

        // Optional client-side encryption envelope
        bool client_provided_encryption = false;
        if (request_json.isMember("encryption") && request_json["encryption"].isObject()) {
            std::string err;
            if (!validate_encryption_envelope(request_json["encryption"], err)) {
                return create_http_response(400, "Bad Request",
                    APIResponse::error("Invalid encryption envelope: " + err, "INVALID_ENCRYPTION"));
            }

            // Replay protection using iv+tag scoped to chat and user
            const auto& enc = request_json["encryption"];
            const std::string iv_b64 = enc["iv"].asString();
            const std::string tag_b64 = enc["tag"].asString();
            {
                std::lock_guard<std::mutex> lock(replay_mutex_);
                replay_cleanup_locked_();
                if (!check_and_mark_replay_locked_(chat_id, user_id, iv_b64, tag_b64)) {
                    return create_http_response(409, "Conflict",
                        APIResponse::error("Replay detected", "REPLAY"));
                }
            }

            // Build canonical envelope to store (include ciphertext from content)
            Json::Value envelope = request_json["encryption"];
            envelope["v"] = envelope.isMember("v") ? envelope["v"] : 1;
            envelope["ct"] = content; // content is ciphertext (base64)
            envelope["msgId"] = message->message_id; // bind message id
            envelope["chatId"] = chat_id; // bind chat id
            envelope["senderId"] = user_id; // bind sender

            Json::StreamWriterBuilder w;
            message->content = Json::writeString(w, envelope);
            message->is_encrypted = true;
            if (envelope["keyId"].isString()) {
                message->encryption_key_id = envelope["keyId"].asString();
            }
            client_provided_encryption = true;
        }

        // Handle message type
        if (request_json.isMember("type")) {
            std::string type_str = request_json["type"].asString();
            if (type_str == "sticker") message->type = core::MessageType::STICKER;
            else if (type_str == "voice") message->type = core::MessageType::VOICE;
            else if (type_str == "location") message->type = core::MessageType::LOCATION;
        }

        // Server-side encryption path (only if chat is encrypted and client did not encrypt)
        auto chat = chat_service_->get_chat(chat_id);
        if (chat && chat->settings.is_encrypted && !client_provided_encryption) {
            auto session_key = encryption_manager_->create_session_key(
                chat_id, user_id, encryption::EncryptionAlgorithm::X25519_CHACHA20_POLY1305);
            if (!session_key.session_id.empty()) {
                std::string additional_data = message->message_id + "|" + chat_id + "|" + user_id;
                auto encrypted_msg = encryption_manager_->encrypt_message(session_key.session_id, content, additional_data);
                if (!encrypted_msg.message_id.empty()) {
                    Json::Value envelope;
                    envelope["v"] = 1;
                    envelope["alg"] = static_cast<int>(session_key.algorithm);
                    envelope["sid"] = session_key.session_id;
                    envelope["ct"] = encrypted_msg.ciphertext;
                    envelope["n"] = encrypted_msg.nonce;
                    envelope["t"] = encrypted_msg.tag;
                    envelope["aad"] = encrypted_msg.additional_data;
                    Json::StreamWriterBuilder w;
                    message->content = Json::writeString(w, envelope);
                    message->encryption_key_id = session_key.session_id;
                    message->is_encrypted = true;
                }
            }
        }

        // Save message
        if (message_service_->create_message(message)) {
            realtime::RealtimeEvent event;
            event.type = realtime::MessageEventType::NEW_MESSAGE;
            event.chat_id = chat_id;
            event.user_id = user_id;
            event.data = message->to_json();
            event.timestamp = message->timestamp;
            event.event_id = generate_event_id();
            websocket_manager_->broadcast_to_chat(chat_id, event);
            chat_service_->update_last_message(chat_id, message->message_id, message->timestamp);
            send_delivery_receipts(chat_id, message->message_id, user_id);
            Json::Value response_data;
            response_data["message"] = message->to_json();
            return create_http_response(201, "Created",
                APIResponse::success("Message sent successfully", response_data));
        } else {
            return create_http_response(500, "Internal Server Error",
                APIResponse::error("Failed to send message", "SEND_FAILED"));
        }

    } catch (const std::exception& e) {
        return create_http_response(500, "Internal Server Error",
            APIResponse::error("Internal server error", "INTERNAL_ERROR"));
    }
}

boost::beast::http::response<boost::beast::http::string_body> 
MessagingController::handle_get_messages(const boost::beast::http::request<boost::beast::http::string_body>& req) {
    
    // Validate authentication
    std::string user_id = extract_user_id(req);
    if (user_id.empty()) {
        return create_http_response(401, "Unauthorized",
            APIResponse::error("Authentication required", "UNAUTHORIZED"));
    }
    
    try {
        // Parse query parameters
        auto query_params = parse_query_params(req.target());
        
        if (query_params.find("chat_id") == query_params.end()) {
            return create_http_response(400, "Bad Request",
                APIResponse::error("chat_id parameter required", "MISSING_CHAT_ID"));
        }
        
        std::string chat_id = query_params["chat_id"];
        
        // Check if user is member of chat
        if (!chat_service_->is_member(chat_id, user_id)) {
            return create_http_response(403, "Forbidden",
                APIResponse::error("Access denied", "ACCESS_DENIED"));
        }
        
        // Parse pagination parameters
        uint32_t limit = 50; // Default limit
        uint32_t offset = 0; // Default offset
        
        if (query_params.find("limit") != query_params.end()) {
            limit = std::stoul(query_params["limit"]);
            limit = std::min(limit, 100u); // Max 100 messages per request
        }
        
        if (query_params.find("offset") != query_params.end()) {
            offset = std::stoul(query_params["offset"]);
        }
        
        // Get messages
        auto messages = message_service_->get_messages_by_chat(chat_id, limit, offset);
        
        // Do not decrypt on server; return ciphertext envelopes as-is for clients to decrypt
        for (auto& message : messages) {
            if (message->is_encrypted && !message->encryption_key_id.empty()) {
                // Intentionally left blank to preserve ciphertext
                continue;
            }
        }
        
        // Convert to JSON
        Json::Value messages_json(Json::arrayValue);
        for (const auto& message : messages) {
            messages_json.append(message->to_json());
        }
        
        Json::Value response_data;
        response_data["messages"] = messages_json;
        response_data["total_count"] = static_cast<uint32_t>(messages.size());
        response_data["limit"] = limit;
        response_data["offset"] = offset;
        
        return create_http_response(200, "OK",
            APIResponse::success("Messages retrieved successfully", response_data));
        
    } catch (const std::exception& e) {
        return create_http_response(500, "Internal Server Error",
            APIResponse::error("Internal server error", "INTERNAL_ERROR"));
    }
}

boost::beast::http::response<boost::beast::http::string_body> 
MessagingController::handle_attachment_upload(const boost::beast::http::request<boost::beast::http::string_body>& req) {
    
    // Validate authentication
    std::string user_id = extract_user_id(req);
    if (user_id.empty()) {
        return create_http_response(401, "Unauthorized",
            APIResponse::error("Authentication required", "UNAUTHORIZED"));
    }
    
    try {
        // Check concurrent upload limit
        if (active_uploads_.load() >= max_concurrent_uploads_) {
            return create_http_response(429, "Too Many Requests",
                APIResponse::error("Too many concurrent uploads", "RATE_LIMIT"));
        }
        
        active_uploads_++;
        
        // Parse multipart form data (simplified - in production use proper multipart parser)
        std::string content_type = std::string(req["content-type"]);
        if (content_type.find("multipart/form-data") == std::string::npos) {
            active_uploads_--;
            return create_http_response(400, "Bad Request",
                APIResponse::error("Content must be multipart/form-data", "INVALID_CONTENT_TYPE"));
        }
        
        // Extract file data and metadata (simplified)
        std::string file_data = req.body();
        std::string filename = "uploaded_file"; // Extract from multipart headers
        std::string mime_type = "application/octet-stream"; // Extract from multipart headers
        
        // Validate file size
        if (file_data.size() > max_file_size_) {
            active_uploads_--;
            return create_http_response(413, "Payload Too Large",
                APIResponse::error("File too large", "FILE_TOO_LARGE"));
        }
        
        // Validate MIME type
        if (std::find(supported_mime_types_.begin(), supported_mime_types_.end(), mime_type) 
            == supported_mime_types_.end()) {
            active_uploads_--;
            return create_http_response(415, "Unsupported Media Type",
                APIResponse::error("Unsupported file type", "UNSUPPORTED_TYPE"));
        }
        
        // Generate attachment metadata
        AttachmentMetadata metadata;
        metadata.attachment_id = generate_attachment_id();
        metadata.filename = filename;
        metadata.mime_type = mime_type;
        metadata.file_size = file_data.size();
        metadata.uploaded_at = std::chrono::system_clock::now();
        metadata.expires_at = metadata.uploaded_at + std::chrono::hours(24 * 30); // 30 days
        metadata.access_count = 0;
        metadata.is_encrypted = true;
        
        // Generate encryption key for the file
        auto encryption_key = crypto_engine_->generate_key();
        metadata.encryption_key = encryption_key;
        
        // Encrypt file data
        std::string encrypted_data = crypto_engine_->encrypt(file_data, encryption_key);
        
        // Calculate checksum
        metadata.checksum = crypto_engine_->calculate_checksum(file_data);
        
        // Generate storage path
        std::string storage_dir = "/uploads/" + user_id + "/" + 
            std::to_string(metadata.uploaded_at.time_since_epoch().count() / 1000000);
        metadata.storage_path = storage_dir + "/" + metadata.attachment_id;
        
        // Save encrypted file to storage
        if (save_file_to_storage(metadata.storage_path, encrypted_data)) {
            // Generate thumbnail for images
            if (mime_type.substr(0, 6) == "image/") {
                std::string thumbnail_data = generate_thumbnail(file_data, mime_type);
                if (!thumbnail_data.empty()) {
                    std::string thumbnail_path = metadata.storage_path + "_thumb";
                    std::string encrypted_thumbnail = crypto_engine_->encrypt(thumbnail_data, encryption_key);
                    save_file_to_storage(thumbnail_path, encrypted_thumbnail);
                    metadata.thumbnail_path = thumbnail_path;
                }
            }
            
            // Store metadata
            {
                std::lock_guard<std::mutex> lock(attachments_mutex_);
                attachment_metadata_[metadata.attachment_id] = std::make_shared<AttachmentMetadata>(metadata);
            }
            
            active_uploads_--;
            
            Json::Value response_data;
            response_data["attachment"] = metadata.to_json();
            
            return create_http_response(201, "Created",
                APIResponse::success("File uploaded successfully", response_data));
            
        } else {
            active_uploads_--;
            return create_http_response(500, "Internal Server Error",
                APIResponse::error("Failed to save file", "STORAGE_ERROR"));
        }
        
    } catch (const std::exception& e) {
        active_uploads_--;
        return create_http_response(500, "Internal Server Error",
            APIResponse::error("Internal server error", "INTERNAL_ERROR"));
    }
}

std::string MessagingController::extract_user_id(const boost::beast::http::request<boost::beast::http::string_body>& req) {
    // Extract from Authorization header
    auto auth_header = req["authorization"];
    if (auth_header.empty()) {
        return "";
    }
    
    std::string auth_str = std::string(auth_header);
    if (auth_str.substr(0, 7) != "Bearer ") {
        return "";
    }
    
    std::string token = auth_str.substr(7);
    
    // Validate token and extract user ID (simplified)
    // In production, validate JWT token and extract user ID
    return "user_123"; // Placeholder
}

bool MessagingController::validate_auth_token(const std::string& user_id, const std::string& token) {
    // Implement token validation logic
    // This would typically involve JWT verification, database lookup, etc.
    return !user_id.empty() && !token.empty() && token.length() > 10;
}

void MessagingController::send_delivery_receipts(const std::string& chat_id, 
                                                const std::string& message_id,
                                                const std::string& sender_id) {
    // Get chat members
    auto members = chat_service_->get_members(chat_id);
    
    for (const auto& member_id : members) {
        if (member_id != sender_id) {
            // Send delivery receipt via WebSocket
            realtime::RealtimeEvent event;
            event.type = realtime::MessageEventType::MESSAGE_DELIVERED;
            event.chat_id = chat_id;
            event.user_id = sender_id;
            event.target_user_id = member_id;
            event.data["message_id"] = message_id;
            event.timestamp = std::chrono::system_clock::now();
            event.event_id = generate_event_id();
            
            websocket_manager_->broadcast_to_user(member_id, event);
        }
    }
}

std::shared_ptr<AttachmentMetadata> MessagingController::get_attachment_metadata(const std::string& attachment_id) {
    std::lock_guard<std::mutex> lock(attachments_mutex_);
    auto it = attachment_metadata_.find(attachment_id);
    return (it != attachment_metadata_.end()) ? it->second : nullptr;
}

void MessagingController::cleanup_expired_attachments() {
    std::lock_guard<std::mutex> lock(attachments_mutex_);
    
    for (auto it = attachment_metadata_.begin(); it != attachment_metadata_.end();) {
        if (it->second->is_expired()) {
            // Delete files from storage
            delete_file_from_storage(it->second->storage_path);
            if (!it->second->thumbnail_path.empty()) {
                delete_file_from_storage(it->second->thumbnail_path);
            }
            
            it = attachment_metadata_.erase(it);
        } else {
            ++it;
        }
    }
}

std::string MessagingController::generate_message_id() {
    return generate_random_id("msg");
}

std::string MessagingController::generate_attachment_id() {
    return generate_random_id("att");
}

std::string MessagingController::generate_event_id() {
    return generate_random_id("evt");
}

std::string MessagingController::generate_random_id(const std::string& prefix) {
    if (sodium_init() < 0) {
        std::stringstream ss; ss << prefix << "_" << std::hex << std::time(nullptr);
        return ss.str();
    }
    unsigned char buf[16];
    randombytes_buf(buf, sizeof(buf));
    std::stringstream ss;
    ss << prefix << "_";
    for (unsigned char b : buf) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(b);
    }
    return ss.str();
}

boost::beast::http::response<boost::beast::http::string_body>
MessagingController::create_http_response(int status_code, const std::string& reason,
                                        const APIResponse& api_response) {
    namespace http = boost::beast::http;
    
    http::response<http::string_body> response{
        static_cast<http::status>(status_code), 11 // HTTP/1.1
    };
    
    response.set(http::field::server, "Sonet Messaging Service v1.0");
    response.set(http::field::content_type, "application/json");
    response.set(http::field::access_control_allow_origin, "*");
    response.set(http::field::access_control_allow_methods, "GET, POST, PUT, DELETE, OPTIONS");
    response.set(http::field::access_control_allow_headers, "Content-Type, Authorization");
    
    Json::StreamWriterBuilder builder;
    response.body() = Json::writeString(builder, api_response.to_json());
    response.prepare_payload();
    
    return response;
}

void MessagingController::send_http_response(boost::asio::ip::tcp::socket& socket,
                                           const boost::beast::http::response<boost::beast::http::string_body>& response) {
    try {
        boost::beast::http::write(socket, response);
        socket.shutdown(boost::asio::ip::tcp::socket::shutdown_send);
    } catch (const std::exception& e) {
        // Log error and ignore
    }
}

std::map<std::string, std::string> MessagingController::parse_query_params(boost::beast::string_view target) {
    std::map<std::string, std::string> params;
    
    std::string target_str = std::string(target);
    size_t query_pos = target_str.find('?');
    
    if (query_pos == std::string::npos) {
        return params;
    }
    
    std::string query = target_str.substr(query_pos + 1);
    std::istringstream iss(query);
    std::string param;
    
    while (std::getline(iss, param, '&')) {
        size_t eq_pos = param.find('=');
        if (eq_pos != std::string::npos) {
            std::string key = param.substr(0, eq_pos);
            std::string value = param.substr(eq_pos + 1);
            params[key] = value;
        }
    }
    
    return params;
}

bool MessagingController::save_file_to_storage(const std::string& path, const std::string& data) {
    try {
        // Create directory if it doesn't exist
        std::string dir = path.substr(0, path.find_last_of('/'));
        std::system(("mkdir -p " + dir).c_str());
        
        std::ofstream file(path, std::ios::binary);
        if (file.is_open()) {
            file.write(data.data(), data.size());
            file.close();
            return true;
        }
        
        return false;
        
    } catch (const std::exception& e) {
        return false;
    }
}

bool MessagingController::delete_file_from_storage(const std::string& path) {
    try {
        return std::remove(path.c_str()) == 0;
    } catch (const std::exception& e) {
        return false;
    }
}

std::string MessagingController::generate_thumbnail(const std::string& image_data, 
                                                  const std::string& mime_type) {
    // Placeholder for thumbnail generation
    // In production, use image processing library like ImageMagick or OpenCV
    return "";
}

void MessagingController::replay_cleanup_locked_() {
    auto now = std::chrono::system_clock::now();
    for (auto it = replay_seen_.begin(); it != replay_seen_.end();) {
        if (now - it->second > replay_ttl_) it = replay_seen_.erase(it); else ++it;
    }
}

bool MessagingController::check_and_mark_replay_locked_(const std::string& chat_id, const std::string& user_id,
                                                       const std::string& iv_b64, const std::string& tag_b64) {
    std::string key = chat_id + "|" + user_id + "|" + iv_b64 + "|" + tag_b64;
    auto now = std::chrono::system_clock::now();
    auto it = replay_seen_.find(key);
    if (it != replay_seen_.end()) {
        if (now - it->second <= replay_ttl_) return false; // replay
        // expired entry can be reused
    }
    replay_seen_[key] = now;
    return true;
}

} // namespace sonet::messaging::api
