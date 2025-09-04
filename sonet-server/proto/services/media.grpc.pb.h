#pragma once

#include "../grpc_stub.h"

namespace sonet {
namespace media {
    enum MediaType {
        MEDIA_TYPE_UNKNOWN = 0,
        MEDIA_TYPE_IMAGE = 1,
        MEDIA_TYPE_VIDEO = 2,
        MEDIA_TYPE_AUDIO = 3,
        MEDIA_TYPE_GIF = 4,
        MEDIA_TYPE_DOCUMENT = 5
    };
    
    struct UploadInit {
        std::string owner_user_id_;
        MediaType type_;
        std::string mime_type_;
        std::string owner_user_id() const { return owner_user_id_; }
        MediaType type() const { return type_; }
        std::string mime_type() const { return mime_type_; }
        void set_owner_user_id(const std::string& id) { owner_user_id_ = id; }
        void set_type(MediaType t) { type_ = t; }
        void set_mime_type(const std::string& type) { mime_type_ = type; }
    };
    
    struct UploadChunk {
        std::string content_;
        std::string content() const { return content_; }
        void set_content(const std::string& c) { content_ = c; }
    };
    
    struct UploadRequest {
        UploadInit init_;
        UploadChunk chunk_;
        bool has_init() const { return !init_.owner_user_id().empty(); }
        bool has_chunk() const { return !chunk_.content().empty(); }
        const UploadInit& init() const { return init_; }
        const UploadChunk& chunk() const { return chunk_; }
        UploadInit* mutable_init() { return &init_; }
        UploadChunk* mutable_chunk() { return &chunk_; }
    };
    
    struct UploadResponse {
        std::string media_id_;
        std::string url_;
        std::string thumbnail_url_;
        std::string hls_url_;
        std::string webp_url_;
        std::string mp4_url_;
        MediaType type_;
        bool success_;
        std::string error_message_;
        std::string media_id() const { return media_id_; }
        std::string url() const { return url_; }
        std::string thumbnail_url() const { return thumbnail_url_; }
        std::string hls_url() const { return hls_url_; }
        std::string webp_url() const { return webp_url_; }
        std::string mp4_url() const { return mp4_url_; }
        MediaType type() const { return type_; }
        bool success() const { return success_; }
        std::string error_message() const { return error_message_; }
        void set_media_id(const std::string& id) { media_id_ = id; }
        void set_url(const std::string& u) { url_ = u; }
        void set_thumbnail_url(const std::string& u) { thumbnail_url_ = u; }
        void set_hls_url(const std::string& u) { hls_url_ = u; }
        void set_webp_url(const std::string& u) { webp_url_ = u; }
        void set_mp4_url(const std::string& u) { mp4_url_ = u; }
        void set_type(MediaType t) { type_ = t; }
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& msg) { error_message_ = msg; }
    };
    
    struct GetMediaRequest {
        std::string media_id_;
        std::string media_id() const { return media_id_; }
        void set_media_id(const std::string& id) { media_id_ = id; }
    };
    
    struct MediaInfo {
        std::string id_;
        std::string owner_user_id_;
        MediaType type_;
        std::string mime_type_;
        uint64_t size_bytes_;
        uint32_t width_;
        uint32_t height_;
        double duration_seconds_;
        std::string original_url_;
        std::string thumbnail_url_;
        std::string hls_url_;
        std::string webp_url_;
        std::string mp4_url_;
        std::string created_at_;
        std::string id() const { return id_; }
        std::string owner_user_id() const { return owner_user_id_; }
        MediaType type() const { return type_; }
        std::string mime_type() const { return mime_type_; }
        uint64_t size_bytes() const { return size_bytes_; }
        uint32_t width() const { return width_; }
        uint32_t height() const { return height_; }
        double duration_seconds() const { return duration_seconds_; }
        std::string original_url() const { return original_url_; }
        std::string thumbnail_url() const { return thumbnail_url_; }
        std::string hls_url() const { return hls_url_; }
        std::string webp_url() const { return webp_url_; }
        std::string mp4_url() const { return mp4_url_; }
        std::string created_at() const { return created_at_; }
        void set_id(const std::string& id) { id_ = id; }
        void set_owner_user_id(const std::string& id) { owner_user_id_ = id; }
        void set_type(MediaType t) { type_ = t; }
        void set_mime_type(const std::string& type) { mime_type_ = type; }
        void set_size_bytes(uint64_t size) { size_bytes_ = size; }
        void set_width(uint32_t w) { width_ = w; }
        void set_height(uint32_t h) { height_ = h; }
        void set_duration_seconds(double d) { duration_seconds_ = d; }
        void set_original_url(const std::string& u) { original_url_ = u; }
        void set_thumbnail_url(const std::string& u) { thumbnail_url_ = u; }
        void set_hls_url(const std::string& u) { hls_url_ = u; }
        void set_webp_url(const std::string& u) { webp_url_ = u; }
        void set_mp4_url(const std::string& u) { mp4_url_ = u; }
        void set_created_at(const std::string& t) { created_at_ = t; }
    };
    
    struct GetMediaResponse {
        MediaInfo media_;
        bool success_;
        std::string error_message_;
        const MediaInfo& media() const { return media_; }
        MediaInfo* mutable_media() { return &media_; }
        bool success() const { return success_; }
        std::string error_message() const { return error_message_; }
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& msg) { error_message_ = msg; }
    };
    
    struct DeleteMediaRequest {
        std::string media_id_;
        std::string user_id_;
        std::string media_id() const { return media_id_; }
        std::string user_id() const { return user_id_; }
        void set_media_id(const std::string& id) { media_id_ = id; }
        void set_user_id(const std::string& id) { user_id_ = id; }
    };
    
    struct DeleteMediaResponse {
        bool success_;
        bool deleted_;
        std::string error_message_;
        bool success() const { return success_; }
        bool deleted() const { return deleted_; }
        std::string error_message() const { return error_message_; }
        void set_success(bool s) { success_ = s; }
        void set_deleted(bool d) { deleted_ = d; }
        void set_error_message(const std::string& msg) { error_message_ = msg; }
    };
    
    struct ListUserMediaRequest {
        std::string user_id_;
        int32_t page_;
        int32_t page_size_;
        std::string user_id() const { return user_id_; }
        std::string owner_user_id() const { return user_id_; } // Add owner_user_id alias
        int32_t page() const { return page_; }
        int32_t page_size() const { return page_size_; }
        void set_user_id(const std::string& id) { user_id_ = id; }
        void set_owner_user_id(const std::string& id) { user_id_ = id; } // Add set_owner_user_id
        void set_page(int32_t p) { page_ = p; }
        void set_page_size(int32_t size) { page_size_ = size; }
    };
    
    struct ListUserMediaResponse {
        std::vector<MediaInfo> media_items_;
        int32_t total_count_;
        int32_t page_;
        int32_t page_size_;
        int32_t total_pages_;
        bool success_;
        std::string error_message_;
        const std::vector<MediaInfo>& media_items() const { return media_items_; }
        int32_t total_count() const { return total_count_; }
        int32_t page() const { return page_; }
        int32_t page_size() const { return page_size_; }
        int32_t total_pages() const { return total_pages_; }
        bool success() const { return success_; }
        std::string error_message() const { return error_message_; }
        void add_media_items(const MediaInfo& item) { media_items_.push_back(item); }
        MediaInfo* add_items() { media_items_.emplace_back(); return &media_items_.back(); } // Add add_items method
        void set_total_count(int32_t count) { total_count_ = count; }
        void set_page(int32_t p) { page_ = p; }
        void set_page_size(int32_t size) { page_size_ = size; }
        void set_total_pages(int32_t pages) { total_pages_ = pages; }
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& msg) { error_message_ = msg; }
    };
    
    struct HealthCheckRequest {};
    
    struct HealthCheckResponse {
        std::string status_;
        bool success_;
        std::string status() const { return status_; }
        bool success() const { return success_; }
        void set_status(const std::string& s) { status_ = s; }
        void set_success(bool s) { success_ = s; }
    };
    
    class MediaService {
    public:
        class Service {
        public:
            virtual ~Service() = default;
            virtual ::grpc::Status Upload(::grpc::ServerContext* context,
                                          ::grpc::ServerReader<UploadRequest>* reader,
                                          UploadResponse* response) = 0;
            virtual ::grpc::Status GetMedia(::grpc::ServerContext* context,
                                            const GetMediaRequest* request,
                                            GetMediaResponse* response) = 0;
            virtual ::grpc::Status DeleteMedia(::grpc::ServerContext* context,
                                               const DeleteMediaRequest* request,
                                               DeleteMediaResponse* response) = 0;
            virtual ::grpc::Status ListUserMedia(::grpc::ServerContext* context,
                                                 const ListUserMediaRequest* request,
                                                 ListUserMediaResponse* response) = 0;
            virtual ::grpc::Status HealthCheck(::grpc::ServerContext* context,
                                               const HealthCheckRequest* request,
                                               HealthCheckResponse* response) = 0;
        };
        
        // Client-side stub methods
        class Stub {
        public:
            Stub(std::shared_ptr<::grpc::Channel> channel) : channel_(channel) {}
            
            class UploadWriter {
            public:
                UploadWriter() = default;
                bool Write(const UploadRequest& request) { return true; }
                bool WritesDone() { return true; }
                ::grpc::Status Finish() { return ::grpc::Status::OK; }
            };
            
            std::unique_ptr<UploadWriter> Upload(::grpc::ClientContext* context, UploadResponse* response) {
                return std::make_unique<UploadWriter>();
            }
            
        private:
            std::shared_ptr<::grpc::Channel> channel_;
        };
    };
}
}