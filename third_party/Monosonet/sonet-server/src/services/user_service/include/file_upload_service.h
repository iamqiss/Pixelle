/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <memory>
#include <future>
#include <vector>
#include <map>

namespace sonet::user::storage {

struct UploadResult {
    bool success;
    std::string url;
    std::string file_id;
    size_t file_size;
    std::string content_type;
    std::string error_message;
};

struct FileMetadata {
    std::string file_id;
    std::string original_filename;
    std::string content_type;
    size_t file_size;
    std::string user_id;
    std::string file_category; // avatar, banner, media, etc.
    std::string storage_path;
    std::string public_url;
    int64_t created_at;
    int64_t updated_at;
    bool is_deleted = false;
};

enum class StorageProvider {
    LOCAL_FILESYSTEM,
    AWS_S3,
    GOOGLE_CLOUD_STORAGE,
    AZURE_BLOB_STORAGE
};

enum class ImageFormat {
    JPEG,
    PNG,
    WEBP,
    AVIF
};

struct ImageProcessingOptions {
    int max_width = 0;
    int max_height = 0;
    int quality = 80;
    ImageFormat format = ImageFormat::JPEG;
    bool progressive = true;
    bool strip_metadata = true;
    bool generate_thumbnail = false;
    int thumbnail_size = 150;
};

class FileUploadService {
public:
    FileUploadService(StorageProvider provider = StorageProvider::LOCAL_FILESYSTEM);
    ~FileUploadService();

    // Initialize storage service
    bool initialize(const std::map<std::string, std::string>& config);
    
    // Profile picture upload with automatic processing
    std::future<UploadResult> upload_profile_picture(
        const std::string& user_id,
        const std::vector<uint8_t>& file_data,
        const std::string& filename,
        const std::string& content_type);
    
    // Banner upload with automatic processing
    std::future<UploadResult> upload_profile_banner(
        const std::string& user_id,
        const std::vector<uint8_t>& file_data,
        const std::string& filename,
        const std::string& content_type);
    
    // Generic file upload
    std::future<UploadResult> upload_file(
        const std::string& user_id,
        const std::vector<uint8_t>& file_data,
        const std::string& filename,
        const std::string& content_type,
        const std::string& category,
        const ImageProcessingOptions& options = {});
    
    // File retrieval
    std::future<std::vector<uint8_t>> download_file(const std::string& file_id);
    std::future<std::string> get_file_url(const std::string& file_id, int ttl_seconds = 3600);
    std::future<FileMetadata> get_file_metadata(const std::string& file_id);
    
    // File management
    std::future<bool> delete_file(const std::string& file_id);
    std::future<bool> delete_user_files(const std::string& user_id, const std::string& category = "");
    std::future<std::vector<FileMetadata>> list_user_files(const std::string& user_id, const std::string& category = "");
    
    // Image processing
    std::future<UploadResult> process_image(
        const std::vector<uint8_t>& image_data,
        const ImageProcessingOptions& options);
    
    // Validation
    bool is_valid_image_format(const std::string& content_type) const;
    bool is_valid_file_size(size_t file_size, const std::string& category) const;
    std::string validate_upload(const std::vector<uint8_t>& file_data, 
                               const std::string& content_type,
                               const std::string& category) const;
    
    // Configuration for different providers
    void set_local_config(const std::string& base_path, const std::string& public_url_base);
    void set_s3_config(const std::string& access_key, const std::string& secret_key,
                      const std::string& bucket, const std::string& region);
    void set_gcs_config(const std::string& service_account_json, const std::string& bucket);
    void set_azure_config(const std::string& connection_string, const std::string& container);
    
    // Cleanup and maintenance
    std::future<size_t> cleanup_orphaned_files();
    std::future<size_t> cleanup_deleted_files();
    std::future<std::map<std::string, size_t>> get_storage_stats();
    
    // Health and status
    bool is_healthy() const;
    std::string get_status() const;

private:
    class Impl;
    std::unique_ptr<Impl> pimpl_;
};

// Image processing utilities
class ImageProcessor {
public:
    static std::vector<uint8_t> resize_image(
        const std::vector<uint8_t>& image_data,
        int max_width, int max_height,
        ImageFormat output_format = ImageFormat::JPEG,
        int quality = 80);
    
    static std::vector<uint8_t> generate_thumbnail(
        const std::vector<uint8_t>& image_data,
        int size,
        ImageFormat output_format = ImageFormat::JPEG);
    
    static std::vector<uint8_t> crop_to_square(
        const std::vector<uint8_t>& image_data,
        int size,
        ImageFormat output_format = ImageFormat::JPEG);
    
    static std::vector<uint8_t> strip_metadata(const std::vector<uint8_t>& image_data);
    
    static std::pair<int, int> get_image_dimensions(const std::vector<uint8_t>& image_data);
    static bool is_valid_image(const std::vector<uint8_t>& image_data);
    static std::string detect_image_format(const std::vector<uint8_t>& image_data);
};

// File type detection utilities
class FileTypeDetector {
public:
    static std::string detect_content_type(const std::vector<uint8_t>& file_data);
    static bool is_image(const std::string& content_type);
    static bool is_video(const std::string& content_type);
    static bool is_audio(const std::string& content_type);
    static bool is_allowed_type(const std::string& content_type, const std::string& category);
};

// URL generation utilities
std::string generate_file_id();
std::string generate_storage_path(const std::string& user_id, const std::string& file_id, 
                                 const std::string& category, const std::string& extension);
std::string get_file_extension(const std::string& filename);
std::string sanitize_filename(const std::string& filename);

} // namespace sonet::user::storage
