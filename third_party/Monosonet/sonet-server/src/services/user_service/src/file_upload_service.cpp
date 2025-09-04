/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "../include/file_upload_service.h"
#include <opencv2/opencv.hpp>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <fstream>
#include <random>
#include <iomanip>
#include <sstream>
#include <uuid/uuid.h>

namespace sonet::user::storage {

// Image processing implementation using OpenCV
class ImageProcessor {
public:
    static std::vector<uint8_t> resize_image(const std::vector<uint8_t>& image_data,
                                           int max_width, int max_height,
                                           ImageFormat output_format, int quality) {
        try {
            // Decode image
            cv::Mat image = cv::imdecode(image_data, cv::IMREAD_COLOR);
            if (image.empty()) {
                spdlog::error("Failed to decode image for resizing");
                return {};
            }
            
            // Calculate new dimensions while maintaining aspect ratio
            double scale = std::min(static_cast<double>(max_width) / image.cols,
                                  static_cast<double>(max_height) / image.rows);
            
            if (scale < 1.0) {
                int new_width = static_cast<int>(image.cols * scale);
                int new_height = static_cast<int>(image.rows * scale);
                
                cv::Mat resized;
                cv::resize(image, resized, cv::Size(new_width, new_height), 0, 0, cv::INTER_LANCZOS4);
                image = resized;
            }
            
            // Encode with specified format and quality
            std::vector<uint8_t> result;
            std::string ext = format_to_extension(output_format);
            std::vector<int> params;
            
            if (output_format == ImageFormat::JPEG) {
                params = {cv::IMWRITE_JPEG_QUALITY, quality};
            } else if (output_format == ImageFormat::PNG) {
                params = {cv::IMWRITE_PNG_COMPRESSION, 9 - (quality / 10)};
            } else if (output_format == ImageFormat::WEBP) {
                params = {cv::IMWRITE_WEBP_QUALITY, quality};
            }
            
            if (!cv::imencode(ext, image, result, params)) {
                spdlog::error("Failed to encode resized image");
                return {};
            }
            
            return result;
            
        } catch (const std::exception& e) {
            spdlog::error("Image resize error: {}", e.what());
            return {};
        }
    }
    
    static std::vector<uint8_t> generate_thumbnail(const std::vector<uint8_t>& image_data,
                                                 int size, ImageFormat output_format) {
        return crop_to_square(image_data, size, output_format);
    }
    
    static std::vector<uint8_t> crop_to_square(const std::vector<uint8_t>& image_data,
                                             int size, ImageFormat output_format) {
        try {
            cv::Mat image = cv::imdecode(image_data, cv::IMREAD_COLOR);
            if (image.empty()) {
                spdlog::error("Failed to decode image for cropping");
                return {};
            }
            
            // Find the smaller dimension
            int min_dim = std::min(image.cols, image.rows);
            
            // Calculate crop area (center crop)
            int x = (image.cols - min_dim) / 2;
            int y = (image.rows - min_dim) / 2;
            
            cv::Rect crop_area(x, y, min_dim, min_dim);
            cv::Mat cropped = image(crop_area);
            
            // Resize to target size
            cv::Mat resized;
            cv::resize(cropped, resized, cv::Size(size, size), 0, 0, cv::INTER_LANCZOS4);
            
            // Encode
            std::vector<uint8_t> result;
            std::string ext = format_to_extension(output_format);
            std::vector<int> params = {cv::IMWRITE_JPEG_QUALITY, 85};
            
            if (!cv::imencode(ext, resized, result, params)) {
                spdlog::error("Failed to encode cropped image");
                return {};
            }
            
            return result;
            
        } catch (const std::exception& e) {
            spdlog::error("Image crop error: {}", e.what());
            return {};
        }
    }
    
    static std::pair<int, int> get_image_dimensions(const std::vector<uint8_t>& image_data) {
        try {
            cv::Mat image = cv::imdecode(image_data, cv::IMREAD_COLOR);
            if (image.empty()) {
                return {0, 0};
            }
            return {image.cols, image.rows};
        } catch (const std::exception& e) {
            spdlog::error("Failed to get image dimensions: {}", e.what());
            return {0, 0};
        }
    }
    
    static bool is_valid_image(const std::vector<uint8_t>& image_data) {
        try {
            cv::Mat image = cv::imdecode(image_data, cv::IMREAD_COLOR);
            return !image.empty();
        } catch (const std::exception&) {
            return false;
        }
    }
    
    static std::string detect_image_format(const std::vector<uint8_t>& image_data) {
        if (image_data.size() < 8) return "unknown";
        
        // Check magic numbers
        if (image_data[0] == 0xFF && image_data[1] == 0xD8) return "image/jpeg";
        if (image_data[0] == 0x89 && image_data[1] == 0x50 && image_data[2] == 0x4E && image_data[3] == 0x47) return "image/png";
        if (image_data[0] == 0x52 && image_data[1] == 0x49 && image_data[2] == 0x46 && image_data[3] == 0x46) return "image/webp";
        
        return "unknown";
    }

private:
    static std::string format_to_extension(ImageFormat format) {
        switch (format) {
            case ImageFormat::JPEG: return ".jpg";
            case ImageFormat::PNG: return ".png";
            case ImageFormat::WEBP: return ".webp";
            case ImageFormat::AVIF: return ".avif";
            default: return ".jpg";
        }
    }
};

// Local filesystem storage implementation
class LocalFileStorage {
public:
    LocalFileStorage(const std::string& base_path, const std::string& public_url_base)
        : base_path_(base_path), public_url_base_(public_url_base) {
        std::filesystem::create_directories(base_path_);
    }
    
    UploadResult upload_file(const std::string& file_path, const std::vector<uint8_t>& file_data) {
        try {
            std::string full_path = base_path_ + "/" + file_path;
            std::filesystem::create_directories(std::filesystem::path(full_path).parent_path());
            
            std::ofstream file(full_path, std::ios::binary);
            if (!file) {
                return {false, "", "", 0, "", "Failed to create file"};
            }
            
            file.write(reinterpret_cast<const char*>(file_data.data()), file_data.size());
            file.close();
            
            std::string public_url = public_url_base_ + "/" + file_path;
            
            return {true, public_url, file_path, file_data.size(), "", ""};
            
        } catch (const std::exception& e) {
            return {false, "", "", 0, "", e.what()};
        }
    }
    
    std::vector<uint8_t> download_file(const std::string& file_path) {
        try {
            std::string full_path = base_path_ + "/" + file_path;
            std::ifstream file(full_path, std::ios::binary);
            if (!file) {
                return {};
            }
            
            file.seekg(0, std::ios::end);
            size_t size = file.tellg();
            file.seekg(0, std::ios::beg);
            
            std::vector<uint8_t> data(size);
            file.read(reinterpret_cast<char*>(data.data()), size);
            
            return data;
            
        } catch (const std::exception& e) {
            spdlog::error("Failed to download file: {}", e.what());
            return {};
        }
    }
    
    bool delete_file(const std::string& file_path) {
        try {
            std::string full_path = base_path_ + "/" + file_path;
            return std::filesystem::remove(full_path);
        } catch (const std::exception& e) {
            spdlog::error("Failed to delete file: {}", e.what());
            return false;
        }
    }

private:
    std::string base_path_;
    std::string public_url_base_;
};

// S3 storage implementation
class S3FileStorage {
public:
    S3FileStorage(const std::string& access_key, const std::string& secret_key,
                 const std::string& bucket, const std::string& region)
        : bucket_(bucket) {
        
        Aws::Auth::AWSCredentials credentials(access_key, secret_key);
        Aws::S3::S3ClientConfiguration config;
        config.region = region;
        
        s3_client_ = std::make_unique<Aws::S3::S3Client>(credentials, config);
    }
    
    UploadResult upload_file(const std::string& file_path, const std::vector<uint8_t>& file_data,
                           const std::string& content_type) {
        try {
            Aws::S3::Model::PutObjectRequest request;
            request.SetBucket(bucket_);
            request.SetKey(file_path);
            request.SetContentType(content_type);
            
            std::shared_ptr<Aws::IOStream> data_stream = std::make_shared<Aws::StringStream>();
            data_stream->write(reinterpret_cast<const char*>(file_data.data()), file_data.size());
            request.SetBody(data_stream);
            
            auto outcome = s3_client_->PutObject(request);
            if (!outcome.IsSuccess()) {
                return {false, "", "", 0, "", outcome.GetError().GetMessage()};
            }
            
            std::string public_url = "https://" + bucket_ + ".s3.amazonaws.com/" + file_path;
            
            return {true, public_url, file_path, file_data.size(), content_type, ""};
            
        } catch (const std::exception& e) {
            return {false, "", "", 0, "", e.what()};
        }
    }
    
    std::vector<uint8_t> download_file(const std::string& file_path) {
        try {
            Aws::S3::Model::GetObjectRequest request;
            request.SetBucket(bucket_);
            request.SetKey(file_path);
            
            auto outcome = s3_client_->GetObject(request);
            if (!outcome.IsSuccess()) {
                spdlog::error("Failed to download from S3: {}", outcome.GetError().GetMessage());
                return {};
            }
            
            auto& stream = outcome.GetResult().GetBody();
            std::vector<uint8_t> data(std::istreambuf_iterator<char>(stream), {});
            
            return data;
            
        } catch (const std::exception& e) {
            spdlog::error("S3 download error: {}", e.what());
            return {};
        }
    }
    
    bool delete_file(const std::string& file_path) {
        try {
            Aws::S3::Model::DeleteObjectRequest request;
            request.SetBucket(bucket_);
            request.SetKey(file_path);
            
            auto outcome = s3_client_->DeleteObject(request);
            return outcome.IsSuccess();
            
        } catch (const std::exception& e) {
            spdlog::error("S3 delete error: {}", e.what());
            return false;
        }
    }

private:
    std::string bucket_;
    std::unique_ptr<Aws::S3::S3Client> s3_client_;
};

// FileUploadService implementation
class FileUploadService::Impl {
public:
    Impl(StorageProvider provider) : provider_(provider) {}
    
    bool initialize(const std::map<std::string, std::string>& config) {
        try {
            switch (provider_) {
                case StorageProvider::LOCAL_FILESYSTEM: {
                    std::string base_path = config.at("base_path");
                    std::string public_url_base = config.at("public_url_base");
                    local_storage_ = std::make_unique<LocalFileStorage>(base_path, public_url_base);
                    break;
                }
                case StorageProvider::AWS_S3: {
                    std::string access_key = config.at("access_key");
                    std::string secret_key = config.at("secret_key");
                    std::string bucket = config.at("bucket");
                    std::string region = config.at("region");
                    s3_storage_ = std::make_unique<S3FileStorage>(access_key, secret_key, bucket, region);
                    break;
                }
                default:
                    spdlog::error("Unsupported storage provider");
                    return false;
            }
            
            spdlog::info("File upload service initialized successfully");
            return true;
            
        } catch (const std::exception& e) {
            spdlog::error("Failed to initialize file upload service: {}", e.what());
            return false;
        }
    }
    
    std::future<UploadResult> upload_profile_picture(const std::string& user_id,
                                                   const std::vector<uint8_t>& file_data,
                                                   const std::string& filename,
                                                   const std::string& content_type) {
        ImageProcessingOptions options;
        options.max_width = 800;
        options.max_height = 800;
        options.quality = 85;
        options.format = ImageFormat::JPEG;
        options.generate_thumbnail = true;
        options.thumbnail_size = 150;
        
        return upload_file(user_id, file_data, filename, content_type, "avatar", options);
    }
    
    std::future<UploadResult> upload_profile_banner(const std::string& user_id,
                                                   const std::vector<uint8_t>& file_data,
                                                   const std::string& filename,
                                                   const std::string& content_type) {
        ImageProcessingOptions options;
        options.max_width = 1500;
        options.max_height = 500;
        options.quality = 85;
        options.format = ImageFormat::JPEG;
        
        return upload_file(user_id, file_data, filename, content_type, "banner", options);
    }
    
    std::future<UploadResult> upload_file(const std::string& user_id,
                                        const std::vector<uint8_t>& file_data,
                                        const std::string& filename,
                                        const std::string& content_type,
                                        const std::string& category,
                                        const ImageProcessingOptions& options) {
        return std::async(std::launch::async, [this, user_id, file_data, filename, content_type, category, options]() {
            try {
                // Validate file
                std::string validation_error = validate_upload(file_data, content_type, category);
                if (!validation_error.empty()) {
                    return UploadResult{false, "", "", 0, "", validation_error};
                }
                
                std::vector<uint8_t> processed_data = file_data;
                
                // Process image if needed
                if (is_valid_image_format(content_type) && (options.max_width > 0 || options.max_height > 0)) {
                    processed_data = ImageProcessor::resize_image(file_data, options.max_width, options.max_height,
                                                               options.format, options.quality);
                    if (processed_data.empty()) {
                        return UploadResult{false, "", "", 0, "", "Image processing failed"};
                    }
                }
                
                // Generate file ID and path
                std::string file_id = generate_file_id();
                std::string extension = get_file_extension(filename);
                std::string storage_path = generate_storage_path(user_id, file_id, category, extension);
                
                // Upload to storage
                UploadResult result;
                switch (provider_) {
                    case StorageProvider::LOCAL_FILESYSTEM:
                        if (local_storage_) {
                            result = local_storage_->upload_file(storage_path, processed_data);
                            result.file_id = file_id;
                            result.content_type = content_type;
                        }
                        break;
                    case StorageProvider::AWS_S3:
                        if (s3_storage_) {
                            result = s3_storage_->upload_file(storage_path, processed_data, content_type);
                            result.file_id = file_id;
                        }
                        break;
                    default:
                        return UploadResult{false, "", "", 0, "", "Unsupported storage provider"};
                }
                
                // Generate thumbnail if requested
                if (result.success && options.generate_thumbnail && is_valid_image_format(content_type)) {
                    std::vector<uint8_t> thumbnail_data = ImageProcessor::generate_thumbnail(
                        file_data, options.thumbnail_size, ImageFormat::JPEG);
                    
                    if (!thumbnail_data.empty()) {
                        std::string thumb_path = generate_storage_path(user_id, file_id + "_thumb", category, ".jpg");
                        switch (provider_) {
                            case StorageProvider::LOCAL_FILESYSTEM:
                                if (local_storage_) {
                                    local_storage_->upload_file(thumb_path, thumbnail_data);
                                }
                                break;
                            case StorageProvider::AWS_S3:
                                if (s3_storage_) {
                                    s3_storage_->upload_file(thumb_path, thumbnail_data, "image/jpeg");
                                }
                                break;
                        }
                    }
                }
                
                // Store metadata in database (would need database integration)
                if (result.success) {
                    store_file_metadata(FileMetadata{
                        file_id,
                        filename,
                        content_type,
                        result.file_size,
                        user_id,
                        category,
                        storage_path,
                        result.url,
                        std::time(nullptr),
                        std::time(nullptr),
                        false
                    });
                }
                
                return result;
                
            } catch (const std::exception& e) {
                return UploadResult{false, "", "", 0, "", e.what()};
            }
        });
    }
    
    std::string validate_upload(const std::vector<uint8_t>& file_data, const std::string& content_type,
                              const std::string& category) const {
        // Check file size
        if (!is_valid_file_size(file_data.size(), category)) {
            return "File size exceeds limit for category: " + category;
        }
        
        // Check content type
        if (!FileTypeDetector::is_allowed_type(content_type, category)) {
            return "File type not allowed for category: " + category;
        }
        
        // Validate image if it's supposed to be an image
        if (category == "avatar" || category == "banner") {
            if (!is_valid_image_format(content_type)) {
                return "Invalid image format";
            }
            
            if (!ImageProcessor::is_valid_image(file_data)) {
                return "Corrupted or invalid image file";
            }
        }
        
        return "";
    }
    
    bool is_valid_image_format(const std::string& content_type) const {
        return content_type == "image/jpeg" || content_type == "image/png" || 
               content_type == "image/webp" || content_type == "image/gif";
    }
    
    bool is_valid_file_size(size_t file_size, const std::string& category) const {
        const std::map<std::string, size_t> size_limits = {
            {"avatar", 10 * 1024 * 1024},    // 10MB
            {"banner", 15 * 1024 * 1024},    // 15MB
            {"media", 50 * 1024 * 1024},     // 50MB
            {"document", 100 * 1024 * 1024}  // 100MB
        };
        
        auto it = size_limits.find(category);
        if (it == size_limits.end()) {
            return file_size <= 10 * 1024 * 1024; // Default 10MB
        }
        
        return file_size <= it->second;
    }

private:
    StorageProvider provider_;
    std::unique_ptr<LocalFileStorage> local_storage_;
    std::unique_ptr<S3FileStorage> s3_storage_;
    
    void store_file_metadata(const FileMetadata& metadata) {
        // This would store metadata in the database
        // For now, just log it
        spdlog::info("Storing file metadata: {} for user {}", metadata.file_id, metadata.user_id);
    }
};

// FileUploadService public methods
FileUploadService::FileUploadService(StorageProvider provider) : pimpl_(std::make_unique<Impl>(provider)) {}
FileUploadService::~FileUploadService() = default;

bool FileUploadService::initialize(const std::map<std::string, std::string>& config) {
    return pimpl_->initialize(config);
}

std::future<UploadResult> FileUploadService::upload_profile_picture(const std::string& user_id,
                                                                   const std::vector<uint8_t>& file_data,
                                                                   const std::string& filename,
                                                                   const std::string& content_type) {
    return pimpl_->upload_profile_picture(user_id, file_data, filename, content_type);
}

std::future<UploadResult> FileUploadService::upload_profile_banner(const std::string& user_id,
                                                                  const std::vector<uint8_t>& file_data,
                                                                  const std::string& filename,
                                                                  const std::string& content_type) {
    return pimpl_->upload_profile_banner(user_id, file_data, filename, content_type);
}

std::future<UploadResult> FileUploadService::upload_file(const std::string& user_id,
                                                        const std::vector<uint8_t>& file_data,
                                                        const std::string& filename,
                                                        const std::string& content_type,
                                                        const std::string& category,
                                                        const ImageProcessingOptions& options) {
    return pimpl_->upload_file(user_id, file_data, filename, content_type, category, options);
}

std::string FileUploadService::validate_upload(const std::vector<uint8_t>& file_data,
                                              const std::string& content_type,
                                              const std::string& category) const {
    return pimpl_->validate_upload(file_data, content_type, category);
}

bool FileUploadService::is_valid_image_format(const std::string& content_type) const {
    return pimpl_->is_valid_image_format(content_type);
}

bool FileUploadService::is_valid_file_size(size_t file_size, const std::string& category) const {
    return pimpl_->is_valid_file_size(file_size, category);
}

// Utility functions
std::string generate_file_id() {
    uuid_t uuid;
    uuid_generate_random(uuid);
    
    char uuid_str[37];
    uuid_unparse(uuid, uuid_str);
    
    return std::string(uuid_str);
}

std::string generate_storage_path(const std::string& user_id, const std::string& file_id,
                                const std::string& category, const std::string& extension) {
    // Create a path like: users/{user_id}/{category}/{year}/{month}/{file_id}.ext
    std::time_t now = std::time(nullptr);
    std::tm* tm_now = std::gmtime(&now);
    
    std::ostringstream path;
    path << "users/" << user_id << "/" << category << "/"
         << std::setfill('0') << std::setw(4) << (tm_now->tm_year + 1900) << "/"
         << std::setfill('0') << std::setw(2) << (tm_now->tm_mon + 1) << "/"
         << file_id << extension;
    
    return path.str();
}

std::string get_file_extension(const std::string& filename) {
    size_t pos = filename.find_last_of('.');
    if (pos != std::string::npos) {
        return filename.substr(pos);
    }
    return "";
}

std::string sanitize_filename(const std::string& filename) {
    std::string sanitized = filename;
    
    // Replace unsafe characters
    std::string unsafe_chars = "\\/:*?\"<>|";
    for (char c : unsafe_chars) {
        std::replace(sanitized.begin(), sanitized.end(), c, '_');
    }
    
    // Limit length
    if (sanitized.length() > 255) {
        sanitized = sanitized.substr(0, 255);
    }
    
    return sanitized;
}

// FileTypeDetector implementation
std::string FileTypeDetector::detect_content_type(const std::vector<uint8_t>& file_data) {
    if (file_data.size() < 8) return "application/octet-stream";
    
    // Check magic numbers for common formats
    if (file_data[0] == 0xFF && file_data[1] == 0xD8) return "image/jpeg";
    if (file_data[0] == 0x89 && file_data[1] == 0x50 && file_data[2] == 0x4E && file_data[3] == 0x47) return "image/png";
    if (file_data[0] == 0x47 && file_data[1] == 0x49 && file_data[2] == 0x46) return "image/gif";
    if (file_data[0] == 0x52 && file_data[1] == 0x49 && file_data[2] == 0x46 && file_data[3] == 0x46) return "image/webp";
    if (file_data[0] == 0x25 && file_data[1] == 0x50 && file_data[2] == 0x44 && file_data[3] == 0x46) return "application/pdf";
    
    return "application/octet-stream";
}

bool FileTypeDetector::is_image(const std::string& content_type) {
    return content_type.starts_with("image/");
}

bool FileTypeDetector::is_video(const std::string& content_type) {
    return content_type.starts_with("video/");
}

bool FileTypeDetector::is_audio(const std::string& content_type) {
    return content_type.starts_with("audio/");
}

bool FileTypeDetector::is_allowed_type(const std::string& content_type, const std::string& category) {
    if (category == "avatar" || category == "banner") {
        return content_type == "image/jpeg" || content_type == "image/png" || 
               content_type == "image/webp" || content_type == "image/gif";
    }
    
    if (category == "media") {
        return is_image(content_type) || is_video(content_type) || is_audio(content_type);
    }
    
    // Allow most types for general uploads
    return true;
}

} // namespace sonet::user::storage
