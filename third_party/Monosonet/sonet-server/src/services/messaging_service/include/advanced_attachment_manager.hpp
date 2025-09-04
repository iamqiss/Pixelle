/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <set>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <functional>
#include <future>
#include <thread>
#include <queue>
#include <json/json.h>
#include <filesystem>

namespace sonet::messaging::attachments {

/**
 * @brief Attachment type classification
 */
enum class AttachmentType {
    UNKNOWN = 0,
    IMAGE = 1,           // JPEG, PNG, GIF, WebP, etc.
    VIDEO = 2,           // MP4, AVI, MOV, WebM, etc.
    AUDIO = 3,           // MP3, WAV, OGG, AAC, etc.
    DOCUMENT = 4,        // PDF, DOC, TXT, etc.
    SPREADSHEET = 5,     // XLS, CSV, etc.
    PRESENTATION = 6,    // PPT, PDF slides, etc.
    ARCHIVE = 7,         // ZIP, RAR, 7Z, etc.
    CODE = 8,            // Source code files
    EXECUTABLE = 9,      // EXE, DMG, APK, etc.
    FONT = 10,           // TTF, OTF, WOFF, etc.
    MODEL_3D = 11,       // OBJ, STL, FBX, etc.
    CAD = 12,            // DWG, DXF, etc.
    VECTOR = 13,         // SVG, AI, EPS, etc.
    DATABASE = 14,       // SQL, DB, SQLite, etc.
    CONFIGURATION = 15,  // JSON, YAML, XML, etc.
    CERTIFICATE = 16,    // PEM, CRT, P12, etc.
    CONTACT = 17,        // VCF, vCard, etc.
    CALENDAR = 18,       // ICS, etc.
    EMAIL = 19,          // EML, MSG, etc.
    ENCRYPTED = 20       // GPG, encrypted files, etc.
};

/**
 * @brief Attachment processing status
 */
enum class ProcessingStatus {
    PENDING = 0,         // Awaiting processing
    UPLOADING = 1,       // Currently uploading
    PROCESSING = 2,      // Being processed (thumbnails, etc.)
    ENCRYPTING = 3,      // Being encrypted
    SCANNING = 4,        // Virus/malware scanning
    COMPLETED = 5,       // Fully processed and ready
    FAILED = 6,          // Processing failed
    QUARANTINED = 7,     // Flagged by security scan
    EXPIRED = 8,         // Expired and marked for deletion
    DELETED = 9          // Deleted from storage
};

/**
 * @brief Security scan result
 */
enum class SecurityScanResult {
    CLEAN = 0,           // No threats detected
    SUSPICIOUS = 1,      // Potentially suspicious
    MALWARE = 2,         // Malware detected
    VIRUS = 3,           // Virus detected
    PHISHING = 4,        // Phishing attempt
    SPAM = 5,            // Spam content
    INAPPROPRIATE = 6,   // Inappropriate content
    COPYRIGHT = 7,       // Copyright violation
    SCAN_FAILED = 8,     // Scan could not complete
    SCAN_TIMEOUT = 9     // Scan timed out
};

/**
 * @brief Image processing options
 */
struct ImageProcessingOptions {
    bool generate_thumbnails;
    std::vector<std::pair<uint32_t, uint32_t>> thumbnail_sizes; // width, height
    bool extract_metadata;
    bool generate_blur_hash;
    bool detect_faces;
    bool detect_text_ocr;
    uint32_t max_dimension; // Max width or height
    uint32_t quality_percent; // JPEG quality
    bool strip_exif;
    bool watermark;
    std::string watermark_text;
    
    Json::Value to_json() const;
    static ImageProcessingOptions from_json(const Json::Value& json);
    static ImageProcessingOptions default_options();
};

/**
 * @brief Video processing options
 */
struct VideoProcessingOptions {
    bool generate_thumbnails;
    std::vector<double> thumbnail_timestamps; // Seconds into video
    bool extract_audio;
    bool generate_preview;
    uint32_t preview_duration_seconds;
    std::string output_format; // mp4, webm, etc.
    uint32_t max_resolution; // 720, 1080, etc.
    uint32_t max_bitrate; // kbps
    bool compress;
    double compression_factor;
    bool extract_metadata;
    
    Json::Value to_json() const;
    static VideoProcessingOptions from_json(const Json::Value& json);
    static VideoProcessingOptions default_options();
};

/**
 * @brief Audio processing options
 */
struct AudioProcessingOptions {
    bool generate_waveform;
    bool extract_metadata;
    bool normalize_volume;
    bool compress;
    std::string output_format; // mp3, ogg, etc.
    uint32_t bitrate; // kbps
    uint32_t sample_rate; // Hz
    bool mono_conversion;
    uint32_t max_duration_seconds;
    bool noise_reduction;
    
    Json::Value to_json() const;
    static AudioProcessingOptions from_json(const Json::Value& json);
    static AudioProcessingOptions default_options();
};

/**
 * @brief Document processing options
 */
struct DocumentProcessingOptions {
    bool extract_text;
    bool generate_thumbnail;
    bool extract_metadata;
    bool scan_for_links;
    bool scan_for_emails;
    bool scan_for_phone_numbers;
    bool password_protected_check;
    uint32_t max_pages_to_process;
    bool ocr_images;
    std::vector<std::string> supported_languages; // For OCR
    
    Json::Value to_json() const;
    static DocumentProcessingOptions from_json(const Json::Value& json);
    static DocumentProcessingOptions default_options();
};

/**
 * @brief Comprehensive attachment metadata
 */
struct AttachmentMetadata {
    std::string attachment_id;
    std::string original_filename;
    std::string mime_type;
    AttachmentType type;
    uint64_t file_size;
    std::string file_hash_sha256;
    std::string file_hash_md5;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point modified_at;
    
    // Processing info
    ProcessingStatus status;
    double processing_progress; // 0.0 to 1.0
    std::string processing_stage;
    std::chrono::system_clock::time_point processing_started_at;
    std::chrono::system_clock::time_point processing_completed_at;
    
    // Security info
    SecurityScanResult security_scan_result;
    std::string security_scan_details;
    std::chrono::system_clock::time_point scanned_at;
    bool quarantined;
    
    // Storage info
    std::string storage_path;
    std::string encrypted_storage_path;
    std::string encryption_key_id;
    bool encrypted;
    uint64_t compressed_size;
    double compression_ratio;
    
    // Content analysis
    std::string extracted_text;
    std::vector<std::string> detected_languages;
    std::unordered_map<std::string, std::string> custom_metadata;
    
    // Media-specific
    std::unordered_map<std::string, Json::Value> media_metadata; // EXIF, etc.
    std::vector<std::string> thumbnail_paths;
    std::string preview_path;
    std::string waveform_data; // For audio
    std::string blur_hash; // For images
    
    // Access control
    std::vector<std::string> allowed_users;
    std::vector<std::string> allowed_chats;
    std::chrono::system_clock::time_point expires_at;
    uint32_t download_count;
    uint32_t max_downloads;
    
    Json::Value to_json() const;
    static AttachmentMetadata from_json(const Json::Value& json);
    bool is_expired() const;
    bool can_access(const std::string& user_id, const std::string& chat_id) const;
    std::string get_display_size() const;
    std::chrono::milliseconds get_processing_duration() const;
};

/**
 * @brief Attachment upload session for chunked uploads
 */
struct UploadSession {
    std::string session_id;
    std::string user_id;
    std::string chat_id;
    std::string filename;
    uint64_t total_size;
    uint64_t uploaded_size;
    uint32_t chunk_size;
    std::vector<bool> received_chunks;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_activity;
    std::chrono::system_clock::time_point expires_at;
    std::string temp_file_path;
    bool is_complete;
    
    Json::Value to_json() const;
    static UploadSession from_json(const Json::Value& json);
    bool is_expired() const;
    double get_progress() const;
    uint32_t get_next_chunk_index() const;
    std::chrono::milliseconds get_remaining_time() const;
};

/**
 * @brief Attachment download session for controlled access
 */
struct DownloadSession {
    std::string session_id;
    std::string attachment_id;
    std::string user_id;
    std::string chat_id;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point expires_at;
    uint64_t bytes_downloaded;
    bool completed;
    std::string client_ip;
    std::string user_agent;
    
    Json::Value to_json() const;
    static DownloadSession from_json(const Json::Value& json);
    bool is_expired() const;
    bool is_authorized(const std::string& user_id, const std::string& chat_id) const;
};

/**
 * @brief Attachment processing queue item
 */
struct ProcessingQueueItem {
    std::string item_id;
    std::string attachment_id;
    std::string processing_type; // thumbnail, encryption, scan, etc.
    Json::Value processing_options;
    uint32_t priority; // Higher number = higher priority
    std::chrono::system_clock::time_point queued_at;
    std::chrono::system_clock::time_point started_at;
    uint32_t retry_count;
    uint32_t max_retries;
    std::string assigned_worker;
    
    Json::Value to_json() const;
    static ProcessingQueueItem from_json(const Json::Value& json);
    bool should_retry() const;
    std::chrono::milliseconds get_queue_time() const;
};

/**
 * @brief Attachment storage configuration
 */
struct AttachmentStorageConfig {
    std::string storage_type; // local, s3, azure, gcs, etc.
    std::string base_path;
    uint64_t max_file_size;
    uint64_t max_total_storage;
    std::chrono::hours retention_period;
    bool auto_cleanup_enabled;
    
    // Chunked upload settings
    uint32_t chunk_size;
    std::chrono::minutes upload_timeout;
    uint32_t max_concurrent_uploads;
    
    // Processing settings
    uint32_t max_processing_workers;
    std::chrono::minutes processing_timeout;
    bool enable_parallel_processing;
    
    // Security settings
    bool enable_virus_scanning;
    bool enable_content_scanning;
    std::vector<std::string> blocked_extensions;
    std::vector<std::string> blocked_mime_types;
    uint64_t max_scan_size;
    
    // Encryption settings
    bool encrypt_all_files;
    std::string encryption_algorithm;
    uint32_t encryption_key_size;
    bool encrypt_metadata;
    
    Json::Value to_json() const;
    static AttachmentStorageConfig from_json(const Json::Value& json);
    static AttachmentStorageConfig default_config();
};

/**
 * @brief Advanced attachment processing and management system
 * 
 * Provides enterprise-grade attachment handling including:
 * - Chunked upload with resume capability
 * - Multi-format processing (images, videos, audio, documents)
 * - Advanced security scanning and content analysis
 * - Encrypted storage with access control
 * - Real-time processing with progress tracking
 * - Thumbnail and preview generation
 * - Content extraction and indexing
 * - Automatic compression and optimization
 * - Virus and malware detection
 * - Compliance and retention management
 */
class AdvancedAttachmentManager {
public:
    AdvancedAttachmentManager(const AttachmentStorageConfig& config = AttachmentStorageConfig::default_config());
    ~AdvancedAttachmentManager();
    
    // Upload management
    std::future<UploadSession> create_upload_session(const std::string& user_id,
                                                     const std::string& chat_id,
                                                     const std::string& filename,
                                                     uint64_t file_size,
                                                     const std::string& mime_type);
    
    std::future<bool> upload_chunk(const std::string& session_id,
                                  uint32_t chunk_index,
                                  const std::vector<uint8_t>& chunk_data);
    
    std::future<AttachmentMetadata> finalize_upload(const std::string& session_id,
                                                    const std::string& file_hash = "");
    
    std::future<bool> cancel_upload(const std::string& session_id);
    std::future<UploadSession> get_upload_session(const std::string& session_id);
    std::future<std::vector<UploadSession>> get_user_upload_sessions(const std::string& user_id);
    
    // Direct upload (small files)
    std::future<AttachmentMetadata> upload_file(const std::string& user_id,
                                               const std::string& chat_id,
                                               const std::string& filename,
                                               const std::vector<uint8_t>& file_data,
                                               const std::string& mime_type = "");
    
    std::future<AttachmentMetadata> upload_from_path(const std::string& user_id,
                                                    const std::string& chat_id,
                                                    const std::filesystem::path& file_path);
    
    // Download management
    std::future<DownloadSession> create_download_session(const std::string& attachment_id,
                                                         const std::string& user_id,
                                                         const std::string& chat_id);
    
    std::future<std::vector<uint8_t>> download_file(const std::string& session_id);
    std::future<std::vector<uint8_t>> download_chunk(const std::string& session_id,
                                                    uint64_t offset,
                                                    uint32_t size);
    
    std::future<std::filesystem::path> get_file_path(const std::string& attachment_id,
                                                    const std::string& user_id);
    
    std::future<bool> stream_file(const std::string& attachment_id,
                                 const std::string& user_id,
                                 std::function<bool(const uint8_t*, size_t)> callback);
    
    // Processing management
    std::future<bool> process_attachment(const std::string& attachment_id,
                                        const std::string& processing_type,
                                        const Json::Value& options = Json::Value());
    
    std::future<bool> process_image(const std::string& attachment_id,
                                   const ImageProcessingOptions& options = ImageProcessingOptions::default_options());
    
    std::future<bool> process_video(const std::string& attachment_id,
                                   const VideoProcessingOptions& options = VideoProcessingOptions::default_options());
    
    std::future<bool> process_audio(const std::string& attachment_id,
                                   const AudioProcessingOptions& options = AudioProcessingOptions::default_options());
    
    std::future<bool> process_document(const std::string& attachment_id,
                                      const DocumentProcessingOptions& options = DocumentProcessingOptions::default_options());
    
    // Batch processing
    std::future<std::vector<std::string>> batch_process_attachments(const std::vector<std::string>& attachment_ids,
                                                                   const std::string& processing_type,
                                                                   const Json::Value& options = Json::Value());
    
    // Metadata and querying
    std::future<AttachmentMetadata> get_attachment_metadata(const std::string& attachment_id);
    std::future<bool> update_attachment_metadata(const std::string& attachment_id,
                                                 const Json::Value& metadata_updates);
    
    std::future<std::vector<AttachmentMetadata>> get_chat_attachments(const std::string& chat_id,
                                                                     AttachmentType type = AttachmentType::UNKNOWN,
                                                                     uint32_t limit = 50);
    
    std::future<std::vector<AttachmentMetadata>> get_user_attachments(const std::string& user_id,
                                                                     AttachmentType type = AttachmentType::UNKNOWN,
                                                                     uint32_t limit = 50);
    
    std::future<std::vector<AttachmentMetadata>> search_attachments(const std::string& query,
                                                                   const std::string& user_id = "",
                                                                   const std::string& chat_id = "");
    
    // Thumbnail and preview management
    std::future<std::vector<uint8_t>> get_thumbnail(const std::string& attachment_id,
                                                   uint32_t width = 0,
                                                   uint32_t height = 0);
    
    std::future<std::vector<uint8_t>> get_preview(const std::string& attachment_id);
    std::future<std::string> get_blur_hash(const std::string& attachment_id);
    std::future<std::vector<uint8_t>> get_waveform_data(const std::string& attachment_id);
    
    // Security and scanning
    std::future<SecurityScanResult> scan_attachment(const std::string& attachment_id,
                                                   bool deep_scan = false);
    
    std::future<bool> quarantine_attachment(const std::string& attachment_id,
                                           const std::string& reason);
    
    std::future<bool> restore_from_quarantine(const std::string& attachment_id,
                                             const std::string& user_id);
    
    std::future<std::vector<AttachmentMetadata>> get_quarantined_attachments();
    
    // Content analysis
    std::future<std::string> extract_text_content(const std::string& attachment_id);
    std::future<std::vector<std::string>> detect_languages(const std::string& attachment_id);
    std::future<Json::Value> analyze_image_content(const std::string& attachment_id);
    std::future<Json::Value> analyze_audio_content(const std::string& attachment_id);
    
    // Access control
    std::future<bool> set_attachment_permissions(const std::string& attachment_id,
                                                const std::vector<std::string>& allowed_users,
                                                const std::vector<std::string>& allowed_chats);
    
    std::future<bool> set_attachment_expiry(const std::string& attachment_id,
                                           const std::chrono::system_clock::time_point& expires_at);
    
    std::future<bool> set_download_limit(const std::string& attachment_id,
                                        uint32_t max_downloads);
    
    std::future<bool> check_access_permission(const std::string& attachment_id,
                                             const std::string& user_id,
                                             const std::string& chat_id);
    
    // Storage management
    std::future<bool> delete_attachment(const std::string& attachment_id,
                                       bool permanent = false);
    
    std::future<bool> compress_attachment(const std::string& attachment_id,
                                         double compression_factor = 0.8);
    
    std::future<bool> encrypt_attachment(const std::string& attachment_id,
                                        const std::string& encryption_key = "");
    
    std::future<bool> decrypt_attachment(const std::string& attachment_id,
                                        const std::string& encryption_key = "");
    
    // Maintenance and cleanup
    std::future<uint64_t> cleanup_expired_attachments();
    std::future<uint64_t> cleanup_temp_files();
    std::future<uint64_t> optimize_storage();
    
    std::future<Json::Value> get_storage_statistics();
    std::future<std::vector<std::string>> get_orphaned_attachments();
    
    // Processing queue management
    std::future<std::vector<ProcessingQueueItem>> get_processing_queue();
    std::future<ProcessingQueueItem> get_processing_status(const std::string& attachment_id);
    std::future<bool> cancel_processing(const std::string& attachment_id);
    std::future<bool> retry_failed_processing(const std::string& attachment_id);
    
    // Real-time subscriptions
    void subscribe_to_upload_progress(const std::string& session_id,
                                     const std::string& subscriber_id,
                                     std::function<void(const UploadSession&)> callback);
    
    void subscribe_to_processing_progress(const std::string& attachment_id,
                                         const std::string& subscriber_id,
                                         std::function<void(const AttachmentMetadata&)> callback);
    
    void unsubscribe_from_upload_progress(const std::string& session_id,
                                         const std::string& subscriber_id);
    
    void unsubscribe_from_processing_progress(const std::string& attachment_id,
                                             const std::string& subscriber_id);
    
    // Configuration
    void update_configuration(const AttachmentStorageConfig& new_config);
    AttachmentStorageConfig get_configuration() const;
    
    // Analytics and reporting
    std::future<Json::Value> get_usage_analytics(const std::string& user_id = "",
                                                 const std::chrono::system_clock::time_point& start = {},
                                                 const std::chrono::system_clock::time_point& end = {});
    
    std::future<std::vector<AttachmentMetadata>> get_popular_attachments(uint32_t limit = 20);
    std::future<std::map<AttachmentType, uint32_t>> get_attachment_type_distribution();
    
private:
    // Configuration
    AttachmentStorageConfig config_;
    
    // Storage
    std::unordered_map<std::string, AttachmentMetadata> attachments_;
    std::unordered_map<std::string, UploadSession> upload_sessions_;
    std::unordered_map<std::string, DownloadSession> download_sessions_;
    std::queue<ProcessingQueueItem> processing_queue_;
    std::unordered_map<std::string, ProcessingQueueItem> active_processing_;
    
    // Subscriptions
    std::unordered_map<std::string, std::unordered_map<std::string, std::function<void(const UploadSession&)>>> upload_subscribers_;
    std::unordered_map<std::string, std::unordered_map<std::string, std::function<void(const AttachmentMetadata&)>>> processing_subscribers_;
    
    // Thread safety
    mutable std::shared_mutex attachments_mutex_;
    mutable std::shared_mutex sessions_mutex_;
    mutable std::shared_mutex queue_mutex_;
    mutable std::shared_mutex subscriptions_mutex_;
    
    // Background processing
    std::vector<std::thread> processing_workers_;
    std::thread cleanup_thread_;
    std::atomic<bool> background_running_;
    
    // Internal methods
    AttachmentType detect_attachment_type(const std::string& filename, const std::string& mime_type);
    std::string generate_attachment_id();
    std::string generate_session_id();
    std::string calculate_file_hash(const std::vector<uint8_t>& data);
    bool validate_file_type(const std::string& filename, const std::string& mime_type);
    bool validate_file_size(uint64_t size);
    
    // Processing methods
    void run_processing_worker(uint32_t worker_id);
    void run_cleanup_worker();
    bool process_image_internal(const std::string& attachment_id, const ImageProcessingOptions& options);
    bool process_video_internal(const std::string& attachment_id, const VideoProcessingOptions& options);
    bool process_audio_internal(const std::string& attachment_id, const AudioProcessingOptions& options);
    bool process_document_internal(const std::string& attachment_id, const DocumentProcessingOptions& options);
    
    // Security methods
    SecurityScanResult scan_file_for_threats(const std::filesystem::path& file_path);
    bool is_safe_mime_type(const std::string& mime_type);
    bool is_blocked_extension(const std::string& filename);
    
    // Storage methods
    std::filesystem::path get_storage_path(const std::string& attachment_id);
    std::filesystem::path get_thumbnail_path(const std::string& attachment_id, uint32_t width, uint32_t height);
    bool save_file_to_storage(const std::string& attachment_id, const std::vector<uint8_t>& data);
    std::vector<uint8_t> load_file_from_storage(const std::string& attachment_id);
    
    // Notification methods
    void notify_upload_subscribers(const std::string& session_id, const UploadSession& session);
    void notify_processing_subscribers(const std::string& attachment_id, const AttachmentMetadata& metadata);
    
    // Utility methods
    void log_info(const std::string& message);
    void log_warning(const std::string& message);
    void log_error(const std::string& message);
};

/**
 * @brief Attachment processing utilities
 */
class AttachmentUtils {
public:
    // Type detection
    static AttachmentType detect_type_from_extension(const std::string& filename);
    static AttachmentType detect_type_from_mime(const std::string& mime_type);
    static std::string get_mime_type_from_extension(const std::string& extension);
    
    // File validation
    static bool is_valid_filename(const std::string& filename);
    static bool is_safe_file_type(AttachmentType type);
    static uint64_t get_max_file_size_for_type(AttachmentType type);
    
    // Content analysis
    static std::vector<uint8_t> generate_thumbnail(const std::vector<uint8_t>& image_data,
                                                  uint32_t width, uint32_t height);
    static std::string calculate_blur_hash(const std::vector<uint8_t>& image_data);
    static std::vector<double> extract_audio_waveform(const std::vector<uint8_t>& audio_data);
    
    // Metadata extraction
    static Json::Value extract_image_metadata(const std::vector<uint8_t>& image_data);
    static Json::Value extract_video_metadata(const std::vector<uint8_t>& video_data);
    static Json::Value extract_audio_metadata(const std::vector<uint8_t>& audio_data);
    static Json::Value extract_document_metadata(const std::vector<uint8_t>& document_data);
    
    // Compression
    static std::vector<uint8_t> compress_image(const std::vector<uint8_t>& image_data,
                                             uint32_t quality = 85);
    static std::vector<uint8_t> compress_video(const std::vector<uint8_t>& video_data,
                                             uint32_t bitrate = 1000);
    
    // Text extraction
    static std::string extract_text_from_pdf(const std::vector<uint8_t>& pdf_data);
    static std::string extract_text_from_office(const std::vector<uint8_t>& office_data);
    static std::string extract_text_from_image_ocr(const std::vector<uint8_t>& image_data);
    
    // Format conversion
    static std::vector<uint8_t> convert_image_format(const std::vector<uint8_t>& image_data,
                                                    const std::string& target_format);
    static std::vector<uint8_t> convert_audio_format(const std::vector<uint8_t>& audio_data,
                                                    const std::string& target_format,
                                                    uint32_t bitrate = 128);
    
    // Security
    static bool has_malicious_signature(const std::vector<uint8_t>& file_data);
    static bool contains_suspicious_content(const std::string& text_content);
    static std::vector<std::string> extract_embedded_urls(const std::vector<uint8_t>& file_data);
    
    // Utilities
    static std::string format_file_size(uint64_t bytes);
    static std::string get_file_extension(const std::string& filename);
    static std::string sanitize_filename(const std::string& filename);
    static bool is_archive_file(const std::string& filename);
    static std::vector<std::string> list_archive_contents(const std::vector<uint8_t>& archive_data);
};

} // namespace sonet::messaging::attachments
