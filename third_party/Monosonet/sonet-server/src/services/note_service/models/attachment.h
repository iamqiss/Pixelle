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
#include <unordered_map>
#include <optional>
#include <ctime>
#include <nlohmann/json.hpp>

namespace sonet::note::models {

using json = nlohmann::json;

/**
 * @brief Media attachment types supported by the note service
 */
enum class AttachmentType {
    IMAGE = 0,          // Static images (JPEG, PNG, WebP, AVIF)
    VIDEO = 1,          // Video files (MP4, WebM, MOV)
    GIF = 2,            // Animated GIFs
    TENOR_GIF = 3,      // Tenor GIF integration
    AUDIO = 4,          // Audio files (MP3, AAC, OGG)
    DOCUMENT = 5,       // Documents (PDF, TXT)
    LINK_PREVIEW = 6,   // Rich link previews
    POLL = 7,           // Poll attachments
    LOCATION = 8,       // Location/map attachments
    STICKER = 9,        // Custom stickers
    EMOJI_REACTION = 10 // Custom emoji reactions
};

/**
 * @brief Processing status for media attachments
 */
enum class ProcessingStatus {
    PENDING = 0,        // Waiting for processing
    PROCESSING = 1,     // Currently being processed
    COMPLETED = 2,      // Processing completed successfully
    FAILED = 3,         // Processing failed
    CANCELLED = 4,      // Processing was cancelled
    VIRUS_DETECTED = 5, // Virus/malware detected
    REJECTED = 6        // Rejected due to policy violation
};

/**
 * @brief Media quality/resolution variants
 */
enum class MediaQuality {
    THUMBNAIL = 0,      // Small thumbnail (150x150)
    LOW = 1,           // Low quality (480p)
    MEDIUM = 2,        // Medium quality (720p)
    HIGH = 3,          // High quality (1080p)
    ORIGINAL = 4       // Original quality
};

/**
 * @brief Tenor GIF categories and metadata
 */
struct TenorGifData {
    std::string tenor_id;           // Tenor GIF ID
    std::string search_term;        // Original search term
    std::string title;              // GIF title
    std::string content_description; // Content description
    std::vector<std::string> tags;  // Associated tags
    std::string category;           // Tenor category
    bool has_audio = false;         // Whether GIF has audio
    int view_count = 0;             // Tenor view count
    double rating = 0.0;            // Content rating (0.0-10.0)
    
    json to_json() const;
    static TenorGifData from_json(const json& j);
    bool validate() const;
};

/**
 * @brief Media variant with different qualities/formats
 */
struct MediaVariant {
    MediaQuality quality;           // Quality level
    std::string url;                // CDN URL for this variant
    std::string format;             // File format (jpg, webp, mp4, etc.)
    int width = 0;                  // Width in pixels
    int height = 0;                 // Height in pixels
    size_t file_size = 0;           // File size in bytes
    int bitrate = 0;                // Bitrate for video/audio (kbps)
    double duration = 0.0;          // Duration for video/audio (seconds)
    
    json to_json() const;
    static MediaVariant from_json(const json& j);
    bool validate() const;
};

/**
 * @brief Link preview metadata
 */
struct LinkPreview {
    std::string url;                // Original URL
    std::string title;              // Page title
    std::string description;        // Meta description
    std::string site_name;          // Site name (e.g., "YouTube")
    std::string author;             // Content author
    std::string thumbnail_url;      // Preview image URL
    std::string favicon_url;        // Site favicon URL
    std::string canonical_url;      // Canonical URL
    std::vector<std::string> keywords; // Meta keywords
    bool is_video = false;          // Whether it's a video link
    bool is_image = false;          // Whether it's an image link
    bool is_article = false;        // Whether it's an article
    double reading_time = 0.0;      // Estimated reading time (minutes)
    
    json to_json() const;
    static LinkPreview from_json(const json& j);
    bool validate() const;
};

/**
 * @brief Poll option for poll attachments
 */
struct PollOption {
    std::string option_id;          // Unique option ID
    std::string text;               // Option text
    int vote_count = 0;             // Number of votes
    double percentage = 0.0;        // Percentage of total votes
    std::vector<std::string> voter_ids; // User IDs who voted for this option
    
    json to_json() const;
    static PollOption from_json(const json& j);
    bool validate() const;
};

/**
 * @brief Poll attachment data
 */
struct PollData {
    std::string poll_id;            // Unique poll ID
    std::string question;           // Poll question
    std::vector<PollOption> options; // Poll options
    bool multiple_choice = false;   // Allow multiple selections
    bool anonymous = true;          // Anonymous voting
    std::time_t expires_at = 0;     // Poll expiration time
    int total_votes = 0;            // Total number of votes
    bool is_expired = false;        // Whether poll has expired
    std::vector<std::string> voted_user_ids; // Users who have voted
    
    json to_json() const;
    static PollData from_json(const json& j);
    bool validate() const;
};

/**
 * @brief Location/geographic data
 */
struct LocationData {
    std::string place_id;           // Unique place identifier
    std::string name;               // Location name
    std::string address;            // Full address
    double latitude = 0.0;          // Latitude coordinate
    double longitude = 0.0;         // Longitude coordinate
    std::string city;               // City name
    std::string country;            // Country name
    std::string country_code;       // ISO country code
    std::string timezone;           // Timezone identifier
    std::unordered_map<std::string, std::string> metadata; // Additional metadata
    
    json to_json() const;
    static LocationData from_json(const json& j);
    bool validate() const;
};

/**
 * @brief Comprehensive media attachment model
 * 
 * Supports multiple media types with rich metadata, processing status,
 * content moderation, and CDN integration for Twitter-scale performance.
 */
class Attachment {
public:
    // Core identification
    std::string attachment_id;      // Unique attachment identifier
    std::string note_id;            // Associated note ID
    std::string uploader_id;        // User who uploaded the attachment
    
    // Media properties
    AttachmentType type;            // Type of attachment
    ProcessingStatus status;        // Current processing status
    std::string original_filename;  // Original filename
    std::string mime_type;          // MIME type
    size_t file_size = 0;           // Original file size in bytes
    std::string checksum;           // File checksum (SHA-256)
    
    // Media dimensions and metadata
    int width = 0;                  // Original width (for images/videos)
    int height = 0;                 // Original height (for images/videos)
    double duration = 0.0;          // Duration for video/audio (seconds)
    int bitrate = 0;                // Bitrate for video/audio (kbps)
    std::string color_palette;      // Dominant colors (JSON array)
    bool has_transparency = false;  // Whether image has transparency
    
    // Content and accessibility
    std::string alt_text;           // Alternative text for accessibility
    std::string caption;            // User-provided caption
    std::string description;        // Auto-generated description
    std::vector<std::string> tags;  // Content tags
    bool is_sensitive = false;      // Whether content is sensitive/NSFW
    bool is_spoiler = false;        // Whether content is a spoiler
    
    // CDN and storage
    std::string primary_url;        // Primary CDN URL
    std::string backup_url;         // Backup CDN URL
    std::string storage_path;       // Internal storage path
    std::vector<MediaVariant> variants; // Different quality variants
    
    // Type-specific data
    std::optional<TenorGifData> tenor_data;     // Tenor GIF metadata
    std::optional<LinkPreview> link_preview;   // Link preview data
    std::optional<PollData> poll_data;         // Poll data
    std::optional<LocationData> location_data; // Location data
    
    // Processing and moderation
    std::string processing_job_id;  // Background job ID
    std::vector<std::string> processing_errors; // Processing error messages
    std::unordered_map<std::string, std::string> moderation_flags; // Content moderation flags
    double content_safety_score = 1.0; // Safety score (0.0-1.0, higher = safer)
    
    // Analytics and engagement
    int view_count = 0;             // Number of views
    int download_count = 0;         // Number of downloads
    int share_count = 0;            // Number of shares
    std::vector<std::string> viewer_ids; // User IDs who viewed this attachment
    
    // Timestamps
    std::time_t created_at = 0;     // Creation timestamp
    std::time_t updated_at = 0;     // Last update timestamp
    std::time_t processed_at = 0;   // Processing completion timestamp
    std::time_t expires_at = 0;     // Expiration timestamp (0 = never expires)
    
    // Constructors
    Attachment() = default;
    explicit Attachment(const std::string& attachment_id);
    
    // Factory methods for different attachment types
    static Attachment create_image_attachment(const std::string& uploader_id, const std::string& filename, 
                                            const std::string& mime_type, size_t file_size);
    static Attachment create_video_attachment(const std::string& uploader_id, const std::string& filename, 
                                            const std::string& mime_type, size_t file_size, double duration);
    static Attachment create_tenor_gif(const std::string& uploader_id, const TenorGifData& tenor_data);
    static Attachment create_link_preview(const std::string& uploader_id, const LinkPreview& preview);
    static Attachment create_poll(const std::string& uploader_id, const PollData& poll);
    static Attachment create_location(const std::string& uploader_id, const LocationData& location);
    
    // Media variant management
    void add_variant(const MediaVariant& variant);
    std::optional<MediaVariant> get_best_variant(MediaQuality preferred_quality) const;
    std::vector<MediaVariant> get_variants_by_format(const std::string& format) const;
    void clear_variants();
    
    // URL generation
    std::string get_url(MediaQuality quality = MediaQuality::HIGH) const;
    std::string get_thumbnail_url() const;
    std::string get_download_url() const;
    
    // Content processing
    void set_processing_status(ProcessingStatus status, const std::string& error_message = "");
    void add_processing_error(const std::string& error);
    void clear_processing_errors();
    bool is_processing_complete() const;
    bool is_processing_failed() const;
    
    // Content moderation
    void add_moderation_flag(const std::string& flag, const std::string& reason);
    void remove_moderation_flag(const std::string& flag);
    bool has_moderation_flags() const;
    std::vector<std::string> get_moderation_flags() const;
    void set_content_safety_score(double score);
    bool is_content_safe(double threshold = 0.7) const;
    
    // Analytics
    void record_view(const std::string& user_id);
    void record_download(const std::string& user_id);
    void record_share(const std::string& user_id);
    int get_unique_viewers() const;
    
    // Validation and constraints
    bool validate() const;
    bool is_within_size_limits() const;
    bool is_supported_format() const;
    static bool is_valid_mime_type(const std::string& mime_type);
    static size_t get_max_file_size(AttachmentType type);
    static std::vector<std::string> get_supported_formats(AttachmentType type);
    
    // Serialization
    json to_json() const;
    static Attachment from_json(const json& j);
    std::string to_string() const;
    
    // Utility methods
    std::string get_file_extension() const;
    std::string get_display_name() const;
    bool is_image() const;
    bool is_video() const;
    bool is_audio() const;
    bool is_animated() const;
    bool requires_processing() const;
    double get_aspect_ratio() const;
    
    // Comparison operators
    bool operator==(const Attachment& other) const;
    bool operator!=(const Attachment& other) const;
    
private:
    // Internal helper methods
    void initialize_defaults();
    void validate_type_specific_data() const;
    std::string generate_storage_path() const;
    void update_timestamps();
    
    // Content safety helpers
    bool contains_sensitive_content() const;
    bool violates_content_policy() const;
    
    // URL generation helpers
    std::string build_cdn_url(const std::string& path, const std::unordered_map<std::string, std::string>& params = {}) const;
    std::string get_variant_url(const MediaVariant& variant) const;
};

/**
 * @brief Collection of attachments with bulk operations
 */
class AttachmentCollection {
public:
    static constexpr size_t MAX_ATTACHMENTS = 10;  // Twitter-like limit
    
    std::vector<Attachment> attachments;
    
    // Collection management
    bool add_attachment(const Attachment& attachment);
    bool remove_attachment(const std::string& attachment_id);
    void clear();
    size_t size() const;
    bool empty() const;
    bool is_full() const;
    
    // Validation
    bool validate() const;
    bool is_within_total_size_limit() const;
    bool has_mixed_types() const;
    
    // Bulk operations
    void set_note_id(const std::string& note_id);
    void mark_all_as_sensitive(bool is_sensitive);
    std::vector<Attachment> get_by_type(AttachmentType type) const;
    std::vector<Attachment> get_processing_attachments() const;
    std::vector<Attachment> get_failed_attachments() const;
    
    // Analytics
    int get_total_views() const;
    int get_total_downloads() const;
    size_t get_total_size() const;
    
    // Serialization
    json to_json() const;
    static AttachmentCollection from_json(const json& j);
    
    // Iterators
    std::vector<Attachment>::iterator begin() { return attachments.begin(); }
    std::vector<Attachment>::iterator end() { return attachments.end(); }
    std::vector<Attachment>::const_iterator begin() const { return attachments.begin(); }
    std::vector<Attachment>::const_iterator end() const { return attachments.end(); }
};

// Global constants
namespace attachment_constants {
    // File size limits (in bytes)
    constexpr size_t MAX_IMAGE_SIZE = 10 * 1024 * 1024;      // 10MB
    constexpr size_t MAX_VIDEO_SIZE = 100 * 1024 * 1024;     // 100MB
    constexpr size_t MAX_AUDIO_SIZE = 25 * 1024 * 1024;      // 25MB
    constexpr size_t MAX_DOCUMENT_SIZE = 50 * 1024 * 1024;   // 50MB
    constexpr size_t MAX_TOTAL_SIZE = 200 * 1024 * 1024;     // 200MB total per note
    
    // Dimension limits
    constexpr int MAX_IMAGE_DIMENSION = 8192;                // 8K max
    constexpr int MAX_VIDEO_DIMENSION = 4096;                // 4K max
    constexpr double MAX_VIDEO_DURATION = 600.0;             // 10 minutes max
    constexpr double MAX_AUDIO_DURATION = 1800.0;            // 30 minutes max
    
    // Processing timeouts
    constexpr int PROCESSING_TIMEOUT_SECONDS = 300;          // 5 minutes
    constexpr int THUMBNAIL_GENERATION_TIMEOUT = 30;         // 30 seconds
    
    // Content safety
    constexpr double DEFAULT_SAFETY_THRESHOLD = 0.7;         // 70% safety score
    constexpr int MAX_MODERATION_FLAGS = 10;                 // Max flags per attachment
}

} // namespace sonet::note::models