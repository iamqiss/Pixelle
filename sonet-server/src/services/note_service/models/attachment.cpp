/*
 * Copyright (c) 2025 Neo Qiss
 * All rights reserved.
 * 
 * This software is proprietary and confidential.
 * Unauthorized copying, distribution, or use is strictly prohibited.
 */

#include "attachment.h"
#include "../../core/utils/id_generator.h"
#include "../../core/utils/string_utils.h"
#include "../../core/validation/input_sanitizer.h"
#include <spdlog/spdlog.h>
#include <algorithm>
#include <regex>
#include <unordered_set>
#include <filesystem>

namespace sonet::note::models {

// TenorGifData implementation

json TenorGifData::to_json() const {
    return json{
        {"tenor_id", tenor_id},
        {"search_term", search_term},
        {"title", title},
        {"content_description", content_description},
        {"tags", tags},
        {"category", category},
        {"has_audio", has_audio},
        {"view_count", view_count},
        {"rating", rating}
    };
}

TenorGifData TenorGifData::from_json(const json& j) {
    TenorGifData data;
    
    if (j.contains("tenor_id")) data.tenor_id = j["tenor_id"];
    if (j.contains("search_term")) data.search_term = j["search_term"];
    if (j.contains("title")) data.title = j["title"];
    if (j.contains("content_description")) data.content_description = j["content_description"];
    if (j.contains("tags")) data.tags = j["tags"];
    if (j.contains("category")) data.category = j["category"];
    if (j.contains("has_audio")) data.has_audio = j["has_audio"];
    if (j.contains("view_count")) data.view_count = j["view_count"];
    if (j.contains("rating")) data.rating = j["rating"];
    
    return data;
}

bool TenorGifData::validate() const {
    return !tenor_id.empty() && !title.empty() && rating >= 0.0 && rating <= 10.0;
}

// MediaVariant implementation

json MediaVariant::to_json() const {
    return json{
        {"quality", static_cast<int>(quality)},
        {"url", url},
        {"format", format},
        {"width", width},
        {"height", height},
        {"file_size", file_size},
        {"bitrate", bitrate},
        {"duration", duration}
    };
}

MediaVariant MediaVariant::from_json(const json& j) {
    MediaVariant variant;
    
    if (j.contains("quality")) variant.quality = static_cast<MediaQuality>(j["quality"]);
    if (j.contains("url")) variant.url = j["url"];
    if (j.contains("format")) variant.format = j["format"];
    if (j.contains("width")) variant.width = j["width"];
    if (j.contains("height")) variant.height = j["height"];
    if (j.contains("file_size")) variant.file_size = j["file_size"];
    if (j.contains("bitrate")) variant.bitrate = j["bitrate"];
    if (j.contains("duration")) variant.duration = j["duration"];
    
    return variant;
}

bool MediaVariant::validate() const {
    return !url.empty() && !format.empty() && width >= 0 && height >= 0 && 
           file_size > 0 && bitrate >= 0 && duration >= 0.0;
}

// LinkPreview implementation

json LinkPreview::to_json() const {
    return json{
        {"url", url},
        {"title", title},
        {"description", description},
        {"site_name", site_name},
        {"author", author},
        {"thumbnail_url", thumbnail_url},
        {"favicon_url", favicon_url},
        {"canonical_url", canonical_url},
        {"keywords", keywords},
        {"is_video", is_video},
        {"is_image", is_image},
        {"is_article", is_article},
        {"reading_time", reading_time}
    };
}

LinkPreview LinkPreview::from_json(const json& j) {
    LinkPreview preview;
    
    if (j.contains("url")) preview.url = j["url"];
    if (j.contains("title")) preview.title = j["title"];
    if (j.contains("description")) preview.description = j["description"];
    if (j.contains("site_name")) preview.site_name = j["site_name"];
    if (j.contains("author")) preview.author = j["author"];
    if (j.contains("thumbnail_url")) preview.thumbnail_url = j["thumbnail_url"];
    if (j.contains("favicon_url")) preview.favicon_url = j["favicon_url"];
    if (j.contains("canonical_url")) preview.canonical_url = j["canonical_url"];
    if (j.contains("keywords")) preview.keywords = j["keywords"];
    if (j.contains("is_video")) preview.is_video = j["is_video"];
    if (j.contains("is_image")) preview.is_image = j["is_image"];
    if (j.contains("is_article")) preview.is_article = j["is_article"];
    if (j.contains("reading_time")) preview.reading_time = j["reading_time"];
    
    return preview;
}

bool LinkPreview::validate() const {
    // Basic URL validation
    std::regex url_pattern(R"(^https?://.+)");
    return std::regex_match(url, url_pattern) && !title.empty();
}

// PollOption implementation

json PollOption::to_json() const {
    return json{
        {"option_id", option_id},
        {"text", text},
        {"vote_count", vote_count},
        {"percentage", percentage},
        {"voter_ids", voter_ids}
    };
}

PollOption PollOption::from_json(const json& j) {
    PollOption option;
    
    if (j.contains("option_id")) option.option_id = j["option_id"];
    if (j.contains("text")) option.text = j["text"];
    if (j.contains("vote_count")) option.vote_count = j["vote_count"];
    if (j.contains("percentage")) option.percentage = j["percentage"];
    if (j.contains("voter_ids")) option.voter_ids = j["voter_ids"];
    
    return option;
}

bool PollOption::validate() const {
    return !option_id.empty() && !text.empty() && text.length() <= 100 && 
           vote_count >= 0 && percentage >= 0.0 && percentage <= 100.0;
}

// PollData implementation

json PollData::to_json() const {
    json options_json = json::array();
    for (const auto& option : options) {
        options_json.push_back(option.to_json());
    }
    
    return json{
        {"poll_id", poll_id},
        {"question", question},
        {"options", options_json},
        {"multiple_choice", multiple_choice},
        {"anonymous", anonymous},
        {"expires_at", expires_at},
        {"total_votes", total_votes},
        {"is_expired", is_expired},
        {"voted_user_ids", voted_user_ids}
    };
}

PollData PollData::from_json(const json& j) {
    PollData poll;
    
    if (j.contains("poll_id")) poll.poll_id = j["poll_id"];
    if (j.contains("question")) poll.question = j["question"];
    if (j.contains("multiple_choice")) poll.multiple_choice = j["multiple_choice"];
    if (j.contains("anonymous")) poll.anonymous = j["anonymous"];
    if (j.contains("expires_at")) poll.expires_at = j["expires_at"];
    if (j.contains("total_votes")) poll.total_votes = j["total_votes"];
    if (j.contains("is_expired")) poll.is_expired = j["is_expired"];
    if (j.contains("voted_user_ids")) poll.voted_user_ids = j["voted_user_ids"];
    
    if (j.contains("options") && j["options"].is_array()) {
        for (const auto& option_json : j["options"]) {
            poll.options.push_back(PollOption::from_json(option_json));
        }
    }
    
    return poll;
}

bool PollData::validate() const {
    return !poll_id.empty() && !question.empty() && question.length() <= 500 &&
           options.size() >= 2 && options.size() <= 10 && total_votes >= 0 &&
           std::all_of(options.begin(), options.end(), [](const auto& opt) { return opt.validate(); });
}

// LocationData implementation

json LocationData::to_json() const {
    return json{
        {"place_id", place_id},
        {"name", name},
        {"address", address},
        {"latitude", latitude},
        {"longitude", longitude},
        {"city", city},
        {"country", country},
        {"country_code", country_code},
        {"timezone", timezone},
        {"metadata", metadata}
    };
}

LocationData LocationData::from_json(const json& j) {
    LocationData location;
    
    if (j.contains("place_id")) location.place_id = j["place_id"];
    if (j.contains("name")) location.name = j["name"];
    if (j.contains("address")) location.address = j["address"];
    if (j.contains("latitude")) location.latitude = j["latitude"];
    if (j.contains("longitude")) location.longitude = j["longitude"];
    if (j.contains("city")) location.city = j["city"];
    if (j.contains("country")) location.country = j["country"];
    if (j.contains("country_code")) location.country_code = j["country_code"];
    if (j.contains("timezone")) location.timezone = j["timezone"];
    if (j.contains("metadata")) location.metadata = j["metadata"];
    
    return location;
}

bool LocationData::validate() const {
    return !place_id.empty() && !name.empty() && 
           latitude >= -90.0 && latitude <= 90.0 &&
           longitude >= -180.0 && longitude <= 180.0;
}

// Attachment implementation

Attachment::Attachment(const std::string& attachment_id) 
    : attachment_id(attachment_id) {
    initialize_defaults();
}

void Attachment::initialize_defaults() {
    status = ProcessingStatus::PENDING;
    width = 0;
    height = 0;
    duration = 0.0;
    bitrate = 0;
    has_transparency = false;
    is_sensitive = false;
    is_spoiler = false;
    content_safety_score = 1.0;
    view_count = 0;
    download_count = 0;
    share_count = 0;
    
    auto now = std::time(nullptr);
    created_at = now;
    updated_at = now;
    processed_at = 0;
    expires_at = 0;
}

// Factory methods

Attachment Attachment::create_image_attachment(const std::string& uploader_id, const std::string& filename, 
                                             const std::string& mime_type, size_t file_size) {
    Attachment attachment(core::utils::generate_attachment_id());
    attachment.uploader_id = uploader_id;
    attachment.type = AttachmentType::IMAGE;
    attachment.original_filename = filename;
    attachment.mime_type = mime_type;
    attachment.file_size = file_size;
    attachment.storage_path = attachment.generate_storage_path();
    
    return attachment;
}

Attachment Attachment::create_video_attachment(const std::string& uploader_id, const std::string& filename, 
                                             const std::string& mime_type, size_t file_size, double duration) {
    Attachment attachment(core::utils::generate_attachment_id());
    attachment.uploader_id = uploader_id;
    attachment.type = AttachmentType::VIDEO;
    attachment.original_filename = filename;
    attachment.mime_type = mime_type;
    attachment.file_size = file_size;
    attachment.duration = duration;
    attachment.storage_path = attachment.generate_storage_path();
    
    return attachment;
}

Attachment Attachment::create_tenor_gif(const std::string& uploader_id, const TenorGifData& tenor_data) {
    Attachment attachment(core::utils::generate_attachment_id());
    attachment.uploader_id = uploader_id;
    attachment.type = AttachmentType::TENOR_GIF;
    attachment.tenor_data = tenor_data;
    attachment.original_filename = tenor_data.tenor_id + ".gif";
    attachment.mime_type = "image/gif";
    attachment.status = ProcessingStatus::COMPLETED; // Tenor GIFs are pre-processed
    
    return attachment;
}

Attachment Attachment::create_link_preview(const std::string& uploader_id, const LinkPreview& preview) {
    Attachment attachment(core::utils::generate_attachment_id());
    attachment.uploader_id = uploader_id;
    attachment.type = AttachmentType::LINK_PREVIEW;
    attachment.link_preview = preview;
    attachment.original_filename = "link_preview.json";
    attachment.mime_type = "application/json";
    attachment.status = ProcessingStatus::COMPLETED;
    
    return attachment;
}

Attachment Attachment::create_poll(const std::string& uploader_id, const PollData& poll) {
    Attachment attachment(core::utils::generate_attachment_id());
    attachment.uploader_id = uploader_id;
    attachment.type = AttachmentType::POLL;
    attachment.poll_data = poll;
    attachment.original_filename = "poll.json";
    attachment.mime_type = "application/json";
    attachment.status = ProcessingStatus::COMPLETED;
    
    return attachment;
}

Attachment Attachment::create_location(const std::string& uploader_id, const LocationData& location) {
    Attachment attachment(core::utils::generate_attachment_id());
    attachment.uploader_id = uploader_id;
    attachment.type = AttachmentType::LOCATION;
    attachment.location_data = location;
    attachment.original_filename = "location.json";
    attachment.mime_type = "application/json";
    attachment.status = ProcessingStatus::COMPLETED;
    
    return attachment;
}

// Media variant management

void Attachment::add_variant(const MediaVariant& variant) {
    if (!variant.validate()) {
        spdlog::warn("Invalid media variant for attachment {}", attachment_id);
        return;
    }
    
    // Remove existing variant of same quality and format
    variants.erase(
        std::remove_if(variants.begin(), variants.end(),
            [&variant](const MediaVariant& existing) {
                return existing.quality == variant.quality && existing.format == variant.format;
            }),
        variants.end()
    );
    
    variants.push_back(variant);
    update_timestamps();
}

std::optional<MediaVariant> Attachment::get_best_variant(MediaQuality preferred_quality) const {
    if (variants.empty()) {
        return std::nullopt;
    }
    
    // First try to find exact quality match
    for (const auto& variant : variants) {
        if (variant.quality == preferred_quality) {
            return variant;
        }
    }
    
    // Fall back to closest quality
    auto best_variant = variants.begin();
    int best_distance = std::abs(static_cast<int>(preferred_quality) - static_cast<int>(best_variant->quality));
    
    for (auto it = variants.begin() + 1; it != variants.end(); ++it) {
        int distance = std::abs(static_cast<int>(preferred_quality) - static_cast<int>(it->quality));
        if (distance < best_distance) {
            best_distance = distance;
            best_variant = it;
        }
    }
    
    return *best_variant;
}

std::vector<MediaVariant> Attachment::get_variants_by_format(const std::string& format) const {
    std::vector<MediaVariant> result;
    
    for (const auto& variant : variants) {
        if (variant.format == format) {
            result.push_back(variant);
        }
    }
    
    return result;
}

void Attachment::clear_variants() {
    variants.clear();
    update_timestamps();
}

// URL generation

std::string Attachment::get_url(MediaQuality quality) const {
    auto variant = get_best_variant(quality);
    if (variant) {
        return variant->url;
    }
    
    // Fall back to primary URL
    return primary_url;
}

std::string Attachment::get_thumbnail_url() const {
    auto thumbnail = get_best_variant(MediaQuality::THUMBNAIL);
    if (thumbnail) {
        return thumbnail->url;
    }
    
    // Generate thumbnail URL from primary URL
    std::unordered_map<std::string, std::string> params = {
        {"w", "150"},
        {"h", "150"},
        {"fit", "crop"}
    };
    
    return build_cdn_url(primary_url, params);
}

std::string Attachment::get_download_url() const {
    auto original = get_best_variant(MediaQuality::ORIGINAL);
    if (original) {
        return original->url;
    }
    
    return primary_url;
}

// Content processing

void Attachment::set_processing_status(ProcessingStatus new_status, const std::string& error_message) {
    status = new_status;
    
    if (new_status == ProcessingStatus::COMPLETED) {
        processed_at = std::time(nullptr);
        clear_processing_errors();
    } else if (new_status == ProcessingStatus::FAILED && !error_message.empty()) {
        add_processing_error(error_message);
    }
    
    update_timestamps();
}

void Attachment::add_processing_error(const std::string& error) {
    processing_errors.push_back(error);
    spdlog::error("Processing error for attachment {}: {}", attachment_id, error);
}

void Attachment::clear_processing_errors() {
    processing_errors.clear();
}

bool Attachment::is_processing_complete() const {
    return status == ProcessingStatus::COMPLETED;
}

bool Attachment::is_processing_failed() const {
    return status == ProcessingStatus::FAILED || status == ProcessingStatus::VIRUS_DETECTED || 
           status == ProcessingStatus::REJECTED;
}

// Content moderation

void Attachment::add_moderation_flag(const std::string& flag, const std::string& reason) {
    if (moderation_flags.size() >= attachment_constants::MAX_MODERATION_FLAGS) {
        spdlog::warn("Maximum moderation flags reached for attachment {}", attachment_id);
        return;
    }
    
    moderation_flags[flag] = reason;
    
    // Automatically mark as sensitive if certain flags are present
    if (flag == "nsfw" || flag == "violence" || flag == "disturbing") {
        is_sensitive = true;
    }
    
    update_timestamps();
}

void Attachment::remove_moderation_flag(const std::string& flag) {
    moderation_flags.erase(flag);
    update_timestamps();
}

bool Attachment::has_moderation_flags() const {
    return !moderation_flags.empty();
}

std::vector<std::string> Attachment::get_moderation_flags() const {
    std::vector<std::string> flags;
    flags.reserve(moderation_flags.size());
    
    for (const auto& [flag, reason] : moderation_flags) {
        flags.push_back(flag);
    }
    
    return flags;
}

void Attachment::set_content_safety_score(double score) {
    content_safety_score = std::max(0.0, std::min(1.0, score));
    update_timestamps();
}

bool Attachment::is_content_safe(double threshold) const {
    return content_safety_score >= threshold && !contains_sensitive_content() && !violates_content_policy();
}

// Analytics

void Attachment::record_view(const std::string& user_id) {
    if (std::find(viewer_ids.begin(), viewer_ids.end(), user_id) == viewer_ids.end()) {
        viewer_ids.push_back(user_id);
    }
    view_count++;
}

void Attachment::record_download(const std::string& user_id) {
    download_count++;
    // Could track individual download events if needed
}

void Attachment::record_share(const std::string& user_id) {
    share_count++;
    // Could track individual share events if needed
}

int Attachment::get_unique_viewers() const {
    return static_cast<int>(viewer_ids.size());
}

// Validation and constraints

bool Attachment::validate() const {
    // Basic validation
    if (attachment_id.empty() || uploader_id.empty()) {
        return false;
    }
    
    // File size validation
    if (!is_within_size_limits()) {
        return false;
    }
    
    // MIME type validation
    if (!is_valid_mime_type(mime_type)) {
        return false;
    }
    
    // Type-specific validation
    try {
        validate_type_specific_data();
    } catch (const std::exception& e) {
        spdlog::error("Type-specific validation failed for attachment {}: {}", attachment_id, e.what());
        return false;
    }
    
    // Dimension validation for media
    if ((is_image() || is_video()) && (width < 0 || height < 0)) {
        return false;
    }
    
    if (is_image() && (width > attachment_constants::MAX_IMAGE_DIMENSION || 
                       height > attachment_constants::MAX_IMAGE_DIMENSION)) {
        return false;
    }
    
    if (is_video() && (width > attachment_constants::MAX_VIDEO_DIMENSION || 
                       height > attachment_constants::MAX_VIDEO_DIMENSION)) {
        return false;
    }
    
    // Duration validation
    if (is_video() && duration > attachment_constants::MAX_VIDEO_DURATION) {
        return false;
    }
    
    if (is_audio() && duration > attachment_constants::MAX_AUDIO_DURATION) {
        return false;
    }
    
    // Content safety validation
    if (content_safety_score < 0.0 || content_safety_score > 1.0) {
        return false;
    }
    
    return true;
}

bool Attachment::is_within_size_limits() const {
    size_t max_size = get_max_file_size(type);
    return file_size <= max_size;
}

bool Attachment::is_supported_format() const {
    auto supported_formats = get_supported_formats(type);
    std::string extension = get_file_extension();
    
    return std::find(supported_formats.begin(), supported_formats.end(), extension) != supported_formats.end();
}

bool Attachment::is_valid_mime_type(const std::string& mime_type) {
    static const std::unordered_set<std::string> valid_mime_types = {
        // Images
        "image/jpeg", "image/jpg", "image/png", "image/gif", "image/webp", "image/avif", "image/svg+xml",
        // Videos
        "video/mp4", "video/webm", "video/mov", "video/avi", "video/mkv", "video/3gp",
        // Audio
        "audio/mpeg", "audio/mp3", "audio/aac", "audio/ogg", "audio/wav", "audio/flac",
        // Documents
        "application/pdf", "text/plain", "application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        // Special types
        "application/json"
    };
    
    return valid_mime_types.find(mime_type) != valid_mime_types.end();
}

size_t Attachment::get_max_file_size(AttachmentType type) {
    switch (type) {
        case AttachmentType::IMAGE:
        case AttachmentType::GIF:
            return attachment_constants::MAX_IMAGE_SIZE;
        case AttachmentType::VIDEO:
            return attachment_constants::MAX_VIDEO_SIZE;
        case AttachmentType::AUDIO:
            return attachment_constants::MAX_AUDIO_SIZE;
        case AttachmentType::DOCUMENT:
            return attachment_constants::MAX_DOCUMENT_SIZE;
        default:
            return attachment_constants::MAX_IMAGE_SIZE;
    }
}

std::vector<std::string> Attachment::get_supported_formats(AttachmentType type) {
    switch (type) {
        case AttachmentType::IMAGE:
            return {"jpg", "jpeg", "png", "webp", "avif", "svg"};
        case AttachmentType::VIDEO:
            return {"mp4", "webm", "mov", "avi", "mkv", "3gp"};
        case AttachmentType::GIF:
        case AttachmentType::TENOR_GIF:
            return {"gif"};
        case AttachmentType::AUDIO:
            return {"mp3", "aac", "ogg", "wav", "flac"};
        case AttachmentType::DOCUMENT:
            return {"pdf", "txt", "doc", "docx"};
        default:
            return {};
    }
}

// Serialization

json Attachment::to_json() const {
    json j = {
        {"attachment_id", attachment_id},
        {"note_id", note_id},
        {"uploader_id", uploader_id},
        {"type", static_cast<int>(type)},
        {"status", static_cast<int>(status)},
        {"original_filename", original_filename},
        {"mime_type", mime_type},
        {"file_size", file_size},
        {"checksum", checksum},
        {"width", width},
        {"height", height},
        {"duration", duration},
        {"bitrate", bitrate},
        {"color_palette", color_palette},
        {"has_transparency", has_transparency},
        {"alt_text", alt_text},
        {"caption", caption},
        {"description", description},
        {"tags", tags},
        {"is_sensitive", is_sensitive},
        {"is_spoiler", is_spoiler},
        {"primary_url", primary_url},
        {"backup_url", backup_url},
        {"storage_path", storage_path},
        {"processing_job_id", processing_job_id},
        {"processing_errors", processing_errors},
        {"moderation_flags", moderation_flags},
        {"content_safety_score", content_safety_score},
        {"view_count", view_count},
        {"download_count", download_count},
        {"share_count", share_count},
        {"viewer_ids", viewer_ids},
        {"created_at", created_at},
        {"updated_at", updated_at},
        {"processed_at", processed_at},
        {"expires_at", expires_at}
    };
    
    // Add variants
    json variants_json = json::array();
    for (const auto& variant : variants) {
        variants_json.push_back(variant.to_json());
    }
    j["variants"] = variants_json;
    
    // Add type-specific data
    if (tenor_data) {
        j["tenor_data"] = tenor_data->to_json();
    }
    
    if (link_preview) {
        j["link_preview"] = link_preview->to_json();
    }
    
    if (poll_data) {
        j["poll_data"] = poll_data->to_json();
    }
    
    if (location_data) {
        j["location_data"] = location_data->to_json();
    }
    
    return j;
}

Attachment Attachment::from_json(const json& j) {
    Attachment attachment;
    
    if (j.contains("attachment_id")) attachment.attachment_id = j["attachment_id"];
    if (j.contains("note_id")) attachment.note_id = j["note_id"];
    if (j.contains("uploader_id")) attachment.uploader_id = j["uploader_id"];
    if (j.contains("type")) attachment.type = static_cast<AttachmentType>(j["type"]);
    if (j.contains("status")) attachment.status = static_cast<ProcessingStatus>(j["status"]);
    if (j.contains("original_filename")) attachment.original_filename = j["original_filename"];
    if (j.contains("mime_type")) attachment.mime_type = j["mime_type"];
    if (j.contains("file_size")) attachment.file_size = j["file_size"];
    if (j.contains("checksum")) attachment.checksum = j["checksum"];
    if (j.contains("width")) attachment.width = j["width"];
    if (j.contains("height")) attachment.height = j["height"];
    if (j.contains("duration")) attachment.duration = j["duration"];
    if (j.contains("bitrate")) attachment.bitrate = j["bitrate"];
    if (j.contains("color_palette")) attachment.color_palette = j["color_palette"];
    if (j.contains("has_transparency")) attachment.has_transparency = j["has_transparency"];
    if (j.contains("alt_text")) attachment.alt_text = j["alt_text"];
    if (j.contains("caption")) attachment.caption = j["caption"];
    if (j.contains("description")) attachment.description = j["description"];
    if (j.contains("tags")) attachment.tags = j["tags"];
    if (j.contains("is_sensitive")) attachment.is_sensitive = j["is_sensitive"];
    if (j.contains("is_spoiler")) attachment.is_spoiler = j["is_spoiler"];
    if (j.contains("primary_url")) attachment.primary_url = j["primary_url"];
    if (j.contains("backup_url")) attachment.backup_url = j["backup_url"];
    if (j.contains("storage_path")) attachment.storage_path = j["storage_path"];
    if (j.contains("processing_job_id")) attachment.processing_job_id = j["processing_job_id"];
    if (j.contains("processing_errors")) attachment.processing_errors = j["processing_errors"];
    if (j.contains("moderation_flags")) attachment.moderation_flags = j["moderation_flags"];
    if (j.contains("content_safety_score")) attachment.content_safety_score = j["content_safety_score"];
    if (j.contains("view_count")) attachment.view_count = j["view_count"];
    if (j.contains("download_count")) attachment.download_count = j["download_count"];
    if (j.contains("share_count")) attachment.share_count = j["share_count"];
    if (j.contains("viewer_ids")) attachment.viewer_ids = j["viewer_ids"];
    if (j.contains("created_at")) attachment.created_at = j["created_at"];
    if (j.contains("updated_at")) attachment.updated_at = j["updated_at"];
    if (j.contains("processed_at")) attachment.processed_at = j["processed_at"];
    if (j.contains("expires_at")) attachment.expires_at = j["expires_at"];
    
    // Load variants
    if (j.contains("variants") && j["variants"].is_array()) {
        for (const auto& variant_json : j["variants"]) {
            attachment.variants.push_back(MediaVariant::from_json(variant_json));
        }
    }
    
    // Load type-specific data
    if (j.contains("tenor_data")) {
        attachment.tenor_data = TenorGifData::from_json(j["tenor_data"]);
    }
    
    if (j.contains("link_preview")) {
        attachment.link_preview = LinkPreview::from_json(j["link_preview"]);
    }
    
    if (j.contains("poll_data")) {
        attachment.poll_data = PollData::from_json(j["poll_data"]);
    }
    
    if (j.contains("location_data")) {
        attachment.location_data = LocationData::from_json(j["location_data"]);
    }
    
    return attachment;
}

std::string Attachment::to_string() const {
    return to_json().dump();
}

// Utility methods

std::string Attachment::get_file_extension() const {
    std::filesystem::path path(original_filename);
    std::string ext = path.extension().string();
    
    if (!ext.empty() && ext[0] == '.') {
        ext = ext.substr(1);
    }
    
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
    return ext;
}

std::string Attachment::get_display_name() const {
    if (!caption.empty()) {
        return caption;
    }
    
    if (!alt_text.empty()) {
        return alt_text;
    }
    
    return original_filename;
}

bool Attachment::is_image() const {
    return type == AttachmentType::IMAGE;
}

bool Attachment::is_video() const {
    return type == AttachmentType::VIDEO;
}

bool Attachment::is_audio() const {
    return type == AttachmentType::AUDIO;
}

bool Attachment::is_animated() const {
    return type == AttachmentType::GIF || type == AttachmentType::TENOR_GIF || 
           (type == AttachmentType::VIDEO && duration > 0.0);
}

bool Attachment::requires_processing() const {
    return type == AttachmentType::IMAGE || type == AttachmentType::VIDEO || 
           type == AttachmentType::AUDIO || type == AttachmentType::GIF;
}

double Attachment::get_aspect_ratio() const {
    if (height == 0) return 0.0;
    return static_cast<double>(width) / static_cast<double>(height);
}

// Comparison operators

bool Attachment::operator==(const Attachment& other) const {
    return attachment_id == other.attachment_id;
}

bool Attachment::operator!=(const Attachment& other) const {
    return !(*this == other);
}

// Private helper methods

void Attachment::validate_type_specific_data() const {
    switch (type) {
        case AttachmentType::TENOR_GIF:
            if (!tenor_data || !tenor_data->validate()) {
                throw std::invalid_argument("Invalid Tenor GIF data");
            }
            break;
            
        case AttachmentType::LINK_PREVIEW:
            if (!link_preview || !link_preview->validate()) {
                throw std::invalid_argument("Invalid link preview data");
            }
            break;
            
        case AttachmentType::POLL:
            if (!poll_data || !poll_data->validate()) {
                throw std::invalid_argument("Invalid poll data");
            }
            break;
            
        case AttachmentType::LOCATION:
            if (!location_data || !location_data->validate()) {
                throw std::invalid_argument("Invalid location data");
            }
            break;
            
        default:
            // No specific validation needed for other types
            break;
    }
}

std::string Attachment::generate_storage_path() const {
    // Generate storage path based on uploader ID and timestamp
    auto now = std::time(nullptr);
    auto* tm = std::gmtime(&now);
    
    char path_buffer[256];
    std::snprintf(path_buffer, sizeof(path_buffer), 
                 "attachments/%s/%04d/%02d/%02d/%s",
                 uploader_id.c_str(),
                 tm->tm_year + 1900,
                 tm->tm_mon + 1,
                 tm->tm_mday,
                 attachment_id.c_str());
    
    return std::string(path_buffer);
}

void Attachment::update_timestamps() {
    updated_at = std::time(nullptr);
}

bool Attachment::contains_sensitive_content() const {
    return is_sensitive || has_moderation_flags();
}

bool Attachment::violates_content_policy() const {
    // Check for specific policy violations
    auto flags = get_moderation_flags();
    for (const auto& flag : flags) {
        if (flag == "violence" || flag == "hate_speech" || flag == "illegal_content") {
            return true;
        }
    }
    
    return false;
}

std::string Attachment::build_cdn_url(const std::string& path, 
                                    const std::unordered_map<std::string, std::string>& params) const {
    std::string url = path;
    
    if (!params.empty()) {
        url += "?";
        bool first = true;
        for (const auto& [key, value] : params) {
            if (!first) url += "&";
            url += key + "=" + value;
            first = false;
        }
    }
    
    return url;
}

std::string Attachment::get_variant_url(const MediaVariant& variant) const {
    return variant.url;
}

// AttachmentCollection implementation

bool AttachmentCollection::add_attachment(const Attachment& attachment) {
    if (is_full()) {
        spdlog::warn("Cannot add attachment: collection is full");
        return false;
    }
    
    if (!attachment.validate()) {
        spdlog::warn("Cannot add invalid attachment: {}", attachment.attachment_id);
        return false;
    }
    
    // Check for duplicates
    for (const auto& existing : attachments) {
        if (existing.attachment_id == attachment.attachment_id) {
            spdlog::warn("Attachment already exists in collection: {}", attachment.attachment_id);
            return false;
        }
    }
    
    attachments.push_back(attachment);
    return true;
}

bool AttachmentCollection::remove_attachment(const std::string& attachment_id) {
    auto it = std::find_if(attachments.begin(), attachments.end(),
        [&attachment_id](const Attachment& att) {
            return att.attachment_id == attachment_id;
        });
    
    if (it != attachments.end()) {
        attachments.erase(it);
        return true;
    }
    
    return false;
}

void AttachmentCollection::clear() {
    attachments.clear();
}

size_t AttachmentCollection::size() const {
    return attachments.size();
}

bool AttachmentCollection::empty() const {
    return attachments.empty();
}

bool AttachmentCollection::is_full() const {
    return attachments.size() >= MAX_ATTACHMENTS;
}

bool AttachmentCollection::validate() const {
    if (attachments.size() > MAX_ATTACHMENTS) {
        return false;
    }
    
    if (!is_within_total_size_limit()) {
        return false;
    }
    
    for (const auto& attachment : attachments) {
        if (!attachment.validate()) {
            return false;
        }
    }
    
    return true;
}

bool AttachmentCollection::is_within_total_size_limit() const {
    size_t total_size = get_total_size();
    return total_size <= attachment_constants::MAX_TOTAL_SIZE;
}

bool AttachmentCollection::has_mixed_types() const {
    if (attachments.empty()) {
        return false;
    }
    
    AttachmentType first_type = attachments[0].type;
    for (const auto& attachment : attachments) {
        if (attachment.type != first_type) {
            return true;
        }
    }
    
    return false;
}

void AttachmentCollection::set_note_id(const std::string& note_id) {
    for (auto& attachment : attachments) {
        attachment.note_id = note_id;
    }
}

void AttachmentCollection::mark_all_as_sensitive(bool is_sensitive) {
    for (auto& attachment : attachments) {
        attachment.is_sensitive = is_sensitive;
    }
}

std::vector<Attachment> AttachmentCollection::get_by_type(AttachmentType type) const {
    std::vector<Attachment> result;
    
    for (const auto& attachment : attachments) {
        if (attachment.type == type) {
            result.push_back(attachment);
        }
    }
    
    return result;
}

std::vector<Attachment> AttachmentCollection::get_processing_attachments() const {
    std::vector<Attachment> result;
    
    for (const auto& attachment : attachments) {
        if (attachment.status == ProcessingStatus::PROCESSING || 
            attachment.status == ProcessingStatus::PENDING) {
            result.push_back(attachment);
        }
    }
    
    return result;
}

std::vector<Attachment> AttachmentCollection::get_failed_attachments() const {
    std::vector<Attachment> result;
    
    for (const auto& attachment : attachments) {
        if (attachment.is_processing_failed()) {
            result.push_back(attachment);
        }
    }
    
    return result;
}

int AttachmentCollection::get_total_views() const {
    int total = 0;
    for (const auto& attachment : attachments) {
        total += attachment.view_count;
    }
    return total;
}

int AttachmentCollection::get_total_downloads() const {
    int total = 0;
    for (const auto& attachment : attachments) {
        total += attachment.download_count;
    }
    return total;
}

size_t AttachmentCollection::get_total_size() const {
    size_t total = 0;
    for (const auto& attachment : attachments) {
        total += attachment.file_size;
    }
    return total;
}

json AttachmentCollection::to_json() const {
    json attachments_json = json::array();
    
    for (const auto& attachment : attachments) {
        attachments_json.push_back(attachment.to_json());
    }
    
    return json{
        {"attachments", attachments_json},
        {"count", attachments.size()},
        {"total_size", get_total_size()},
        {"total_views", get_total_views()},
        {"total_downloads", get_total_downloads()}
    };
}

AttachmentCollection AttachmentCollection::from_json(const json& j) {
    AttachmentCollection collection;
    
    if (j.contains("attachments") && j["attachments"].is_array()) {
        for (const auto& attachment_json : j["attachments"]) {
            Attachment attachment = Attachment::from_json(attachment_json);
            collection.add_attachment(attachment);
        }
    }
    
    return collection;
}

} // namespace sonet::note::models