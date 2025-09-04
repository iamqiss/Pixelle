//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

#include "service.h"

#include <filesystem>
#include <fstream>
#include <random>
#include <chrono>
#include <algorithm>
#include <cstdlib>
#include <cctype>
#include <mutex>
#include <unordered_map>
#include <sstream>

#include "logger.h"

namespace fs = std::filesystem;

namespace sonet::media_service {

// Persistent like state (development-grade durability)
static std::mutex g_like_mu;
static std::unordered_map<std::string, uint32_t> g_media_like_counts; // media_id -> count
static std::unordered_map<std::string, bool> g_user_media_liked;      // user_id|media_id -> liked
static bool g_likes_loaded = false;

static std::string LikesStorePath() {
	const char* env = std::getenv("SONET_MEDIA_LIKES_PATH");
	return env && *env ? std::string(env) : std::string("/tmp/media_likes.json");
}

static void LoadLikesIfNeeded() {
	if (g_likes_loaded) return;
	std::lock_guard<std::mutex> lock(g_like_mu);
	if (g_likes_loaded) return;
	std::ifstream ifs(LikesStorePath());
	if (ifs) {
		// very small hand-rolled parser: two sections separated by a blank line
		// section 1: media_id count
		// section 2: user_id|media_id liked(0/1)
		std::string line;
		bool second = false;
		while (std::getline(ifs, line)) {
			if (line.empty()) { second = true; continue; }
			std::istringstream iss(line);
			if (!second) {
				std::string media; uint32_t cnt;
				if (iss >> media >> cnt) g_media_like_counts[media] = cnt;
			} else {
				std::string key; int liked;
				if (iss >> key >> liked) g_user_media_liked[key] = (liked != 0);
			}
		}
	}
	g_likes_loaded = true;
}

static void SaveLikes() {
	std::lock_guard<std::mutex> lock(g_like_mu);
	std::ofstream ofs(LikesStorePath(), std::ios::trunc);
	if (!ofs) return;
	for (const auto& [media, cnt] : g_media_like_counts) {
		ofs << media << ' ' << cnt << '\n';
	}
	ofs << '\n';
	for (const auto& [key, liked] : g_user_media_liked) {
		ofs << key << ' ' << (liked ? 1 : 0) << '\n';
	}
}

// ---------------- In-memory Repo (simple, for dev/testing) ----------------
class InMemoryRepo final : public MediaRepository {
public:
	bool Save(const MediaRecord& rec) override {
		std::lock_guard<std::mutex> lock(mu_);
		MediaRecord copy = rec;
		if (copy.created_at.empty()) {
			auto now = std::chrono::system_clock::now();
			auto t = std::chrono::system_clock::to_time_t(now);
			std::tm tm{};
#if defined(_WIN32)
			gmtime_s(&tm, &t);
#else
			gmtime_r(&t, &tm);
#endif
			char buf[32];
			std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
			copy.created_at = buf;
		}
		// If updating an existing record, fix owner index if owner changed
		auto existing = store_.find(copy.id);
		if (existing != store_.end()) {
			const std::string& old_owner = existing->second.owner_user_id;
			if (old_owner != copy.owner_user_id) {
				auto it = by_owner_.find(old_owner);
				if (it != by_owner_.end()) {
					auto& vec = it->second;
					vec.erase(std::remove(vec.begin(), vec.end(), copy.id), vec.end());
				}
			}
		}
		store_[copy.id] = copy;
		// Add to new owner's index if not already present
		auto& vec = by_owner_[copy.owner_user_id];
		if (std::find(vec.begin(), vec.end(), copy.id) == vec.end()) {
			vec.push_back(copy.id);
		}
		return true;
	}
	bool Get(const std::string& id, MediaRecord& out) override {
		std::lock_guard<std::mutex> lock(mu_);
		auto it = store_.find(id);
		if (it == store_.end()) return false;
		out = it->second;
		return true;
	}
	bool Delete(const std::string& id) override {
		std::lock_guard<std::mutex> lock(mu_);
		auto it = store_.find(id);
		if (it == store_.end()) return false;
		auto owner = it->second.owner_user_id;
		store_.erase(it);
		auto& vec = by_owner_[owner];
		vec.erase(std::remove(vec.begin(), vec.end(), id), vec.end());
		return true;
	}
	std::vector<MediaRecord> ListByOwner(const std::string& owner, uint32_t page, uint32_t page_size, uint32_t& total_pages) override {
		std::lock_guard<std::mutex> lock(mu_);
		std::vector<MediaRecord> res;
		auto it = by_owner_.find(owner);
		if (it == by_owner_.end()) { total_pages = 0; return res; }
		const auto& ids = it->second;
		if (page_size == 0) page_size = 20;
		total_pages = (ids.size() + page_size - 1) / page_size;
		if (page == 0) page = 1;
		uint32_t start = (page - 1) * page_size;
		for (uint32_t i = start; i < ids.size() && res.size() < page_size; ++i) {
			MediaRecord r{}; auto sit = store_.find(ids[i]); if (sit != store_.end()) res.push_back(sit->second);
		}
		return res;
	}
private:
	std::unordered_map<std::string, MediaRecord> store_;
	std::unordered_map<std::string, std::vector<std::string>> by_owner_;
	std::mutex mu_;
};

std::unique_ptr<MediaRepository> CreateInMemoryRepo() { return std::make_unique<InMemoryRepo>(); }

// ---------------- Local Storage backend (writes to disk, returns file:// URL) ----------------
class LocalStorage final : public StorageBackend {
public:
	LocalStorage(std::string base_dir, std::string base_url) : base_dir_(std::move(base_dir)), base_url_(std::move(base_url)) {
		fs::create_directories(base_dir_);
	}
	bool Put(const std::string& local_path, const std::string& object_key, std::string& out_url) override {
		auto target = fs::path(base_dir_) / object_key;
		fs::create_directories(target.parent_path());
		std::error_code ec;
		fs::rename(local_path, target, ec);
		if (ec) {
			// fallback to copy if rename across devices fails
			ec.clear();
			fs::copy_file(local_path, target, fs::copy_options::overwrite_existing, ec);
			if (ec) return false;
			fs::remove(local_path);
		}
		out_url = base_url_ + "/" + object_key;
		return true;
	}
	bool PutDir(const std::string& local_dir, const std::string& object_prefix, std::string& out_base_url) override {
		auto target_dir = fs::path(base_dir_) / object_prefix;
		std::error_code ec;
		fs::create_directories(target_dir, ec);
		if (ec) return false;
		for (auto& entry : fs::recursive_directory_iterator(local_dir)) {
			if (!entry.is_regular_file()) continue;
			auto rel = fs::relative(entry.path(), local_dir, ec);
			if (ec) return false;
			auto dest = target_dir / rel;
			fs::create_directories(dest.parent_path(), ec);
			if (ec) return false;
			fs::copy_file(entry.path(), dest, fs::copy_options::overwrite_existing, ec);
			if (ec) return false;
		}
		out_base_url = base_url_ + "/" + object_prefix;
		return true;
	}
	bool Delete(const std::string& object_key) override {
		std::error_code ec;
		fs::remove(fs::path(base_dir_) / object_key, ec);
		return !ec;
	}
	bool DeletePrefix(const std::string& object_prefix) override {
		std::error_code ec; auto p = fs::path(base_dir_) / object_prefix; if (fs::exists(p)) fs::remove_all(p, ec); return !ec; }
	std::string Sign(const std::string& object_key, int /*ttl_seconds*/) override {
		// For local storage, return resolved URL path
		return base_url_ + "/" + object_key;
	}
private:
	std::string base_dir_;
	std::string base_url_;
};

std::unique_ptr<StorageBackend> CreateLocalStorage(const std::string& base_dir, const std::string& base_url) {
	return std::make_unique<LocalStorage>(base_dir, base_url);
}

// ---------------- Processors ----------------
class BasicGifProcessor final : public GifProcessor {
public:
    bool Process(const std::string& path_in, std::string& path_out, std::string& thumb_out, double& duration, uint32_t& width, uint32_t& height) override {
        auto ShellQuote = [](const std::string& s){ std::string r; r.reserve(s.size()+2); r.push_back('\''); for(char c: s){ if(c=='\'') r += "'\"'\"'"; else r.push_back(c);} r.push_back('\''); return r; };
        path_out = path_in; thumb_out = path_in; duration = 0.0; width = 0; height = 0;
        std::string dim_file = path_in + ".gifdim";
        std::string dim_cmd = "identify -format '%w %h' " + ShellQuote(path_in + "[0]") + " 2>/dev/null > " + ShellQuote(dim_file);
        int drc = std::system(dim_cmd.c_str()); (void)drc;
        { std::ifstream ifs(dim_file); if (ifs) { ifs >> width >> height; } }
        std::error_code ec; fs::remove(dim_file, ec);
        std::string delay_file = path_in + ".gifdelay";
        std::string delay_cmd = "identify -format '%T ' " + ShellQuote(path_in) + " 2>/dev/null > " + ShellQuote(delay_file);
        int rdc = std::system(delay_cmd.c_str()); (void)rdc;
        { std::ifstream ifs(delay_file); unsigned long centi=0; while (ifs >> centi) duration += static_cast<double>(centi)/100.0; }
        fs::remove(delay_file, ec);
        return true;
    }
};
std::unique_ptr<GifProcessor> CreateGifProcessor() { return std::make_unique<BasicGifProcessor>(); }

// ---------------- NSFW Scanner (placeholder) ----------------
class BasicScanner final : public NsfwScanner {
public:
	explicit BasicScanner(bool enable): enable_(enable) {}
	bool IsAllowed(const std::string& /*local_path*/, ::sonet::media::MediaType /*type*/, std::string& reason) override {
		if (!enable_) return true; // scanning disabled -> allow
		// TODO: plug a real model or API here. For now always allow.
		reason.clear();
		return true;
	}
private:
	bool enable_{};
};

std::unique_ptr<NsfwScanner> CreateBasicScanner(bool enable) { return std::make_unique<BasicScanner>(enable); }

// -------------- Utility --------------
static std::string GenId() {
	// Natural human-like: just a short random id; replace with ULID/UUID later
	static std::mt19937_64 rng{std::random_device{}()};
	static const char* alphabet = "0123456789abcdef";
	uint64_t x = rng();
	std::string s(16, '0');
	for (int i = 15; i >= 0; --i) { s[i] = alphabet[x & 0xF]; x >>= 4; }
	return s;
}

// Simple MIME sniffing from magic bytes
static std::string SniffMime(const fs::path& p) {
	std::ifstream ifs(p, std::ios::binary);
	if (!ifs) return "application/octet-stream";
	unsigned char buf[16]{}; ifs.read(reinterpret_cast<char*>(buf), sizeof(buf));
	size_t n = static_cast<size_t>(ifs.gcount());
	auto starts_with = [&](std::initializer_list<unsigned char> sig){
		size_t i=0; for (auto b: sig) { if (i>=n || buf[i]!=b) return false; ++i;} return true; };
	if (n>=8 && starts_with({0x89,'P','N','G',0x0D,0x0A,0x1A,0x0A})) return "image/png";
	if (n>=3 && buf[0]==0xFF && buf[1]==0xD8 && buf[2]==0xFF) return "image/jpeg";
	if (n>=6 && (memcmp(buf,"GIF87a",6)==0 || memcmp(buf,"GIF89a",6)==0)) return "image/gif";
	if (n>=12 && memcmp(buf,"RIFF",4)==0 && memcmp(buf+8,"WEBP",4)==0) return "image/webp";
	// MP4: look for 'ftyp' at offset 4
	if (n>=12 && memcmp(buf+4,"ftyp",4)==0) return "video/mp4";
	return "application/octet-stream";
}

static std::string GetMetadataValue(grpc::ServerContext* ctx, const std::string& key) {
	auto& client_md = ctx->client_metadata();
	auto it = client_md.find(grpc::string_ref(key.c_str(), key.size()));
	if (it != client_md.end()) return std::string(it->second.data(), it->second.length());
	return {};
}

static bool IsAdmin(grpc::ServerContext* ctx) {
    auto v = GetMetadataValue(ctx, "x-admin");
    return !v.empty() && (v=="1" || v=="true" || v=="yes");
}

class RateLimiter {
public:
	RateLimiter() {
		const char* env = std::getenv("SONET_MEDIA_UPLOADS_PER_MIN");
		if (env) { try { limit_per_min_ = std::stoul(env); } catch(...) {} }
	}
	bool Allow(const std::string& user) {
		if (limit_per_min_ == 0) return true;
		auto now = std::chrono::steady_clock::now();
		std::lock_guard<std::mutex> lock(mu_);
		auto& ent = buckets_[user];
		if (ent.window_start.time_since_epoch().count()==0 || std::chrono::duration_cast<std::chrono::minutes>(now - ent.window_start).count()>=1) {
			ent.window_start = now; ent.count = 0;
		}
		if (ent.count >= limit_per_min_) {
			return false;
		}
		++ent.count;
		return true;
	}
private:
	struct Bucket { std::chrono::steady_clock::time_point window_start{}; size_t count{}; };
	std::mutex mu_; std::unordered_map<std::string,Bucket> buckets_; size_t limit_per_min_ = 60;
};
static RateLimiter g_upload_rate_limiter;

// -------------- gRPC methods --------------
::grpc::Status MediaServiceImpl::Upload(::grpc::ServerContext* context,
                          ::grpc::ServerReader< ::sonet::media::UploadRequest>* reader,
                          ::sonet::media::UploadResponse* response) {
    using namespace ::sonet::media;
    UploadRequest req; UploadInit init; bool got_init=false;
    auto tmp_dir = fs::temp_directory_path(); auto tmp_path = tmp_dir / ("upload-" + GenId());
    std::ofstream ofs(tmp_path, std::ios::binary); if (!ofs) return {::grpc::StatusCode::INTERNAL, "Failed to open temp file"};
    uint64_t total=0;
    while (reader->Read(&req)) {
        if (req.has_init()) {
            if (got_init) return {::grpc::StatusCode::INVALID_ARGUMENT, "Duplicate init"};
            init = req.init(); got_init = true;
            if (init.owner_user_id().empty()) return {::grpc::StatusCode::INVALID_ARGUMENT, "owner_user_id required"};
            if (init.type()==MEDIA_TYPE_UNKNOWN) return {::grpc::StatusCode::INVALID_ARGUMENT, "media type required"};
            if (!g_upload_rate_limiter.Allow(init.owner_user_id())) return {::grpc::StatusCode::RESOURCE_EXHAUSTED, "rate limit"};
			// Allow env override for max upload bytes
			if (const char* e = std::getenv("SONET_MEDIA_MAX_UPLOAD")) { try { uint64_t v = std::stoull(e); if (v > 0) max_upload_bytes_ = v; } catch(...) {} }
			LOG_INFO("upload_init", (std::unordered_map<std::string,std::string>{{"owner", init.owner_user_id()},{"type", std::to_string(init.type())}}));
            continue;
        }
        if (!got_init) return {::grpc::StatusCode::INVALID_ARGUMENT, "init frame required first"};
        if (req.has_chunk()) {
            const auto& c = req.chunk().content(); total += c.size();
            if (total > max_upload_bytes_) return {::grpc::StatusCode::RESOURCE_EXHAUSTED, "file too large"};
            ofs.write(c.data(), static_cast<std::streamsize>(c.size())); if (!ofs) return {::grpc::StatusCode::INTERNAL, "failed to write temp"};
        }
    }
    ofs.close(); if (!got_init) return {::grpc::StatusCode::INVALID_ARGUMENT, "missing init"};
    std::string caller = GetMetadataValue(context, "x-user-id");
    if (!caller.empty() && caller != init.owner_user_id() && !IsAdmin(context)) { fs::remove(tmp_path); return {::grpc::StatusCode::PERMISSION_DENIED, "owner mismatch"}; }
    if (nsfw_) { std::string reason; if (!nsfw_->IsAllowed(tmp_path.string(), init.type(), reason)) { fs::remove(tmp_path); return {::grpc::StatusCode::PERMISSION_DENIED, reason.empty()?"blocked by moderation":reason}; } }
    std::string sniffed = SniffMime(tmp_path);
    if (init.mime_type().empty()) { init.set_mime_type(sniffed); } else {
        bool mismatch=false; switch(init.type()){case MEDIA_TYPE_IMAGE: mismatch=(sniffed.rfind("image/",0)!=0); break; case MEDIA_TYPE_VIDEO: mismatch=(sniffed.rfind("video/",0)!=0); break; case MEDIA_TYPE_GIF: mismatch=(sniffed!="image/gif"); break; default: break;}
        if (mismatch) { fs::remove(tmp_path); return {::grpc::StatusCode::INVALID_ARGUMENT, "mime/type mismatch"}; }
    }
    std::string processed = tmp_path, thumb = tmp_path, hls_url; uint32_t width=0,height=0; double duration=0; bool ok=false;
    switch(init.type()) { case MEDIA_TYPE_IMAGE: ok = img_->Process(tmp_path.string(), processed, thumb, width, height); break; case MEDIA_TYPE_VIDEO: ok = vid_->Process(tmp_path.string(), processed, thumb, duration, width, height); break; case MEDIA_TYPE_GIF: ok = gif_->Process(tmp_path.string(), processed, thumb, duration, width, height); break; default: return {::grpc::StatusCode::INVALID_ARGUMENT, "unsupported type"}; }
	if (!ok) { fs::remove(tmp_path); LOG_ERROR("processing_failed", (std::unordered_map<std::string,std::string>{{"owner", init.owner_user_id()}})); return {::grpc::StatusCode::INTERNAL, "processing failed"}; }
    std::vector<std::string> uploaded_keys; bool failure=false; std::string failure_reason; auto id = GenId();
    std::string object_key = init.owner_user_id() + "/" + id; std::string url;
	if (!storage_->Put(processed, object_key, url)) { fs::remove(processed); LOG_ERROR("storage_put_failed", (std::unordered_map<std::string,std::string>{{"key", object_key}})); return {::grpc::StatusCode::INTERNAL, "storage failed"}; }
    uploaded_keys.push_back(object_key);
    std::string thumb_url = url; if (thumb != processed) { std::string tkey = object_key + ".thumb.jpg"; if (storage_->Put(thumb, tkey, thumb_url)) uploaded_keys.push_back(tkey); else { failure=true; failure_reason="thumbnail upload failed"; }}
    std::string webp_url; if (!failure && init.type()==MEDIA_TYPE_IMAGE) { fs::path wpath = fs::path(processed).parent_path() / (fs::path(processed).filename().string() + ".webp"); auto ShellQuote = [](const std::string& s){ std::string r; r.reserve(s.size()+2); r.push_back('\''); for(char c: s){ if(c=='\'') r += "'\"'\"'"; else r.push_back(c);} r.push_back('\''); return r; }; std::string cmd = "convert "+ShellQuote(processed)+" -quality 85 "+ShellQuote(wpath.string())+" >/dev/null 2>&1"; int rc = std::system(cmd.c_str()); (void)rc; if (fs::exists(wpath)) { if (storage_->Put(wpath.string(), object_key+".webp", webp_url)) uploaded_keys.push_back(object_key+".webp"); }}
    std::string mp4_url; if (!failure && init.type()==MEDIA_TYPE_GIF) { fs::path mpath = fs::path(processed).parent_path()/(fs::path(processed).filename().string()+".mp4"); auto ShellQuote = [](const std::string& s){ std::string r; r.reserve(s.size()+2); r.push_back('\''); for(char c: s){ if(c=='\'') r += "'\"'\"'"; else r.push_back(c);} r.push_back('\''); return r; }; std::string cmd = "ffmpeg -y -i "+ShellQuote(processed)+" -movflags +faststart -pix_fmt yuv420p -vf 'scale=trunc(iw/2)*2:trunc(ih/2)*2' "+ShellQuote(mpath.string())+" >/dev/null 2>&1"; int rc= std::system(cmd.c_str()); (void)rc; if (fs::exists(mpath)) { if (storage_->Put(mpath.string(), object_key+".mp4", mp4_url)) uploaded_keys.push_back(object_key+".mp4"); }}
    if (!failure && init.type()==MEDIA_TYPE_VIDEO) { fs::path hls_tmp = fs::temp_directory_path()/("hls-"+GenId()); std::error_code dir_ec; fs::create_directories(hls_tmp, dir_ec); if (!dir_ec) { struct Rendition{const char* name; int w; int h; const char* vb; const char* ab;}; Rendition variants[]={{"360p",640,360,"800k","96k"},{"480p",854,480,"1400k","128k"},{"720p",1280,720,"2800k","128k"}}; for (auto& v: variants){ fs::path outdir = hls_tmp / v.name; fs::create_directories(outdir, dir_ec); if (dir_ec) break; std::string seg=(outdir/"seg_%03d.ts").string(); std::string m3u8=(outdir/"index.m3u8").string(); auto ShellQuote = [](const std::string& s){ std::string r; r.reserve(s.size()+2); r.push_back('\''); for(char c: s){ if(c=='\'') r += "'\"'\"'"; else r.push_back(c);} r.push_back('\''); return r; }; std::string cmd="ffmpeg -y -i "+ShellQuote(processed)+" -vf 'scale=w="+std::to_string(v.w)+":h="+std::to_string(v.h)+":force_original_aspect_ratio=decrease' -c:v h264 -profile:v main -crf 20 -g 48 -keyint_min 48 -sc_threshold 0 -b:v "+v.vb+" -maxrate "+v.vb+" -bufsize "+v.vb+" -c:a aac -ar 48000 -b:a "+v.ab+" -hls_time 4 -hls_playlist_type vod -hls_segment_filename "+ShellQuote(seg)+" "+ShellQuote(m3u8)+" >/dev/null 2>&1"; std::system(cmd.c_str()); }
        { std::ofstream mf(hls_tmp/"master.m3u8"); mf << "#EXTM3U\n#EXT-X-VERSION:3\n"; mf << "#EXT-X-STREAM-INF:BANDWIDTH=900000,RESOLUTION=640x360\n360p/index.m3u8\n"; mf << "#EXT-X-STREAM-INF:BANDWIDTH=1600000,RESOLUTION=854x480\n480p/index.m3u8\n"; mf << "#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720\n720p/index.m3u8\n"; }
        std::string base_hls_url; if (storage_->PutDir(hls_tmp.string(), object_key+"/hls", base_hls_url)) { hls_url = base_hls_url + "/master.m3u8"; }
        std::error_code ec; fs::remove_all(hls_tmp, ec);
    } }
	if (failure) { for (const auto& k : uploaded_keys) { storage_->Delete(k); } LOG_ERROR("upload_rollback", (std::unordered_map<std::string,std::string>{{"owner", init.owner_user_id()},{"reason", failure_reason}})); return {::grpc::StatusCode::INTERNAL, failure_reason}; }
    MediaRecord rec{}; rec.id=id; rec.owner_user_id=init.owner_user_id(); rec.type=init.type(); rec.mime_type=init.mime_type(); rec.size_bytes=total; rec.width=width; rec.height=height; rec.duration_seconds=duration; rec.original_url=url; rec.thumbnail_url=thumb_url; rec.hls_url=hls_url; rec.webp_url=webp_url; rec.mp4_url=mp4_url;
    repo_->Save(rec);
	LOG_INFO("upload_complete", (std::unordered_map<std::string,std::string>{{"id", id},{"owner", init.owner_user_id()},{"size", std::to_string(total)},{"type", std::to_string(init.type())}}));
    response->set_media_id(id); response->set_type(init.type()); response->set_url(url); response->set_thumbnail_url(thumb_url); if(!hls_url.empty()) response->set_hls_url(hls_url); if(!webp_url.empty()) response->set_webp_url(webp_url); if(!mp4_url.empty()) response->set_mp4_url(mp4_url);
    int ttl=3600; if (std::string ttl_md = GetMetadataValue(context, "x-url-ttl"); !ttl_md.empty() && std::all_of(ttl_md.begin(), ttl_md.end(), ::isdigit)) { try { ttl = std::stoi(ttl_md); } catch(...) {} if (ttl<=0) ttl=3600; }
    response->set_url(storage_->SignUrl(response->url(), ttl)); response->set_thumbnail_url(storage_->SignUrl(response->thumbnail_url(), ttl)); if(!hls_url.empty()) response->set_hls_url(storage_->SignUrl(response->hls_url(), ttl)); if(!webp_url.empty()) response->set_webp_url(storage_->SignUrl(response->webp_url(), ttl)); if(!mp4_url.empty()) response->set_mp4_url(storage_->SignUrl(response->mp4_url(), ttl));
    return ::grpc::Status::OK;
}

::grpc::Status MediaServiceImpl::GetMedia(::grpc::ServerContext* context,
										  const ::sonet::media::GetMediaRequest* request,
										  ::sonet::media::GetMediaResponse* response) {
	LOG_INFO("get_media", (std::unordered_map<std::string,std::string>{{"media_id", request->media_id()}}));
	MediaRecord rec{};
	if (!repo_->Get(request->media_id(), rec)) return {::grpc::StatusCode::NOT_FOUND, "not found"};
	std::string caller = GetMetadataValue(context, "x-user-id");
	if (!caller.empty() && caller != rec.owner_user_id && !IsAdmin(context)) {
		return {::grpc::StatusCode::PERMISSION_DENIED, "forbidden"};
	}
	auto* m = response->mutable_media();
	m->set_id(rec.id);
	m->set_owner_user_id(rec.owner_user_id);
	m->set_type(rec.type);
	m->set_mime_type(rec.mime_type);
	m->set_size_bytes(rec.size_bytes);
	m->set_width(rec.width);
	m->set_height(rec.height);
	m->set_duration_seconds(rec.duration_seconds);
	// Optionally sign URLs (best-effort)
	m->set_original_url(storage_->SignUrl(rec.original_url, 3600));
	m->set_thumbnail_url(storage_->SignUrl(rec.thumbnail_url, 3600));
	m->set_hls_url(storage_->SignUrl(rec.hls_url, 3600));
	m->set_webp_url(storage_->SignUrl(rec.webp_url, 3600));
	m->set_mp4_url(storage_->SignUrl(rec.mp4_url, 3600));
	m->set_created_at(rec.created_at);
	return ::grpc::Status::OK;
}

::grpc::Status MediaServiceImpl::DeleteMedia(::grpc::ServerContext* context,
											 const ::sonet::media::DeleteMediaRequest* request,
											 ::sonet::media::DeleteMediaResponse* response) {
	LOG_INFO("delete_media", (std::unordered_map<std::string,std::string>{{"media_id", request->media_id()}}));
	// We delete from storage best-effort, then repo
	MediaRecord rec{};
	bool ok = repo_->Get(request->media_id(), rec);
	if (ok) {
		std::string caller = GetMetadataValue(context, "x-user-id");
		if (!caller.empty() && caller != rec.owner_user_id && !IsAdmin(context)) {
			return {::grpc::StatusCode::PERMISSION_DENIED, "forbidden"};
		}
		// Remove all variants and HLS under owner/id prefix
		storage_->DeletePrefix(rec.owner_user_id + "/" + rec.id);
	}
	response->set_deleted(repo_->Delete(request->media_id()));
	return ::grpc::Status::OK;
}

::grpc::Status MediaServiceImpl::ListUserMedia(::grpc::ServerContext* context,
											   const ::sonet::media::ListUserMediaRequest* request,
											   ::sonet::media::ListUserMediaResponse* response) {
	// Authorization: caller must match requested owner unless admin
	std::string caller = GetMetadataValue(context, "x-user-id");
	if (!caller.empty() && caller != request->owner_user_id() && !IsAdmin(context)) {
		return {::grpc::StatusCode::PERMISSION_DENIED, "forbidden"};
	}
	uint32_t total_pages = 0;
	auto items = repo_->ListByOwner(request->owner_user_id(), request->page(), request->page_size(), total_pages);
	LOG_INFO("list_user_media", (std::unordered_map<std::string,std::string>{{"owner", request->owner_user_id()},{"count", std::to_string(items.size())},{"page", std::to_string(request->page())}}));
	response->set_page(request->page());
	response->set_page_size(request->page_size());
	response->set_total_pages(total_pages);
	for (const auto& r : items) {
		auto* m = response->add_items();
		m->set_id(r.id);
		m->set_owner_user_id(r.owner_user_id);
		m->set_type(r.type);
		m->set_mime_type(r.mime_type);
		m->set_size_bytes(r.size_bytes);
		m->set_width(r.width);
		m->set_height(r.height);
		m->set_duration_seconds(r.duration_seconds);
		m->set_original_url(storage_->SignUrl(r.original_url, 3600));
		m->set_thumbnail_url(storage_->SignUrl(r.thumbnail_url, 3600));
		m->set_hls_url(storage_->SignUrl(r.hls_url, 3600));
		m->set_webp_url(storage_->SignUrl(r.webp_url, 3600));
		m->set_mp4_url(storage_->SignUrl(r.mp4_url, 3600));
		m->set_created_at(r.created_at);
	}
	return ::grpc::Status::OK;
}

::grpc::Status MediaServiceImpl::HealthCheck(::grpc::ServerContext* /*context*/,
										 const ::sonet::media::HealthCheckRequest* /*request*/,
										 ::sonet::media::HealthCheckResponse* response) {
	response->set_status("ok");
	return ::grpc::Status::OK;
}

::grpc::Status MediaServiceImpl::ToggleMediaLike(::grpc::ServerContext* context,
													 const ::sonet::media::ToggleMediaLikeRequest* request,
													 ::sonet::media::ToggleMediaLikeResponse* response) {
	if (!request || request->media_id().empty()) {
		return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "media_id required");
	}
	LoadLikesIfNeeded();
	std::string media_id = request->media_id();
	std::string user_id = request->user_id();
	if (user_id.empty() && context) {
		std::string md = GetMetadataValue(context, "x-user-id");
		if (!md.empty()) user_id = md;
	}
	if (user_id.empty()) user_id = "anon";
	bool desired = request->is_liked();
	uint32_t count = 0;
	{
		std::lock_guard<std::mutex> lock(g_like_mu);
		std::string key = user_id + "|" + media_id;
		bool prev = g_user_media_liked[key]; // default false
		if (prev != desired) {
			uint32_t& ref = g_media_like_counts[media_id];
			if (desired) {
				ref += 1;
			} else {
				if (ref > 0) ref -= 1;
			}
			g_user_media_liked[key] = desired;
			count = ref;
		} else {
			// no change
			count = g_media_like_counts[media_id];
		}
	}
	SaveLikes();
	response->set_media_id(media_id);
	response->set_like_count(count);
	response->set_is_liked(desired);
	return ::grpc::Status::OK;
}

} // namespace sonet::media_service

