//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

// S3/R2/MinIO backend: prefers AWS SDK (if available) else falls back to AWS CLI.

#include "../service.h"
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <unistd.h>
#ifdef USE_AWS_SDK_S3
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#endif

namespace fs = std::filesystem;
namespace sonet::media_service {

// Abstract base that chooses implementation at runtime only by compile flags.
class S3StorageBase : public StorageBackend { };

#ifdef USE_AWS_SDK_S3
class AwsSdkS3Storage final : public S3StorageBase {
public:
	AwsSdkS3Storage(std::string bucket, std::string base_url, std::string endpoint)
		: bucket_(std::move(bucket)), base_url_(std::move(base_url)) {
		Aws::Client::ClientConfiguration cfg;
		if (!endpoint.empty()) { cfg.endpointOverride = endpoint; cfg.scheme = Aws::Http::Scheme::HTTPS; }
		client_ = std::make_shared<Aws::S3::S3Client>(cfg);
	}
	bool Put(const std::string& local_path, const std::string& object_key, std::string& out_url) override {
		// Decide between simple put and multipart (threshold 50MB)
		constexpr size_t THRESHOLD = 50ULL * 1024ULL * 1024ULL;
		std::error_code ec; auto sz = fs::file_size(local_path, ec);
		if (ec) return false;
		if (sz <= THRESHOLD) {
			std::ifstream ifs(local_path, std::ios::binary);
			if (!ifs) return false;
			Aws::S3::Model::PutObjectRequest req; req.SetBucket(bucket_.c_str()); req.SetKey(object_key.c_str());
			auto stream = Aws::MakeShared<Aws::StringStream>("putobj");
			(*stream) << ifs.rdbuf();
			req.SetBody(stream);
			auto outcome = client_->PutObject(req);
			if (!outcome.IsSuccess()) return false;
		} else {
			if (!MultipartUpload(local_path, object_key, sz)) return false;
		}
		out_url = base_url_.empty() ? ("https://" + bucket_ + "/" + object_key) : (base_url_ + "/" + object_key);
		return true;
	}
	bool PutDir(const std::string& local_dir, const std::string& object_prefix, std::string& out_base_url) override {
		for (auto& entry : fs::recursive_directory_iterator(local_dir)) {
			if (!entry.is_regular_file()) continue;
			auto rel = fs::relative(entry.path(), local_dir);
			std::string key = object_prefix + "/" + rel.generic_string();
			std::string dummy;
			if (!Put(entry.path().string(), key, dummy)) return false;
		}
		out_base_url = base_url_.empty() ? ("https://" + bucket_ + "/" + object_prefix) : (base_url_ + "/" + object_prefix);
		return true;
	}
	bool Delete(const std::string& object_key) override {
		Aws::S3::Model::DeleteObjectRequest req; req.SetBucket(bucket_.c_str()); req.SetKey(object_key.c_str());
		return client_->DeleteObject(req).IsSuccess();
	}
	bool DeletePrefix(const std::string& object_prefix) override {
		Aws::S3::Model::ListObjectsV2Request listReq; listReq.SetBucket(bucket_.c_str()); listReq.SetPrefix(object_prefix.c_str());
		bool ok = true; while (true) {
			auto outcome = client_->ListObjectsV2(listReq);
			if (!outcome.IsSuccess()) return false;
			Aws::Vector<Aws::S3::Model::ObjectIdentifier> objs;
			for (auto& o : outcome.GetResult().GetContents()) { Aws::S3::Model::ObjectIdentifier id; id.SetKey(o.GetKey()); objs.push_back(id); }
			if (!objs.empty()) {
				Aws::S3::Model::Delete del; del.SetObjects(objs); Aws::S3::Model::DeleteObjectsRequest delReq; delReq.SetBucket(bucket_.c_str()); delReq.SetDelete(del);
				auto d = client_->DeleteObjects(delReq); if (!d.IsSuccess()) ok = false;
			}
			if (!outcome.GetResult().GetIsTruncated()) break;
			listReq.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
		}
		return ok;
	}
	std::string Sign(const std::string& object_key, int ttl_seconds) override {
		// Simple pre-signed URL generation
		Aws::Http::URI uri;
		Aws::S3::Model::GetObjectRequest req; req.SetBucket(bucket_.c_str()); req.SetKey(object_key.c_str());
		auto presigned = client_->GeneratePresignedUrl(bucket_.c_str(), object_key.c_str(), Aws::Http::HttpMethod::HTTP_GET, ttl_seconds);
		if (presigned.empty()) return base_url_ + "/" + object_key;
		return std::string(presigned.c_str());
	}
	std::string SignUrl(const std::string& url, int ttl_seconds) override {
		if (!base_url_.empty() && url.rfind(base_url_,0)==0) {
			std::string key = url.substr(base_url_.size()+1);
			return Sign(key, ttl_seconds);
		}
		return url;
	}
private:
	std::string bucket_;
	std::string base_url_;
	std::shared_ptr<Aws::S3::S3Client> client_;
	bool MultipartUpload(const std::string& path, const std::string& key, size_t size) {
		Aws::S3::Model::CreateMultipartUploadRequest createReq; createReq.SetBucket(bucket_.c_str()); createReq.SetKey(key.c_str());
		auto createOut = client_->CreateMultipartUpload(createReq);
		if (!createOut.IsSuccess()) return false;
		auto uploadId = createOut.GetResult().GetUploadId();
		static const size_t PART_SIZE = 8ULL * 1024ULL * 1024ULL; // 8MB
		std::ifstream ifs(path, std::ios::binary);
		if (!ifs) return false;
		std::vector<Aws::S3::Model::CompletedPart> completed;
		int partNumber = 1;
		while (ifs && ifs.tellg() < static_cast<std::streampos>(size)) {
			std::vector<char> buf(PART_SIZE);
			ifs.read(buf.data(), buf.size());
			std::streamsize got = ifs.gcount();
			Aws::S3::Model::UploadPartRequest upr;
			upr.SetBucket(bucket_.c_str()); upr.SetKey(key.c_str()); upr.SetPartNumber(partNumber); upr.SetUploadId(uploadId);
			auto stream = Aws::MakeShared<Aws::StringStream>("mpupart");
			stream->write(buf.data(), got);
			stream->seekg(0);
			upr.SetBody(stream);
			upr.SetContentLength(got);
			auto upOut = client_->UploadPart(upr);
			if (!upOut.IsSuccess()) { AbortMultipart(key, uploadId); return false; }
			Aws::S3::Model::CompletedPart cp; cp.SetPartNumber(partNumber); cp.SetETag(upOut.GetResult().GetETag()); completed.push_back(cp);
			++partNumber;
		}
		Aws::S3::Model::CompletedMultipartUpload comp;
		for (auto& p : completed) comp.AddParts(p);
		Aws::S3::Model::CompleteMultipartUploadRequest compReq; compReq.SetBucket(bucket_.c_str()); compReq.SetKey(key.c_str()); compReq.SetUploadId(uploadId);
		compReq.SetMultipartUpload(comp);
		auto compOut = client_->CompleteMultipartUpload(compReq);
		if (!compOut.IsSuccess()) { AbortMultipart(key, uploadId); return false; }
		return true;
	}
	void AbortMultipart(const std::string& key, const Aws::String& uploadId) {
		Aws::S3::Model::AbortMultipartUploadRequest req; req.SetBucket(bucket_.c_str()); req.SetKey(key.c_str()); req.SetUploadId(uploadId);
		client_->AbortMultipartUpload(req);
	}
};
#else
class AwsCliS3Storage final : public S3StorageBase {
public:
	AwsCliS3Storage(std::string bucket, std::string base_url, std::string endpoint)
		: bucket_(std::move(bucket)), base_url_(std::move(base_url)), endpoint_(std::move(endpoint)) {}
	// ... existing CLI implementation moved unchanged ...
	bool Put(const std::string& local_path, const std::string& object_key, std::string& out_url) override {
		auto SQ = [](const std::string& s){ std::string r; r.reserve(s.size()+2); r.push_back('\''); for(char c: s){ if(c=='\'') r += "'\"'\"'"; else r.push_back(c);} r.push_back('\''); return r; };
		std::string extra = endpoint_.empty() ? "" : (" --endpoint-url '" + endpoint_ + "'");
		std::string s3url = "s3://" + bucket_ + "/" + object_key;
		std::string cmd = "aws s3 cp " + SQ(local_path) + " " + SQ(s3url) + extra + " >/dev/null 2>&1";
		int rc = std::system(cmd.c_str());
		if (rc != 0) return false;
		out_url = base_url_.empty() ? ("s3://" + bucket_ + "/" + object_key) : (base_url_ + "/" + object_key);
		return true;
	}
	bool PutDir(const std::string& local_dir, const std::string& object_prefix, std::string& out_base_url) override {
		auto SQ = [](const std::string& s){ std::string r; r.reserve(s.size()+2); r.push_back('\''); for(char c: s){ if(c=='\'') r += "'\"'\"'"; else r.push_back(c);} r.push_back('\''); return r; };
		std::string extra = endpoint_.empty() ? "" : (" --endpoint-url '" + endpoint_ + "'");
		std::string s3url = "s3://" + bucket_ + "/" + object_prefix;
		std::string cmd = "aws s3 sync " + SQ(local_dir) + " " + SQ(s3url) + extra + " --delete >/dev/null 2>&1";
		int rc = std::system(cmd.c_str());
		if (rc != 0) return false;
		out_base_url = base_url_.empty() ? ("s3://" + bucket_ + "/" + object_prefix) : (base_url_ + "/" + object_prefix);
		return true;
	}
	bool Delete(const std::string& object_key) override {
		auto SQ = [](const std::string& s){ std::string r; r.reserve(s.size()+2); r.push_back('\''); for(char c: s){ if(c=='\'') r += "'\"'\"'"; else r.push_back(c);} r.push_back('\''); return r; };
		std::string extra = endpoint_.empty() ? "" : (" --endpoint-url '" + endpoint_ + "'");
		std::string s3url = "s3://" + bucket_ + "/" + object_key;
		std::string cmd = "aws s3 rm " + SQ(s3url) + extra + " >/dev/null 2>&1";
		int rc = std::system(cmd.c_str());
		return rc == 0;
	}
	bool DeletePrefix(const std::string& object_prefix) override {
		auto SQ = [](const std::string& s){ std::string r; r.reserve(s.size()+2); r.push_back('\''); for(char c: s){ if(c=='\'') r += "'\"'\"'"; else r.push_back(c);} r.push_back('\''); return r; };
		std::string extra = endpoint_.empty() ? "" : (" --endpoint-url '" + endpoint_ + "'");
		std::string s3url = "s3://" + bucket_ + "/" + object_prefix;
		std::string cmd = "aws s3 rm " + SQ(s3url) + " --recursive" + extra + " >/dev/null 2>&1";
		int rc = std::system(cmd.c_str()); return rc == 0;
	}
	std::string Sign(const std::string& object_key, int ttl_seconds) override {
		char tmpl[] = "/tmp/presignXXXXXX";
		int fd = mkstemp(tmpl);
		if (fd != -1) close(fd);
		std::string tmpfile = tmpl;
		auto SQ = [](const std::string& s){ std::string r; r.reserve(s.size()+2); r.push_back('\''); for(char c: s){ if(c=='\'') r += "'\"'\"'"; else r.push_back(c);} r.push_back('\''); return r; };
		std::string extra = endpoint_.empty() ? "" : (" --endpoint-url '" + endpoint_ + "'");
		std::string s3url = "s3://" + bucket_ + "/" + object_key;
		std::string cmd = "aws s3 presign " + SQ(s3url) + " --expires-in " + std::to_string(ttl_seconds) + extra + " > " + SQ(tmpfile) + " 2>/dev/null";
		int rc = std::system(cmd.c_str());
		if (rc == 0) { std::ifstream ifs(tmpfile); std::string url; std::getline(ifs,url); fs::remove(tmpfile); if (!url.empty()) return url; }
		fs::remove(tmpfile);
		return base_url_.empty() ? ("s3://" + bucket_ + "/" + object_key) : (base_url_ + "/" + object_key);
	}
	std::string SignUrl(const std::string& url, int ttl_seconds) override {
		if (!base_url_.empty() && url.rfind(base_url_,0)==0) { std::string key = url.substr(base_url_.size()+1); return Sign(key, ttl_seconds);} return url;
	}
private:
	std::string bucket_; std::string base_url_; std::string endpoint_;
};
#endif

std::unique_ptr<StorageBackend> CreateS3Storage(const std::string& bucket,
												const std::string& public_base_url,
												const std::string& endpoint) {
#ifdef USE_AWS_SDK_S3
	static bool aws_init = false; static Aws::SDKOptions options;
	if (!aws_init) { Aws::InitAPI(options); aws_init = true; }
	return std::make_unique<AwsSdkS3Storage>(bucket, public_base_url, endpoint);
#else
	return std::make_unique<AwsCliS3Storage>(bucket, public_base_url, endpoint);
#endif
}

} // namespace sonet::media_service
