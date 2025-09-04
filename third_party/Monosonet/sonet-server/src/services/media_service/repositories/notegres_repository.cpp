//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

#include "../service.h"

#include <pqxx/pqxx>
#include <optional>

namespace sonet::media_service {

class NotegresRepo final : public MediaRepository {
public:
    explicit NotegresRepo(std::string conn): conn_str_(std::move(conn)) {
        pqxx::connection c(conn_str_);
        pqxx::work tx(c);
        tx.exec(R"SQL(
            CREATE TABLE IF NOT EXISTS media (
                id TEXT PRIMARY KEY,
                owner_user_id TEXT NOT NULL,
                type INT NOT NULL,
                mime_type TEXT,
                size_bytes BIGINT,
                width INT,
                height INT,
                duration_seconds DOUBLE PRECISION,
                original_url TEXT,
                thumbnail_url TEXT,
                hls_url TEXT,
                webp_url TEXT,
                mp4_url TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_media_owner ON media(owner_user_id);
        )SQL");
        tx.commit();
    }

    bool Save(const MediaRecord& r) override {
        try {
            pqxx::connection c(conn_str_);
            pqxx::work tx(c);
                        tx.exec_params(R"SQL(
                                INSERT INTO media (id, owner_user_id, type, mime_type, size_bytes, width, height, duration_seconds, original_url, thumbnail_url, hls_url, webp_url, mp4_url)
                                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                                ON CONFLICT (id) DO UPDATE SET
                                    owner_user_id=EXCLUDED.owner_user_id,
                                    type=EXCLUDED.type,
                                    mime_type=EXCLUDED.mime_type,
                                    size_bytes=EXCLUDED.size_bytes,
                                    width=EXCLUDED.width,
                                    height=EXCLUDED.height,
                                    duration_seconds=EXCLUDED.duration_seconds,
                                    original_url=EXCLUDED.original_url,
                                    thumbnail_url=EXCLUDED.thumbnail_url,
                                    hls_url=EXCLUDED.hls_url,
                                    webp_url=EXCLUDED.webp_url,
                                    mp4_url=EXCLUDED.mp4_url
                        )SQL",
                                r.id, r.owner_user_id, static_cast<int>(r.type), r.mime_type, static_cast<long long>(r.size_bytes),
                                static_cast<int>(r.width), static_cast<int>(r.height), r.duration_seconds, r.original_url, r.thumbnail_url, r.hls_url, r.webp_url, r.mp4_url);
            tx.commit();
            return true;
        } catch (...) { return false; }
    }

    bool Get(const std::string& id, MediaRecord& out) override {
        try {
            pqxx::connection c(conn_str_);
            pqxx::read_transaction tx(c);
            auto r = tx.exec_params1("SELECT id, owner_user_id, type, mime_type, size_bytes, width, height, duration_seconds, original_url, thumbnail_url, hls_url, webp_url, mp4_url, to_char(created_at,'YYYY-MM-DD""T""HH24:MI:SSZ') FROM media WHERE id=$1", id);
            out.id = r[0].c_str();
            out.owner_user_id = r[1].c_str();
            out.type = static_cast<::sonet::media::MediaType>(r[2].as<int>());
            out.mime_type = r[3].c_str();
            out.size_bytes = r[4].as<long long>();
            out.width = r[5].as<int>();
            out.height = r[6].as<int>();
            out.duration_seconds = r[7].as<double>();
            out.original_url = r[8].c_str();
            out.thumbnail_url = r[9].c_str();
            out.hls_url = r[10].c_str();
            out.webp_url = r[11].c_str();
            out.mp4_url = r[12].c_str();
            out.created_at = r[13].c_str();
            return true;
        } catch (...) { return false; }
    }

    bool Delete(const std::string& id) override {
        try {
            pqxx::connection c(conn_str_);
            pqxx::work tx(c);
            tx.exec_params("DELETE FROM media WHERE id=$1", id);
            tx.commit();
            return true;
        } catch (...) { return false; }
    }

    std::vector<MediaRecord> ListByOwner(const std::string& owner, uint32_t page, uint32_t page_size, uint32_t& total_pages) override {
        std::vector<MediaRecord> res;
        try {
            if (page_size == 0) page_size = 20;
            if (page == 0) page = 1;
            pqxx::connection c(conn_str_);
            pqxx::read_transaction tx(c);
            auto cnt = tx.exec_params1("SELECT COUNT(*) FROM media WHERE owner_user_id=$1", owner)[0].as<long long>();
            total_pages = static_cast<uint32_t>((cnt + page_size - 1) / page_size);
            auto offset = static_cast<long long>((page - 1) * page_size);
            auto rs = tx.exec_params(
                "SELECT id, owner_user_id, type, mime_type, size_bytes, width, height, duration_seconds, original_url, thumbnail_url, hls_url, webp_url, mp4_url, to_char(created_at,'YYYY-MM-DD""T""HH24:MI:SSZ') FROM media WHERE owner_user_id=$1 ORDER BY created_at DESC OFFSET $2 LIMIT $3",
                owner, offset, static_cast<int>(page_size));
            for (const auto& row : rs) {
                MediaRecord m{};
                m.id = row[0].c_str();
                m.owner_user_id = row[1].c_str();
                m.type = static_cast<::sonet::media::MediaType>(row[2].as<int>());
                m.mime_type = row[3].c_str();
                m.size_bytes = row[4].as<long long>();
                m.width = row[5].as<int>();
                m.height = row[6].as<int>();
                m.duration_seconds = row[7].as<double>();
                m.original_url = row[8].c_str();
                m.thumbnail_url = row[9].c_str();
                m.hls_url = row[10].c_str();
                m.webp_url = row[11].c_str();
                m.mp4_url = row[12].c_str();
                m.created_at = row[13].c_str();
                res.emplace_back(std::move(m));
            }
        } catch (...) { total_pages = 0; }
        return res;
    }

private:
    std::string conn_str_;
};

std::unique_ptr<MediaRepository> CreateNotegresRepo(const std::string& conn_str) {
    try { return std::make_unique<NotegresRepo>(conn_str); } catch (...) { return nullptr; }
}

} // namespace sonet::media_service
