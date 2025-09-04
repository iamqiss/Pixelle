# Sonet API Inventory & Endpoint Plan

Comprehensive extraction of current gRPC proto service surface (as of repo state) plus proposed REST mapping, identified gaps, and potential loopholes. Use this to accelerate client integration and guide secure build‑out.

---
## 1. Service Inventory (from /proto/services)

Legend: ✅ implemented proto with messages; ⚠️ file exists but empty/placeholder; ❗ design concern.

| Service File | Package / Service | Status | Key RPCs (abridged) |
|--------------|------------------|--------|---------------------|
| user.proto | sonet.user.UserService | ✅ | RegisterUser, LoginUser, LogoutUser, VerifyToken, RefreshToken, Change/Reset Password, Email + 2FA flows, Get/Terminate Sessions, Get/UpdateUserProfile |
| note.proto | sonet.note.NoteService | ✅ (basic) | CreateNote, GetNote, DeleteNote, LikeNote, RenoteNote, GetUserNotes, GetNoteThread, SearchNotes, HealthCheck |
| note_service.proto | sonet.note.grpc.NoteService | ✅ (expanded) ❗ | High‑perf superset: CreateNote, GetNote, UpdateNote, DeleteNote, GetNotesBatch, LikeNote, RenoteNote, QuoteRenote, BookmarkNote, multiple timeline + search variants, analytics, thread, streaming, moderation |
| timeline.proto | sonet.timeline.TimelineService | ✅ | GetTimeline, GetUserTimeline, RefreshTimeline, MarkTimelineRead, Update/GetTimelinePreferences, SubscribeTimelineUpdates (stream), HealthCheck |
| media.proto | sonet.media.MediaService | ✅ | Upload (client stream), GetMedia, DeleteMedia, ListUserMedia, HealthCheck |
| fanout.proto | sonet.fanout.FanoutService | ✅ | InitiateFanout, GetFanoutJobStatus, CancelFanoutJob, GetUserTier, ProcessFollowerBatch, GetFanoutMetrics, HealthCheck |
| messaging.proto | sonet.messaging.MessagingService | ✅ | SendMessage, GetMessages, UpdateMessageStatus, SearchMessages, CreateChat, GetChats, UploadAttachment, SetTyping, StreamMessages (bidi) |
| follow.proto | sonet.follow.FollowService | ✅ | FollowUser, UnfollowUser, MuteUser, BlockUser, GetFollowers, GetFollowing, GetFollowSuggestions, GetRelationship |
| notification.proto | sonet.notification.NotificationService | ✅ | ListNotifications, MarkNotificationsRead, StreamNotifications |
| analytics.proto | (missing service declarations) | ⚠️ | — |
| search.proto | (missing service declarations) | ⚠️ | — |
| common/*.proto | Shared types | Partial | timestamp.proto OK; pagination.proto only defines Pagination while other protos reference PaginationRequest/Response (missing) |

### Duplication Warning
Two distinct Note services (`note.proto` and `note_service.proto`) define overlapping but different models & RPCs. This creates drift risk. Recommend a consolidation strategy (see Section 7).

---
## 2. Core Domain Models (selected)

| Domain | Key Fields / Notes |
|--------|--------------------|
| Note (basic) | id, author_id, text, visibility, content_warning, media_ids, entities (mentions/hashtags/links), reply & renote context, metrics, language_code, flags |
| Note (high‑perf) | note_id, author_id, content, reply/quote/thread IDs, granular metrics (like/renote/bookmark/view), attachments (rich typed), location, engagement & user interaction state, analytics structs |
| TimelineItem | note, source, ranking_signals (affinity, quality, velocity, recency, personalization, diversity), final_score, injection_reason |
| UserProfile | username, email, display_name, stats, settings, privacy_settings |
| Media | id, owner_user_id, type, mime/size/dimensions, variant URLs |
| Message | message_id, chat_id, sender_id, content, type/status/encryption, attachments, reply_to, timestamps |

Model fragmentation: high‑perf `Note` vs basic `Note` diverge (naming: content vs text, renote vs renote). Standardize before client binding.

---
## 3. Proposed Public REST Mapping (Phase 1 Adapter)

While gRPC proto is canonical, the mobile client currently consumes REST. Provide minimal REST façade:

| REST Endpoint | Method | Maps To RPC | Request Params | Notes |
|---------------|--------|-------------|----------------|-------|
| /v1/auth/register | NOTE | RegisterUser | body | Returns user + verification_token |
| /v1/auth/login | NOTE | LoginUser | body | Returns access & refresh tokens |
| /v1/auth/refresh | NOTE | RefreshToken | body | Standard refresh rotation |
| /v1/auth/logout | NOTE | LogoutUser | body/session header | Support logout_all_devices flag |
| /v1/users/:id | GET | GetUserProfile | path id | Auth required if private |
| /v1/users/:id | PATCH | UpdateUserProfile | path id + body | Partial updates; restrict immutable fields |
| /v1/notes | NOTE | CreateNote | body | Validate length, media limits |
| /v1/notes/:id | GET | GetNote | path id | include_thread param |
| /v1/notes/:id | DELETE | DeleteNote | path id | Soft delete semantics |
| /v1/notes/:id/like | NOTE | LikeNote | path id | Body { like: boolean } |
| /v1/notes/:id/renote | NOTE | RenoteNote | path id | Quote via is_quote_renote |
| /v1/notes/:id/bookmark | NOTE | BookmarkNote | high‑perf variant | Optional early deferral |
| /v1/timeline/home | GET | GetTimeline/GetHomeTimeline | query pagination | algorithm param (chrono|algo|hybrid) |
| /v1/timeline/user/:id | GET | GetUserTimeline | path id | include_replies, include_renotes flags |
| /v1/timeline/refresh | GET | RefreshTimeline | since param | Returns new items only |
| /v1/timeline/preferences | GET | GetTimelinePreferences | auth user | |
| /v1/timeline/preferences | PUT | UpdateTimelinePreferences | body | Validate lists size |
| /v1/media/upload | NOTE (multipart) | Upload | chunked alt | Provide resumable later |
| /v1/media/:id | GET | GetMedia | path id | Signed URL handing |
| /v1/media/:id | DELETE | DeleteMedia | path id | Owner/admin only |
| /v1/messages | NOTE | SendMessage | body | Validate encryption selection |
| /v1/chats | GET | GetChats | query pagination | Filter by type |
| /v1/chats/:id/messages | GET | GetMessages | path id + pagination | before/after cursors |
| /v1/search/notes | GET | SearchNotes (basic) | query params | Filters mapped to RPC fields |
| /v1/fanout/jobs/:id | GET | GetFanoutJobStatus | path id | Admin/observability scope |
| /v1/fanout/metrics | GET | GetFanoutMetrics | query | Protected (internal) |

Streaming: Use WebSocket endpoints `/ws/timeline` and `/ws/engagement` bridging to StreamTimeline / StreamEngagement.

---
## 4. Gaps & Missing Definitions

| Gap | Detail | Impact | Recommendation |
|-----|--------|--------|----------------|
| (Resolved) PaginationRequest / PaginationResponse undefined | Implemented in pagination.proto | — | — |
| (Resolved) follow.proto empty | Implemented FollowService | — | — |
| (Resolved) notification.proto empty | Implemented NotificationService | — | — |
| Empty search.proto / analytics.proto | Specialized search & analytics absent | Forced overloading of NoteService | Split advanced search & analytics for separation of concerns |
| Dual Note services | Basic vs high‑perf duplicates with naming divergence | Drift & client confusion | Merge or mark one as internal; adopt consistent field names (text vs content, renote vs renote) |
| Inconsistent naming (Renote/Renote, Like vs Bookmark vs Favorite not unified) | Semantic friction | Developer cognitive load | (Resolved) Standardize on renote; verbs: like, renote, quote, bookmark |
| Security context absent in many RPCs | Most requests just carry user_id | Spoof risk | Enforce auth via metadata (JWT) and drop explicit user_id where derivable; add server-side authorization checks |
| No rate limit schema in protos | DDoS / abuse risk | Hard to communicate limits | Add RateLimit headers in REST, and a RateLimit service/status RPC for introspection |
| Lack of error codes taxonomy | Mixed success bool + error_message | Hard to programmatically handle | Introduce enum ErrorCode across services; replace freeform strings |
| Missing idempotency on create operations | Potential duplicate note/media on retries | Data duplication | Support Idempotency-Key header (REST) / idempotency_token field (RPC) |
| No versioning strategy | Single service definitions only | Breaking change risk | Namespace v1; use explicit version headers for REST |
| Timeline ranking signals exposed optionally | Potential reverse‑engineering | Abuse of algorithm | Gate include_ranking_signals behind admin/debug flag |

---
## 5. Security & Privacy Loopholes (Early Focus)

1. User Impersonation: RPCs pass `user_id` directly (e.g., LikeNoteRequest). Without authenticated context binding, a client could like on behalf of others. Fix: derive user from token claims.
2. Excessive Data in Streaming: TimelineUpdate currently could leak injection_reason, ranking_signals if misconfigured. Provide server‑side filtering by audience.
3. Fanout Metrics Exposure: GetFanoutMetrics could allow enumeration of delivery performance. Restrict to internal/admin scopes.
4. Missing Privacy Filters: GetUserTimelineRequest lacks a privacy enforcement field; implement server checks for private accounts & blocks.
5. Attachment/Media Validation: Media Upload lacks fields for checksum or content safety scanning status in request; risk of malicious payloads. Add checksum & size limit validation step.
6. Moderation Gaps: Basic NoteService doesn't expose moderation endpoints except flags array; unify with high‑perf moderation structures to avoid inconsistent enforcement.
7. Rate Limiting Blindness: No feedback headers; clients can't self‑throttle—risk of cascading failures. Add standard X-RateLimit-* response headers.
8. Overexposed Analytics: Live engagement streaming could leak view counts to non-author viewers; restrict metrics scope.
9. Sensitive Lists: TimelinePreferences includes muted_keywords/users; ensure not leaked in other endpoints.
10. Lack of Soft Delete Handling: Deleted notes have `deleted_at` but RPCs (e.g., GetNote) may still return full content. Mask text & attachments note delete (except for moderators).

---
## 6. Standardization Recommendations

| Concern | Current | Proposed Standard |
|---------|---------|-------------------|
| ID Field Naming | mix of id, note_id, message_id | Use `<entity>_id` internally; REST path uses plain `:id` |
| Text Field | text vs content | Adopt `text` for user-authored plain text; `content` reserved for enriched/HTML (if ever) |
| Renote Verb | renote/renote | Pick `renote` externally; keep `renote` only if legacy compat required |
| Timestamps | string RFC3339 vs structured Timestamp | Use proto Timestamp everywhere; REST returns ISO8601 Z format |
| Pagination | offset/limit & page/page_size mix | Cursor-based: `cursor` + `limit`; responses include `next_cursor` |
| Success/Error | success bool + error_message | Remove success when using gRPC Status; else unify `{ ok: boolean, code, message }` |

---
## 7. Consolidation Plan (Notes & Timelines)

Phase A: Declare high‑perf `note_service.proto` authoritative for Note CRUD + engagement.

Phase B: Deprecate overlapping RPCs in basic `note.proto` by:
1. Marking them with option (deprecated = true)
2. Generating transitional REST endpoints pointing to high‑perf logic
3. Removing in next major version.

Phase C: Merge Timeline logic: either keep dedicated `timeline.proto` (clean separation) or fold minimal getters into NoteService but keep ranking APIs isolated.

---
## 8. Minimal Client Binding Set (MVP)

Prioritize these for first Sonet client integration:
1. Auth: RegisterUser, LoginUser, RefreshToken, GetUserProfile
2. Notes: CreateNote, GetNote, LikeNote (renote/quote later), GetUserNotes
3. Timeline: GetTimeline (home), GetUserTimeline
4. Media: Upload, GetMedia
5. Messaging (defer) – only if launch scope requires DMs

---
## 9. Actionable Next Steps

Immediate (Week 1):
- Renote naming unified (renote removed). Add deprecation notices if legacy clients existed.
- Idempotency fields added (CreateNote, UploadInit).
- Follow & Notification services added.

Short Term (Week 2–3):
- Implement REST façade gateway (FastAPI / Node / Go) mapping to gRPC.
- Introduce auth interceptor to strip user_id from client-supplied fields.
- Add rate limit middleware + response headers.
- Add Idempotency-Key support to CreateNote & Upload.

Security Hardening:
- Gate ranking_signals & fanout metrics behind admin scopes.
- Add token-scoped permissions (read:profile, write:note, write:engagement, stream:timeline, admin:metrics).

Observability:
- Emit structured logs: service, rpc, latency_ms, status_code, user_id (hashed), request_size, response_size.
- Add tracing spans around fanout pipeline steps.

---
## 10. Glossary

| Term | Definition |
|------|------------|
| Fanout | Distribution of a newly created note to follower timelines |
| Renote | Sharing another user's note into your timeline |
| Quote Renote | Renote with additional commentary |
| Timeline Algorithm | Ranking strategy determining item order |
| Engagement | Aggregated interactions (likes, renotes, replies, views) |

---
Generated automatically from current proto files; update after any service schema changes.
