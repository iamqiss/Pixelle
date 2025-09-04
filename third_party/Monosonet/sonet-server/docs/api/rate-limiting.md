# Rate Limiting Strategy (Draft)

Purpose: Protect core services (auth, notes, timeline, media, messaging) from abuse while preserving UX. Multi-layer approach: edge (REST gateway), gRPC interceptor, and internal operation quotas.

## 1. Classification
| Category | Endpoints | Sensitivity | Suggested Limits (per user unless noted) |
|----------|-----------|-------------|------------------------------------------|
| Auth | /v1/auth/login, /v1/auth/register, 2FA verify | High | 5 attempts / 5 min / IP + user; burst 10 |
| Token Refresh | /v1/auth/refresh | Medium | 30 / hour |
| Notes Create | NOTE /v1/notes | High | 300 / day; 30 / 15 min |
| Engagement | like/renote/bookmark | High | 1000 / day; 60 / min |
| Timeline Fetch | /v1/timeline/* | Medium | 1200 / hour; 30 / min |
| Search | /v1/search/notes | High | 60 / 5 min |
| Media Upload Init | /v1/media/upload | High | 50 / day |
| Messaging Send | NOTE /v1/messages | High | 1000 / day; 60 / min; adaptive for spam |
| Fanout Metrics/Admin | /v1/fanout/* | Admin | 120 / hour (admin) |

IP-based global caps layered above user-specific for unauth endpoints.

## 2. Token Bucket Implementation Outline
Edge gateway maintains Redis token buckets keyed by: `{scope}:{user_id}` and `{scope}:ip:{ip}`.

Bucket Config Example (YAML snippet):
```
limits:
	auth_login:
		capacity: 5
		refill_tokens: 5
		refill_interval: 300s
		block_interval: 900s
	notes_create_burst:
		capacity: 30
		refill_tokens: 30
		refill_interval: 900s
	notes_create_daily:
		capacity: 300
		refill_tokens: 300
		refill_interval: 86400s
```

## 3. Response Headers
Return standard headers on REST responses:
* X-RateLimit-Limit
* X-RateLimit-Remaining
* X-RateLimit-Reset (epoch seconds)
* Retry-After (when blocked)

Suppress for admin/internal endpoints to reduce inference.

## 4. Adaptive & Anomaly Controls
1. Spike Detection: If user triggers >3x average timeline fetch rate within 2 min, apply temporary stricter bucket.
2. Engagement Ratio Guard: If likes per note viewed > 5 for >100 actions, flag for review and slow limit.
3. Media Upload Size Guard: Aggregate bytes uploaded per day; threshold (e.g., 2GB) triggers manual review.

## 5. gRPC Interceptor Logic (Pseudo)
```
OnRPC(ctx, method):
	user = ctx.user_id or anon_ip
	bucketKey = classify(method)
	if !tokenBucket.consume(bucketKey):
		 return ResourceExhausted(error: RATE_LIMITED)
	proceed()
```

## 6. Abuse Scenarios & Mitigations
| Scenario | Vector | Mitigation |
|----------|--------|-----------|
| Credential Stuffing | High-rate login attempts | IP + user buckets; captcha after N failures |
| Timeline Crawling | Rapid pagination | Cursor invalidation + rate limit + anomaly detector |
| Engagement Inflation | Automated like/renote | Behavioral heuristics + per-action caps + delayed write path if suspicious |
| Media Exhaustion | Large file floods | Enforce max file size + per-day byte quota + virus scanning queue |
| Messaging Spam | DM flood to many users | Recipient-based rate limit (messages received per sender), spam scoring |

## 7. Monitoring Metrics
Expose Prometheus counters/gauges:
* ratelimit_requests_total{bucket, outcome}
* ratelimit_tokens_remaining{bucket}
* ratelimit_blocks_total{bucket}
* ratelimit_anomalies_total{type}

Dashboards: Top blocked users/IPs, token exhaustion rate, 95th percentile request consumption speed.

## 8. Next Steps
* Finalize bucket taxonomy aligned with scopes.
* Implement Redis Lua script for atomic multi-bucket consume (burst + rolling + daily).
* Add consistent error response: `{ ok:false, code:"RATE_LIMITED", retry_after_seconds:n }`.
* Integrate into gateway & gRPC interceptors.

Update after FollowService & NotificationService finalize additional categories.
