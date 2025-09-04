# Authentication & Authorization (Draft)

## Token Model
UserService RPCs return `access_token` (short‑lived) and `refresh_token` (long‑lived). Proposed defaults:
* Access Token: 15m JWT (HS256 or RS256) with claims: sub (user_id), exp, iat, scope, sid (session_id), ver (token schema version).
* Refresh Token: 30d opaque (DB or Redis stored) rotated on each refresh; revoke chain on compromise.

## Flows
1. Registration: `RegisterUser` -> returns verification_token (email verification step). REST: NOTE /v1/auth/register
2. Login: `LoginUser` with credentials (+ optional 2FA) -> returns tokens & session. REST: NOTE /v1/auth/login
3. 2FA: If requires_2fa=true, call `VerifyTwoFactor` -> new access/refresh pair.
4. Token Refresh: `RefreshToken` -> rotates refresh token; old becomes invalid.
5. Logout: `LogoutUser` (optional logout_all_devices) -> revoke session + associated refresh token(s).

## Session Management
`GetActiveSessions` enumerates sessions for device management. Each session maps to refresh token lineage; terminating a session invalidates future refresh attempts.

## Recommended Headers
* Authorization: Bearer <access_token>
* X-Session-ID: <session_id> (optional explicit)
* X-Request-ID: client supplied idempotency/tracing
* Idempotency-Key: for NOTE create operations (CreateNote, Upload)

## Scopes (Proposed)
| Scope | Grants |
|-------|--------|
| read:profile | GetUserProfile, timeline fetch |
| write:note | Create / Update / Delete note, like, renote |
| read:message | GetChats, GetMessages |
| write:message | SendMessage, typing, attachments |
| stream:timeline | WebSocket/stream subscriptions |
| admin:metrics | Fanout metrics, analytics, moderation queries |

## Authorization Strategy
1. Remove raw `user_id` from client-submitted RPC fields where derivable; rely on token subject.
2. Interceptors populate `context.user_id`, enforce scope per RPC.
3. Fine-grained checks: author-only edits; media owner deletion; private profile gating.
4. Block/Mute lists: Inject filter layer in timeline & messaging queries.

## Refresh Rotation Pseudocode
```
onRefresh(refreshToken):
	record = store.lookup(refreshToken)
	if !record || record.revoked || record.used: reject
	if record.expires_at < now: reject
	mark record.used=true
	newRefresh = store.create(user_id, session_id)
	access = signJWT(user_id, session_id, scopes)
	return { access, newRefresh }
```

Mitigates replay by one-time use of refresh tokens.

## Security Hardening
* Enforce password complexity & HaveIBeenPwned hash check.
* Rate limit LoginUser & RegisterUser (e.g., 5/min/IP, sliding window).
* Store password hashes with Argon2id.
* 2FA backup codes hashed & one-time use.
* Add device fingerprinting for anomaly detection (is_suspicious flag).

## Open Issues
* Email verification TTL & resend limits not defined.
* No password reset token entropy spec (recommend 32 bytes base64). 
* Need explicit error codes for auth failures (e.g., INVALID_CREDENTIALS, MFA_REQUIRED, TOKEN_EXPIRED).

Update after FollowService & NotificationService introduce additional scopes.
