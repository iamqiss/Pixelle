# Sonet Police

External moderation dashboard for Sonet.

## Getting started

1. Copy env:

```
cp .env.example .env.local
```

2. Install and run:

```
pnpm i
pnpm dev
```

The app expects the moderation API at `NEXT_PUBLIC_MOD_API` (default in `.env.example` is `http://localhost:8080`). It consumes:

- GET `/api/v1/reports` with filters
- GET `/api/v1/audit`
- SSE `/api/v1/stream/reports`, `/api/v1/stream/signals`

## Notes
- This is a scaffold: add auth (OIDC), RBAC, and bulk actions next.
- Use a reverse proxy to attach JWTs/cookies for secured environments.