//
// Copyright (c) 2025 Neo Qiss
// All rights reserved.
//
// This software is proprietary and confidential.
// Unauthorized copying, distribution, or use is strictly prohibited.
//

# Media Service (Sonet)

This is a minimal, runnable gRPC media service implementing:

- Client-streaming Upload (chunked)
- GetMedia, DeleteMedia, ListUserMedia
- Local disk storage backend (file:/// URLs) for dev
- In-memory repository for metadata

Processing is stubbed (image/video/gif); replace with real implementations (FFmpeg, ImageMagick, etc.).

## Build

The top-level build can be configured to build only this service and generate gRPC sources locally.

Try:

```bash
cd sonet
./scripts/build/build.sh Debug
# or direct:
cd build
cmake .. -DBUILD_GLOBAL_PROTO=OFF -DBUILD_OTHER_SERVICES=OFF -DBUILD_EXTERNAL=OFF -DBUILD_TESTS=OFF
cmake --build . -j
```

## Run

```bash
./build/src/services/media_service/media_service
```

Listens on 0.0.0.0:50053.

## Proto

Service proto lives at `proto/services/media.proto`.

## AWS SDK (Optional Native S3 Backend)

To enable native AWS SDK S3 backend instead of CLI fallback:

```bash
./scripts/build/install_aws_sdk.sh --version v1.11.210
cmake -S sonet -B sonet/build
cmake --build sonet/build --target media_service -j
```

If the SDK is found, build output will show `AWS SDK found; enabling native S3 backend`.

## Notes

- Replace LocalStorage with S3/R2/MinIO implementation for production.
- Add NSFW/content scanning hook in Upload before saving.
- Generate thumbnails and HLS/DASH playlists for videos using FFmpeg.
