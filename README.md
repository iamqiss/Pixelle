# Pixelle (monorepo scaffold)

This repository contains generator scripts and example scaffolds for a monorepo with two platform targets:

- `Pixelle-iOS` — a SwiftUI / Swift Package Manager based iOS app scaffold
- `Pixelle-Android` — a Kotlin / Gradle based Android app scaffold

Run the main scaffold script to generate both projects and a large set of placeholder files:

```bash
bash scripts/scaffold_monorepo.sh
```

Notes
- The script is idempotent and won't overwrite existing files unless you run it with `--force`.
- iOS project requires macOS + Xcode to open and build.
- Android project requires Android Studio / Gradle to open and build.

The scaffold produces a deliberately large and nested file layout to emulate a complex social app codebase. Use it as a starting point for experiments, architecture tests, and local development.