# Pixelle monorepo scaffolding script

This folder contains scripts that scaffold a monorepo layout for Pixelle with two top-level apps: `Pixelle-iOS` and `Pixelle-Android`.

Files:
- `scaffold_monorepo.sh` — generates the project skeleton with many placeholder files for both iOS and Android apps, shared libs, and tooling directories.
- `bootstrap.sh` — small helper to run initial tasks (placeholder).

Usage:
1. Make the script executable: `chmod +x scripts/scaffold_monorepo.sh`
2. Run it from the repository root: `./scripts/scaffold_monorepo.sh --force`
3. Use `--force` to overwrite previously generated files.

What it creates (expanded):
- apps/Pixelle-iOS — Large Swift/SwiftUI layout with many feature modules: Core, FeatureFeed, FeaturePost, FeatureProfile, FeatureChat, UI Kit, Resources, Tests, fastlane placeholder.
- apps/Pixelle-Android — Multi-module Gradle/Kotlin Compose layout (app, feature modules, core, data, network).
- libs/design-tokens — shared design tokens and guidance for codegen.
- libs/protos — placeholder for protobuf definitions.
- packages/kmp, packages/ui — conceptual KMP and cross-platform UI packages.
- tools/ — codegen, formatting and linting helper scripts and READMEs.
- ci/ — GitHub Actions CI placeholder for cross-platform jobs.
- docs/ — architecture and onboarding docs placeholders.

Notes:
- The scaffold intentionally creates many placeholder files to mirror a large, established app structure. Use it as a starting point: replace placeholders with real implementation and gradually remove unused files.
- For iOS development, open `apps/Pixelle-iOS` in Xcode and create an Xcode workspace that includes the generated SwiftPM targets or convert the layout to an `.xcodeproj` as needed.
- For Android, import the `apps/Pixelle-Android` directory into Android Studio and configure Gradle as needed.

Quick commands:

```bash
chmod +x scripts/scaffold_monorepo.sh
./scripts/scaffold_monorepo.sh --force
```

This README is a companion to `scripts/scaffold_monorepo.sh` and documents the intent and top-level structure of the generated monorepo.
