#!/usr/bin/env bash
# Scaffolding script for Pixelle monorepo (Pixelle-iOS + Pixelle-Android)
# This script generates a large, intentionally verbose project tree with
# many placeholder modules, features, infra and tooling files so you can
# iterate quickly. Use --force to overwrite generated files.

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
FORCE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force|-f) FORCE=1; shift ;;
    --help|-h) echo "Usage: $0 [--force]"; exit 0 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

echo "Scaffolding Pixelle monorepo under: $ROOT_DIR"

create_file() {
  local path="$1"; local content="$2"
  mkdir -p "$(dirname "$path")"
  if [[ -e "$path" && $FORCE -ne 1 ]]; then
    echo "Skipping existing $path (use --force to overwrite)"
    return
  fi
  printf '%s\n' "$content" > "$path"
  echo "Wrote $path"
}

# Top-level directories
mkdir -p "$ROOT_DIR/apps/Pixelle-iOS"
mkdir -p "$ROOT_DIR/apps/Pixelle-Android"
mkdir -p "$ROOT_DIR/libs/design-tokens"
mkdir -p "$ROOT_DIR/libs/protos"
mkdir -p "$ROOT_DIR/packages/ui"
#!/usr/bin/env bash
set -euo pipefail

# Comprehensive monorepo scaffold for Pixelle
# Creates a deliberately large and nested iOS + Android codebase layout
# Usage: bash scripts/scaffold_monorepo.sh [--force]

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FORCE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --force|-f) FORCE=1; shift ;;
    --help|-h) echo "Usage: $0 [--force]"; exit 0 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

info() { echo "[scaffold] $*"; }

need() { command -v "$1" >/dev/null 2>&1 || { echo "Please install $1" >&2; return 1; } }

info "Starting Pixelle monorepo scaffold in: $ROOT_DIR"

ensure_dir() { mkdir -p "$1"; }

# write a file only if it doesn't exist or when FORCE=1
write_file() {
  local path="$1"; shift
  local content="$@"
  ensure_dir "$(dirname "$path")"
  if [[ -e "$path" && $FORCE -ne 1 ]]; then
    info "Skipping existing $path"
    return
  fi
  printf "%s" "$content" > "$path"
  info "Wrote $path"
}

# create many directories to simulate a large codebase
info "Creating repository layout..."
ensure_dir "$ROOT_DIR/apps/Pixelle-iOS"
ensure_dir "$ROOT_DIR/apps/Pixelle-Android"
ensure_dir "$ROOT_DIR/libs/design-tokens"
ensure_dir "$ROOT_DIR/packages/ui"
ensure_dir "$ROOT_DIR/tools/formatting"
ensure_dir "$ROOT_DIR/ci/workflows"
ensure_dir "$ROOT_DIR/docs"

###############################################################################
# iOS scaffold (Swift + SwiftUI + SPM layout)
###############################################################################
IOS_ROOT="$ROOT_DIR/apps/Pixelle-iOS"
info "Building iOS scaffold at $IOS_ROOT"

write_file "$IOS_ROOT/README.md" "# Pixelle-iOS\n\nGenerated SwiftUI scaffold for Pixelle. Open the Sources folder as a Swift package in Xcode or import into an Xcode project."

write_file "$IOS_ROOT/.gitignore" "# Xcode\n.DS_Store\nbuild/\nDerivedData/\n*.xcworkspace\n*.xcuserdata\n*.xcodeproj\n"

write_file "$IOS_ROOT/Package.swift" "// swift-tools-version:5.8\nimport PackageDescription\n\nlet package = Package(\n  name: \"Pixelle-iOS\",\n  platforms: [.iOS(.v15)],\n  products: [ .executable(name: \"PixelleApp\", targets: [\"PixelleApp\"]) ],\n  targets: [ .executableTarget(name: \"PixelleApp\", path: \"Sources/PixelleApp\"), .testTarget(name: \"PixelleTests\", dependencies: [\"PixelleApp\"], path: \"Tests/PixelleTests\") ]\n)\n"

# Create a sprawling sources layout
ensure_dir "$IOS_ROOT/Sources/PixelleApp/Models"
ensure_dir "$IOS_ROOT/Sources/PixelleApp/Services/Auth"
ensure_dir "$IOS_ROOT/Sources/PixelleApp/Services/Feed"
ensure_dir "$IOS_ROOT/Sources/PixelleApp/Networking/Adapters"
ensure_dir "$IOS_ROOT/Sources/PixelleApp/Views/{Feed,Post,Profile,Chat,Auth,Onboarding,Shared,Components,Admin}"
ensure_dir "$IOS_ROOT/Sources/PixelleApp/Utilities"
ensure_dir "$IOS_ROOT/Sources/PixelleApp/Resources/Assets.xcassets"
ensure_dir "$IOS_ROOT/Tests/PixelleTests"

write_file "$IOS_ROOT/Sources/PixelleApp/PixelleApp.swift" "import SwiftUI\n\n@main\nstruct PixelleApp: App {\n  @StateObject private var session = SessionStore()\n  var body: some Scene {\n    WindowGroup {\n      MainTabView().environmentObject(session)\n    }\n  }\n}\n"

write_file "$IOS_ROOT/Sources/PixelleApp/Models/User.swift" "import Foundation\n\npublic struct User: Identifiable, Codable {\n  public let id: String\n  public var name: String\n  public var username: String\n  public var email: String?\n  public var avatarURL: URL?\n}\n\npublic struct Post: Identifiable, Codable {\n  public let id: String\n  public let author: User\n  public let content: String\n  public let images: [URL]?\n  public let timestamp: Date\n}\n"

write_file "$IOS_ROOT/Sources/PixelleApp/Models/SessionStore.swift" "import Foundation\nimport Combine\n\npublic final class SessionStore: ObservableObject {\n  @Published public var currentUser: User? = nil\n  private var cancellables = Set<AnyCancellable>()\n  public init() {}\n  public func signIn(email: String, password: String, completion: @escaping (Result<User, Error>) -> Void) {\n    DispatchQueue.main.asyncAfter(deadline: .now() + 0.6) {\n      let u = User(id: UUID().uuidString, name: \"Demo\", username: \"demo\", email: email, avatarURL: nil)\n      self.currentUser = u\n      completion(.success(u))\n    }\n  }\n}\n"

write_file "$IOS_ROOT/Sources/PixelleApp/Networking/APIClient.swift" "import Foundation\n\npublic enum APIError: Error { case network, decoding, unknown }\n\npublic final class APIClient {\n  public static let shared = APIClient()\n  private init() {}\n  public func fetchFeed(completion: @escaping (Result<[Post], APIError>) -> Void) {\n    DispatchQueue.global().asyncAfter(deadline: .now() + 0.8) { completion(.success([])) }\n  }\n}\n"

write_file "$IOS_ROOT/Sources/PixelleApp/Networking/Adapters/NetworkAdapter.swift" "import Foundation\n\npublic protocol NetworkAdapter { func request(path: String, completion: @escaping (Result<Data, Error>) -> Void) }\n\npublic final class URLSessionAdapter: NetworkAdapter { public init() {} public func request(path: String, completion: @escaping (Result<Data, Error>) -> Void) { completion(.failure(NSError(domain: \"not\", code: -1))) } }\n"

write_file "$IOS_ROOT/Sources/PixelleApp/Utilities/ImageCache.swift" "import UIKit\n\npublic final class ImageCache {\n  public static let shared = ImageCache()\n  private var cache = NSCache<NSString, UIImage>()\n  private init() {}\n  public func image(for key: String) -> UIImage? { cache.object(forKey: key as NSString) }\n  public func insert(_ image: UIImage, for key: String) { cache.setObject(image, forKey: key as NSString) }\n}\n"

write_file "$IOS_ROOT/Sources/PixelleApp/Views/MainTabView.swift" "import SwiftUI\n\npublic struct MainTabView: View {\n  public init() {}\n  public var body: some View {\n    TabView {\n      FeedView().tabItem { Label(\"Feed\", systemImage: \"house\") }\n      SearchPlaceholderView().tabItem { Label(\"Search\", systemImage: \"magnifyingglass\") }\n      NotificationsPlaceholderView().tabItem { Label(\"Notifications\", systemImage: \"bell\") }\n      ProfileView().tabItem { Label(\"Profile\", systemImage: \"person.crop.circle\") }\n    }\n  }\n}\n\nstruct SearchPlaceholderView: View { var body: some View { Text(\"Search\") } }\nstruct NotificationsPlaceholderView: View { var body: some View { Text(\"Notifications\") } }\n"

write_file "$IOS_ROOT/Sources/PixelleApp/Views/Feed/FeedView.swift" "import SwiftUI\n\npublic struct FeedView: View {\n  @State private var posts: [Post] = []\n  public init() {}\n  public var body: some View {\n    NavigationView {\n      List(posts) { p in FeedRow(post: p) }\n        .navigationTitle(\"Pixelle\")\n        .onAppear { APIClient.shared.fetchFeed { _ in } }\n    }\n  }\n}\n\nstruct FeedRow: View { let post: Post; var body: some View { VStack(alignment: .leading) { Text(post.author.name).bold(); Text(post.content) } } }\n"

write_file "$IOS_ROOT/Sources/PixelleApp/Views/Post/PostCell.swift" "import SwiftUI\n\npublic struct PostCell: View { public var post: Post; public init(post: Post) { self.post = post } public var body: some View { VStack(alignment: .leading) { Text(post.author.name).bold(); Text(post.content) } } }\n"

write_file "$IOS_ROOT/Tests/PixelleTests/PixelleTests.swift" "import XCTest\n@testable import PixelleApp\n\nfinal class PixelleTests: XCTestCase { func testUser() { let u = User(id: \"1\", name: \"A\", username: \"a\", email: nil, avatarURL: nil); XCTAssertEqual(u.username, \"a\") } }\n"

info "iOS scaffold created"

###############################################################################
# Android scaffold (Kotlin + Compose placeholders)
###############################################################################
ANDROID_ROOT="$ROOT_DIR/apps/Pixelle-Android"
info "Building Android scaffold at $ANDROID_ROOT"

write_file "$ANDROID_ROOT/README.md" "# Pixelle-Android\n\nGenerated Android skeleton with Compose and a large module layout. Open in Android Studio to convert to a real Gradle project."

write_file "$ANDROID_ROOT/.gitignore" "# Gradle\n.gradle\n/local.properties\n/.idea\n/build\n/captures\n.externalNativeBuild\n*/build/\n"

write_file "$ANDROID_ROOT/settings.gradle.kts" "rootProject.name = \"Pixelle-Android\"\ninclude(\":app\", \":features:feed\", \":features:chat\", \":core:network\", \":core:model\")\n"

write_file "$ANDROID_ROOT/build.gradle.kts" "// Top-level build file (placeholder)\nplugins { kotlin(\"jvm\") version \"1.9.0\" apply false }\nallprojects { repositories { mavenCentral() } }\n"

ensure_dir "$ANDROID_ROOT/app/src/main/java/com/pixelle/app"
ensure_dir "$ANDROID_ROOT/features/feed/src/main/java/com/pixelle/feature/feed"
ensure_dir "$ANDROID_ROOT/core/network/src/main/java/com/pixelle/core/network"
ensure_dir "$ANDROID_ROOT/core/model/src/main/java/com/pixelle/core/model"

write_file "$ANDROID_ROOT/app/src/main/AndroidManifest.xml" "<manifest package=\"com.pixelle.app\">\n  <application android:label=\"Pixelle\"></application>\n</manifest>\n"

write_file "$ANDROID_ROOT/app/src/main/java/com/pixelle/app/MainActivity.kt" "package com.pixelle.app\n\nimport android.os.Bundle\nimport androidx.activity.ComponentActivity\nimport androidx.activity.compose.setContent\nimport androidx.compose.material3.Text\n\nclass MainActivity: ComponentActivity() {\n  override fun onCreate(savedInstanceState: Bundle?) {\n    super.onCreate(savedInstanceState)\n    setContent { Text(\"Pixelle Android placeholder\") }\n  }\n}\n"

write_file "$ANDROID_ROOT/core/model/src/main/java/com/pixelle/core/model/User.kt" "package com.pixelle.core.model\n\nimport java.util.UUID\n\ndata class User(val id: UUID = UUID.randomUUID(), val name: String, val username: String)\n"

write_file "$ANDROID_ROOT/core/network/src/main/java/com/pixelle/core/network/ApiClient.kt" "package com.pixelle.core.network\n\nobject ApiClient { suspend fun <T> fetch(path: String): Result<T> = Result.failure(Exception(\"not implemented\")) }\n"

write_file "$ANDROID_ROOT/features/feed/src/main/java/com/pixelle/feature/feed/FeedRepository.kt" "package com.pixelle.feature.feed\n\nclass FeedRepository { suspend fun loadFeed() = emptyList<Any>() }\n"

info "Android scaffold created"

###############################################################################
# Shared libs, docs and CI
###############################################################################
write_file "$ROOT_DIR/libs/design-tokens/README.md" "# Design tokens\nColors, spacing and tokens shared across apps (conceptual)."
write_file "$ROOT_DIR/packages/ui/README.md" "# UI package\nCross-platform UI component concepts."
write_file "$ROOT_DIR/ci/workflows/placeholder.yml" "# CI pipeline placeholder\n# Add real CI workflows here (GitHub Actions / Bitrise / CircleCI)\n"
write_file "$ROOT_DIR/tools/formatting/README.md" "# Formatting and linting\nPlace scripts to run swiftformat, swiftlint, ktlint, detekt, etc.\n"

write_file "$ROOT_DIR/scripts/bootstrap.sh" "#!/usr/bin/env bash\nset -euo pipefail\necho \"Bootstrapping repo - no-op placeholder\"\n"
chmod +x "$ROOT_DIR/scripts/bootstrap.sh" || true

info "Scaffold complete.\nNext steps:\n - Open $IOS_ROOT in Xcode (on macOS) as a Swift package or import to an Xcode project.\n - Open $ANDROID_ROOT in Android Studio and convert placeholders to real Gradle modules.\n - Customize libs, CI and tooling as needed."

exit 0
