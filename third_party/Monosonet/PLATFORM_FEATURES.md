# 🚀 Sonet Platform-Specific Features

This document outlines the exclusive platform-specific features implemented for iOS and Android to provide native experiences that leverage each platform's unique capabilities.

## 🍎 iOS-Exclusive Sprinkles

### 1. Haptic Touch Feedback ✅

**Location**: `ios/Sonet/NativeApp/Utils/Haptics.swift`

**Features**:
- **Subtle vibration** when liking posts, long-pressing media, or dragging comments
- **Custom haptic patterns** for different interactions using Core Haptics (iOS 13+)
- **Fallback support** for older devices
- **Sonet-specific haptics**:
  - `likePost()` - Light impact for post interactions
  - `longPress()` - Medium impact for long-press gestures
  - `dragStart()` / `dragEnd()` - Light/medium impacts for drag operations
  - `commentInteraction()` - Light feedback for comment interactions
  - `mediaInteraction()` - Medium feedback for media interactions

**Usage**:
```swift
import Haptics

// Basic haptics
Haptics.light()
Haptics.medium()
Haptics.success()

// Sonet-specific haptics
Haptics.likePost()
Haptics.longPress()
Haptics.dragStart()
```

### 2. Siri Shortcuts Integration ✅

**Location**: `ios/Sonet/NativeApp/Utils/SiriShortcutsManager.swift`

**Features**:
- **Custom voice commands** like "Hey Siri, open my Sonet DMs"
- **Multiple shortcut types**:
  - Open DMs, Home, Profile, Notifications, Video Feed
  - Compose new post
- **Automatic shortcut suggestions** in iOS Settings
- **Deep linking** with custom URL schemes
- **Siri integration** for hands-free Sonet access

**Available Shortcuts**:
- "Hey Siri, open my Sonet DMs"
- "Hey Siri, open Sonet"
- "Hey Siri, post on Sonet"
- "Hey Siri, open Sonet notifications"
- "Hey Siri, open Sonet videos"

**Usage**:
```swift
let shortcutsManager = SiriShortcutsManager.shared
shortcutsManager.addShortcutToSiri(for: .openDMs)
shortcutsManager.suggestShortcuts()
```

### 3. App Clips ✅

**Location**: `ios/SonetClip/`

**Features**:
- **Lightweight Sonet preview** without full app installation
- **Interactive mini-feed** for non-users
- **Starter pack sharing** via App Clips
- **Seamless transition** to full app with App Store overlay
- **URL handling** for deep links

**Implementation**:
- WebView-based feed display
- App Store overlay integration
- Shared container for data persistence
- URL scheme handling for `bsky.app` and `go.bsky.app`

### 4. System-Level Share Sheet Integration ✅

**Location**: `ios/Share-with-Sonet/`

**Features**:
- **Deep custom options** for sharing to Sonet
- **Prefilled hashtags, location, and draft saving**
- **Media handling** for images, videos, and text
- **Multiple file support** (up to 4 images)
- **Custom URL schemes** for app integration

**Supported Share Types**:
- Images (PNG, JPG, JPEG, GIF, HEIC)
- Videos (MOV, MP4, M4V)
- Text content
- URLs with automatic type detection

### 5. Custom Camera Extensions ✅

**Location**: `ios/Sonet/NativeApp/Utils/CameraExtensionManager.swift`

**Features**:
- **Sonet camera inside iOS Photos** app
- **iMessage app extension** support
- **Direct posting** to Sonet from camera
- **Permission management** for camera and photo library
- **Shared container integration** for data transfer

**Usage**:
```swift
let cameraManager = CameraExtensionManager.shared
cameraManager.presentCamera(from: self) { image in
    if let image = image {
        cameraManager.postImageDirectlyToSonet(image, from: self)
    }
}
```

## 🤖 Android-Exclusive Sprinkles

### 1. Advanced Widgets ✅

**Location**: `android/app/src/main/java/xyz/sonet/app/widgets/`

**Features**:
- **Scrolling mini-feed widget** with real-time updates
- **Quick-compose text/video widget** for instant posting
- **Interactive feed items** with click handling
- **Refresh functionality** for content updates
- **Empty state handling** with loading views

**Widget Components**:
- `SonetWidgetProvider` - Main widget provider
- `SonetWidgetService` - Remote views service
- `SonetWidgetRemoteViewsFactory` - Data factory for widget items

**Widget Features**:
- Live feed updates
- Quick compose button
- Refresh button
- Clickable post items
- Progress indicators

### 2. Custom Intents & Share Targets ✅

**Location**: `android/app/src/main/java/xyz/sonet/app/services/CustomShareTargetService.kt`

**Features**:
- **Direct sharing to Sonet** from any other app
- **Multiple share targets**:
  - Share to Sonet Story
  - Share to Sonet Feed
  - Save as Draft
- **Rich media support** for images, videos, and text
- **Custom intent handling** for specialized sharing

**Share Actions**:
- `xyz.sonet.app.SHARE_TO_STORY`
- `xyz.sonet.app.SHARE_TO_FEED`
- `xyz.sonet.app.SAVE_DRAFT`

**Supported Content**:
- Single images/videos
- Multiple images
- Text content
- URLs and articles

### 3. Picture-in-Picture (PIP) ✅

**Location**: `android/app/src/main/java/xyz/sonet/app/utils/PictureInPictureManager.kt`

**Features**:
- **Keep watching Sonet videos** while scrolling elsewhere
- **Native Android PIP** that iOS can't match yet
- **Custom aspect ratios** for optimal viewing
- **Seamless transitions** with source rect hints
- **Auto-enter PIP** when user navigates away

**PIP Capabilities**:
- Video playback in floating window
- Custom aspect ratios (16:9, 4:3, etc.)
- Source rect hints for smooth transitions
- Auto-enter when leaving app
- Seamless resize support (Android 12+)

**Usage**:
```kotlin
val pipManager = PictureInPictureManager(context)
if (pipManager.isPipSupported()) {
    pipManager.enterPipMode(
        sourceRectHint = videoRect,
        aspectRatio = Rational(16, 9)
    )
}
```

### 4. Material You Theming ✅

**Location**: `android/app/src/main/java/xyz/sonet/app/theme/MaterialYouThemeManager.kt`

**Features**:
- **Sonet UI adapts to device wallpaper colors**
- **Very Android-core** theming approach
- **Dynamic color extraction** from wallpaper
- **Material 3 color scheme** generation
- **Fallback colors** for unsupported devices

**Theming Features**:
- Wallpaper color extraction
- Dynamic color scheme generation
- Material 3 color palette
- Contrast-aware color selection
- Fallback to default Material colors

**Usage**:
```kotlin
val themeManager = MaterialYouThemeManager(context)
if (themeManager.isMaterialYouSupported()) {
    val colors = themeManager.extractColorsFromWallpaper()
    // Apply colors to UI components
}
```

### 5. Background Uploads & Smart Downloads ✅

**Location**: `android/app/src/main/java/xyz/sonet/app/services/BackgroundUploadManager.kt`

**Features**:
- **Queue big video posts for Wi-Fi only**
- **Preload media in feed** for smoother offline experience
- **Network-aware uploads** with constraints
- **Battery optimization** for background tasks
- **Smart retry policies** with exponential backoff

**Upload Features**:
- Wi-Fi-only upload constraints
- Battery level monitoring
- Network connectivity monitoring
- Progress notifications
- Retry policies with backoff

**Download Features**:
- Smart content preloading
- Offline content caching
- Background download management
- Network type constraints
- Progress tracking

## 🔧 Integration & Setup

### iOS Setup

1. **Add to Info.plist**:
```xml
<key>NSUserActivityTypes</key>
<array>
    <string>open_dms</string>
    <string>compose_post</string>
    <string>open_profile</string>
</array>
```

2. **Import in SwiftUI views**:
```swift
import Haptics
import SiriShortcutsManager
import CameraExtensionManager
```

### Android Setup

1. **Add to AndroidManifest.xml**:
```xml
<service android:name=".services.SonetWidgetService" />
<service android:name=".services.CustomShareTargetService" />
<receiver android:name=".widgets.SonetWidgetProvider" />
```

2. **Add dependencies to build.gradle**:
```gradle
implementation 'androidx.work:work-runtime-ktx:2.8.1'
implementation 'androidx.palette:palette-ktx:1.0.0'
```

## 🎯 Benefits

### iOS Benefits
- **Premium feel** with haptic feedback
- **Apple ecosystem integration** with Siri Shortcuts
- **Lightweight sharing** via App Clips
- **Native camera integration**
- **System-level share sheet** customization

### Android Benefits
- **Widget ecosystem** for quick access
- **Advanced sharing** with custom intents
- **PIP video playback** for multitasking
- **Dynamic theming** with Material You
- **Background processing** for better UX

## 🚀 Future Enhancements

### iOS Roadmap
- [ ] Live Activities integration
- [ ] Dynamic Island support
- [ ] Apple Watch companion app
- [ ] CarPlay integration
- [ ] HomeKit shortcuts

### Android Roadmap
- [ ] Live tiles support
- [ ] Edge lighting integration
- [ ] One-handed mode optimization
- [ ] Foldable device support
- [ ] Android Auto integration

## 📱 Platform Comparison

| Feature | iOS | Android |
|---------|-----|---------|
| Haptic Feedback | ✅ Core Haptics | ⚠️ Basic vibration |
| Voice Commands | ✅ Siri Shortcuts | ⚠️ Google Assistant |
| App Clips | ✅ Native support | ❌ Not available |
| Share Sheet | ✅ Deep integration | ✅ Custom intents |
| Camera Extensions | ✅ Photos integration | ⚠️ Limited support |
| Widgets | ❌ Basic only | ✅ Advanced widgets |
| PIP Video | ❌ Not available | ✅ Native support |
| Dynamic Theming | ❌ Limited | ✅ Material You |
| Background Tasks | ❌ Restricted | ✅ WorkManager |

## 🔍 Testing

### iOS Testing
- Test haptics on different devices
- Verify Siri Shortcuts in Settings
- Test App Clips with different URLs
- Verify share sheet integration
- Test camera permissions

### Android Testing
- Test widgets on different launchers
- Verify custom share targets
- Test PIP with video playback
- Verify Material You theming
- Test background uploads

## 📚 Resources

- [iOS Haptics Documentation](https://developer.apple.com/design/human-interface-guidelines/haptics)
- [Siri Shortcuts Guide](https://developer.apple.com/siri/)
- [App Clips Overview](https://developer.apple.com/app-clips/)
- [Android Widgets Guide](https://developer.android.com/guide/topics/appwidgets)
- [Material You Design](https://m3.material.io/)
- [Android WorkManager](https://developer.android.com/topic/libraries/architecture/workmanager)

---

*This document is maintained by the Sonet development team. For questions or contributions, please refer to the project repository.*