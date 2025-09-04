# Sonet Commenting Implementation

This document outlines the implementation of commenting functionality for Sonet, both Android and iOS platforms, providing a full post view similar to Threads.

## Overview

The commenting system allows users to:
- View full posts with all details
- See threaded conversations (replies to replies)
- Reply to posts and comments
- Like, repost, and share posts
- Navigate through media content
- Access post actions (edit, delete, report, etc.)

## Architecture

### Server Side
The server already has the necessary API endpoints:
- `CreateReply` - Create replies to posts
- `GetThread` - Get threaded conversations
- `GetNote` - Get individual post details
- `LikeNote` - Like/unlike posts
- `RenoteNote` - Repost posts

### Client Side
Both Android and iOS clients implement:
- **PostDetailView** - Main view showing full post with comments
- **PostDetailCard** - Individual post/comment display
- **ReplyInput** - Input field for creating replies
- **PostDetailViewModel** - Business logic and state management

## Android Implementation

### Files Created
- `sonet-android/app/src/main/java/xyz/sonet/app/views/post/PostDetailView.kt`
- `sonet-android/app/src/main/java/xyz/sonet/app/views/post/PostDetailCard.kt`
- `sonet-android/app/src/main/java/xyz/sonet/app/views/post/ReplyInput.kt`
- `sonet-android/app/src/main/java/xyz/sonet/app/viewmodels/PostDetailViewModel.kt`

### Key Features
- **Material Design 3** components
- **Jetpack Compose** UI framework
- **StateFlow** for reactive state management
- **Coroutines** for asynchronous operations
- **Media carousel** with lightbox support
- **Thread visualization** with indented replies

### Integration Points
- Added missing GRPC methods to `SonetGRPCClient`:
  - `getNote()`
  - `getThread()`
  - `createReply()`
  - `likeNote()` (overloaded)
  - `renoteNote()`

## iOS Implementation

### Files Created
- `sonet-ios/Sonet/Views/Post/PostDetailView.swift`
- `sonet-ios/Sonet/Views/Post/PostDetailViewModel.swift`

### Key Features
- **SwiftUI** framework
- **Combine** for reactive programming
- **Async/await** for asynchronous operations
- **NavigationView** with custom toolbar
- **TabView** for media carousel
- **FocusState** for keyboard management

## UI Components

### PostDetailView
- **Top Bar**: Back button, "Thread" title, notifications, more options
- **Post Display**: Original post with full content and media
- **Thread Separator**: Visual divider between original and replies
- **Replies**: Indented comment threads
- **Reply Input**: Bottom input field for new replies

### PostDetailCard
- **User Header**: Avatar, name, handle, verification badge, timestamp, menu
- **Content**: Post text and media
- **Media Carousel**: Horizontal scrolling with page indicators
- **Action Buttons**: Reply, repost, like, share with counts
- **Reply Styling**: Indented appearance for replies

### ReplyInput
- **Reply Context**: Shows who you're replying to
- **Input Field**: Multi-line text input with placeholder
- **Send Button**: Disabled until text is entered
- **Keyboard Integration**: Auto-focus and send on return

### Media Handling
- **Carousel**: Horizontal scrolling through multiple media items
- **Lightbox**: Full-screen media viewer with navigation
- **Page Indicators**: Dots showing current media position
- **Placeholders**: Loading states for media content

## State Management

### PostDetailViewModel
- **Post State**: Current post being viewed
- **Thread State**: List of replies and nested comments
- **Loading States**: Loading indicators for operations
- **Error Handling**: User-friendly error messages
- **Reply Management**: Current reply target and input state

### Data Flow
1. **Load Post**: Fetch main post details
2. **Load Thread**: Fetch all replies and nested comments
3. **User Actions**: Handle likes, reposts, replies
4. **State Updates**: Update UI based on server responses
5. **Error Recovery**: Retry failed operations

## Navigation Integration

### Android
```kotlin
// Navigate to post detail
NavHostController.navigate("post_detail/$noteId")

// In navigation graph
composable(
    route = "post_detail/{noteId}",
    arguments = listOf(navArgument("noteId") { type = NavType.StringType })
) { backStackEntry ->
    val noteId = backStackEntry.arguments?.getString("noteId") ?: return@composable
    PostDetailView(
        noteId = noteId,
        sessionViewModel = sessionViewModel,
        themeViewModel = themeViewModel,
        onBackPressed = { navController.popBackStack() }
    )
}
```

### iOS
```swift
// Navigate to post detail
NavigationLink(destination: PostDetailView(noteId: note.noteId)) {
    // Post row content
}

// Or programmatically
@State private var showingPostDetail = false
@State private var selectedNoteId: String?

Button("View Post") {
    selectedNoteId = note.noteId
    showingPostDetail = true
}
.sheet(isPresented: $showingPostDetail) {
    if let noteId = selectedNoteId {
        PostDetailView(noteId: noteId)
    }
}
```

## Usage Examples

### Opening a Post
1. User taps on a post in feed/profile/search
2. App navigates to `PostDetailView`
3. View loads post content and thread
4. User can scroll through replies
5. User can reply to any post or comment

### Creating a Reply
1. User taps reply button on any post/comment
2. Reply input shows "Replying to @username"
3. User types reply content
4. User taps send button
5. Reply is posted and thread refreshes

### Media Interaction
1. User taps on media in post
2. Lightbox opens with full-screen view
3. User can swipe between multiple media items
4. Page indicators show current position
5. User can close lightbox to return to post

## Styling and Theming

### Android
- **Material Design 3** color scheme
- **Dynamic colors** support
- **Dark/light theme** adaptation
- **Elevation and shadows** for depth
- **Typography scale** for consistent text

### iOS
- **System colors** for light/dark mode
- **SF Symbols** for consistent icons
- **Dynamic Type** for accessibility
- **System materials** for visual effects
- **Adaptive layouts** for different screen sizes

## Performance Considerations

### Lazy Loading
- **LazyColumn** for Android (Jetpack Compose)
- **LazyVStack** for iOS (SwiftUI)
- **Pagination** for large threads
- **Image caching** with Coil (Android) and AsyncImage (iOS)

### State Optimization
- **StateFlow** for reactive updates
- **Compose recomposition** optimization
- **SwiftUI view updates** batching
- **Memory management** for media content

## Testing

### Unit Tests
- **ViewModel logic** testing
- **State management** validation
- **Error handling** scenarios
- **API integration** mocking

### UI Tests
- **Navigation flow** testing
- **User interaction** validation
- **Media display** verification
- **Accessibility** compliance

## Future Enhancements

### Planned Features
- **Real-time updates** for new replies
- **Push notifications** for mentions
- **Rich text** support in replies
- **Media replies** (images, videos)
- **Thread search** and filtering
- **Bookmarking** threads

### Technical Improvements
- **Offline support** for cached threads
- **Background sync** for new content
- **Analytics** for engagement metrics
- **A/B testing** for UI improvements
- **Performance monitoring** and optimization

## Conclusion

The commenting implementation provides a comprehensive solution for viewing and interacting with posts on Sonet. The design follows platform conventions while maintaining consistency across Android and iOS. The architecture is scalable and can easily accommodate future enhancements like real-time updates and advanced media features.

Both implementations leverage modern UI frameworks (Jetpack Compose and SwiftUI) and follow best practices for state management, error handling, and user experience. The integration with the existing server infrastructure ensures seamless operation with the current Sonet backend.