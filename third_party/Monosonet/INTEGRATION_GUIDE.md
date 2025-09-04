# Quick Integration Guide

## Adding Post Detail Navigation

### Android

1. **Add to Navigation Graph**
```kotlin
// In your navigation graph
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

2. **Navigate from Post Rows**
```kotlin
// In your existing post row composable
Card(
    modifier = Modifier.clickable {
        navController.navigate("post_detail/${note.noteId}")
    }
) {
    // Your existing post content
}
```

### iOS

1. **Add Navigation Link**
```swift
// In your existing post row view
NavigationLink(destination: PostDetailView(noteId: note.noteId)) {
    // Your existing post row content
}
```

2. **Or Use Sheet Presentation**
```swift
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

## Required Dependencies

### Android
Make sure you have these in your `build.gradle`:
```gradle
implementation "androidx.compose.material3:material3:1.1.0"
implementation "androidx.compose.foundation:foundation:1.5.0"
implementation "io.coil-kt:coil-compose:2.4.0"
```

### iOS
The implementation uses standard SwiftUI components, no additional dependencies required.

## Testing the Implementation

1. **Build and run** both apps
2. **Navigate to any post** in your feed
3. **Tap on the post** to open the detail view
4. **Test reply functionality** by tapping reply buttons
5. **Test media interaction** by tapping on images/videos
6. **Verify navigation** works correctly

## Common Issues

### Android
- **Missing imports**: Make sure all imports are resolved
- **Navigation errors**: Verify navigation graph is properly configured
- **StateFlow issues**: Ensure ViewModel is properly scoped

### iOS
- **Missing SessionManager**: Create a simple SessionManager class if it doesn't exist
- **GRPC client issues**: Ensure SonetGRPCClient.shared is properly configured
- **Navigation problems**: Check that NavigationView is properly set up

## Next Steps

1. **Customize styling** to match your app's design
2. **Add analytics** for user engagement tracking
3. **Implement real-time updates** for live commenting
4. **Add push notifications** for mentions and replies
5. **Optimize performance** for large threads

## Support

For issues or questions:
1. Check the `COMMENTING_IMPLEMENTATION.md` for detailed documentation
2. Review the example code in the created files
3. Ensure all required server endpoints are working
4. Verify GRPC client configuration