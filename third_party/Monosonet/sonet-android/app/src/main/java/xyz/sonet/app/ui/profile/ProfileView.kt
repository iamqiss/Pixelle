package xyz.sonet.app.ui.profile

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInVertically
import androidx.compose.animation.slideOutVertically
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.scale
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewmodel.compose.viewModel
import coil.compose.AsyncImage
import coil.request.ImageRequest
import xyz.sonet.app.grpc.proto.*
import xyz.sonet.app.viewmodels.ProfileViewModel
import java.text.SimpleDateFormat
import java.util.*

@Composable
fun ProfileView(
    userId: String,
    isOwnProfile: Boolean,
    modifier: Modifier = Modifier,
    viewModel: ProfileViewModel = viewModel()
) {
    val context = LocalContext.current
    val userProfile by viewModel.userProfile.collectAsState()
    val selectedTab by viewModel.selectedTab.collectAsState()
    val posts by viewModel.posts.collectAsState()
    val replies by viewModel.replies.collectAsState()
    val media by viewModel.media.collectAsState()
    val likes by viewModel.likes.collectAsState()
    val isFollowing by viewModel.isFollowing.collectAsState()
    val isBlocked by viewModel.isBlocked.collectAsState()
    val isLoading by viewModel.isLoading.collectAsState()
    val error by viewModel.error.collectAsState()
    val authorCache by viewModel.authorCache.collectAsState()
    
    // Load profile when view is created
    LaunchedEffect(userId) {
        viewModel.loadProfile(userId)
    }
    
    Box(
        modifier = modifier.fillMaxSize()
    ) {
        LazyColumn {
            // Profile Header
            userProfile?.let { profile ->
                item {
                    ProfileHeader(
                        profile = profile,
                        isFollowing = isFollowing,
                        isBlocked = isBlocked,
                        isOwnProfile = isOwnProfile,
                        onFollowToggle = { viewModel.toggleFollow() },
                        onBlock = { viewModel.blockUser() },
                        onUnblock = { viewModel.unblockUser() }
                    )
                }
            }
            
            // Profile Tabs
            item {
                ProfileTabs(
                    selectedTab = selectedTab,
                    onTabSelected = { viewModel.selectTab(it) }
                )
            }
            
            // Tab Content
            item {
                val profile = userProfile
                if (profile != null) {
                    TabContent(
                        selectedTab = selectedTab,
                        posts = posts,
                        replies = replies,
                        media = media,
                        likes = likes,
                        authorProfile = profile,
                        authorCache = authorCache
                    )
                }
            }
        }
        
        // Loading and Error States
        when {
            isLoading -> LoadingView()
            error != null -> ErrorView(
                error = error!!,
                onRetry = { viewModel.refreshProfile() }
            )
        }
    }
}

// MARK: - Profile Header
@Composable
private fun ProfileHeader(
    profile: UserProfile,
    isFollowing: Boolean,
    isBlocked: Boolean,
    isOwnProfile: Boolean,
    onFollowToggle: () -> Unit,
    onBlock: () -> Unit,
    onUnblock: () -> Unit
) {
    Column(spacing = 0) {
        // Cover Photo
        CoverPhotoView(profile = profile)
        
        // Profile Info
        ProfileInfoView(
            profile = profile,
            isFollowing = isFollowing,
            isBlocked = isBlocked,
            isOwnProfile = isOwnProfile,
            onFollowToggle = onFollowToggle,
            onBlock = onBlock,
            onUnblock = onUnblock
        )
    }
}

// MARK: - Cover Photo
@Composable
private fun CoverPhotoView(profile: UserProfile) {
    Box(
        modifier = Modifier
            .fillMaxWidth()
            .height(200.dp)
    ) {
        // Cover photo or gradient
        if (profile.coverPhotoUrl.isNotEmpty()) {
            AsyncImage(
                model = ImageRequest.Builder(LocalContext.current)
                    .data(profile.coverPhotoUrl)
                    .crossfade(true)
                    .build(),
                contentDescription = "Cover photo",
                modifier = Modifier.fillMaxSize(),
                contentScale = ContentScale.Crop
            )
        } else {
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .background(
                        Brush.linearGradient(
                            colors = listOf(
                                MaterialTheme.colorScheme.primary,
                                MaterialTheme.colorScheme.tertiary
                            )
                        )
                    )
            )
        }
        
        // Avatar overlay
        Column(
            modifier = Modifier.fillMaxSize(),
            verticalArrangement = Arrangement.Bottom
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.Center
            ) {
                AsyncImage(
                    model = ImageRequest.Builder(LocalContext.current)
                        .data(profile.avatarUrl)
                        .crossfade(true)
                        .build(),
                    contentDescription = "Profile avatar",
                    modifier = Modifier
                        .size(80.dp)
                        .clip(CircleShape)
                        .background(
                            CircleShape,
                            MaterialTheme.colorScheme.surfaceVariant
                        ),
                    contentScale = ContentScale.Crop
                )
            }
        }
    }
}

// MARK: - Profile Info
@Composable
private fun ProfileInfoView(
    profile: UserProfile,
    isFollowing: Boolean,
    isBlocked: Boolean,
    isOwnProfile: Boolean,
    onFollowToggle: () -> Unit,
    onBlock: () -> Unit,
    onUnblock: () -> Unit
) {
    Column(
        modifier = Modifier.padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        // Action buttons
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.End
        ) {
            Row(
                horizontalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                if (isOwnProfile) {
                    Button(
                        onClick = { /* Navigate to edit profile */ },
                        colors = ButtonDefaults.buttonColors(
                            containerColor = MaterialTheme.colorScheme.primary
                        ),
                        shape = RoundedCornerShape(20.dp)
                    ) {
                        Text(
                            text = "Edit Profile",
                            fontSize = 15.sp,
                            fontWeight = FontWeight.SemiBold,
                            color = MaterialTheme.colorScheme.onPrimary
                        )
                    }
                } else {
                    // Message button
                    Surface(
                        onClick = { /* Navigate to messages */ },
                        shape = CircleShape,
                        color = MaterialTheme.colorScheme.surfaceVariant,
                        modifier = Modifier.size(36.dp)
                    ) {
                        Box(
                            contentAlignment = Alignment.Center
                        ) {
                            Icon(
                                imageVector = xyz.sonet.app.ui.AppIcons.Messages,
                                contentDescription = "Message",
                                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                                modifier = Modifier.size(20.dp)
                            )
                        }
                    }
                    
                    // Follow/Following button
                    Button(
                        onClick = onFollowToggle,
                        colors = ButtonDefaults.buttonColors(
                            containerColor = if (isFollowing) {
                                MaterialTheme.colorScheme.surfaceVariant
                            } else {
                                MaterialTheme.colorScheme.primary
                            }
                        ),
                        shape = RoundedCornerShape(20.dp)
                    ) {
                        Text(
                            text = if (isFollowing) "Following" else "Follow",
                            fontSize = 15.sp,
                            fontWeight = FontWeight.SemiBold,
                            color = if (isFollowing) {
                                MaterialTheme.colorScheme.onSurface
                            } else {
                                MaterialTheme.colorScheme.onPrimary
                            }
                        )
                    }
                }
            }
        }
        
        // User info
        Column(verticalArrangement = Arrangement.spacedBy(12.dp)) {
            // Name and verification
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(
                    text = profile.displayName,
                    fontSize = 24.sp,
                    fontWeight = FontWeight.Bold,
                    color = MaterialTheme.colorScheme.onSurface
                )
                
                if (profile.isVerified) {
                    Spacer(modifier = Modifier.width(8.dp))
                    Icon(
                        imageVector = xyz.sonet.app.ui.AppIcons.Verified,
                        contentDescription = "Verified",
                        tint = MaterialTheme.colorScheme.primary,
                        modifier = Modifier.size(20.dp)
                    )
                }
            }
            
            // Username
            Text(
                text = "@${profile.username}",
                fontSize = 16.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            
            // Bio
            if (profile.bio.isNotEmpty()) {
                Text(
                    text = profile.bio,
                    fontSize = 16.sp,
                    color = MaterialTheme.colorScheme.onSurface
                )
            }
            
            // Stats
            Row(
                horizontalArrangement = Arrangement.spacedBy(20.dp)
            ) {
                // Following
                Button(
                    onClick = { /* Navigate to following */ },
                    colors = ButtonDefaults.buttonColors(
                        containerColor = Color.Transparent
                    ),
                    modifier = Modifier.padding(0.dp)
                ) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Text(
                            text = profile.followingCount.toString(),
                            fontSize = 16.sp,
                            fontWeight = FontWeight.SemiBold,
                            color = MaterialTheme.colorScheme.onSurface
                        )
                        
                        Spacer(modifier = Modifier.width(4.dp))
                        
                        Text(
                            text = "Following",
                            fontSize = 16.sp,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
                
                // Followers
                Button(
                    onClick = { /* Navigate to followers */ },
                    colors = ButtonDefaults.buttonColors(
                        containerColor = Color.Transparent
                    ),
                    modifier = Modifier.padding(0.dp)
                ) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Text(
                            text = profile.followerCount.toString(),
                            fontSize = 16.sp,
                            fontWeight = FontWeight.SemiBold,
                            color = MaterialTheme.colorScheme.onSurface
                        )
                        
                        Spacer(modifier = Modifier.width(4.dp))
                        
                        Text(
                            text = "Followers",
                            fontSize = 16.sp,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
            }
            
            // Additional info
            if (profile.location.isNotEmpty() || profile.website.isNotEmpty()) {
                Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    if (profile.location.isNotEmpty()) {
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Icon(
                                imageVector = Icons.Default.LocationOn,
                                contentDescription = null,
                                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                                modifier = Modifier.size(16.dp)
                            )
                            
                            Spacer(modifier = Modifier.width(8.dp))
                            
                            Text(
                                text = profile.location,
                                fontSize = 14.sp,
                                color = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                    }
                    
                    if (profile.website.isNotEmpty()) {
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Icon(
                                imageVector = Icons.Default.Link,
                                contentDescription = null,
                                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                                modifier = Modifier.size(16.dp)
                            )
                            
                            Spacer(modifier = Modifier.width(8.dp))
                            
                            Text(
                                text = profile.website,
                                fontSize = 14.sp,
                                color = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                    }
                }
            }
            
            // Joined date
            Row(verticalAlignment = Alignment.CenterVertically) {
                Icon(
                    imageVector = Icons.Default.CalendarToday,
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(16.dp)
                )
                
                Spacer(modifier = Modifier.width(8.dp))
                
                Text(
                    text = "Joined ${formatDate(profile.createdAt.seconds * 1000)}",
                    fontSize = 14.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
    }
}

// MARK: - Profile Tabs
@Composable
private fun ProfileTabs(
    selectedTab: ProfileViewModel.ProfileTab,
    onTabSelected: (ProfileViewModel.ProfileTab) -> Unit
) {
    Column {
        // Tab buttons
        Row(
            modifier = Modifier.fillMaxWidth()
        ) {
            ProfileViewModel.ProfileTab.values().forEach { tab ->
                ProfileTabButton(
                    tab = tab,
                    isSelected = selectedTab == tab,
                    onTap = { onTabSelected(tab) },
                    modifier = Modifier.weight(1f)
                )
            }
        }
        
        // Tab indicator
        Box(
            modifier = Modifier
                .fillMaxWidth()
                .height(2.dp)
                .background(MaterialTheme.colorScheme.primary)
        )
    }
}

// MARK: - Profile Tab Button
@Composable
private fun ProfileTabButton(
    tab: ProfileViewModel.ProfileTab,
    isSelected: Boolean,
    onTap: () -> Unit,
    modifier: Modifier = Modifier
) {
    Button(
        onClick = onTap,
        colors = ButtonDefaults.buttonColors(
            containerColor = Color.Transparent
        ),
        modifier = modifier.padding(vertical = 12.dp)
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Icon(
                imageVector = getIconForTab(tab.icon),
                contentDescription = null,
                tint = if (isSelected) {
                    MaterialTheme.colorScheme.primary
                } else {
                    MaterialTheme.colorScheme.onSurfaceVariant
                },
                modifier = Modifier.size(20.dp)
            )
            
            Text(
                text = tab.title,
                fontSize = 14.sp,
                fontWeight = FontWeight.Medium,
                color = if (isSelected) {
                    MaterialTheme.colorScheme.primary
                } else {
                    MaterialTheme.colorScheme.onSurfaceVariant
                }
            )
        }
    }
}

// MARK: - Tab Content
@Composable
private fun TabContent(
    selectedTab: ProfileViewModel.ProfileTab,
    posts: List<Note>,
    replies: List<Note>,
    media: List<Note>,
    likes: List<Note>,
    authorProfile: UserProfile,
    authorCache: Map<String, UserProfile>
) {
    when (selectedTab) {
        ProfileViewModel.ProfileTab.POSTS -> ProfilePostsView(posts = posts, authorCache = authorCache)
        ProfileViewModel.ProfileTab.REPLIES -> ProfileRepliesView(replies = replies, authorCache = authorCache)
        ProfileViewModel.ProfileTab.MEDIA -> ProfileMediaView(media = media, authorCache = authorCache)
        ProfileViewModel.ProfileTab.LIKES -> ProfileLikesView(likes = likes, authorCache = authorCache)
    }
}

// MARK: - Profile Posts View
@Composable
private fun ProfilePostsView(posts: List<Note>, authorCache: Map<String, UserProfile>) {
    if (posts.isEmpty()) {
        EmptyStateView(
            icon = "bubble_left",
            title = "No posts yet",
            message = "When User posts, they'll show up here."
        )
    } else {
        LazyColumn {
            items(posts) { post ->
                ProfileNoteRow(note = post, authorCache = authorCache)
            }
        }
    }
}

// MARK: - Profile Replies View
@Composable
private fun ProfileRepliesView(replies: List<Note>, authorCache: Map<String, UserProfile>) {
    if (replies.isEmpty()) {
        EmptyStateView(
            icon = "arrowshape_turn_up_left",
            title = "No replies yet",
            message = "When User replies, they'll show up here."
        )
    } else {
        LazyColumn {
            items(replies) { reply ->
                ProfileNoteRow(note = reply, authorCache = authorCache)
            }
        }
    }
}

// MARK: - Profile Media View
@Composable
private fun ProfileMediaView(media: List<Note>, authorCache: Map<String, UserProfile>) {
    if (media.isEmpty()) {
        EmptyStateView(
            icon = "photo",
            title = "No media yet",
            message = "When User posts photos or videos, they'll show up here."
        )
    } else {
        LazyColumn {
            items(media) { mediaNote ->
                ProfileNoteRow(note = mediaNote, authorCache = authorCache)
            }
        }
    }
}

// MARK: - Profile Likes View
@Composable
private fun ProfileLikesView(likes: List<Note>, authorCache: Map<String, UserProfile>) {
    if (likes.isEmpty()) {
        EmptyStateView(
            icon = "heart",
            title = "No likes yet",
            message = "Likes from User will show up here."
        )
    } else {
        LazyColumn {
            items(likes) { likedNote ->
                ProfileNoteRow(note = likedNote, authorCache = authorCache)
            }
        }
    }
}

// MARK: - Profile Note Row
@Composable
private fun ProfileNoteRow(note: Note, authorCache: Map<String, UserProfile>) {
    var isPressed by remember { mutableStateOf(false) }
    var isLiked by remember { mutableStateOf(note.engagement.isLiked) }
    var isReposted by remember { mutableStateOf(note.engagement.isReposted) }
    
    val scale by animateFloatAsState(
        targetValue = if (isPressed) 0.98f else 1f,
        animationSpec = tween(100)
    )
    
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .scale(scale)
            .clickable { /* Navigate to note detail */ }
            .padding(horizontal = 16.dp, vertical = 12.dp)
    ) {
        // Header
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier.fillMaxWidth()
        ) {
            // Author avatar
            val author = authorCache[note.authorId]
            if (author?.avatarUrl?.isNotEmpty() == true) {
                AsyncImage(
                    model = ImageRequest.Builder(LocalContext.current)
                        .data(author.avatarUrl)
                        .crossfade(true)
                        .build(),
                    contentDescription = "Author avatar",
                    modifier = Modifier
                        .size(32.dp)
                        .clip(CircleShape),
                    contentScale = ContentScale.Crop,
                    error = {
                        Surface(shape = CircleShape, color = MaterialTheme.colorScheme.surfaceVariant, modifier = Modifier.size(32.dp)) {}
                    }
                )
            } else {
                Surface(
                    shape = CircleShape,
                    color = MaterialTheme.colorScheme.surfaceVariant,
                    modifier = Modifier.size(32.dp)
                ) {
                    Box(contentAlignment = Alignment.Center) {
                        Icon(
                            imageVector = xyz.sonet.app.ui.AppIcons.Profile,
                            contentDescription = null,
                            tint = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.size(16.dp)
                        )
                    }
                }
            }
            
            Spacer(modifier = Modifier.width(12.dp))
            
            // Author info
            Column {
                val authorName = author?.displayName ?: author?.username ?: note.authorId
                Text(
                    text = authorName,
                    fontSize = 14.sp,
                    fontWeight = FontWeight.SemiBold,
                    color = MaterialTheme.colorScheme.onSurface
                )
                
                Text(
                    text = "2h", // This would be calculated from note.createdAt
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            Spacer(modifier = Modifier.weight(1f))
            
            // More options
            IconButton(onClick = { /* Show more options */ }) {
                Icon(
                    imageVector = xyz.sonet.app.ui.AppIcons.More,
                    contentDescription = "More options",
                    tint = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        
        // Content
        Text(
            text = note.content,
            fontSize = 15.sp,
            color = MaterialTheme.colorScheme.onSurface,
            maxLines = 3,
            overflow = TextOverflow.Ellipsis,
            modifier = Modifier.padding(top = 12.dp)
        )
        
        // Media preview
        if (note.mediaCount > 0) {
            MediaPreviewView(media = note.mediaList)
        }
        
        // Engagement
        Row(
            horizontalArrangement = Arrangement.spacedBy(20.dp),
            modifier = Modifier.padding(top = 12.dp)
        ) {
            // Like button
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier.clickable { isLiked = !isLiked }
            ) {
                Icon(
                    imageVector = if (isLiked) Icons.Default.Favorite else Icons.Default.FavoriteBorder,
                    contentDescription = "Like",
                    tint = if (isLiked) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(18.dp)
                )
                
                Spacer(modifier = Modifier.width(6.dp))
                
                Text(
                    text = note.engagement.likeCount.toString(),
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            // Reply button
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier.clickable { /* Navigate to reply */ }
            ) {
                Icon(
                    imageVector = xyz.sonet.app.ui.AppIcons.Comment,
                    contentDescription = "Reply",
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(18.dp)
                )
                
                Spacer(modifier = Modifier.width(6.dp))
                
                Text(
                    text = note.engagement.replyCount.toString(),
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            // Repost button
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier.clickable { isReposted = !isReposted }
            ) {
                Icon(
                    imageVector = if (isReposted) Icons.Default.Reply else Icons.Default.Reply,
                    contentDescription = "Repost",
                    tint = if (isReposted) MaterialTheme.colorScheme.tertiary else MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(18.dp)
                )
                
                Spacer(modifier = Modifier.width(6.dp))
                
                Text(
                    text = note.engagement.repostCount.toString(),
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            Spacer(modifier = Modifier.weight(1f))
            
            // Share button
            IconButton(onClick = { /* Share note */ }) {
                Icon(
                    imageVector = xyz.sonet.app.ui.AppIcons.Share,
                    contentDescription = "Share",
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(18.dp)
                )
            }
        }
    }
}

// MARK: - Media Preview View
@Composable
private fun MediaPreviewView(media: List<MediaItem>) {
    LazyRow(
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        contentPadding = PaddingValues(horizontal = 16.dp)
    ) {
        items(media.take(3)) { mediaItem ->
            AsyncImage(
                model = ImageRequest.Builder(LocalContext.current)
                    .data(mediaItem.url)
                    .crossfade(true)
                    .build(),
                contentDescription = "Media preview",
                modifier = Modifier
                    .size(80.dp)
                    .clip(RoundedCornerShape(8.dp)),
                contentScale = ContentScale.Crop
            )
        }
    }
}

// MARK: - Empty State View
@Composable
private fun EmptyStateView(
    icon: String,
    title: String,
    message: String
) {
    Column(
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
        modifier = Modifier
            .fillMaxSize()
            .padding(32.dp)
    ) {
        Icon(
            imageVector = getIconForEmptyState(icon),
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(48.dp)
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Text(
            text = title,
            fontSize = 20.sp,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colorScheme.onSurface
        )
        
        Spacer(modifier = Modifier.height(8.dp))
        
        Text(
            text = message,
            fontSize = 16.sp,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            textAlign = TextAlign.Center
        )
    }
}

// MARK: - Loading View
@Composable
private fun LoadingView() {
    Column(
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
        modifier = Modifier.fillMaxSize()
    ) {
        CircularProgressIndicator(
            modifier = Modifier.size(48.dp),
            color = MaterialTheme.colorScheme.primary
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Text(
            text = "Loading profile...",
            fontSize = 16.sp,
            fontWeight = FontWeight.Medium,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
    }
}

// MARK: - Error View
@Composable
private fun ErrorView(
    error: String,
    onRetry: () -> Unit
) {
    Column(
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
        modifier = Modifier.fillMaxSize()
    ) {
        Icon(
            imageVector = Icons.Default.Warning,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.tertiary,
            modifier = Modifier.size(48.dp)
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Text(
            text = "Profile Error",
            fontSize = 20.sp,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colorScheme.onSurface
        )
        
        Spacer(modifier = Modifier.height(8.dp))
        
        Text(
            text = error,
            fontSize = 16.sp,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            textAlign = TextAlign.Center,
            modifier = Modifier.padding(horizontal = 32.dp)
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Button(onClick = onRetry) {
            Text("Try Again")
        }
    }
}

// MARK: - Helper Functions
private fun getIconForTab(iconName: String): ImageVector {
    return when (iconName) {
        "bubble_left" -> Icons.Default.ChatBubbleOutline
        "arrowshape_turn_up_left" -> Icons.Default.Reply
        "photo" -> Icons.Default.Photo
        "heart" -> Icons.Default.Favorite
        else -> Icons.Default.ChatBubbleOutline
    }
}

private fun getIconForEmptyState(iconName: String): ImageVector {
    return when (iconName) {
        "bubble_left" -> Icons.Default.ChatBubbleOutline
        "arrowshape_turn_up_left" -> Icons.Default.Reply
        "photo" -> Icons.Default.Photo
        "heart" -> Icons.Default.Favorite
        else -> Icons.Default.ChatBubbleOutline
    }
}

private fun formatDate(timestamp: Long): String {
    val date = Date(timestamp)
    val formatter = SimpleDateFormat("MMM yyyy", Locale.getDefault())
    return formatter.format(date)
}