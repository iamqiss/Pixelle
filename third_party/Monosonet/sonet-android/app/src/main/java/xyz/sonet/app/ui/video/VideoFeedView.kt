package xyz.sonet.app.ui.video

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
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewmodel.compose.viewModel
import androidx.compose.ui.viewinterop.AndroidView
import androidx.media3.common.MediaItem
import androidx.media3.exoplayer.ExoPlayer
import androidx.media3.ui.PlayerView
import coil.compose.AsyncImage
import coil.request.ImageRequest
import xyz.sonet.app.viewmodels.VideoFeedViewModel
import xyz.sonet.app.viewmodels.VideoItem
import xyz.sonet.app.viewmodels.VideoTab
import xyz.sonet.app.viewmodels.VideoMusic

@Composable
fun VideoFeedView(
    modifier: Modifier = Modifier,
    viewModel: VideoFeedViewModel = viewModel()
) {
    val videos by viewModel.videos.collectAsState()
    val currentVideoIndex by viewModel.currentVideoIndex.collectAsState()
    val selectedTab by viewModel.selectedTab.collectAsState()
    val isLoading by viewModel.isLoading.collectAsState()
    val error by viewModel.error.collectAsState()
    val isLiked by viewModel.isLiked.collectAsState()
    val isFollowing by viewModel.isFollowing.collectAsState()
    val currentVideo by viewModel.currentVideo.collectAsState()
    
    LaunchedEffect(Unit) {
        viewModel.loadVideos()
    }
    
    Box(
        modifier = modifier.fillMaxSize()
    ) {
        Column(
            modifier = Modifier.fillMaxSize()
        ) {
            // Top tabs
            VideoTabsView(
                selectedTab = selectedTab,
                onTabSelected = { viewModel.switchTab(it) }
            )
            
            // Video feed
            when {
                isLoading && videos.isEmpty() -> LoadingView()
                error != null -> ErrorView(
                    error = error!!,
                    onRetry = { viewModel.refreshVideos() }
                )
                videos.isEmpty() -> EmptyVideoView()
                else -> VideoFeedContent(
                    videos = videos,
                    currentVideoIndex = currentVideoIndex,
                    currentVideo = currentVideo,
                    isLiked = isLiked,
                    isFollowing = isFollowing,
                    onVideoChanged = { index ->
                        viewModel.selectVideo(index)
                    },
                    onLike = { viewModel.toggleLike() },
                    onFollow = { viewModel.toggleFollow() },
                    onComment = { viewModel.showComments() },
                    onShare = { viewModel.showShare() }
                )
            }
        }
    }
}

// MARK: - Video Tabs View
@Composable
fun VideoTabsView(
    selectedTab: VideoTab,
    onTabSelected: (VideoTab) -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.background)
            .padding(horizontal = 20.dp, vertical = 8.dp),
        horizontalArrangement = Arrangement.SpaceEvenly
    ) {
        VideoTab.allCases.forEach { tab ->
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                modifier = Modifier
                    .weight(1f)
                    .clickable { onTabSelected(tab) }
            ) {
                Text(
                    text = tab.displayName,
                    fontSize = 16.sp,
                    fontWeight = if (selectedTab == tab) FontWeight.SemiBold else FontWeight.Medium,
                    color = if (selectedTab == tab) MaterialTheme.colorScheme.onBackground else MaterialTheme.colorScheme.onBackground.copy(alpha = 0.7f)
                )
                
                Spacer(modifier = Modifier.height(4.dp))
                
                // Underline indicator
                Box(
                    modifier = Modifier
                        .height(2.dp)
                        .fillMaxWidth()
                        .background(
                            color = if (selectedTab == tab) MaterialTheme.colorScheme.onBackground else Color.Transparent,
                            shape = RoundedCornerShape(1.dp)
                        )
                )
            }
        }
    }
}

// MARK: - Video Feed Content
@Composable
fun VideoFeedContent(
    videos: List<VideoItem>,
    currentVideoIndex: Int,
    currentVideo: VideoItem?,
    isLiked: Boolean,
    isFollowing: Boolean,
    onVideoChanged: (Int) -> Void,
    onLike: () -> Unit,
    onFollow: () -> Unit,
    onComment: () -> Unit,
    onShare: () -> Unit
) {
    LazyColumn(
        modifier = Modifier.fillMaxSize(),
        contentPadding = PaddingValues(bottom = 80.dp) // Account for bottom tab bar
    ) {
        items(videos.size) { index ->
            VideoPlayerView(
                video = videos[index],
                isLiked = if (index == currentVideoIndex) isLiked else videos[index].isLiked,
                isFollowing = if (index == currentVideoIndex) isFollowing else videos[index].isFollowing,
                onLike = onLike,
                onFollow = onFollow,
                onComment = onComment,
                onShare = onShare
            )
        }
    }
}

// MARK: - Video Player View
@Composable
fun VideoPlayerView(
    video: VideoItem,
    isLiked: Boolean,
    isFollowing: Boolean,
    onLike: () -> Unit,
    onFollow: () -> Unit,
    onComment: () -> Unit,
    onShare: () -> Unit
) {
    var player by remember { mutableStateOf<ExoPlayer?>(null) }
    var isPlaying by remember { mutableStateOf(false) }
    
    val context = LocalContext.current
    
    DisposableEffect(video.id) {
        val exoPlayer = ExoPlayer.Builder(context).build().apply {
            val mediaItem = MediaItem.fromUri(video.videoUrl)
            setMediaItem(mediaItem)
            prepare()
            play()
            isPlaying = true
        }
        player = exoPlayer
        
        onDispose {
            exoPlayer.release()
            player = null
        }
    }
    
    Box(
        modifier = Modifier
            .fillMaxWidth()
            .height(600.dp)
            .background(MaterialTheme.colorScheme.background)
    ) {
        // Video player
        if (player != null) {
            AndroidView(
                factory = { context ->
                    PlayerView(context).apply {
                        this.player = player
                    }
                },
                modifier = Modifier.fillMaxSize()
            )
        } else {
            // Thumbnail placeholder
            AsyncImage(
                model = ImageRequest.Builder(LocalContext.current)
                    .data(video.thumbnailUrl)
                    .crossfade(true)
                    .build(),
                contentDescription = "Video thumbnail",
                modifier = Modifier.fillMaxSize(),
                contentScale = ContentScale.Crop
            )
        }
        
        // Video overlay content
        Column(
            modifier = Modifier
                .fillMaxSize()
                .padding(16.dp),
            verticalArrangement = Arrangement.SpaceBetween
        ) {
            // Top section - empty for now
            Spacer(modifier = Modifier.weight(1f))
            
            // Bottom section - Caption and actions
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.Bottom
            ) {
                // Left side - Caption and author
                Column(
                    modifier = Modifier.weight(1f),
                    verticalArrangement = Arrangement.spacedBy(12.dp)
                ) {
                    // Author info
                    Row(
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.spacedBy(12.dp)
                    ) {
                        AsyncImage(
                            model = ImageRequest.Builder(LocalContext.current)
                                .data(video.author.avatarUrl)
                                .crossfade(true)
                                .build(),
                            contentDescription = "Author avatar",
                            modifier = Modifier
                                .size(50.dp)
                                .clip(CircleShape),
                            contentScale = ContentScale.Crop
                        )
                        
                        Column {
                            Text(
                                text = video.author.displayName,
                                fontSize = 16.sp,
                                fontWeight = FontWeight.SemiBold,
                                color = MaterialTheme.colorScheme.onBackground
                            )
                            
                            Text(
                                text = "@${video.author.username}",
                                fontSize = 14.sp,
                                color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.8f)
                            )
                        }
                        
                        Spacer(modifier = Modifier.weight(1f))
                        
                        Button(
                            onClick = onFollow,
                            colors = ButtonDefaults.buttonColors(
                                containerColor = if (isFollowing) MaterialTheme.colorScheme.onBackground.copy(alpha = 0.2f) else MaterialTheme.colorScheme.onBackground
                            ),
                            shape = RoundedCornerShape(20.dp)
                        ) {
                            Text(
                                text = if (isFollowing) "Following" else "Follow",
                                color = if (isFollowing) MaterialTheme.colorScheme.onBackground else MaterialTheme.colorScheme.background,
                                fontSize = 14.sp,
                                fontWeight = FontWeight.SemiBold
                            )
                        }
                    }
                    
                    // Caption
                    Text(
                        text = video.caption,
                        fontSize = 16.sp,
                        color = MaterialTheme.colorScheme.onBackground,
                        maxLines = 3
                    )
                    
                    // Hashtags
                    if (video.hashtags.isNotEmpty()) {
                        LazyRow(
                            horizontalArrangement = Arrangement.spacedBy(8.dp)
                        ) {
                            items(video.hashtags.take(3)) { hashtag ->
                                Text(
                                    text = "#$hashtag",
                                    fontSize = 14.sp,
                                    color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.8f)
                                )
                            }
                        }
                    }
                    
                    // Music info
                    video.music?.let { music ->
                        Row(
                            verticalAlignment = Alignment.CenterVertically,
                            horizontalArrangement = Arrangement.spacedBy(8.dp)
                        ) {
                            Icon(
                                imageVector = Icons.Default.MusicNote,
                                contentDescription = "Music",
                                tint = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.8f),
                                modifier = Modifier.size(14.dp)
                            )
                            
                            Text(
                                text = "${music.title} - ${music.artist}",
                                fontSize = 14.sp,
                                color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.8f),
                                maxLines = 1
                            )
                        }
                    }
                }
                
                // Right side - Action buttons
                Column(
                    horizontalAlignment = Alignment.CenterHorizontally,
                    verticalArrangement = Arrangement.spacedBy(24.dp)
                ) {
                    // Like button
                    Column(
                        horizontalAlignment = Alignment.CenterHorizontally,
                        verticalArrangement = Arrangement.spacedBy(4.dp)
                    ) {
                        IconButton(
                            onClick = onLike,
                            modifier = Modifier.size(48.dp)
                        ) {
                            Icon(
                                imageVector = if (isLiked) xyz.sonet.app.ui.AppIcons.Like else xyz.sonet.app.ui.AppIcons.LikeBorder,
                                contentDescription = "Like",
                                tint = if (isLiked) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onBackground,
                                modifier = Modifier.size(28.dp)
                            )
                        }
                        
                        Text(
                            text = "${video.likeCount}",
                            fontSize = 12.sp,
                            fontWeight = FontWeight.Medium,
                            color = MaterialTheme.colorScheme.onBackground
                        )
                    }
                    
                    // Comment button
                    Column(
                        horizontalAlignment = Alignment.CenterHorizontally,
                        verticalArrangement = Arrangement.spacedBy(4.dp)
                    ) {
                        IconButton(
                            onClick = onComment,
                            modifier = Modifier.size(48.dp)
                        ) {
                            Icon(
                                imageVector = xyz.sonet.app.ui.AppIcons.Comment,
                                contentDescription = "Comment",
                                tint = MaterialTheme.colorScheme.onBackground,
                                modifier = Modifier.size(28.dp)
                            )
                        }
                        
                        Text(
                            text = "${video.commentCount}",
                            fontSize = 12.sp,
                            fontWeight = FontWeight.Medium,
                            color = MaterialTheme.colorScheme.onBackground
                        )
                    }
                    
                    // Share button
                    Column(
                        horizontalAlignment = Alignment.CenterHorizontally,
                        verticalArrangement = Arrangement.spacedBy(4.dp)
                    ) {
                        IconButton(
                            onClick = onShare,
                            modifier = Modifier.size(48.dp)
                        ) {
                            Icon(
                                imageVector = xyz.sonet.app.ui.AppIcons.Share,
                                contentDescription = "Share",
                                tint = MaterialTheme.colorScheme.onBackground,
                                modifier = Modifier.size(28.dp)
                            )
                        }
                        
                        Text(
                            text = "${video.shareCount}",
                            fontSize = 12.sp,
                            fontWeight = FontWeight.Medium,
                            color = MaterialTheme.colorScheme.onBackground
                        )
                    }
                    
                    // Play/Pause button
                    IconButton(
                        onClick = {
                            if (isPlaying) {
                                player?.pause()
                            } else {
                                player?.play()
                            }
                            isPlaying = !isPlaying
                        },
                        modifier = Modifier.size(48.dp)
                    ) {
                        Icon(
                            imageVector = if (isPlaying) Icons.Filled.PauseCircle else Icons.Filled.PlayCircle,
                            contentDescription = "Play/Pause",
                            tint = MaterialTheme.colorScheme.onBackground,
                            modifier = Modifier.size(28.dp)
                        )
                    }
                }
            }
        }
    }
}

// MARK: - Loading View
@Composable
fun LoadingView() {
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.background),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(20.dp)
        ) {
            CircularProgressIndicator(
                color = MaterialTheme.colorScheme.onBackground,
                modifier = Modifier.size(48.dp)
            )
            
            Text(
                text = "Loading videos...",
                fontSize = 16.sp,
                fontWeight = FontWeight.Medium,
                color = MaterialTheme.colorScheme.onBackground
            )
        }
    }
}

// MARK: - Error View
@Composable
fun ErrorView(
    error: String,
    onRetry: () -> Unit
) {
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.background),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(20.dp),
            modifier = Modifier.padding(horizontal = 32.dp)
        ) {
            Icon(
                imageVector = Icons.Default.Error,
                contentDescription = "Error",
                tint = MaterialTheme.colorScheme.onBackground,
                modifier = Modifier.size(48.dp)
            )
            
            Text(
                text = "Error",
                fontSize = 20.sp,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.onBackground
            )
            
            Text(
                text = error,
                fontSize = 16.sp,
                color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.8f),
                textAlign = TextAlign.Center
            )
            
            Button(
                onClick = onRetry,
                colors = ButtonDefaults.buttonColors(
                    containerColor = MaterialTheme.colorScheme.onBackground
                ),
                shape = RoundedCornerShape(20.dp)
            ) {
                Text(
                    text = "Try Again",
                    color = MaterialTheme.colorScheme.background,
                    fontSize = 14.sp,
                    fontWeight = FontWeight.SemiBold,
                    modifier = Modifier.padding(horizontal = 24.dp, vertical = 12.dp)
                )
            }
        }
    }
}

// MARK: - Empty Video View
@Composable
fun EmptyVideoView() {
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.background),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(20.dp),
            modifier = Modifier.padding(horizontal = 32.dp)
        ) {
            Icon(
                imageVector = Icons.Default.VideoLibrary,
                contentDescription = "No videos",
                tint = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.6f),
                modifier = Modifier.size(60.dp)
            )
            
            Text(
                text = "No videos yet",
                fontSize = 24.sp,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.onBackground
            )
            
            Text(
                text = "Videos will appear here based on your interests",
                fontSize = 16.sp,
                color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.8f),
                textAlign = TextAlign.Center
            )
        }
    }
}