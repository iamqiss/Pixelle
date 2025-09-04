package xyz.sonet.app.views.home

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.pager.HorizontalPager
import androidx.compose.foundation.pager.PagerState
import androidx.compose.foundation.pager.rememberPagerState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import xyz.sonet.app.viewmodels.SessionViewModel
import xyz.sonet.app.viewmodels.ThemeViewModel
import xyz.sonet.app.viewmodels.HomeViewModel
import androidx.compose.foundation.Image
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import coil.compose.rememberAsyncImagePainter
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.draw.background
import androidx.compose.foundation.clickable
import androidx.compose.ui.window.Dialog
import androidx.compose.runtime.DisposableEffect
import com.google.android.exoplayer2.ExoPlayer
import com.google.android.exoplayer2.MediaItem as ExoMediaItem
import androidx.compose.ui.viewinterop.AndroidView
import com.google.android.exoplayer2.ui.PlayerView
import xyz.sonet.app.grpc.SonetGRPCClient
import xyz.sonet.app.ui.AppIcons

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun HomeTabView(
    sessionViewModel: SessionViewModel,
    themeViewModel: ThemeViewModel
) {
    val homeViewModel = androidx.lifecycle.viewmodel.compose.viewModel<HomeViewModel>()
    val currentUser by sessionViewModel.currentUser.collectAsState()
    
    Column(
        modifier = Modifier.fillMaxSize()
    ) {
        // Custom Navigation Bar
        HomeNavigationBar(
            selectedFeed = homeViewModel.selectedFeed,
            onFeedChanged = { feed ->
                homeViewModel.selectFeed(feed)
            }
        )
        
        // Feed Content
        when {
            homeViewModel.isLoading -> {
                LoadingView()
            }
            homeViewModel.error != null -> {
                ErrorView(
                    error = homeViewModel.error!!,
                    onRetry = {
                        homeViewModel.refreshFeed()
                    }
                )
            }
            else -> {
                FeedView(
                    feed = homeViewModel.selectedFeed,
                    notes = homeViewModel.notes,
                    onRefresh = {
                        homeViewModel.refreshFeed()
                    },
                    onLoadMore = {
                        homeViewModel.loadMoreNotes()
                    },
                    currentUserId = currentUser?.id
                )
            }
        }
    }
    
    LaunchedEffect(Unit) {
        homeViewModel.loadFeed()
    }
}

@Composable
fun HomeNavigationBar(
    selectedFeed: FeedType,
    onFeedChanged: (FeedType) -> Unit
) {
    Column(
        modifier = Modifier.fillMaxWidth()
    ) {
        // Main Header
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp, vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Home",
                style = MaterialTheme.typography.headlineLarge,
                fontWeight = FontWeight.Bold
            )
            
            Spacer(modifier = Modifier.weight(1f))
            
            IconButton(onClick = { /* Open composer */ }) { Icon(imageVector = AppIcons.Add, contentDescription = "Compose", tint = MaterialTheme.colorScheme.primary) }
        }
        
        // Feed Type Selector
        LazyRow(
            contentPadding = PaddingValues(horizontal = 16.dp, vertical = 8.dp),
            horizontalArrangement = Arrangement.spacedBy(16.dp)
        ) {
            items(FeedType.values()) { feedType ->
                FeedTypeButton(
                    feedType = feedType,
                    isSelected = selectedFeed == feedType,
                    onClick = {
                        onFeedChanged(feedType)
                    }
                )
            }
        }
        
        Divider()
    }
}

@Composable
fun FeedTypeButton(
    feedType: FeedType,
    isSelected: Boolean,
    onClick: () -> Unit
) {
    Button(
        onClick = onClick,
        colors = ButtonDefaults.buttonColors(
            containerColor = if (isSelected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.surface,
            contentColor = if (isSelected) MaterialTheme.colorScheme.onPrimary else MaterialTheme.colorScheme.onSurface
        ),
        shape = MaterialTheme.shapes.medium
    ) {
        Text(
            text = feedType.displayName,
            style = MaterialTheme.typography.bodyMedium,
            fontWeight = if (isSelected) FontWeight.Semibold else FontWeight.Medium
        )
    }
}

@Composable
fun FeedView(
    feed: FeedType,
    notes: List<SonetNote>,
    onRefresh: () -> Unit,
    onLoadMore: () -> Unit,
    currentUserId: String?
) {
    LazyColumn(
        contentPadding = PaddingValues(vertical = 16.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        items(notes) { note ->
            NoteCard(note = note, currentUserId = currentUserId)
        }
        
        // Load More Button
        if (notes.isNotEmpty()) {
            item {
                Button(
                    onClick = onLoadMore,
                    modifier = Modifier.fillMaxWidth()
                ) {
                    Text("Load More")
                }
            }
        }
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun NoteCard(note: SonetNote, currentUserId: String?) {
    var isLiked by remember { mutableStateOf(false) }
    var isReposted by remember { mutableStateOf(false) }
    
    Card(
        modifier = Modifier.fillMaxWidth(),
        elevation = CardDefaults.cardElevation(defaultElevation = 2.dp)
    ) {
        Column(
            modifier = Modifier.padding(vertical = 12.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            // User Header
            Row(
                verticalAlignment = Alignment.CenterVertically
            ) {
                // Avatar placeholder
                Surface(
                    modifier = Modifier.size(40.dp),
                    shape = MaterialTheme.shapes.circular,
                    color = MaterialTheme.colorScheme.surfaceVariant
                ) {
                    Icon(
                        imageVector = Icons.Default.Person,
                        contentDescription = null,
                        modifier = Modifier.padding(8.dp)
                    )
                }
                
                Spacer(modifier = Modifier.width(12.dp))
                
                Column {
                    Row(
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(
                            text = note.author.displayName,
                            style = MaterialTheme.typography.bodyMedium,
                            fontWeight = FontWeight.Semibold
                        )
                        
                        if (note.author.isVerified) {
                            Spacer(modifier = Modifier.width(4.dp))
                            Icon(imageVector = AppIcons.Verified, contentDescription = "Verified", tint = MaterialTheme.colorScheme.primary, modifier = Modifier.size(16.dp))
                        }
                    }
                    
                    Text(
                        text = note.author.handle,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                
                Spacer(modifier = Modifier.weight(1f))
                
                Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text(
                        text = note.timeAgo,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    var showMenu by remember { mutableStateOf(false) }
                    Box {
                        IconButton(onClick = { showMenu = true }) {
                            Icon(imageVector = AppIcons.More, contentDescription = "More options", tint = MaterialTheme.colorScheme.onSurfaceVariant)
                        }
                        DropdownMenu(expanded = showMenu, onDismissRequest = { showMenu = false }) {
                            val isOwn = currentUserId != null && currentUserId == note.author.id
                            if (isOwn) {
                                DropdownMenuItem(text = { Text("Edit") }, onClick = { showMenu = false })
                                DropdownMenuItem(text = { Text("Delete", color = MaterialTheme.colorScheme.error) }, onClick = { showMenu = false })
                            } else {
                                DropdownMenuItem(text = { Text("Mute @${note.author.username}") }, onClick = { showMenu = false })
                                DropdownMenuItem(text = { Text("Block @${note.author.username}", color = MaterialTheme.colorScheme.error) }, onClick = { showMenu = false })
                                DropdownMenuItem(text = { Text("Report", color = MaterialTheme.colorScheme.error) }, onClick = { showMenu = false })
                            }
                        }
                    }
                }
            }
            
            // Note Content
            if (note.text.isNotEmpty()) {
                Text(
                    text = note.text,
                    style = MaterialTheme.typography.bodyLarge
                )
            }
            
            // Media carousel (full-bleed)
            if (note.media != null && note.media.isNotEmpty()) {
                var showLightbox by remember { mutableStateOf(false) }
                var startIndex by remember { mutableStateOf(0) }
                val mediaList = note.media
                MediaCarousel(media = mediaList!!) { tappedIndex ->
                    startIndex = tappedIndex
                    showLightbox = true
                }
                if (showLightbox) {
                    Dialog(onDismissRequest = { showLightbox = false }) {
                        Box(modifier = Modifier.fillMaxSize()) {
                            MediaLightbox(media = mediaList, startIndex = startIndex) { showLightbox = false }
                        }
                    }
                }
            }
            
            // Action Buttons
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 12.dp),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                // Reply Button
                IconButton(onClick = { /* Handle reply */ }) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Icon(imageVector = AppIcons.Comment, contentDescription = "Reply")
                        Spacer(modifier = Modifier.width(4.dp))
                        Text("${note.replyCount}")
                    }
                }
                
                // Repost Button
                IconButton(
                    onClick = { isReposted = !isReposted }
                ) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Icon(imageVector = AppIcons.Reply, contentDescription = "Repost", tint = if (isReposted) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant)
                        Spacer(modifier = Modifier.width(4.dp))
                        Text("${note.repostCount}")
                    }
                }
                
                // Like Button
                IconButton(
                    onClick = { isLiked = !isLiked }
                ) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Icon(imageVector = if (isLiked) AppIcons.Like else AppIcons.LikeBorder, contentDescription = "Like", tint = if (isLiked) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onSurfaceVariant)
                        Spacer(modifier = Modifier.width(4.dp))
                        Text("${note.likeCount}")
                    }
                }
                
                // Share Button
                IconButton(onClick = { /* Handle share */ }) {
                    Icon(imageVector = AppIcons.Share, contentDescription = "Share")
                }
            }
        }
    }
}

@Composable
fun LoadingView() {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(16.dp)
        ) {
            CircularProgressIndicator(
                modifier = Modifier.size(48.dp)
            )
            
            Text(
                text = "Loading feed...",
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

@Composable
fun ErrorView(
    error: Throwable,
    onRetry: () -> Unit
) {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(16.dp)
        ) {
            Icon(
                imageVector = Icons.Default.Error,
                contentDescription = null,
                modifier = Modifier.size(48.dp),
                tint = MaterialTheme.colorScheme.error
            )
            
            Text(
                text = "Something went wrong",
                style = MaterialTheme.typography.headlineSmall
            )
            
            Text(
                text = error.message ?: "An unknown error occurred",
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                textAlign = TextAlign.Center
            )
            
            Button(onClick = onRetry) {
                Text("Try Again")
            }
        }
    }
}

// Data models (these would typically be in separate files)
enum class FeedType(val displayName: String) {
    FOLLOWING("Following"),
    FOR_YOU("For You"),
    TRENDING("Trending"),
    LATEST("Latest")
}

data class SonetNote(
    val id: String,
    val text: String,
    val author: SonetUser,
    val createdAt: Long,
    val likeCount: Int,
    val repostCount: Int,
    val replyCount: Int,
    val media: List<MediaItem>?,
    var isLiked: Boolean,
    var isReposted: Boolean
) {
    val timeAgo: String
        get() = "2h ago" // Simplified for now
}

data class SonetUser(
    val id: String,
    val username: String,
    val displayName: String,
    val avatarUrl: String?,
    val isVerified: Boolean,
    val createdAt: Long
) {
    val handle: String
        get() = "@$username"
}

data class MediaItem(
    val id: String,
    val url: String,
    val type: MediaType,
    val altText: String?
)

enum class MediaType {
    IMAGE, VIDEO, GIF
}

@Composable
private fun MediaCarousel(media: List<MediaItem>, onOpen: (Int) -> Unit = {}) {
    val pagerState = rememberPagerState(initialPage = 0, pageCount = { media.size })
    Column(modifier = Modifier.fillMaxWidth()) {
        HorizontalPager(
            state = pagerState,
            modifier = Modifier
                .fillMaxWidth()
                .height(300.dp)
        ) { page ->
            val item = media[page]
            Box(modifier = Modifier.fillMaxSize().clickable { onOpen(page) }) {
                when (item.type) {
                    MediaType.VIDEO -> VideoPage(url = item.url)
                    else -> Image(
                        painter = rememberAsyncImagePainter(item.url),
                        contentDescription = item.altText,
                        contentScale = ContentScale.Crop,
                        modifier = Modifier.fillMaxSize()
                    )
                }
            }
        }

        // Indicator aligned to media left edge
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            repeat(media.size) { index ->
                val isActive = pagerState.currentPage == index
                Box(
                    modifier = Modifier
                        .size(6.dp)
                        .background(
                            color = if (isActive) MaterialTheme.colorScheme.onSurface else MaterialTheme.colorScheme.onSurface.copy(alpha = 0.4f),
                            shape = MaterialTheme.shapes.small
                        )
                )
                if (index != media.lastIndex) Spacer(modifier = Modifier.width(6.dp))
            }
            Spacer(modifier = Modifier.weight(1f))
        }
    }
}

@Composable
private fun VideoPage(url: String, onClick: () -> Unit = {}) {
    val context = LocalContext.current
    val exoPlayer = remember(url) {
        ExoPlayer.Builder(context).build().apply {
            repeatMode = ExoPlayer.REPEAT_MODE_ALL
            setMediaItem(ExoMediaItem.fromUri(url))
            prepare()
            playWhenReady = true
            volume = 0f
        }
    }

    DisposableEffect(exoPlayer) {
        onDispose {
            exoPlayer.release()
        }
    }

    AndroidView(
        modifier = Modifier
            .fillMaxSize(),
        factory = { ctx ->
            PlayerView(ctx).apply {
                useController = false
                player = exoPlayer
            }
        },
        update = { view ->
            // Tap to toggle mute/unmute
            view.setOnClickListener {
                val muted = exoPlayer.volume == 0f
                exoPlayer.volume = if (muted) 1f else 0f
            }
        }
    )
}

// Full-screen lightbox
@Composable
private fun MediaLightbox(
    media: List<MediaItem>,
    startIndex: Int,
    onDismiss: () -> Unit
) {
    var index by remember { mutableStateOf(startIndex) }
    val likeState = remember { mutableStateMapOf<String, Pair<Boolean, Int>>() }
    LaunchedEffect(media) {
        if (likeState.isEmpty()) media.forEach { likeState[it.id] = false to 0 }
    }
    Box(modifier = Modifier.fillMaxSize().background(MaterialTheme.colorScheme.surface)) {
        HorizontalPager(
            state = rememberPagerState(initialPage = startIndex, pageCount = { media.size }),
            modifier = Modifier.fillMaxSize()
        ) { page ->
            val item = media[page]
            when (item.type) {
                MediaType.VIDEO -> VideoPage(url = item.url)
                else -> Image(
                    painter = rememberAsyncImagePainter(item.url),
                    contentDescription = item.altText,
                    contentScale = ContentScale.Fit,
                    modifier = Modifier.fillMaxSize()
                )
            }
        }

        // Bottom-centered heart only
        val current = media[index]
        val state = likeState[current.id] ?: (false to 0)
        Box(modifier = Modifier.align(Alignment.BottomCenter).padding(bottom = 24.dp)) {
            Surface(shape = CircleShape, color = MaterialTheme.colorScheme.inverseSurface.copy(alpha = 0.35f)) {
                IconButton(onClick = {
                    val (liked, count) = state
                    val newLiked = !liked
                    val newCount = (count + if (newLiked) 1 else if (count > 0) -1 else 0).coerceAtLeast(0)
                    likeState[current.id] = newLiked to newCount
                    // Persist via gRPC
                    val client = SonetGRPCClient(LocalContext.current, SonetConfiguration.development)
                    val userId = sessionViewModel.currentUser.collectAsState().value?.id ?: "anon"
                    androidx.compose.runtime.LaunchedEffect(current.id, newLiked, userId) {
                        try { client.toggleMediaLike(current.id, userId = userId, isLiked = newLiked) } catch (_: Exception) {}
                    }
                }) {
                    Icon(
                        imageVector = if (state.first) Icons.Default.Favorite else Icons.Default.FavoriteBorder,
                        contentDescription = null,
                        tint = if (state.first) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onInverseSurface
                    )
                }
            }
        }

        IconButton(onClick = onDismiss, modifier = Modifier.align(Alignment.TopStart).padding(16.dp)) {
            Icon(imageVector = Icons.Default.Close, contentDescription = null, tint = MaterialTheme.colorScheme.onSurface)
        }
    }
}