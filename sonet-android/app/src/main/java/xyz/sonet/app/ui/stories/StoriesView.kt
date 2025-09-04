package xyz.sonet.app.ui.stories

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
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
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewmodel.compose.viewModel
import coil.compose.AsyncImage
import coil.request.ImageRequest
import xyz.sonet.app.models.*
import xyz.sonet.app.viewmodels.StoriesViewModel

@Composable
fun StoriesView(
    modifier: Modifier = Modifier,
    viewModel: StoriesViewModel = viewModel(),
    onNavigateBack: () -> Unit = {}
) {
    val stories by viewModel.stories.collectAsState()
    val isLoading by viewModel.isLoading.collectAsState()
    val showCreateStory by viewModel.showCreateStory.collectAsState()
    val showMyStories by viewModel.showMyStories.collectAsState()
    val showStoryViewer by viewModel.showStoryViewer.collectAsState()
    val selectedStory by viewModel.selectedStory.collectAsState()
    
    LaunchedEffect(Unit) {
        viewModel.loadStories()
    }
    
    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("Stories") },
                navigationIcon = {
                    IconButton(onClick = onNavigateBack) {
                        Icon(Icons.Default.ArrowBack, contentDescription = "Back")
                    }
                },
                actions = {
                    IconButton(onClick = { viewModel.showCreateStory.value = true }) {
                        Icon(Icons.Default.Add, contentDescription = "Create Story")
                    }
                }
            )
        }
    ) { paddingValues ->
        Column(
            modifier = modifier
                .fillMaxSize()
                .padding(paddingValues)
        ) {
            // Stories Header
            StoriesHeader(
                onCreateStory = { viewModel.showCreateStory.value = true },
                onViewMyStories = { viewModel.showMyStories.value = true }
            )
            
            // Stories Content
            when {
                isLoading -> StoriesLoadingView()
                stories.isEmpty() -> StoriesEmptyView(
                    onCreateStory = { viewModel.showCreateStory.value = true }
                )
                else -> StoriesContent(
                    stories = stories,
                    onStoryTap = { story ->
                        viewModel.selectedStory.value = story
                        viewModel.showStoryViewer.value = true
                    }
                )
            }
        }
    }
    
    // Handle navigation
    LaunchedEffect(showCreateStory) {
        if (showCreateStory) {
            // Navigate to CreateStoryView
            viewModel.showCreateStory.value = false
        }
    }
    
    LaunchedEffect(showMyStories) {
        if (showMyStories) {
            // Navigate to MyStoriesView
            viewModel.showMyStories.value = false
        }
    }
    
    LaunchedEffect(showStoryViewer) {
        if (showStoryViewer && selectedStory != null) {
            // Navigate to StoryViewer
            viewModel.showStoryViewer.value = false
        }
    }
}

@Composable
fun StoriesHeader(
    onCreateStory: () -> Unit,
    onViewMyStories: () -> Unit
) {
    Column(spacing = 16.dp) {
        // My Story Section
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp),
            horizontalArrangement = Arrangement.spacedBy(16.dp)
        ) {
            // Add Story Button
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                modifier = Modifier.clickable { onCreateStory() }
            ) {
                Box(
                    modifier = Modifier
                        .size(60.dp)
                        .clip(CircleShape)
                        .background(MaterialTheme.colorScheme.primary.copy(alpha = 0.1f)),
                    contentAlignment = Alignment.Center
                ) {
                    Icon(
                        Icons.Default.Add,
                        contentDescription = "Add Story",
                        tint = MaterialTheme.colorScheme.primary,
                        modifier = Modifier.size(24.dp)
                    )
                }
                
                Spacer(modifier = Modifier.height(8.dp))
                
                Text(
                    text = "Add Story",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.primary
                )
            }
            
            // My Stories Button
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                modifier = Modifier.clickable { onViewMyStories() }
            ) {
                Box(
                    modifier = Modifier
                        .size(60.dp)
                        .clip(CircleShape)
                        .background(MaterialTheme.colorScheme.surfaceVariant),
                    contentAlignment = Alignment.Center
                ) {
                    Icon(
                        Icons.Default.Person,
                        contentDescription = "My Stories",
                        tint = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.size(24.dp)
                    )
                }
                
                Spacer(modifier = Modifier.height(8.dp))
                
                Text(
                    text = "My Stories",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            Spacer(modifier = Modifier.weight(1f))
        }
        
        Divider()
    }
}

@Composable
fun StoriesContent(
    stories: List<Story>,
    onStoryTap: (Story) -> Unit
) {
    LazyRow(
        contentPadding = PaddingValues(horizontal = 16.dp),
        horizontalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        items(stories) { story ->
            StoryPreviewCard(
                story = story,
                onTap = { onStoryTap(story) }
            )
        }
    }
}

@Composable
fun StoryPreviewCard(
    story: Story,
    onTap: () -> Unit
) {
    Column(
        horizontalAlignment = Alignment.CenterHorizontally,
        modifier = Modifier.clickable { onTap() }
    ) {
        // Story Avatar
        Box {
            // Avatar Image
            if (story.avatarUrl != null) {
                AsyncImage(
                    model = ImageRequest.Builder(LocalContext.current)
                        .data(story.avatarUrl)
                        .crossfade(true)
                        .build(),
                    contentDescription = "Avatar",
                    modifier = Modifier
                        .size(60.dp)
                        .clip(CircleShape),
                    contentScale = ContentScale.Crop
                )
            } else {
                Box(
                    modifier = Modifier
                        .size(60.dp)
                        .clip(CircleShape)
                        .background(MaterialTheme.colorScheme.surfaceVariant),
                    contentAlignment = Alignment.Center
                ) {
                    Icon(
                        Icons.Default.Person,
                        contentDescription = "Avatar",
                        tint = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
            
            // Story Ring
            Box(
                modifier = Modifier
                    .size(68.dp)
                    .clip(CircleShape)
                    .background(Color.Transparent)
                    .padding(2.dp)
                    .background(
                        if (story.isViewed) MaterialTheme.colorScheme.outline
                        else MaterialTheme.colorScheme.primary,
                        CircleShape
                    )
            )
        }
        
        Spacer(modifier = Modifier.height(8.dp))
        
        // Username
        Text(
            text = if (story.displayName.isNotEmpty()) story.displayName else story.username,
            fontSize = 12.sp,
            color = MaterialTheme.colorScheme.onSurface,
            maxLines = 1,
            modifier = Modifier.width(60.dp),
            textAlign = TextAlign.Center
        )
    }
}

@Composable
fun StoriesLoadingView() {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(16.dp)
        ) {
            CircularProgressIndicator(
                modifier = Modifier.size(48.dp),
                color = MaterialTheme.colorScheme.primary
            )
            
            Text(
                text = "Loading stories...",
                fontSize = 16.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

@Composable
fun StoriesEmptyView(
    onCreateStory: () -> Unit
) {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(24.dp)
        ) {
            Icon(
                Icons.Default.CameraAlt,
                contentDescription = "Camera",
                modifier = Modifier.size(80.dp),
                tint = MaterialTheme.colorScheme.outline
            )
            
            Column(
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                Text(
                    text = "No Stories Yet",
                    fontSize = 20.sp,
                    fontWeight = FontWeight.SemiBold,
                    color = MaterialTheme.colorScheme.onSurface
                )
                
                Text(
                    text = "Be the first to share a story with your friends!",
                    fontSize = 16.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    textAlign = TextAlign.Center
                )
            }
            
            Button(
                onClick = onCreateStory,
                colors = ButtonDefaults.buttonColors(
                    containerColor = MaterialTheme.colorScheme.primary
                )
            ) {
                Text("Create Your First Story")
            }
        }
    }
}

// MARK: - Stories View Model
@Composable
fun StoriesViewModel(): StoriesViewModel {
    return remember { StoriesViewModel() }
}

// MARK: - Story Extensions for gRPC (Placeholder - replace with actual gRPC types)
fun Story.Companion.from(grpcStory: GRPCStory): Story {
    return Story(
        id = grpcStory.storyId,
        userId = grpcStory.userId,
        username = grpcStory.username,
        displayName = grpcStory.displayName,
        avatarUrl = grpcStory.avatarUrl,
        mediaItems = grpcStory.mediaItems.map { StoryMediaItem.from(it) },
        createdAt = grpcStory.createdAt.date,
        expiresAt = grpcStory.expiresAt.date,
        isViewed = grpcStory.isViewed,
        viewCount = grpcStory.viewCount.toInt(),
        reactions = grpcStory.reactions.map { StoryReaction.from(it) },
        mentions = grpcStory.mentions,
        hashtags = grpcStory.hashtags,
        location = grpcStory.location?.let { StoryLocation.from(it) },
        music = grpcStory.music?.let { StoryMusic.from(it) },
        filters = StoryFilters.from(grpcStory.filters),
        privacy = StoryPrivacy.valueOf(grpcStory.privacy.uppercase())
    )
}

fun StoryMediaItem.Companion.from(grpcMediaItem: GRPCStoryMediaItem): StoryMediaItem {
    return StoryMediaItem(
        id = grpcMediaItem.mediaId,
        type = StoryMediaType.valueOf(grpcMediaItem.type.uppercase()),
        url = grpcMediaItem.url,
        thumbnailUrl = grpcMediaItem.thumbnailUrl,
        duration = grpcMediaItem.duration,
        order = grpcMediaItem.order.toInt(),
        filters = StoryFilters.from(grpcMediaItem.filters),
        text = grpcMediaItem.text?.let { StoryText.from(it) },
        stickers = grpcMediaItem.stickers.map { StorySticker.from(it) },
        drawings = grpcMediaItem.drawings.map { StoryDrawing.from(it) }
    )
}

fun StoryFilters.Companion.from(grpcFilters: GRPCStoryFilters): StoryFilters {
    return StoryFilters(
        brightness = grpcFilters.brightness,
        contrast = grpcFilters.contrast,
        saturation = grpcFilters.saturation,
        warmth = grpcFilters.warmth,
        sharpness = grpcFilters.sharpness,
        vignette = grpcFilters.vignette,
        grain = grpcFilters.grain,
        preset = StoryFilterPreset.valueOf(grpcFilters.preset.uppercase())
    )
}

fun StoryText.Companion.from(grpcText: GRPCStoryText): StoryText {
    return StoryText(
        content = grpcText.content,
        font = StoryFont.valueOf(grpcText.font.uppercase()),
        color = StoryColor.valueOf(grpcText.color.uppercase()),
        size = grpcText.size,
        position = StoryTextPosition.valueOf(grpcText.position.uppercase()),
        alignment = StoryTextAlignment.valueOf(grpcText.alignment.uppercase()),
        effects = grpcText.effects.map { StoryTextEffect.valueOf(it.uppercase()) }
    )
}

fun StorySticker.Companion.from(grpcSticker: GRPCStorySticker): StorySticker {
    return StorySticker(
        id = grpcSticker.stickerId,
        type = StoryStickerType.valueOf(grpcSticker.type.uppercase()),
        url = grpcSticker.url,
        positionX = grpcSticker.positionX.toFloat(),
        positionY = grpcSticker.positionY.toFloat(),
        scale = grpcSticker.scale.toFloat(),
        rotation = grpcSticker.rotation.toFloat(),
        isAnimated = grpcSticker.isAnimated
    )
}

fun StoryDrawing.Companion.from(grpcDrawing: GRPCStoryDrawing): StoryDrawing {
    return StoryDrawing(
        id = grpcDrawing.drawingId,
        points = grpcDrawing.points.map { DrawingPoint.from(it) },
        color = StoryColor.valueOf(grpcDrawing.color.uppercase()),
        brushSize = grpcDrawing.brushSize.toFloat(),
        opacity = grpcDrawing.opacity.toFloat()
    )
}

fun DrawingPoint.Companion.from(grpcPoint: GRPCPoint): DrawingPoint {
    return DrawingPoint(
        x = grpcPoint.x.toFloat(),
        y = grpcPoint.y.toFloat()
    )
}

fun StoryReaction.Companion.from(grpcReaction: GRPCStoryReaction): StoryReaction {
    return StoryReaction(
        id = grpcReaction.reactionId,
        userId = grpcReaction.userId,
        username = grpcReaction.username,
        type = StoryReactionType.valueOf(grpcReaction.type.uppercase()),
        timestamp = grpcReaction.timestamp.date
    )
}

fun StoryLocation.Companion.from(grpcLocation: GRPCStoryLocation): StoryLocation {
    return StoryLocation(
        name = grpcLocation.name,
        address = grpcLocation.address,
        latitude = grpcLocation.latitude,
        longitude = grpcLocation.longitude,
        category = grpcLocation.category
    )
}

fun StoryMusic.Companion.from(grpcMusic: GRPCStoryMusic): StoryMusic {
    return StoryMusic(
        title = grpcMusic.title,
        artist = grpcMusic.artist,
        album = grpcMusic.album,
        duration = grpcMusic.duration,
        url = grpcMusic.url,
        startTime = grpcMusic.startTime,
        isLooping = grpcMusic.isLooping
    )
}

// MARK: - gRPC Models (Placeholder - replace with actual gRPC types)
data class GRPCStory(
    val storyId: String,
    val userId: String,
    val username: String,
    val displayName: String,
    val avatarUrl: String,
    val mediaItems: List<GRPCStoryMediaItem>,
    val createdAt: GRPCTimestamp,
    val expiresAt: GRPCTimestamp,
    val isViewed: Boolean,
    val viewCount: ULong,
    val reactions: List<GRPCStoryReaction>,
    val mentions: List<String>,
    val hashtags: List<String>,
    val location: GRPCStoryLocation?,
    val music: GRPCStoryMusic?,
    val filters: GRPCStoryFilters,
    val privacy: String
)

data class GRPCStoryMediaItem(
    val mediaId: String,
    val type: String,
    val url: String,
    val thumbnailUrl: String,
    val duration: Long,
    val order: UInt,
    val filters: GRPCStoryFilters,
    val text: GRPCStoryText?,
    val stickers: List<GRPCStorySticker>,
    val drawings: List<GRPCStoryDrawing>
)

data class GRPCStoryFilters(
    val brightness: Double,
    val contrast: Double,
    val saturation: Double,
    val warmth: Double,
    val sharpness: Double,
    val vignette: Double,
    val grain: Double,
    val preset: String
)

data class GRPCStoryText(
    val content: String,
    val font: String,
    val color: String,
    val size: Float,
    val position: String,
    val alignment: String,
    val effects: List<String>
)

data class GRPCStorySticker(
    val stickerId: String,
    val type: String,
    val url: String,
    val positionX: Double,
    val positionY: Double,
    val scale: Double,
    val rotation: Double,
    val isAnimated: Boolean
)

data class GRPCStoryDrawing(
    val drawingId: String,
    val points: List<GRPCPoint>,
    val color: String,
    val brushSize: Double,
    val opacity: Double
)

data class GRPCStoryReaction(
    val reactionId: String,
    val userId: String,
    val username: String,
    val type: String,
    val timestamp: GRPCTimestamp
)

data class GRPCStoryLocation(
    val name: String,
    val address: String,
    val latitude: Double,
    val longitude: Double,
    val category: String
)

data class GRPCStoryMusic(
    val title: String,
    val artist: String,
    val album: String,
    val duration: Long,
    val url: String,
    val startTime: Long,
    val isLooping: Boolean
)

data class GRPCPoint(
    val x: Double,
    val y: Double
)

data class GRPCTimestamp(
    val date: Date
)