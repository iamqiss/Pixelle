package xyz.sonet.app.models

import android.os.Parcelable
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import kotlinx.parcelize.Parcelize
import java.util.*

@Parcelize
data class Story(
    val id: String,
    val userId: String,
    val username: String,
    val displayName: String,
    val avatarUrl: String?,
    val mediaItems: List<StoryMediaItem>,
    val createdAt: Date,
    val expiresAt: Date,
    val isViewed: Boolean,
    val viewCount: Int,
    val reactions: List<StoryReaction>,
    val mentions: List<String>,
    val hashtags: List<String>,
    val location: StoryLocation?,
    val music: StoryMusic?,
    val filters: StoryFilters,
    val privacy: StoryPrivacy
) : Parcelable {
    
    // Computed properties
    val isExpired: Boolean
        get() = Date() > expiresAt
    
    val timeRemaining: Long
        get() = maxOf(0, expiresAt.time - Date().time)
    
    val isOwnStory: Boolean
        get() = false // Call sites should compare story.userId to session user id
    
    val canReply: Boolean
        get() = privacy.allowsReplies && !isExpired
    
    val canReact: Boolean
        get() = !isExpired
}

@Parcelize
data class StoryMediaItem(
    val id: String,
    val type: StoryMediaType,
    val url: String,
    val thumbnailUrl: String?,
    val duration: Long,
    val order: Int,
    val filters: StoryFilters,
    val text: StoryText?,
    val stickers: List<StorySticker>,
    val drawings: List<StoryDrawing>
) : Parcelable {
    
    // Computed properties
    val isVideo: Boolean
        get() = type == StoryMediaType.VIDEO
    
    val isImage: Boolean
        get() = type == StoryMediaType.IMAGE
    
    val isText: Boolean
        get() = type == StoryMediaType.TEXT
}

enum class StoryMediaType(val displayName: String, val icon: ImageVector) {
    IMAGE("Photo", Icons.Default.Photo),
    VIDEO("Video", Icons.Default.VideoLibrary),
    TEXT("Text", Icons.Default.TextFields),
    BOOMERANG("Boomerang", Icons.Default.Refresh),
    COLLAGE("Collage", Icons.Default.ViewModule)
}

@Parcelize
data class StoryText(
    val content: String,
    val font: StoryFont,
    val color: StoryColor,
    val size: Float,
    val position: StoryTextPosition,
    val alignment: StoryTextAlignment,
    val effects: List<StoryTextEffect>
) : Parcelable

enum class StoryFont(val fontName: String) {
    SYSTEM("Roboto"),
    ROUNDED("Roboto Rounded"),
    SERIF("Merriweather"),
    MONOSPACE("Roboto Mono"),
    HANDWRITTEN("Dancing Script"),
    BOLD("Roboto Bold"),
    LIGHT("Roboto Light")
}

enum class StoryColor(val color: Color) {
    WHITE(Color.White),
    BLACK(Color.Black),
    RED(Color.Red),
    BLUE(Color.Blue),
    GREEN(Color.Green),
    YELLOW(Color.Yellow),
    PURPLE(Color.Magenta),
    ORANGE(Color(0xFFFF9800)),
    PINK(Color(0xFFE91E63)),
    GRADIENT(Color.Blue)
}

enum class StoryTextPosition {
    TOP, CENTER, BOTTOM, CUSTOM
}

enum class StoryTextAlignment {
    LEFT, CENTER, RIGHT
}

enum class StoryTextEffect {
    NONE, SHADOW, OUTLINE, GLOW, NEON, RAINBOW
}

@Parcelize
data class StorySticker(
    val id: String,
    val type: StoryStickerType,
    val url: String,
    val positionX: Float,
    val positionY: Float,
    val scale: Float,
    val rotation: Float,
    val isAnimated: Boolean
) : Parcelable

enum class StoryStickerType {
    EMOJI, GIF, CUSTOM, TRENDING, BRAND
}

@Parcelize
data class StoryDrawing(
    val id: String,
    val points: List<DrawingPoint>,
    val color: StoryColor,
    val brushSize: Float,
    val opacity: Float
) : Parcelable

@Parcelize
data class DrawingPoint(
    val x: Float,
    val y: Float
) : Parcelable

@Parcelize
data class StoryFilters(
    val brightness: Double,
    val contrast: Double,
    val saturation: Double,
    val warmth: Double,
    val sharpness: Double,
    val vignette: Double,
    val grain: Double,
    val preset: StoryFilterPreset
) : Parcelable {
    
    companion object {
        val DEFAULT = StoryFilters(
            brightness = 0.0,
            contrast = 0.0,
            saturation = 0.0,
            warmth = 0.0,
            sharpness = 0.0,
            vignette = 0.0,
            grain = 0.0,
            preset = StoryFilterPreset.NONE
        )
    }
}

enum class StoryFilterPreset(val displayName: String) {
    NONE("Original"),
    VIBRANT("Vibrant"),
    DRAMATIC("Dramatic"),
    VINTAGE("Vintage"),
    NOIR("Noir"),
    WARM("Warm"),
    COOL("Cool"),
    BRIGHT("Bright"),
    MOODY("Moody"),
    CINEMATIC("Cinematic")
}

@Parcelize
data class StoryReaction(
    val id: String,
    val userId: String,
    val username: String,
    val type: StoryReactionType,
    val timestamp: Date
) : Parcelable

enum class StoryReactionType(val emoji: String, val displayName: String) {
    LIKE("👍", "Like"),
    LOVE("❤️", "Love"),
    LAUGH("😂", "Laugh"),
    WOW("😮", "Wow"),
    SAD("😢", "Sad"),
    ANGRY("😠", "Angry")
}

@Parcelize
data class StoryLocation(
    val name: String,
    val address: String,
    val latitude: Double,
    val longitude: Double,
    val category: String?
) : Parcelable

@Parcelize
data class StoryMusic(
    val title: String,
    val artist: String,
    val album: String?,
    val duration: Long,
    val url: String,
    val startTime: Long,
    val isLooping: Boolean
) : Parcelable

enum class StoryPrivacy(val displayName: String, val allowsReplies: Boolean) {
    PUBLIC("Everyone", true),
    FRIENDS("Friends", true),
    CLOSE_FRIENDS("Close Friends", true),
    CUSTOM("Custom", false)
}

@Parcelize
data class StoryCreation(
    var mediaItems: List<StoryMediaItem> = emptyList(),
    var text: StoryText? = null,
    var stickers: List<StorySticker> = emptyList(),
    var drawings: List<StoryDrawing> = emptyList(),
    var filters: StoryFilters = StoryFilters.DEFAULT,
    var privacy: StoryPrivacy = StoryPrivacy.FRIENDS,
    var music: StoryMusic? = null,
    var location: StoryLocation? = null,
    var mentions: List<String> = emptyList(),
    var hashtags: List<String> = emptyList()
) : Parcelable {
    
    val isValid: Boolean
        get() = mediaItems.isNotEmpty() || text != null
    
    val estimatedDuration: Long
        get() {
            return if (text != null) {
                5000L // Text stories show for 5 seconds
            } else {
                mediaItems.sumOf { it.duration }
            }
        }
}

@Parcelize
data class StoryAnalytics(
    val views: Int,
    val uniqueViews: Int,
    val replies: Int,
    val reactions: Int,
    val shares: Int,
    val saves: Int,
    val completionRate: Double,
    val averageViewTime: Long,
    val topReactions: Map<StoryReactionType, Int>,
    val viewerDemographics: StoryViewerDemographics
) : Parcelable

@Parcelize
data class StoryViewerDemographics(
    val ageGroups: Map<String, Int>,
    val genders: Map<String, Int>,
    val locations: Map<String, Int>,
    val devices: Map<String, Int>,
    val timeOfDay: Map<String, Int>
) : Parcelable

@Parcelize
data class StoryReply(
    val id: String,
    val storyId: String,
    val userId: String,
    val username: String,
    val displayName: String,
    val avatarUrl: String?,
    val content: String,
    val mediaUrl: String?,
    val timestamp: Date,
    val isPrivate: Boolean
) : Parcelable

@Parcelize
data class StoryCollection(
    val id: String,
    val name: String,
    val description: String?,
    val coverImageUrl: String?,
    val stories: List<Story>,
    val createdAt: Date,
    val isPublic: Boolean,
    val ownerId: String,
    val collaboratorIds: List<String>,
    val tags: List<String>
) : Parcelable {
    
    val storyCount: Int
        get() = stories.size
    
    val totalDuration: Long
        get() = stories.sumOf { story ->
            story.mediaItems.sumOf { it.duration }
        }
}