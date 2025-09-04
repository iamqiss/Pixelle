package xyz.sonet.app.viewmodels

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import xyz.sonet.app.models.Story
import xyz.sonet.app.grpc.SonetGRPCClient
import xyz.sonet.app.grpc.SonetConfiguration

class StoriesViewModel(application: Application) : AndroidViewModel(application) {
    
    // State flows
    private val _stories = MutableStateFlow<List<Story>>(emptyList())
    val stories: StateFlow<List<Story>> = _stories.asStateFlow()
    
    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()
    
    private val _showCreateStory = MutableStateFlow(false)
    val showCreateStory: StateFlow<Boolean> = _showCreateStory.asStateFlow()
    
    private val _showMyStories = MutableStateFlow(false)
    val showMyStories: StateFlow<Boolean> = _showMyStories.asStateFlow()
    
    private val _showStoryViewer = MutableStateFlow(false)
    val showStoryViewer: StateFlow<Boolean> = _showStoryViewer.asStateFlow()
    
    private val _selectedStory = MutableStateFlow<Story?>(null)
    val selectedStory: StateFlow<Story?> = _selectedStory.asStateFlow()
    
    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()
    
    // gRPC client
    private val grpcClient = SonetGRPCClient(application, SonetConfiguration.development)
    
    init {
        loadStories()
    }
    
    fun loadStories() {
        viewModelScope.launch {
            try {
                _isLoading.value = true
                _error.value = null
                
                val request = GetStoriesRequest.newBuilder()
                    .setUserId("current_user")
                    .setLimit(50)
                    .build()
                
                val response = grpcClient.getStories(request)
                
                if (response.success) {
                    _stories.value = response.stories.map { Story.from(it) }
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to load stories: ${e.message}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    fun refreshStories() {
        viewModelScope.launch {
            try {
                _isLoading.value = true
                _error.value = null
                
                val request = GetStoriesRequest.newBuilder()
                    .setUserId("current_user")
                    .setLimit(50)
                    .build()
                
                val response = grpcClient.getStories(request)
                
                if (response.success) {
                    _stories.value = response.stories.map { Story.from(it) }
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to refresh stories: ${e.message}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    fun clearError() {
        _error.value = null
    }
    
    fun markStoryAsViewed(storyId: String) {
        viewModelScope.launch {
            try {
                val request = MarkStoryViewedRequest.newBuilder()
                    .setStoryId(storyId)
                    .setUserId("current_user")
                    .build()
                
                val response = grpcClient.markStoryViewed(request)
                
                if (response.success) {
                    // Update local story state
                    _stories.value = _stories.value.map { story ->
                        if (story.id == storyId) {
                            story.copy(isViewed = true, viewCount = story.viewCount + 1)
                        } else {
                            story
                        }
                    }
                }
            } catch (e: Exception) {
                // Handle error silently for view tracking
                println("Failed to mark story as viewed: ${e.message}")
            }
        }
    }
    
    fun reactToStory(storyId: String, reactionType: String) {
        viewModelScope.launch {
            try {
                val request = ReactToStoryRequest.newBuilder()
                    .setStoryId(storyId)
                    .setUserId("current_user")
                    .setReactionType(reactionType)
                    .build()
                
                val response = grpcClient.reactToStory(request)
                
                if (response.success) {
                    // Update local story state with new reaction
                    _stories.value = _stories.value.map { story ->
                        if (story.id == storyId) {
                            val newReaction = StoryReaction(
                                id = response.reactionId,
                                userId = "current_user",
                                username = "current_user", // Replace with actual username
                                type = StoryReactionType.valueOf(reactionType.uppercase()),
                                timestamp = Date()
                            )
                            story.copy(reactions = story.reactions + newReaction)
                        } else {
                            story
                        }
                    }
                }
            } catch (e: Exception) {
                _error.value = "Failed to react to story: ${e.message}"
            }
        }
    }
    
    fun replyToStory(storyId: String, content: String, mediaUrl: String? = null) {
        viewModelScope.launch {
            try {
                val request = ReplyToStoryRequest.newBuilder()
                    .setStoryId(storyId)
                    .setUserId("current_user")
                    .setContent(content)
                    .setMediaUrl(mediaUrl ?: "")
                    .build()
                
                val response = grpcClient.replyToStory(request)
                
                if (response.success) {
                    // Handle successful reply
                    // Could update local state or show confirmation
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to reply to story: ${e.message}"
            }
        }
    }
    
    fun shareStory(storyId: String) {
        viewModelScope.launch {
            try {
                val request = ShareStoryRequest.newBuilder()
                    .setStoryId(storyId)
                    .setUserId("current_user")
                    .build()
                
                val response = grpcClient.shareStory(request)
                
                if (response.success) {
                    // Handle successful share
                    // Could update local state or show confirmation
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to share story: ${e.message}"
            }
        }
    }
    
    fun saveStory(storyId: String) {
        viewModelScope.launch {
            try {
                val request = SaveStoryRequest.newBuilder()
                    .setStoryId(storyId)
                    .setUserId("current_user")
                    .build()
                
                val response = grpcClient.saveStory(request)
                
                if (response.success) {
                    // Handle successful save
                    // Could update local state or show confirmation
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to save story: ${e.message}"
            }
        }
    }
    
    fun getStoryAnalytics(storyId: String) {
        viewModelScope.launch {
            try {
                val request = GetStoryAnalyticsRequest.newBuilder()
                    .setStoryId(storyId)
                    .setUserId("current_user")
                    .build()
                
                val response = grpcClient.getStoryAnalytics(request)
                
                if (response.success) {
                    // Handle analytics data
                    // Could store in local state for display
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to get story analytics: ${e.message}"
            }
        }
    }
    
    fun reportStory(storyId: String, reason: String, description: String? = null) {
        viewModelScope.launch {
            try {
                val request = ReportStoryRequest.newBuilder()
                    .setStoryId(storyId)
                    .setUserId("current_user")
                    .setReason(reason)
                    .setDescription(description ?: "")
                    .build()
                
                val response = grpcClient.reportStory(request)
                
                if (response.success) {
                    // Handle successful report
                    // Could show confirmation or update UI
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to report story: ${e.message}"
            }
        }
    }
    
    fun getStoryReplies(storyId: String) {
        viewModelScope.launch {
            try {
                val request = GetStoryRepliesRequest.newBuilder()
                    .setStoryId(storyId)
                    .setLimit(50)
                    .build()
                
                val response = grpcClient.getStoryReplies(request)
                
                if (response.success) {
                    // Handle replies data
                    // Could store in local state for display
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to get story replies: ${e.message}"
            }
        }
    }
    
    fun getStoryLocation(storyId: String) {
        viewModelScope.launch {
            try {
                val request = GetStoryLocationRequest.newBuilder()
                    .setStoryId(storyId)
                    .build()
                
                val response = grpcClient.getStoryLocation(request)
                
                if (response.success) {
                    // Handle location data
                    // Could store in local state for display
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to get story location: ${e.message}"
            }
        }
    }
    
    fun getStoryMusic(storyId: String) {
        viewModelScope.launch {
            try {
                val request = GetStoryMusicRequest.newBuilder()
                    .setStoryId(storyId)
                    .build()
                
                val response = grpcClient.getStoryMusic(request)
                
                if (response.success) {
                    // Handle music data
                    // Could store in local state for display
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to get story music: ${e.message}"
            }
        }
    }
    
    fun getStoryFilters(storyId: String) {
        viewModelScope.launch {
            try {
                val request = GetStoryFiltersRequest.newBuilder()
                    .setStoryId(storyId)
                    .build()
                
                val response = grpcClient.getStoryFilters(request)
                
                if (response.success) {
                    // Handle filters data
                    // Could store in local state for display
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to get story filters: ${e.message}"
            }
        }
    }
    
    fun getStoryStickers(storyId: String) {
        viewModelScope.launch {
            try {
                val request = GetStoryStickersRequest.newBuilder()
                    .setStoryId(storyId)
                    .build()
                
                val response = grpcClient.getStoryStickers(request)
                
                if (response.success) {
                    // Handle stickers data
                    // Could store in local state for display
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to get story stickers: ${e.message}"
            }
        }
    }
    
    fun getStoryDrawings(storyId: String) {
        viewModelScope.launch {
            try {
                val request = GetStoryDrawingsRequest.newBuilder()
                    .setStoryId(storyId)
                    .build()
                
                val response = grpcClient.getStoryDrawings(request)
                
                if (response.success) {
                    // Handle drawings data
                    // Could store in local state for display
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to get story drawings: ${e.message}"
            }
        }
    }
}

// MARK: - gRPC Request/Response Models (Placeholder - replace with actual gRPC types)
data class GetStoriesRequest(
    val userId: String,
    val limit: Int
) {
    companion object {
        fun newBuilder(): GetStoriesRequestBuilder {
            return GetStoriesRequestBuilder()
        }
    }
}

class GetStoriesRequestBuilder {
    private var userId: String = ""
    private var limit: Int = 50
    
    fun setUserId(userId: String): GetStoriesRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun setLimit(limit: Int): GetStoriesRequestBuilder {
        this.limit = limit
        return this
    }
    
    fun build(): GetStoriesRequest {
        return GetStoriesRequest(userId, limit)
    }
}

data class GetStoriesResponse(
    val success: Boolean,
    val stories: List<GRPCStory>,
    val errorMessage: String
)

data class MarkStoryViewedRequest(
    val storyId: String,
    val userId: String
) {
    companion object {
        fun newBuilder(): MarkStoryViewedRequestBuilder {
            return MarkStoryViewedRequestBuilder()
        }
    }
}

class MarkStoryViewedRequestBuilder {
    private var storyId: String = ""
    private var userId: String = ""
    
    fun setStoryId(storyId: String): MarkStoryViewedRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun setUserId(userId: String): MarkStoryViewedRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun build(): MarkStoryViewedRequest {
        return MarkStoryViewedRequest(storyId, userId)
    }
}

data class MarkStoryViewedResponse(
    val success: Boolean,
    val errorMessage: String
)

data class ReactToStoryRequest(
    val storyId: String,
    val userId: String,
    val reactionType: String
) {
    companion object {
        fun newBuilder(): ReactToStoryRequestBuilder {
            return ReactToStoryRequestBuilder()
        }
    }
}

class ReactToStoryRequestBuilder {
    private var storyId: String = ""
    private var userId: String = ""
    private var reactionType: String = ""
    
    fun setStoryId(storyId: String): ReactToStoryRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun setUserId(userId: String): ReactToStoryRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun setReactionType(reactionType: String): ReactToStoryRequestBuilder {
        this.reactionType = reactionType
        return this
    }
    
    fun build(): ReactToStoryRequest {
        return ReactToStoryRequest(storyId, userId, reactionType)
    }
}

data class ReactToStoryResponse(
    val success: Boolean,
    val reactionId: String,
    val errorMessage: String
)

data class ReplyToStoryRequest(
    val storyId: String,
    val userId: String,
    val content: String,
    val mediaUrl: String
) {
    companion object {
        fun newBuilder(): ReplyToStoryRequestBuilder {
            return ReplyToStoryRequestBuilder()
        }
    }
}

class ReplyToStoryRequestBuilder {
    private var storyId: String = ""
    private var userId: String = ""
    private var content: String = ""
    private var mediaUrl: String = ""
    
    fun setStoryId(storyId: String): ReplyToStoryRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun setUserId(userId: String): ReplyToStoryRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun setContent(content: String): ReplyToStoryRequestBuilder {
        this.content = content
        return this
    }
    
    fun setMediaUrl(mediaUrl: String): ReplyToStoryRequestBuilder {
        this.mediaUrl = mediaUrl
        return this
    }
    
    fun build(): ReplyToStoryRequest {
        return ReplyToStoryRequest(storyId, userId, content, mediaUrl)
    }
}

data class ReplyToStoryResponse(
    val success: Boolean,
    val replyId: String,
    val errorMessage: String
)

data class ShareStoryRequest(
    val storyId: String,
    val userId: String
) {
    companion object {
        fun newBuilder(): ShareStoryRequestBuilder {
            return ShareStoryRequestBuilder()
        }
    }
}

class ShareStoryRequestBuilder {
    private var storyId: String = ""
    private var userId: String = ""
    
    fun setStoryId(storyId: String): ShareStoryRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun setUserId(userId: String): ShareStoryRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun build(): ShareStoryRequest {
        return ShareStoryRequest(storyId, userId)
    }
}

data class ShareStoryResponse(
    val success: Boolean,
    val shareId: String,
    val errorMessage: String
)

data class SaveStoryRequest(
    val storyId: String,
    val userId: String
) {
    companion object {
        fun newBuilder(): SaveStoryRequestBuilder {
            return SaveStoryRequestBuilder()
        }
    }
}

class SaveStoryRequestBuilder {
    private var storyId: String = ""
    private var userId: String = ""
    
    fun setStoryId(storyId: String): SaveStoryRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun setUserId(userId: String): SaveStoryRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun build(): SaveStoryRequest {
        return SaveStoryRequest(storyId, userId)
    }
}

data class SaveStoryResponse(
    val success: Boolean,
    val saveId: String,
    val errorMessage: String
)

data class GetStoryAnalyticsRequest(
    val storyId: String,
    val userId: String
) {
    companion object {
        fun newBuilder(): GetStoryAnalyticsRequestBuilder {
            return GetStoryAnalyticsRequestBuilder()
        }
    }
}

class GetStoryAnalyticsRequestBuilder {
    private var storyId: String = ""
    private var userId: String = ""
    
    fun setStoryId(storyId: String): GetStoryAnalyticsRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun setUserId(userId: String): GetStoryAnalyticsRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun build(): GetStoryAnalyticsRequest {
        return GetStoryAnalyticsRequest(storyId, userId)
    }
}

data class GetStoryAnalyticsResponse(
    val success: Boolean,
    val analytics: StoryAnalytics,
    val errorMessage: String
)

data class ReportStoryRequest(
    val storyId: String,
    val userId: String,
    val reason: String,
    val description: String
) {
    companion object {
        fun newBuilder(): ReportStoryRequestBuilder {
            return ReportStoryRequestBuilder()
        }
    }
}

class ReportStoryRequestBuilder {
    private var storyId: String = ""
    private var userId: String = ""
    private var reason: String = ""
    private var description: String = ""
    
    fun setStoryId(storyId: String): ReportStoryRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun setUserId(userId: String): ReportStoryRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun setReason(reason: String): ReportStoryRequestBuilder {
        this.reason = reason
        return this
    }
    
    fun setDescription(description: String): ReportStoryRequestBuilder {
        this.description = description
        return this
    }
    
    fun build(): ReportStoryRequest {
        return ReportStoryRequest(storyId, userId, reason, description)
    }
}

data class ReportStoryResponse(
    val success: Boolean,
    val reportId: String,
    val errorMessage: String
)

data class GetStoryRepliesRequest(
    val storyId: String,
    val limit: Int
) {
    companion object {
        fun newBuilder(): GetStoryRepliesRequestBuilder {
            return GetStoryRepliesRequestBuilder()
        }
    }
}

class GetStoryRepliesRequestBuilder {
    private var storyId: String = ""
    private var limit: Int = 50
    
    fun setStoryId(storyId: String): GetStoryRepliesRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun setLimit(limit: Int): GetStoryRepliesRequestBuilder {
        this.limit = limit
        return this
    }
    
    fun build(): GetStoryRepliesRequest {
        return GetStoryRepliesRequest(storyId, limit)
    }
}

data class GetStoryRepliesResponse(
    val success: Boolean,
    val replies: List<StoryReply>,
    val errorMessage: String
)

data class GetStoryLocationRequest(
    val storyId: String
) {
    companion object {
        fun newBuilder(): GetStoryLocationRequestBuilder {
            return GetStoryLocationRequestBuilder()
        }
    }
}

class GetStoryLocationRequestBuilder {
    private var storyId: String = ""
    
    fun setStoryId(storyId: String): GetStoryLocationRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun build(): GetStoryLocationRequest {
        return GetStoryLocationRequest(storyId)
    }
}

data class GetStoryLocationResponse(
    val success: Boolean,
    val location: StoryLocation,
    val errorMessage: String
)

data class GetStoryMusicRequest(
    val storyId: String
) {
    companion object {
        fun newBuilder(): GetStoryMusicRequestBuilder {
            return GetStoryMusicRequestBuilder()
        }
    }
}

class GetStoryMusicRequestBuilder {
    private var storyId: String = ""
    
    fun setStoryId(storyId: String): GetStoryMusicRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun build(): GetStoryMusicRequest {
        return GetStoryMusicRequest(storyId)
    }
}

data class GetStoryMusicResponse(
    val success: Boolean,
    val music: StoryMusic,
    val errorMessage: String
)

data class GetStoryFiltersRequest(
    val storyId: String
) {
    companion object {
        fun newBuilder(): GetStoryFiltersRequestBuilder {
            return GetStoryFiltersRequestBuilder()
        }
    }
}

class GetStoryFiltersRequestBuilder {
    private var storyId: String = ""
    
    fun setStoryId(storyId: String): GetStoryFiltersRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun build(): GetStoryFiltersRequest {
        return GetStoryFiltersRequest(storyId)
    }
}

data class GetStoryFiltersResponse(
    val success: Boolean,
    val filters: StoryFilters,
    val errorMessage: String
)

data class GetStoryStickersRequest(
    val storyId: String
) {
    companion object {
        fun newBuilder(): GetStoryStickersRequestBuilder {
            return GetStoryStickersRequestBuilder()
        }
    }
}

class GetStoryStickersRequestBuilder {
    private var storyId: String = ""
    
    fun setStoryId(storyId: String): GetStoryStickersRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun build(): GetStoryStickersRequest {
        return GetStoryStickersRequest(storyId)
    }
}

data class GetStoryStickersResponse(
    val success: Boolean,
    val stickers: List<StorySticker>,
    val errorMessage: String
)

data class GetStoryDrawingsRequest(
    val storyId: String
) {
    companion object {
        fun newBuilder(): GetStoryDrawingsRequestBuilder {
            return GetStoryDrawingsRequestBuilder()
        }
    }
}

class GetStoryDrawingsRequestBuilder {
    private var storyId: String = ""
    
    fun setStoryId(storyId: String): GetStoryDrawingsRequestBuilder {
        this.storyId = storyId
        return this
    }
    
    fun build(): GetStoryDrawingsRequest {
        return GetStoryDrawingsRequest(storyId)
    }
}

data class GetStoryDrawingsResponse(
    val success: Boolean,
    val drawings: List<StoryDrawing>,
    val errorMessage: String
)

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

// MARK: - Import statements for models
import xyz.sonet.app.models.*
import java.util.*