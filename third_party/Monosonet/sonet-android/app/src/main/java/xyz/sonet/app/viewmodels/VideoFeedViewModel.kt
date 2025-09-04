package xyz.sonet.app.viewmodels

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import xyz.sonet.app.grpc.SonetGRPCClient
import xyz.sonet.app.grpc.SonetConfiguration
import xyz.sonet.app.grpc.proto.*
import java.util.*

class VideoFeedViewModel(application: Application) : AndroidViewModel(application) {
    
    // MARK: - State Properties
    private val _videos = MutableStateFlow<List<VideoItem>>(emptyList())
    val videos: StateFlow<List<VideoItem>> = _videos.asStateFlow()
    
    private val _currentVideoIndex = MutableStateFlow(0)
    val currentVideoIndex: StateFlow<Int> = _currentVideoIndex.asStateFlow()
    
    private val _selectedTab = MutableStateFlow(VideoTab.FOR_YOU)
    val selectedTab: StateFlow<VideoTab> = _selectedTab.asStateFlow()
    
    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()
    
    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()
    
    private val _isLiked = MutableStateFlow(false)
    val isLiked: StateFlow<Boolean> = _isLiked.asStateFlow()
    
    private val _isFollowing = MutableStateFlow(false)
    val isFollowing: StateFlow<Boolean> = _isFollowing.asStateFlow()
    
    private val _showComments = MutableStateFlow(false)
    val showComments: StateFlow<Boolean> = _showComments.asStateFlow()
    
    private val _showShare = MutableStateFlow(false)
    val showShare: StateFlow<Boolean> = _showShare.asStateFlow()
    
    private val _currentVideo = MutableStateFlow<VideoItem?>(null)
    val currentVideo: StateFlow<VideoItem?> = _currentVideo.asStateFlow()
    
    // MARK: - Private Properties
    private val grpcClient = SonetGRPCClient(application, SonetConfiguration.development)
    private var currentPage = 0
    private val pageSize = 10
    
    // MARK: - Initialization
    init {
        loadVideos()
    }
    
    // MARK: - Public Methods
    fun loadVideos() {
        viewModelScope.launch {
            _isLoading.value = true
            _error.value = null
            
            try {
                val response = grpcClient.getVideos(
                    tab = selectedTab.value.grpcType,
                    page = currentPage,
                    pageSize = pageSize
                )
                
                val newVideos = response.videosList.map { VideoItem.from(it) }
                
                if (currentPage == 0) {
                    _videos.value = newVideos
                } else {
                    _videos.value = _videos.value + newVideos
                }
                
                if (_videos.value.isNotEmpty()) {
                    _currentVideo.value = _videos.value[0]
                }
                
                currentPage++
            } catch (e: Exception) {
                _error.value = "Failed to load videos: ${e.localizedMessage}"
            }
            
            _isLoading.value = false
        }
    }
    
    fun switchTab(tab: VideoTab) {
        _selectedTab.value = tab
        currentPage = 0
        _videos.value = emptyList()
        _currentVideoIndex.value = 0
        loadVideos()
    }
    
    fun nextVideo() {
        if (_currentVideoIndex.value < _videos.value.size - 1) {
            _currentVideoIndex.value++
            _currentVideo.value = _videos.value[_currentVideoIndex.value]
            updateVideoState()
        } else {
            // Load more videos if needed
            loadVideos()
        }
    }
    
    fun previousVideo() {
        if (_currentVideoIndex.value > 0) {
            _currentVideoIndex.value--
            _currentVideo.value = _videos.value[_currentVideoIndex.value]
            updateVideoState()
        }
    }
    
    fun selectVideo(index: Int) {
        if (index >= 0 && index < _videos.value.size) {
            _currentVideoIndex.value = index
            _currentVideo.value = _videos.value[index]
            updateVideoState()
        }
    }
    
    fun toggleLike() {
        val video = _currentVideo.value ?: return
        
        _isLiked.value = !_isLiked.value
        
        viewModelScope.launch {
            try {
                val request = ToggleVideoLikeRequest.newBuilder()
                    .setVideoId(video.id)
                    .setIsLiked(_isLiked.value)
                    .build()
                
                val response = grpcClient.toggleVideoLike(request)
                if (!response.success) {
                    // Revert if failed
                    _isLiked.value = !_isLiked.value
                }
            } catch (e: Exception) {
                // Revert if failed
                _isLiked.value = !_isLiked.value
            }
        }
    }
    
    fun toggleFollow() {
        val video = _currentVideo.value ?: return
        
        _isFollowing.value = !_isFollowing.value
        
        viewModelScope.launch {
            try {
                val request = ToggleFollowRequest.newBuilder()
                    .setUserId(video.author.userId)
                    .setIsFollowing(_isFollowing.value)
                    .build()
                
                val response = grpcClient.toggleFollow(request)
                if (!response.success) {
                    // Revert if failed
                    _isFollowing.value = !_isFollowing.value
                }
            } catch (e: Exception) {
                // Revert if failed
                _isFollowing.value = !_isFollowing.value
                }
        }
    }
    
    fun showComments() {
        _showComments.value = true
    }
    
    fun hideComments() {
        _showComments.value = false
    }
    
    fun showShare() {
        _showShare.value = true
    }
    
    fun hideShare() {
        _showShare.value = false
    }
    
    fun refreshVideos() {
        currentPage = 0
        _videos.value = emptyList()
        _currentVideoIndex.value = 0
        loadVideos()
    }
    
    fun clearError() {
        _error.value = null
    }
    
    // MARK: - Private Methods
    private fun updateVideoState() {
        val video = _currentVideo.value ?: return
        
        // Update like and follow state based on current video
        _isLiked.value = video.isLiked
        _isFollowing.value = video.author.isFollowing
    }
}

// MARK: - Data Models
enum class VideoTab(
    val value: String,
    val displayName: String
) {
    FOR_YOU("for_you", "For You"),
    TRENDING("trending", "Trending");
    
    val grpcType: xyz.sonet.app.grpc.proto.VideoTab
        get() = when (this) {
            FOR_YOU -> xyz.sonet.app.grpc.proto.VideoTab.VIDEO_TAB_FOR_YOU
            TRENDING -> xyz.sonet.app.grpc.proto.VideoTab.VIDEO_TAB_TRENDING
        }
}

data class VideoItem(
    val id: String,
    val videoUrl: String,
    val thumbnailUrl: String,
    val caption: String,
    val author: UserProfile,
    val likeCount: Int,
    val commentCount: Int,
    val shareCount: Int,
    val viewCount: Int,
    val duration: Long,
    val isLiked: Boolean,
    val isFollowing: Boolean,
    val timestamp: Date,
    val hashtags: List<String>,
    val music: VideoMusic?
) {
    companion object {
        fun from(grpcVideo: xyz.sonet.app.grpc.proto.VideoItem): VideoItem {
            return VideoItem(
                id = grpcVideo.videoId,
                videoUrl = grpcVideo.videoUrl,
                thumbnailUrl = grpcVideo.thumbnailUrl,
                caption = grpcVideo.caption,
                author = UserProfile.from(grpcVideo.author),
                likeCount = grpcVideo.likeCount.toInt(),
                commentCount = grpcVideo.commentCount.toInt(),
                shareCount = grpcVideo.shareCount.toInt(),
                viewCount = grpcVideo.viewCount.toInt(),
                duration = grpcVideo.duration,
                isLiked = grpcVideo.isLiked,
                isFollowing = grpcVideo.author.isFollowing,
                timestamp = Date(grpcVideo.timestamp.seconds * 1000),
                hashtags = grpcVideo.hashtagsList,
                music = if (grpcVideo.hasMusic) VideoMusic.from(grpcVideo.music) else null
            )
        }
    }
}

data class VideoMusic(
    val id: String,
    val title: String,
    val artist: String,
    val audioUrl: String,
    val duration: Long
) {
    companion object {
        fun from(grpcMusic: xyz.sonet.app.grpc.proto.VideoMusic): VideoMusic {
            return VideoMusic(
                id = grpcMusic.musicId,
                title = grpcMusic.title,
                artist = grpcMusic.artist,
                audioUrl = grpcMusic.audioUrl,
                duration = grpcMusic.duration
            )
        }
    }
}

data class VideoComment(
    val id: String,
    val text: String,
    val author: UserProfile,
    val likeCount: Int,
    val isLiked: Boolean,
    val timestamp: Date,
    val replies: List<VideoComment>
) {
    companion object {
        fun from(grpcComment: xyz.sonet.app.grpc.proto.VideoComment): VideoComment {
            return VideoComment(
                id = grpcComment.commentId,
                text = grpcComment.text,
                author = UserProfile.from(grpcComment.author),
                likeCount = grpcComment.likeCount.toInt(),
                isLiked = grpcComment.isLiked,
                timestamp = Date(grpcComment.timestamp.seconds * 1000),
                replies = grpcComment.repliesList.map { from(it) }
            )
        }
    }
}