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

class ProfileViewModel(application: Application) : AndroidViewModel(application) {
    
    // MARK: - State Flows
    private val _userProfile = MutableStateFlow<UserProfile?>(null)
    val userProfile: StateFlow<UserProfile?> = _userProfile.asStateFlow()
    
    private val _selectedTab = MutableStateFlow(ProfileTab.POSTS)
    val selectedTab: StateFlow<ProfileTab> = _selectedTab.asStateFlow()
    
    private val _posts = MutableStateFlow<List<Note>>(emptyList())
    val posts: StateFlow<List<Note>> = _posts.asStateFlow()
    
    private val _replies = MutableStateFlow<List<Note>>(emptyList())
    val replies: StateFlow<List<Note>> = _replies.asStateFlow()
    
    private val _media = MutableStateFlow<List<Note>>(emptyList())
    val media: StateFlow<List<Note>> = _media.asStateFlow()
    
    private val _likes = MutableStateFlow<List<Note>>(emptyList())
    val likes: StateFlow<List<Note>> = _likes.asStateFlow()
    
    private val _isFollowing = MutableStateFlow(false)
    val isFollowing: StateFlow<Boolean> = _isFollowing.asStateFlow()
    
    private val _isBlocked = MutableStateFlow(false)
    val isBlocked: StateFlow<Boolean> = _isBlocked.asStateFlow()
    
    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()
    
    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()
    
    // MARK: - Private Properties
    private val grpcClient = SonetGRPCClient(application, SonetConfiguration.development)
    private var userId: String? = null
    private val sessionViewModel: SessionViewModel = SessionViewModel(application)

    // Author cache for resolving note authorId -> UserProfile
    private val _authorCache = MutableStateFlow<Map<String, UserProfile>>(emptyMap())
    val authorCache: StateFlow<Map<String, UserProfile>> = _authorCache.asStateFlow()

    private fun cacheAuthor(profile: UserProfile) {
        _authorCache.value = _authorCache.value + (profile.userId to profile)
    }

    fun resolveAuthor(authorId: String) {
        if (_authorCache.value.containsKey(authorId)) return
        viewModelScope.launch {
            try {
                val profile = grpcClient.getUserProfile(authorId)
                cacheAuthor(profile)
            } catch (_: Exception) {
                // Ignore failures silently for UI; can log if needed
            }
        }
    }
    
    // MARK: - Profile Tabs
    enum class ProfileTab(val title: String, val icon: String) {
        POSTS("Posts", "bubble_left"),
        REPLIES("Replies", "arrowshape_turn_up_left"),
        MEDIA("Media", "photo"),
        LIKES("Likes", "heart")
    }
    
    // MARK: - Public Methods
    fun loadProfile(userId: String) {
        this.userId = userId
        loadUserProfile()
        loadProfileContent()
    }
    
    fun selectTab(tab: ProfileTab) {
        _selectedTab.value = tab
        loadContentForSelectedTab()
    }
    
    fun toggleFollow() {
        val profile = _userProfile.value ?: return
        if (profile.userId == sessionViewModel.currentUser.value?.id) return
        
        viewModelScope.launch {
            try {
                if (_isFollowing.value) {
                    // Unfollow user
                    val response = grpcClient.unfollowUser(profile.userId)
                    if (response.success) {
                        _isFollowing.value = false
                        _userProfile.value = profile.copy(followerCount = profile.followerCount - 1)
                    }
                } else {
                    // Follow user
                    val response = grpcClient.followUser(profile.userId)
                    if (response.success) {
                        _isFollowing.value = true
                        _userProfile.value = profile.copy(followerCount = profile.followerCount + 1)
                    }
                }
            } catch (error: Exception) {
                _error.value = "Failed to ${if (_isFollowing.value) "unfollow" else "follow"} user"
            }
        }
    }
    
    fun blockUser() {
        val profile = _userProfile.value ?: return
        
        viewModelScope.launch {
            try {
                val response = grpcClient.blockUser(profile.userId)
                if (response.success) {
                    _isBlocked.value = true
                }
            } catch (error: Exception) {
                _error.value = "Failed to block user"
            }
        }
    }
    
    fun unblockUser() {
        val profile = _userProfile.value ?: return
        
        viewModelScope.launch {
            try {
                val response = grpcClient.unblockUser(profile.userId)
                if (response.success) {
                    _isBlocked.value = false
                }
            } catch (error: Exception) {
                _error.value = "Failed to unblock user"
            }
        }
    }
    
    fun refreshProfile() {
        userId?.let { loadProfile(it) }
    }
    
    // MARK: - Private Methods
    private fun loadUserProfile() {
        val currentUserId = userId ?: return
        
        viewModelScope.launch {
            _isLoading.value = true
            _error.value = null
            
            try {
                val profile = grpcClient.getUserProfile(currentUserId)
                _userProfile.value = profile
                _isFollowing.value = profile.isFollowing
                _isBlocked.value = profile.isBlocked
                cacheAuthor(profile)
            } catch (error: Exception) {
                _error.value = "Failed to load profile: ${error.message}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    private fun loadProfileContent() {
        loadContentForSelectedTab()
    }
    
    private fun loadContentForSelectedTab() {
        val currentUserId = userId ?: return
        
        viewModelScope.launch {
            try {
                when (_selectedTab.value) {
                    ProfileTab.POSTS -> {
                        val notes = grpcClient.getUserPosts(currentUserId, 0, 20)
                        _posts.value = notes
                        notes.forEach { note -> if (note.authorId != currentUserId) resolveAuthor(note.authorId) }
                    }
                    
                    ProfileTab.REPLIES -> {
                        val notes = grpcClient.getUserReplies(currentUserId, 0, 20)
                        _replies.value = notes
                        notes.forEach { note -> if (note.authorId != currentUserId) resolveAuthor(note.authorId) }
                    }
                    
                    ProfileTab.MEDIA -> {
                        val notes = grpcClient.getUserMedia(currentUserId, 0, 20)
                        _media.value = notes
                        notes.forEach { note -> if (note.authorId != currentUserId) resolveAuthor(note.authorId) }
                    }
                    
                    ProfileTab.LIKES -> {
                        val notes = grpcClient.getUserLikes(currentUserId, 0, 20)
                        _likes.value = notes
                        notes.forEach { note -> resolveAuthor(note.authorId) }
                    }
                }
            } catch (error: Exception) {
                _error.value = "Failed to load ${_selectedTab.value.title.lowercase()}: ${error.message}"
            }
        }
    }
}

// MARK: - Profile Actions
data class ProfileActions(
    val canFollow: Boolean = true,
    val canMessage: Boolean = true,
    val canBlock: Boolean = true,
    val canReport: Boolean = true,
    val isOwnProfile: Boolean = false
)

// MARK: - Profile Stats
data class ProfileStats(
    val posts: Int = 0,
    val followers: Int = 0,
    val following: Int = 0,
    val likes: Int = 0,
    val joinedDate: Long = 0,
    val location: String? = null,
    val website: String? = null
)