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

class NotificationsViewModel(application: Application) : AndroidViewModel(application) {
    
    // MARK: - State Flows
    private val _notifications = MutableStateFlow<List<NotificationItem>>(emptyList())
    val notifications: StateFlow<List<NotificationItem>> = _notifications.asStateFlow()
    
    private val _filteredNotifications = MutableStateFlow<List<NotificationItem>>(emptyList())
    val filteredNotifications: StateFlow<List<NotificationItem>> = _filteredNotifications.asStateFlow()
    
    private val _selectedFilter = MutableStateFlow(NotificationFilter.ALL)
    val selectedFilter: StateFlow<NotificationFilter> = _selectedFilter.asStateFlow()
    
    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()
    
    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()
    
    private val _unreadCount = MutableStateFlow(0)
    val unreadCount: StateFlow<Int> = _unreadCount.asStateFlow()
    
    private val _showAppUpdate = MutableStateFlow(false)
    val showAppUpdate: StateFlow<Boolean> = _showAppUpdate.asStateFlow()
    
    private val _appUpdateInfo = MutableStateFlow<AppUpdateInfo?>(null)
    val appUpdateInfo: StateFlow<AppUpdateInfo?> = _appUpdateInfo.asStateFlow()
    
    private val _notificationPreferences = MutableStateFlow(NotificationPreferences())
    val notificationPreferences: StateFlow<NotificationPreferences> = _notificationPreferences.asStateFlow()
    
    // MARK: - Private Properties
    private val grpcClient = SonetGRPCClient(application, SonetConfiguration.development)
    private var notificationStream: kotlinx.coroutines.Job? = null
    private var refreshJob: kotlinx.coroutines.Job? = null
    
    // MARK: - Notification Filters
    enum class NotificationFilter(val title: String, val icon: String) {
        ALL("All", "bell"),
        MENTIONS("Mentions", "at"),
        LIKES("Likes", "heart"),
        REPOSTS("Reposts", "arrow_2_squarepath"),
        FOLLOWS("Follows", "person_badge_plus"),
        REPLIES("Replies", "bubble_left"),
        HASHTAGS("Hashtags", "number"),
        APP_UPDATES("App Updates", "app_badge")
    }
    
    // MARK: - Initialization
    init {
        setupNotificationStream()
        setupRefreshTimer()
        loadNotificationPreferences()
        requestNotificationPermissions()
    }
    
    // MARK: - Public Methods
    fun loadNotifications() {
        viewModelScope.launch {
            _isLoading.value = true
            _error.value = null
            
            try {
                val response = grpcClient.getNotifications(0, 50)
                _notifications.value = response.notificationsList.map { NotificationItem.from(it) }
                applyFilter()
                updateUnreadCount()
            } catch (error: Exception) {
                _error.value = "Failed to load notifications: ${error.message}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    fun refreshNotifications() {
        loadNotifications()
    }
    
    fun selectFilter(filter: NotificationFilter) {
        _selectedFilter.value = filter
        applyFilter()
    }
    
    fun markAsRead(notification: NotificationItem) {
        if (notification.isRead) return
        
        viewModelScope.launch {
            try {
                val response = grpcClient.markNotificationAsRead(notification.id)
                if (response.success) {
                    val currentNotifications = _notifications.value.toMutableList()
                    val index = currentNotifications.indexOfFirst { it.id == notification.id }
                    if (index != -1) {
                        currentNotifications[index] = notification.copy(isRead = true)
                        _notifications.value = currentNotifications
                        updateUnreadCount()
                    }
                }
            } catch (error: Exception) {
                // Handle error silently
            }
        }
    }
    
    fun markAllAsRead() {
        viewModelScope.launch {
            try {
                val response = grpcClient.markAllNotificationsAsRead()
                if (response.success) {
                    val currentNotifications = _notifications.value.map { it.copy(isRead = true) }
                    _notifications.value = currentNotifications
                    updateUnreadCount()
                }
            } catch (error: Exception) {
                // Handle error silently
            }
        }
    }
    
    fun deleteNotification(notification: NotificationItem) {
        viewModelScope.launch {
            try {
                val response = grpcClient.deleteNotification(notification.id)
                if (response.success) {
                    val currentNotifications = _notifications.value.toMutableList()
                    currentNotifications.removeAll { it.id == notification.id }
                    _notifications.value = currentNotifications
                    applyFilter()
                    updateUnreadCount()
                }
            } catch (error: Exception) {
                // Handle error silently
            }
        }
    }
    
    fun clearAllNotifications() {
        viewModelScope.launch {
            try {
                val response = grpcClient.clearAllNotifications()
                if (response.success) {
                    _notifications.value = emptyList()
                    applyFilter()
                    updateUnreadCount()
                }
            } catch (error: Exception) {
                // Handle error silently
            }
        }
    }
    
    fun checkForAppUpdates() {
        viewModelScope.launch {
            try {
                val response = grpcClient.checkForAppUpdates()
                if (response.hasUpdate) {
                    _appUpdateInfo.value = AppUpdateInfo.from(response)
                    _showAppUpdate.value = true
                }
            } catch (error: Exception) {
                // Handle error silently
            }
        }
    }
    
    fun updateNotificationPreferences() {
        viewModelScope.launch {
            try {
                val response = grpcClient.updateNotificationPreferences(_notificationPreferences.value.toGRPC())
                if (response.success) {
                    // Preferences updated successfully
                }
            } catch (error: Exception) {
                // Handle error silently
            }
        }
    }
    
    // MARK: - Private Methods
    private fun setupNotificationStream() {
        notificationStream = viewModelScope.launch {
            try {
                grpcClient.notificationStream().collect { notification ->
                    handleNewNotification(notification)
                }
            } catch (error: Exception) {
                _error.value = "Notification stream error: ${error.message}"
            }
        }
    }
    
    private fun setupRefreshTimer() {
        refreshJob = viewModelScope.launch {
            kotlinx.coroutines.delay(30000) // 30 seconds
            refreshNotifications()
        }
    }
    
    private fun handleNewNotification(notification: xyz.sonet.app.grpc.proto.Notification) {
        val newNotification = NotificationItem.from(notification)
        
        // Add to beginning of list
        val currentNotifications = _notifications.value.toMutableList()
        currentNotifications.add(0, newNotification)
        _notifications.value = currentNotifications
        
        // Apply current filter
        applyFilter()
        
        // Update unread count
        updateUnreadCount()
        
        // Show local notification if app is in background
        // This would be implemented with WorkManager or AlarmManager
    }
    
    private fun applyFilter() {
        val currentNotifications = _notifications.value
        
        _filteredNotifications.value = when (_selectedFilter.value) {
            NotificationFilter.ALL -> currentNotifications
            NotificationFilter.MENTIONS -> currentNotifications.filter { it.type == NotificationType.MENTION }
            NotificationFilter.LIKES -> currentNotifications.filter { it.type == NotificationType.LIKE }
            NotificationFilter.REPOSTS -> currentNotifications.filter { it.type == NotificationType.REPOST }
            NotificationFilter.FOLLOWS -> currentNotifications.filter { it.type == NotificationType.FOLLOW }
            NotificationFilter.REPLIES -> currentNotifications.filter { it.type == NotificationType.REPLY }
            NotificationFilter.HASHTAGS -> currentNotifications.filter { it.type == NotificationType.HASHTAG }
            NotificationFilter.APP_UPDATES -> currentNotifications.filter { it.type == NotificationType.APP_UPDATE }
        }
    }
    
    private fun updateUnreadCount() {
        _unreadCount.value = _notifications.value.count { !it.isRead }
    }
    
    private fun loadNotificationPreferences() {
        // Load from SharedPreferences
        // This would be implemented for preference persistence
    }
    
    private fun requestNotificationPermissions() {
        // Request notification permissions
        // This would be implemented with the notification permission request
    }
    
    override fun onCleared() {
        super.onCleared()
        notificationStream?.cancel()
        refreshJob?.cancel()
    }
}

// MARK: - Data Models
data class NotificationItem(
    val id: String,
    val type: NotificationType,
    val title: String,
    val message: String,
    val timestamp: Long,
    val isRead: Boolean,
    val sender: UserProfile?,
    val targetNote: Note?,
    val targetUser: UserProfile?,
    val metadata: Map<String, String>
) {
    companion object {
        fun from(grpcNotification: xyz.sonet.app.grpc.proto.Notification): NotificationItem {
            return NotificationItem(
                id = grpcNotification.notificationId,
                type = NotificationType.from(grpcNotification.type),
                title = grpcNotification.title,
                message = grpcNotification.message,
                timestamp = grpcNotification.timestamp.seconds * 1000,
                isRead = grpcNotification.isRead,
                sender = if (grpcNotification.hasSender) UserProfile.from(grpcNotification.sender) else null,
                targetNote = if (grpcNotification.hasTargetNote) Note.from(grpcNotification.targetNote) else null,
                targetUser = if (grpcNotification.hasTargetUser) UserProfile.from(grpcNotification.targetUser) else null,
                metadata = grpcNotification.metadataMap
            )
        }
    }
}

enum class NotificationType(val icon: String) {
    LIKE("heart_fill"),
    REPLY("bubble_left_fill"),
    REPOST("arrow_2_squarepath_fill"),
    FOLLOW("person_badge_plus_fill"),
    MENTION("at"),
    HASHTAG("number"),
    APP_UPDATE("app_badge_fill"),
    SYSTEM("bell_fill");
    
    companion object {
        fun from(grpcType: xyz.sonet.app.grpc.proto.NotificationType): NotificationType {
            return when (grpcType) {
                xyz.sonet.app.grpc.proto.NotificationType.NOTIFICATION_TYPE_LIKE -> LIKE
                xyz.sonet.app.grpc.proto.NotificationType.NOTIFICATION_TYPE_REPLY -> REPLY
                xyz.sonet.app.grpc.proto.NotificationType.NOTIFICATION_TYPE_REPOST -> REPOST
                xyz.sonet.app.grpc.proto.NotificationType.NOTIFICATION_TYPE_FOLLOW -> FOLLOW
                xyz.sonet.app.grpc.proto.NotificationType.NOTIFICATION_TYPE_MENTION -> MENTION
                xyz.sonet.app.grpc.proto.NotificationType.NOTIFICATION_TYPE_HASHTAG -> HASHTAG
                xyz.sonet.app.grpc.proto.NotificationType.NOTIFICATION_TYPE_APP_UPDATE -> APP_UPDATE
                xyz.sonet.app.grpc.proto.NotificationType.NOTIFICATION_TYPE_SYSTEM -> SYSTEM
                else -> SYSTEM
            }
        }
    }
}

data class AppUpdateInfo(
    val id: String,
    val version: String,
    val title: String,
    val description: String,
    val changelog: List<String>,
    val isRequired: Boolean,
    val downloadUrl: String,
    val releaseDate: Long
) {
    companion object {
        fun from(grpcUpdate: xyz.sonet.app.grpc.proto.AppUpdate): AppUpdateInfo {
            return AppUpdateInfo(
                id = grpcUpdate.updateId,
                version = grpcUpdate.version,
                title = grpcUpdate.title,
                description = grpcUpdate.description,
                changelog = grpcUpdate.changelogList,
                isRequired = grpcUpdate.isRequired,
                downloadUrl = grpcUpdate.downloadUrl,
                releaseDate = grpcUpdate.releaseDate.seconds * 1000
            )
        }
    }
}

data class NotificationPreferences(
    val pushEnabled: Boolean = true,
    val emailEnabled: Boolean = true,
    val likeNotifications: Boolean = true,
    val replyNotifications: Boolean = true,
    val repostNotifications: Boolean = true,
    val followNotifications: Boolean = true,
    val mentionNotifications: Boolean = true,
    val hashtagNotifications: Boolean = true,
    val appUpdateNotifications: Boolean = true,
    val systemNotifications: Boolean = true,
    val quietHoursEnabled: Boolean = false,
    val quietHoursStart: Long = 0L,
    val quietHoursEnd: Long = 0L
) {
    fun toGRPC(): xyz.sonet.app.grpc.proto.NotificationPreferences {
        return xyz.sonet.app.grpc.proto.NotificationPreferences.newBuilder()
            .setPushEnabled(pushEnabled)
            .setEmailEnabled(emailEnabled)
            .setLikeNotifications(likeNotifications)
            .setReplyNotifications(replyNotifications)
            .setRepostNotifications(repostNotifications)
            .setFollowNotifications(followNotifications)
            .setMentionNotifications(mentionNotifications)
            .setHashtagNotifications(hashtagNotifications)
            .setAppUpdateNotifications(appUpdateNotifications)
            .setSystemNotifications(systemNotifications)
            .setQuietHoursEnabled(quietHoursEnabled)
            .setQuietHoursStart(Timestamp.newBuilder().setSeconds(quietHoursStart / 1000).build())
            .setQuietHoursEnd(Timestamp.newBuilder().setSeconds(quietHoursEnd / 1000).build())
            .build()
    }
}