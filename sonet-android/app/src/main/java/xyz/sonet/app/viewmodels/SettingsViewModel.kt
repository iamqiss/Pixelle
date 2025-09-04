package xyz.sonet.app.viewmodels

import android.app.Application
import android.content.SharedPreferences
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

class SettingsViewModel(application: Application) : AndroidViewModel(application) {
    
    // MARK: - State Flows
    private val _userProfile = MutableStateFlow<UserProfile?>(null)
    val userProfile: StateFlow<UserProfile?> = _userProfile.asStateFlow()
    
    private val _isDarkMode = MutableStateFlow(false)
    val isDarkMode: StateFlow<Boolean> = _isDarkMode.asStateFlow()
    
    private val _notificationsEnabled = MutableStateFlow(true)
    val notificationsEnabled: StateFlow<Boolean> = _notificationsEnabled.asStateFlow()
    
    private val _pushNotificationsEnabled = MutableStateFlow(true)
    val pushNotificationsEnabled: StateFlow<Boolean> = _pushNotificationsEnabled.asStateFlow()
    
    private val _emailNotificationsEnabled = MutableStateFlow(true)
    val emailNotificationsEnabled: StateFlow<Boolean> = _emailNotificationsEnabled.asStateFlow()
    
    private val _inAppNotificationsEnabled = MutableStateFlow(true)
    val inAppNotificationsEnabled: StateFlow<Boolean> = _inAppNotificationsEnabled.asStateFlow()
    
    private val _accountVisibility = MutableStateFlow(AccountVisibility.PUBLIC)
    val accountVisibility: StateFlow<AccountVisibility> = _accountVisibility.asStateFlow()
    
    private val _contentLanguage = MutableStateFlow("English")
    val contentLanguage: StateFlow<String> = _contentLanguage.asStateFlow()
    
    private val _contentFiltering = MutableStateFlow(ContentFiltering.MODERATE)
    val contentFiltering: StateFlow<ContentFiltering> = _contentFiltering.asStateFlow()
    
    private val _autoPlayVideos = MutableStateFlow(true)
    val autoPlayVideos: StateFlow<Boolean> = _autoPlayVideos.asStateFlow()
    
    private val _dataUsage = MutableStateFlow(DataUsage.STANDARD)
    val dataUsage: StateFlow<DataUsage> = _dataUsage.asStateFlow()
    
    private val _storageUsage = MutableStateFlow(StorageUsage())
    val storageUsage: StateFlow<StorageUsage> = _storageUsage.asStateFlow()
    
    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()
    
    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()
    
    private val _showLogoutAlert = MutableStateFlow(false)
    val showLogoutAlert: StateFlow<Boolean> = _showLogoutAlert.asStateFlow()
    
    private val _showDeleteAccountAlert = MutableStateFlow(false)
    val showDeleteAccountAlert: StateFlow<Boolean> = _showDeleteAccountAlert.asStateFlow()
    
    // MARK: - Private Properties
    private val grpcClient = SonetGRPCClient(application, SonetConfiguration.development)
    private val sharedPreferences: SharedPreferences = application.getSharedPreferences("sonet_settings", 0)
    
    // MARK: - Initialization
    init {
        loadUserProfile()
        loadSettings()
    }
    
    // MARK: - Public Methods
    fun loadUserProfile() {
        viewModelScope.launch {
            _isLoading.value = true
            _error.value = null
            
            try {
                val response = grpcClient.getUserProfile(userId = "current_user")
                _userProfile.value = UserProfile.from(response)
            } catch (e: Exception) {
                _error.value = "Failed to load profile: ${e.localizedMessage}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    fun loadSettings() {
        // Load from SharedPreferences
        _isDarkMode.value = sharedPreferences.getBoolean("isDarkMode", false)
        _notificationsEnabled.value = sharedPreferences.getBoolean("notificationsEnabled", true)
        _pushNotificationsEnabled.value = sharedPreferences.getBoolean("pushNotificationsEnabled", true)
        _emailNotificationsEnabled.value = sharedPreferences.getBoolean("emailNotificationsEnabled", true)
        _inAppNotificationsEnabled.value = sharedPreferences.getBoolean("inAppNotificationsEnabled", true)
        
        val visibilityString = sharedPreferences.getString("accountVisibility", "public")
        _accountVisibility.value = AccountVisibility.from(visibilityString ?: "public")
        
        val language = sharedPreferences.getString("contentLanguage", "English")
        _contentLanguage.value = language ?: "English"
        
        val filteringString = sharedPreferences.getString("contentFiltering", "moderate")
        _contentFiltering.value = ContentFiltering.from(filteringString ?: "moderate")
        
        _autoPlayVideos.value = sharedPreferences.getBoolean("autoPlayVideos", true)
        
        val dataUsageString = sharedPreferences.getString("dataUsage", "standard")
        _dataUsage.value = DataUsage.from(dataUsageString ?: "standard")
        
        loadStorageUsage()
    }
    
    fun toggleDarkMode() {
        _isDarkMode.value = !_isDarkMode.value
        sharedPreferences.edit().putBoolean("isDarkMode", _isDarkMode.value).apply()
        
        // Update app appearance
        // This would typically update the app theme
    }
    
    fun updateNotificationSettings() {
        sharedPreferences.edit()
            .putBoolean("notificationsEnabled", _notificationsEnabled.value)
            .putBoolean("pushNotificationsEnabled", _pushNotificationsEnabled.value)
            .putBoolean("emailNotificationsEnabled", _emailNotificationsEnabled.value)
            .putBoolean("inAppNotificationsEnabled", _inAppNotificationsEnabled.value)
            .apply()
        
        // Update server settings
        viewModelScope.launch {
            try {
                val request = UpdateNotificationPreferencesRequest.newBuilder()
                    .setPushEnabled(_pushNotificationsEnabled.value)
                    .setEmailEnabled(_emailNotificationsEnabled.value)
                    .setInAppEnabled(_inAppNotificationsEnabled.value)
                    .build()
                
                val response = grpcClient.updateNotificationPreferences(request = request)
                if (!response.success) {
                    _error.value = "Failed to update notification settings: ${response.errorMessage}"
                }
            } catch (e: Exception) {
                _error.value = "Failed to update notification settings: ${e.localizedMessage}"
            }
        }
    }
    
    fun updateAccountVisibility(visibility: AccountVisibility) {
        _accountVisibility.value = visibility
        sharedPreferences.edit().putString("accountVisibility", visibility.value).apply()
        
        viewModelScope.launch {
            try {
                val request = UpdatePrivacySettingsRequest.newBuilder()
                    .setAccountVisibility(visibility.grpcType)
                    .build()
                
                val response = grpcClient.updatePrivacySettings(request = request)
                if (!response.success) {
                    _error.value = "Failed to update privacy settings: ${response.errorMessage}"
                }
            } catch (e: Exception) {
                _error.value = "Failed to update privacy settings: ${e.localizedMessage}"
            }
        }
    }
    
    fun updateContentLanguage(language: String) {
        _contentLanguage.value = language
        sharedPreferences.edit().putString("contentLanguage", language).apply()
        
        // Update app localization
        // This would typically involve restarting the app or updating the bundle
    }
    
    fun updateContentFiltering(filtering: ContentFiltering) {
        _contentFiltering.value = filtering
        sharedPreferences.edit().putString("contentFiltering", filtering.value).apply()
        
        viewModelScope.launch {
            try {
                val request = UpdateContentPreferencesRequest.newBuilder()
                    .setContentFiltering(filtering.grpcType)
                    .build()
                
                val response = grpcClient.updateContentPreferences(request = request)
                if (!response.success) {
                    _error.value = "Failed to update content preferences: ${response.errorMessage}"
                }
            } catch (e: Exception) {
                _error.value = "Failed to update content preferences: ${e.localizedMessage}"
            }
        }
    }
    
    fun toggleAutoPlayVideos() {
        _autoPlayVideos.value = !_autoPlayVideos.value
        sharedPreferences.edit().putBoolean("autoPlayVideos", _autoPlayVideos.value).apply()
    }
    
    fun updateDataUsage(usage: DataUsage) {
        _dataUsage.value = usage
        sharedPreferences.edit().putString("dataUsage", usage.value).apply()
    }
    
    fun loadStorageUsage() {
        viewModelScope.launch {
            try {
                val response = grpcClient.getStorageUsage()
                _storageUsage.value = StorageUsage.from(response)
            } catch (e: Exception) {
                // Handle error silently
            }
        }
    }
    
    fun clearCache() {
        viewModelScope.launch {
            try {
                val response = grpcClient.clearCache()
                if (response.success) {
                    loadStorageUsage()
                } else {
                    _error.value = "Failed to clear cache: ${response.errorMessage}"
                }
            } catch (e: Exception) {
                _error.value = "Failed to clear cache: ${e.localizedMessage}"
            }
        }
    }
    
    fun downloadUserData() {
        viewModelScope.launch {
            try {
                val response = grpcClient.exportUserData()
                if (response.success) {
                    // Handle data download
                    // This would typically save to Downloads folder
                } else {
                    _error.value = "Failed to export data: ${response.errorMessage}"
                }
            } catch (e: Exception) {
                _error.value = "Failed to export data: ${e.localizedMessage}"
            }
        }
    }
    
    fun deleteAccount() {
        viewModelScope.launch {
            try {
                val response = grpcClient.deleteAccount()
                if (response.success) {
                    // Handle account deletion
                    // This would typically log out and return to login
                } else {
                    _error.value = "Failed to delete account: ${response.errorMessage}"
                }
            } catch (e: Exception) {
                _error.value = "Failed to delete account: ${e.localizedMessage}"
            }
        }
    }
    
    fun logout() {
        // Clear local data
        sharedPreferences.edit().clear().apply()
        
        // Clear other stored data
        // This would typically clear stored credentials
        
        // Navigate to login
        // This would be handled by the parent view
    }
    
    fun clearError() {
        _error.value = null
    }
    
    fun showLogoutAlert() {
        _showLogoutAlert.value = true
    }
    
    fun hideLogoutAlert() {
        _showLogoutAlert.value = false
    }
    
    fun showDeleteAccountAlert() {
        _showDeleteAccountAlert.value = true
    }
    
    fun hideDeleteAccountAlert() {
        _showDeleteAccountAlert.value = false
    }
}

// MARK: - Data Models
enum class AccountVisibility(val value: String, val displayName: String, val description: String) {
    PUBLIC("public", "Public", "Anyone can see your profile and posts"),
    FOLLOWERS("followers", "Followers Only", "Only your followers can see your posts"),
    PRIVATE("private", "Private", "Only you can see your posts");
    
    companion object {
        fun from(value: String): AccountVisibility {
            return values().find { it.value == value } ?: PUBLIC
        }
    }
    
    val grpcType: xyz.sonet.app.grpc.proto.AccountVisibility
        get() = when (this) {
            PUBLIC -> xyz.sonet.app.grpc.proto.AccountVisibility.ACCOUNT_VISIBILITY_PUBLIC
            FOLLOWERS -> xyz.sonet.app.grpc.proto.AccountVisibility.ACCOUNT_VISIBILITY_FOLLOWERS
            PRIVATE -> xyz.sonet.app.grpc.proto.AccountVisibility.ACCOUNT_VISIBILITY_PRIVATE
        }
}

enum class ContentFiltering(val value: String, val displayName: String, val description: String) {
    OFF("off", "Off", "Show all content"),
    MODERATE("moderate", "Moderate", "Filter some sensitive content"),
    STRICT("strict", "Strict", "Filter most sensitive content");
    
    companion object {
        fun from(value: String): ContentFiltering {
            return values().find { it.value == value } ?: MODERATE
        }
    }
    
    val grpcType: xyz.sonet.app.grpc.proto.ContentFiltering
        get() = when (this) {
            OFF -> xyz.sonet.app.grpc.proto.ContentFiltering.CONTENT_FILTERING_OFF
            MODERATE -> xyz.sonet.app.grpc.proto.ContentFiltering.CONTENT_FILTERING_MODERATE
            STRICT -> xyz.sonet.app.grpc.proto.ContentFiltering.CONTENT_FILTERING_STRICT
        }
}

enum class DataUsage(val value: String, val displayName: String, val description: String) {
    LOW("low", "Low", "Minimize data usage"),
    STANDARD("standard", "Standard", "Balanced data usage"),
    HIGH("high", "High", "Optimize for quality");
    
    companion object {
        fun from(value: String): DataUsage {
            return values().find { it.value == value } ?: STANDARD
        }
    }
}

data class StorageUsage(
    val totalStorage: Long = 0,
    val usedStorage: Long = 0,
    val cacheSize: Long = 0,
    val mediaSize: Long = 0,
    val appSize: Long = 0
) {
    val availableStorage: Long get() = totalStorage - usedStorage
    val usagePercentage: Double get() = if (totalStorage > 0) (usedStorage.toDouble() / totalStorage.toDouble()) * 100 else 0.0
    
    companion object {
        fun from(grpcStorage: xyz.sonet.app.grpc.proto.StorageUsage): StorageUsage {
            return StorageUsage(
                totalStorage = grpcStorage.totalStorage,
                usedStorage = grpcStorage.usedStorage,
                cacheSize = grpcStorage.cacheSize,
                mediaSize = grpcStorage.mediaSize,
                appSize = grpcStorage.appSize
            )
        }
    }
}

data class NotificationPreferences(
    val pushEnabled: Boolean = true,
    val emailEnabled: Boolean = true,
    val inAppEnabled: Boolean = true,
    val mentionsEnabled: Boolean = true,
    val likesEnabled: Boolean = true,
    val repostsEnabled: Boolean = true,
    val followsEnabled: Boolean = true,
    val directMessagesEnabled: Boolean = true
)

data class PrivacySettings(
    val accountVisibility: AccountVisibility = AccountVisibility.PUBLIC,
    val showEmail: Boolean = false,
    val showPhone: Boolean = false,
    val allowMentions: Boolean = true,
    val allowDirectMessages: Boolean = true,
    val allowFollowRequests: Boolean = true,
    val showOnlineStatus: Boolean = true,
    val showLastSeen: Boolean = true
)

data class ContentPreferences(
    val contentLanguage: String = "English",
    val contentFiltering: ContentFiltering = ContentFiltering.MODERATE,
    val autoPlayVideos: Boolean = true,
    val showSensitiveContent: Boolean = false,
    val showTrendingTopics: Boolean = true,
    val showSuggestedAccounts: Boolean = true
)