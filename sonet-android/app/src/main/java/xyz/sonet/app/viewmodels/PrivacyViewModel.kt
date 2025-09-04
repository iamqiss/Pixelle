package xyz.sonet.app.viewmodels

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import java.util.Date

class PrivacyViewModel(
    private val grpcClient: SonetGRPCClient = SonetGRPCClient(Environment.DEV)
) : ViewModel() {

    // Location privacy
    private val _locationServicesEnabled = MutableStateFlow(false)
    val locationServicesEnabled: StateFlow<Boolean> = _locationServicesEnabled

    private val _sharePreciseLocation = MutableStateFlow(false)
    val sharePreciseLocation: StateFlow<Boolean> = _sharePreciseLocation

    private val _allowLocationOnPosts = MutableStateFlow(false)
    val allowLocationOnPosts: StateFlow<Boolean> = _allowLocationOnPosts

    private val _defaultGeotagPrivacy = MutableStateFlow(PostPrivacy.FRIENDS)
    val defaultGeotagPrivacy: StateFlow<PostPrivacy> = _defaultGeotagPrivacy

    private val _locationHistory = MutableStateFlow<List<LocationHistoryItem>>(emptyList())
    val locationHistory: StateFlow<List<LocationHistoryItem>> = _locationHistory

    // Third-party apps
    private val _connectedApps = MutableStateFlow<List<ConnectedApp>>(emptyList())
    val connectedApps: StateFlow<List<ConnectedApp>> = _connectedApps

    // Data export
    private val _previousExports = MutableStateFlow<List<DataExportRecord>>(emptyList())
    val previousExports: StateFlow<List<DataExportRecord>> = _previousExports

    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading

    private val _message = MutableStateFlow("")
    val message: StateFlow<String> = _message

    private val _error = MutableStateFlow("")
    val error: StateFlow<String> = _error

    fun loadAll() {
        loadLocationPrivacy()
        loadConnectedApps()
        loadPreviousExports()
    }

    // Location
    fun loadLocationPrivacy() = viewModelScope.launch {
        _isLoading.value = true
        try {
            val res = grpcClient.getLocationPrivacy("current_user")
            _locationServicesEnabled.value = res.locationServicesEnabled
            _sharePreciseLocation.value = res.sharePreciseLocation
            _allowLocationOnPosts.value = res.allowLocationOnPosts
            _defaultGeotagPrivacy.value = PostPrivacy.from(res.defaultGeotagPrivacy)
            _locationHistory.value = res.locationHistory.map { LocationHistoryItem(it.id, it.name, it.timestamp) }
        } catch (t: Throwable) {
            _error.value = "Failed to load location privacy: ${t.message}"
        } finally { _isLoading.value = false }
    }

    fun setLocationServicesEnabled(enabled: Boolean) = updatePrivacySetting("location_services_enabled", enabled.toString()) {
        _locationServicesEnabled.value = enabled
    }

    fun setSharePreciseLocation(enabled: Boolean) = updatePrivacySetting("share_precise_location", enabled.toString()) {
        _sharePreciseLocation.value = enabled
    }

    fun setAllowLocationOnPosts(enabled: Boolean) = updatePrivacySetting("allow_location_on_posts", enabled.toString()) {
        _allowLocationOnPosts.value = enabled
    }

    fun setDefaultGeotagPrivacy(privacy: PostPrivacy) = updatePrivacySetting("default_geotag_privacy", privacy.key) {
        _defaultGeotagPrivacy.value = privacy
    }

    fun clearLocationHistory() = viewModelScope.launch {
        try {
            val res = grpcClient.clearLocationHistory("current_user")
            if (res.success) {
                _locationHistory.value = emptyList()
                _message.value = "Location history cleared"
            } else {
                _error.value = res.errorMessage
            }
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }

    // Third-party apps
    fun loadConnectedApps() = viewModelScope.launch {
        try {
            val res = grpcClient.getConnectedApps("current_user")
            if (res.success) _connectedApps.value = res.apps.map { ConnectedApp(it.id, it.name, it.scopes, it.lastUsed) }
            else _error.value = res.errorMessage
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }

    fun revokeApp(appId: String) = viewModelScope.launch {
        try {
            val res = grpcClient.revokeConnectedApp("current_user", appId)
            if (res.success) {
                _connectedApps.value = _connectedApps.value.filterNot { it.id == appId }
                _message.value = "App access revoked"
            } else _error.value = res.errorMessage
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }

    // Data export
    fun loadPreviousExports() = viewModelScope.launch {
        try {
            val res = grpcClient.getDataExports("current_user")
            if (res.success) _previousExports.value = res.exports.map { DataExportRecord(it.id, it.requestedAt, DataExportStatus.from(it.status)) }
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }

    fun requestExport(includeMedia: Boolean, includeMessages: Boolean, includeConnections: Boolean, format: DataExportFormat, range: DataExportRange) = viewModelScope.launch {
        try {
            val res = grpcClient.requestDataExport(
                userId = "current_user",
                includeMedia = includeMedia,
                includeMessages = includeMessages,
                includeConnections = includeConnections,
                format = format.key,
                range = range.key
            )
            if (res.success) {
                _message.value = "Export requested"
                loadPreviousExports()
            } else _error.value = res.errorMessage
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }

    fun downloadExport(exportId: String) = viewModelScope.launch {
        try {
            val res = grpcClient.downloadDataExport("current_user", exportId)
            if (res.success) _message.value = "Download started" else _error.value = res.errorMessage
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }

    private fun updatePrivacySetting(key: String, value: String, onSuccess: () -> Unit) = viewModelScope.launch {
        try {
            val res = grpcClient.updatePrivacySetting("current_user", key, value)
            if (res.success) onSuccess() else _error.value = res.errorMessage
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }
}

// Simple models mirroring iOS additions

data class LocationHistoryItem(val id: String, val name: String, val timestamp: Date)

data class ConnectedApp(val id: String, val name: String, val scopes: List<String>, val lastUsed: Date)

data class DataExportRecord(val id: String, val requestedAt: Date, val status: DataExportStatus)

enum class DataExportStatus(val key: String) { PROCESSING("processing"), READY("ready"), FAILED("failed"); companion object { fun from(k: String) = values().firstOrNull { it.key == k } ?: PROCESSING } }

enum class DataExportFormat(val key: String) { JSON("json"), CSV("csv"), HTML("html") }

enum class DataExportRange(val key: String) { LAST_MONTH("last_month"), LAST_YEAR("last_year"), ALL_TIME("all_time") }

enum class PostPrivacy(val key: String, val displayName: String) {
    PUBLIC("public", "Public"), FRIENDS("friends", "Friends Only"), PRIVATE("private", "Private");
    companion object { fun from(k: String) = values().firstOrNull { it.key == k } ?: FRIENDS }
}

// Placeholder client & env to match existing pattern
enum class Environment { DEV }
class SonetGRPCClient(private val env: Environment) {
    data class LocationPrivacyRes(val locationServicesEnabled: Boolean, val sharePreciseLocation: Boolean, val allowLocationOnPosts: Boolean, val defaultGeotagPrivacy: String, val locationHistory: List<LocationItem>)
    data class LocationItem(val id: String, val name: String, val timestamp: Date)
    data class BoolRes(val success: Boolean, val errorMessage: String = "")
    data class AppsRes(val success: Boolean, val apps: List<AppItem> = emptyList(), val errorMessage: String = "")
    data class AppItem(val id: String, val name: String, val scopes: List<String>, val lastUsed: Date)
    data class ExportsRes(val success: Boolean, val exports: List<ExportItem> = emptyList(), val errorMessage: String = "")
    data class ExportItem(val id: String, val requestedAt: Date, val status: String)

    suspend fun getLocationPrivacy(userId: String): LocationPrivacyRes = LocationPrivacyRes(false, false, false, "friends", emptyList())
    suspend fun clearLocationHistory(userId: String): BoolRes = BoolRes(true)
    suspend fun updatePrivacySetting(userId: String, key: String, value: String): BoolRes = BoolRes(true)

    suspend fun getConnectedApps(userId: String): AppsRes = AppsRes(true)
    suspend fun revokeConnectedApp(userId: String, appId: String): BoolRes = BoolRes(true)

    suspend fun getDataExports(userId: String): ExportsRes = ExportsRes(true)
    suspend fun requestDataExport(userId: String, includeMedia: Boolean, includeMessages: Boolean, includeConnections: Boolean, format: String, range: String): BoolRes = BoolRes(true)
    suspend fun downloadDataExport(userId: String, exportId: String): BoolRes = BoolRes(true)
}