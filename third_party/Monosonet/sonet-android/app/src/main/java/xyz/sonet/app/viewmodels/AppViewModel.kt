package xyz.sonet.app.viewmodels

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import xyz.sonet.app.models.AppError
import xyz.sonet.app.utils.PackageUtils

class AppViewModel(application: Application) : AndroidViewModel(application) {
    
    // State flows
    private val _isInitialized = MutableStateFlow(false)
    val isInitialized: StateFlow<Boolean> = _isInitialized.asStateFlow()
    
    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()
    
    private val _currentError = MutableStateFlow<AppError?>(null)
    val currentError: StateFlow<AppError?> = _currentError.asStateFlow()
    
    private val _appVersion = MutableStateFlow("")
    val appVersion: StateFlow<String> = _appVersion.asStateFlow()
    
    private val _buildNumber = MutableStateFlow("")
    val buildNumber: StateFlow<String> = _buildNumber.asStateFlow()
    
    private val _isDebugMode = MutableStateFlow(false)
    val isDebugMode: StateFlow<Boolean> = _isDebugMode.asStateFlow()
    
    init {
        setupAppInfo()
    }
    
    fun initialize() {
        viewModelScope.launch {
            _isLoading.value = true
            
            try {
                // Initialize core services
                initializeCoreServices()
                
                // Load user preferences
                loadUserPreferences()
                
                // Setup background services
                setupBackgroundServices()
                
                _isInitialized.value = true
            } catch (error: Exception) {
                _currentError.value = AppError.InitializationFailed(error)
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    fun reset() {
        _isInitialized.value = false
        _isLoading.value = false
        _currentError.value = null
    }
    
    private fun setupAppInfo() {
        val context = getApplication<Application>()
        _appVersion.value = PackageUtils.getVersionName(context)
        _buildNumber.value = PackageUtils.getVersionCode(context).toString()
        _isDebugMode.value = PackageUtils.isDebugMode(context)
    }
    
    private suspend fun initializeCoreServices() {
        // Initialize networking, caching, etc.
        kotlinx.coroutines.delay(100) // Simulate initialization
    }
    
    private suspend fun loadUserPreferences() {
        // Load user preferences from SharedPreferences or other storage
        kotlinx.coroutines.delay(50) // Simulate loading
    }
    
    private fun setupBackgroundServices() {
        // Setup background refresh, notifications, etc.
    }
}

// App Error Types
sealed class AppError : Exception() {
    object InitializationFailed : AppError() {
        override val message: String = "Failed to initialize app"
    }
    
    object NetworkError : AppError() {
        override val message: String = "Network connection error"
    }
    
    object AuthenticationFailed : AppError() {
        override val message: String = "Authentication failed"
    }
    
    object DataCorruption : AppError() {
        override val message: String = "Data corruption detected"
    }
    
    object Unknown : AppError() {
        override val message: String = "An unknown error occurred"
    }
    
    data class InitializationFailed(override val cause: Throwable) : AppError() {
        override val message: String = "Failed to initialize app: ${cause.message}"
    }
    
    data class NetworkError(override val cause: Throwable) : AppError() {
        override val message: String = "Network error: ${cause.message}"
    }
}