package xyz.sonet.app.viewmodels

import android.app.Application
import android.content.Context
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import xyz.sonet.app.models.SonetUser
import xyz.sonet.app.models.AuthenticationError
import xyz.sonet.app.utils.KeychainUtils
import xyz.sonet.app.utils.PreferenceUtils
import java.util.UUID

class SessionViewModel(application: Application) : AndroidViewModel(application) {
    
    // State flows
    private val _isAuthenticated = MutableStateFlow(false)
    val isAuthenticated: StateFlow<Boolean> = _isAuthenticated.asStateFlow()
    
    private val _currentUser = MutableStateFlow<SonetUser?>(null)
    val currentUser: StateFlow<SonetUser?> = _currentUser.asStateFlow()
    
    private val _isAuthenticating = MutableStateFlow(false)
    val isAuthenticating: StateFlow<Boolean> = _isAuthenticating.asStateFlow()
    
    private val _authenticationError = MutableStateFlow<AuthenticationError?>(null)
    val authenticationError: StateFlow<AuthenticationError?> = _authenticationError.asStateFlow()
    
    // Private properties
    private val keychainUtils = KeychainUtils(application)
    private val preferenceUtils = PreferenceUtils(application)
    private val grpcClient = SonetGRPCClient(application, SonetConfiguration.development)
    
    init {
        loadStoredSession()
    }
    
    fun initialize() {
        // Check for existing valid session
        validateStoredSession()
    }
    
    fun authenticate(username: String, password: String) {
        viewModelScope.launch {
            _isAuthenticating.value = true
            _authenticationError.value = null
            
            try {
                // Perform authentication with Sonet API (now returns tokens)
                val authResp = grpcClient.authenticate(username, password)
                val user = SonetUser(
                    id = authResp.userProfile.userId,
                    username = authResp.userProfile.username,
                    displayName = authResp.userProfile.displayName,
                    avatarUrl = authResp.userProfile.avatarUrl,
                    isVerified = authResp.userProfile.isVerified,
                    createdAt = authResp.userProfile.createdAt.seconds * 1000
                )

                // Store session securely (user + tokens)
                storeSession(user, authResp.accessToken, authResp.refreshToken)
                
                // Update state
                _currentUser.value = user
                _isAuthenticated.value = true
                
                // Clear any previous errors
                _authenticationError.value = null
                
            } catch (error: Exception) {
                _authenticationError.value = AuthenticationError.InvalidCredentials
            } finally {
                _isAuthenticating.value = false
            }
        }
    }
    
    fun signOut() {
        viewModelScope.launch {
            try {
                // Clear stored session
                clearStoredSession()
                
                // Clear current user
                _currentUser.value = null
                _isAuthenticated.value = false
                
                // Reset any cached data
                clearCachedData()
                
            } catch (error: Exception) {
                // Log error but don't show to user
                error.printStackTrace()
            }
        }
    }
    
    fun refreshSession() {
        viewModelScope.launch {
            val user = _currentUser.value ?: throw AuthenticationError.NoActiveSession

            try {
                // Try to refresh access token using stored refresh token
                val refreshToken = keychainUtils.getAuthRefreshToken() ?: throw AuthenticationError.SessionExpired
                val resp = grpcClient.refreshAccessToken(refreshToken)

                // If successful, update stored tokens and keep user
                keychainUtils.storeAuthToken(resp.accessToken)
                keychainUtils.storeAuthRefreshToken(refreshToken)

            } catch (error: Exception) {
                // If refresh fails, sign out the user
                signOut()
                throw error
            }
        }
    }
    
    // Private methods
    private fun loadStoredSession() {
        val userData = preferenceUtils.getCurrentUser()
        if (userData != null) {
            _currentUser.value = userData
            _isAuthenticated.value = true
        }
    }
    
    private fun validateStoredSession() {
        val user = _currentUser.value ?: return
        
        viewModelScope.launch {
            try {
                refreshSession()
            } catch (error: Exception) {
                signOut()
            }
        }
    }
    
    private suspend fun performAuthentication(username: String, password: String): SonetUser {
        // Deprecated: use authenticate() above which returns tokens. Kept for compatibility.
        return performAuthentication(username, password)
    }
    
    private suspend fun performSessionRefresh(user: SonetUser): SonetUser {
        // Simulate session refresh
        kotlinx.coroutines.delay(500) // 0.5 seconds
        
        // Return refreshed user (in real implementation, this would update tokens)
        return user
    }
    
    private suspend fun storeSession(user: SonetUser, accessToken: String, refreshToken: String) {
        // Store user data in SharedPreferences
        preferenceUtils.saveCurrentUser(user)

        // Store sensitive data in keychain
        keychainUtils.storeAuthToken(accessToken)
        keychainUtils.storeAuthRefreshToken(refreshToken)
    }
    
    private suspend fun clearStoredSession() {
        // Clear user data
        preferenceUtils.clearCurrentUser()
        
        // Clear keychain data
    keychainUtils.clearAuthToken()
    keychainUtils.clearAuthRefreshToken()
    }
    
    private suspend fun clearCachedData() {
        // Clear any cached feed data, images, etc.
    }
}

// Authentication Error Types
sealed class AuthenticationError : Exception() {
    object InvalidCredentials : AuthenticationError() {
        override val message: String = "Invalid username or password"
    }
    
    object NetworkError : AuthenticationError() {
        override val message: String = "Network connection error"
    }
    
    object ServerError : AuthenticationError() {
        override val message: String = "Server error, please try again"
    }
    
    object NoActiveSession : AuthenticationError() {
        override val message: String = "No active session"
    }
    
    object SessionExpired : AuthenticationError() {
        override val message: String = "Session expired, please log in again"
    }
    
    object Unknown : AuthenticationError() {
        override val message: String = "An unknown error occurred"
    }
}