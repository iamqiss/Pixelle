package xyz.sonet.app.viewmodels

import android.app.Application
import android.content.SharedPreferences
import androidx.biometric.BiometricManager
import androidx.biometric.BiometricPrompt
import androidx.core.content.ContextCompat
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import xyz.sonet.app.grpc.SonetGRPCClient
import xyz.sonet.app.grpc.SonetConfiguration
import xyz.sonet.app.grpc.proto.*
import xyz.sonet.app.models.SecuritySession
import xyz.sonet.app.models.SecurityLog
import xyz.sonet.app.models.DeviceType
import xyz.sonet.app.models.SecurityEventType
import java.util.*

class SecurityViewModel(application: Application) : AndroidViewModel(application) {
    
    // MARK: - State Flows
    private val _requirePasswordForPurchases = MutableStateFlow(false)
    val requirePasswordForPurchases: StateFlow<Boolean> = _requirePasswordForPurchases.asStateFlow()
    
    private val _requirePasswordForSettings = MutableStateFlow(false)
    val requirePasswordForSettings: StateFlow<Boolean> = _requirePasswordForSettings.asStateFlow()
    
    private val _biometricEnabled = MutableStateFlow(false)
    val biometricEnabled: StateFlow<Boolean> = _biometricEnabled.asStateFlow()
    
    private val _biometricType = MutableStateFlow(DeviceType.NONE)
    val biometricType: StateFlow<DeviceType> = _biometricType.asStateFlow()
    
    private val _activeSessions = MutableStateFlow<List<SecuritySession>>(emptyList())
    val activeSessions: StateFlow<List<SecuritySession>> = _activeSessions.asStateFlow()
    
    private val _securityLogs = MutableStateFlow<List<SecurityLog>>(emptyList())
    val securityLogs: StateFlow<List<SecurityLog>> = _securityLogs.asStateFlow()
    
    private val _loginAlertsEnabled = MutableStateFlow(true)
    val loginAlertsEnabled: StateFlow<Boolean> = _loginAlertsEnabled.asStateFlow()
    
    private val _suspiciousActivityAlertsEnabled = MutableStateFlow(true)
    val suspiciousActivityAlertsEnabled: StateFlow<Boolean> = _suspiciousActivityAlertsEnabled.asStateFlow()
    
    private val _passwordChangeAlertsEnabled = MutableStateFlow(true)
    val passwordChangeAlertsEnabled: StateFlow<Boolean> = _passwordChangeAlertsEnabled.asStateFlow()
    
    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()
    
    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()
    
    // MARK: - Private Properties
    private val grpcClient = SonetGRPCClient(application, SonetConfiguration.development)
    private val sharedPreferences: SharedPreferences = application.getSharedPreferences("sonet_security", 0)
    private val biometricManager = BiometricManager.from(application)
    
    // MARK: - Computed Properties
    val currentSessionId: String
        get() = sharedPreferences.getString("currentSessionId", "") ?: ""
    
    // MARK: - Initialization
    init {
        setupBiometricContext()
        loadSecuritySettings()
    }
    
    // MARK: - Public Methods
    fun loadSecuritySettings() {
        // Load from SharedPreferences
        _requirePasswordForPurchases.value = sharedPreferences.getBoolean("requirePasswordForPurchases", false)
        _requirePasswordForSettings.value = sharedPreferences.getBoolean("requirePasswordForSettings", false)
        _biometricEnabled.value = sharedPreferences.getBoolean("biometricEnabled", false)
        _loginAlertsEnabled.value = sharedPreferences.getBoolean("loginAlertsEnabled", true)
        _suspiciousActivityAlertsEnabled.value = sharedPreferences.getBoolean("suspiciousActivityAlertsEnabled", true)
        _passwordChangeAlertsEnabled.value = sharedPreferences.getBoolean("passwordChangeAlertsEnabled", true)
    }
    
    fun loadActiveSessions() {
        viewModelScope.launch {
            _isLoading.value = true
            _error.value = null
            
            try {
                val request = GetActiveSessionsRequest.newBuilder()
                    .setUserId("current_user")
                    .build()
                
                val response = grpcClient.getActiveSessions(request = request)
                
                if (response.success) {
                    _activeSessions.value = response.sessions.map { SecuritySession.from(it) }
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to load active sessions: ${e.localizedMessage}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    fun loadSecurityLogs() {
        viewModelScope.launch {
            _isLoading.value = true
            _error.value = null
            
            try {
                val request = GetSecurityLogsRequest.newBuilder()
                    .setUserId("current_user")
                    .setLimit(50)
                    .build()
                
                val response = grpcClient.getSecurityLogs(request = request)
                
                if (response.success) {
                    _securityLogs.value = response.logs.map { SecurityLog.from(it) }
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to load security logs: ${e.localizedMessage}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    fun loadAllSessions() {
        viewModelScope.launch {
            try {
                val request = GetActiveSessionsRequest.newBuilder()
                    .setUserId("current_user")
                    .setIncludeInactive(true)
                    .build()
                
                val response = grpcClient.getActiveSessions(request = request)
                
                if (response.success) {
                    _activeSessions.value = response.sessions.map { SecuritySession.from(it) }
                }
            } catch (e: Exception) {
                println("Failed to load all sessions: $e")
            }
        }
    }
    
    fun revokeSession(sessionId: String) {
        viewModelScope.launch {
            _isLoading.value = true
            _error.value = null
            
            try {
                val request = RevokeSessionRequest.newBuilder()
                    .setUserId("current_user")
                    .setSessionId(sessionId)
                    .build()
                
                val response = grpcClient.revokeSession(request = request)
                
                if (response.success) {
                    _activeSessions.value = _activeSessions.value.filter { it.id != sessionId }
                    logSecurityEvent(SecurityEventType.SESSION_REVOKED, "Session revoked: $sessionId")
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to revoke session: ${e.localizedMessage}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    fun updatePasswordRequirement(action: PasswordRequirementAction, required: Boolean) {
        when (action) {
            PasswordRequirementAction.PURCHASES -> {
                _requirePasswordForPurchases.value = required
                sharedPreferences.edit().putBoolean("requirePasswordForPurchases", required).apply()
            }
            PasswordRequirementAction.SETTINGS -> {
                _requirePasswordForSettings.value = required
                sharedPreferences.edit().putBoolean("requirePasswordForSettings", required).apply()
            }
        }
        
        logSecurityEvent(
            SecurityEventType.PASSWORD_REQUIREMENT_CHANGED,
            "Password requirement for ${action.displayName} changed to $required"
        )
    }
    
    fun updateBiometricAuthentication(enabled: Boolean) {
        _biometricEnabled.value = enabled
        sharedPreferences.edit().putBoolean("biometricEnabled", enabled).apply()
        
        if (enabled) {
            logSecurityEvent(SecurityEventType.BIOMETRIC_ENABLED, "Biometric authentication enabled")
        } else {
            logSecurityEvent(SecurityEventType.BIOMETRIC_DISABLED, "Biometric authentication disabled")
        }
    }
    
    fun updateLoginAlerts(enabled: Boolean) {
        _loginAlertsEnabled.value = enabled
        sharedPreferences.edit().putBoolean("loginAlertsEnabled", enabled).apply()
    }
    
    fun updateSuspiciousActivityAlerts(enabled: Boolean) {
        _suspiciousActivityAlertsEnabled.value = enabled
        sharedPreferences.edit().putBoolean("suspiciousActivityAlertsEnabled", enabled).apply()
    }
    
    fun updatePasswordChangeAlerts(enabled: Boolean) {
        _passwordChangeAlertsEnabled.value = enabled
        sharedPreferences.edit().putBoolean("passwordChangeAlertsEnabled", enabled).apply()
    }
    
    // MARK: - Private Methods
    private fun setupBiometricContext() {
        when (biometricManager.canAuthenticate(BiometricManager.Authenticators.BIOMETRIC_WEAK)) {
            BiometricManager.BIOMETRIC_SUCCESS -> {
                _biometricType.value = DeviceType.MOBILE // Default to mobile for biometric
            }
            BiometricManager.BIOMETRIC_ERROR_NO_HARDWARE -> {
                _biometricType.value = DeviceType.NONE
            }
            BiometricManager.BIOMETRIC_ERROR_HW_UNAVAILABLE -> {
                _biometricType.value = DeviceType.NONE
            }
            BiometricManager.BIOMETRIC_ERROR_NONE_ENROLLED -> {
                _biometricType.value = DeviceType.NONE
            }
            else -> {
                _biometricType.value = DeviceType.NONE
            }
        }
    }
    
    private fun logSecurityEvent(eventType: SecurityEventType, description: String) {
        val log = SecurityLog(
            id = UUID.randomUUID().toString(),
            eventType = eventType,
            description = description,
            timestamp = Date(),
            requiresAttention = eventType.requiresAttention,
            metadata = emptyMap()
        )
        
        _securityLogs.value = listOf(log) + _securityLogs.value
        
        // Save to local storage
        saveSecurityLog(log)
    }
    
    private fun saveSecurityLog(log: SecurityLog) {
        // Save to SharedPreferences for now
        // In a real app, this would be saved to Room database or similar
        val logs = sharedPreferences.getStringSet("securityLogs", emptySet())?.toMutableSet() ?: mutableSetOf()
        
        val logString = "${log.id}|${log.eventType.name}|${log.description}|${log.timestamp.time}|${log.requiresAttention}"
        logs.add(logString)
        
        // Keep only last 100 logs
        if (logs.size > 100) {
            val sortedLogs = logs.sortedByDescending { it.split("|")[3].toLongOrNull() ?: 0L }
            logs.clear()
            logs.addAll(sortedLogs.take(100))
        }
        
        sharedPreferences.edit().putStringSet("securityLogs", logs).apply()
    }
}

// MARK: - Supporting Types
enum class PasswordRequirementAction(val displayName: String) {
    PURCHASES("App Store purchases"),
    SETTINGS("Settings changes")
}

// MARK: - gRPC Extensions (Placeholder - replace with actual gRPC types)
extension GetActiveSessionsRequest {
    companion object {
        fun newBuilder(): GetActiveSessionsRequestBuilder {
            return GetActiveSessionsRequestBuilder()
        }
    }
}

extension GetSecurityLogsRequest {
    companion object {
        fun newBuilder(): GetSecurityLogsRequestBuilder {
            return GetSecurityLogsRequestBuilder()
        }
    }
}

extension RevokeSessionRequest {
    companion object {
        fun newBuilder(): RevokeSessionRequestBuilder {
            return RevokeSessionRequestBuilder()
        }
    }
}

// MARK: - Request Builders (Placeholder - replace with actual gRPC types)
class GetActiveSessionsRequestBuilder {
    private var userId: String = ""
    private var includeInactive: Boolean = false
    
    fun setUserId(userId: String): GetActiveSessionsRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun setIncludeInactive(include: Boolean): GetActiveSessionsRequestBuilder {
        this.includeInactive = include
        return this
    }
    
    fun build(): GetActiveSessionsRequest {
        return GetActiveSessionsRequest(userId = userId, includeInactive = includeInactive)
    }
}

class GetSecurityLogsRequestBuilder {
    private var userId: String = ""
    private var limit: Int = 50
    
    fun setUserId(userId: String): GetSecurityLogsRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun setLimit(limit: Int): GetSecurityLogsRequestBuilder {
        this.limit = limit
        return this
    }
    
    fun build(): GetSecurityLogsRequest {
        return GetSecurityLogsRequest(userId = userId, limit = limit)
    }
}

class RevokeSessionRequestBuilder {
    private var userId: String = ""
    private var sessionId: String = ""
    
    fun setUserId(userId: String): RevokeSessionRequestBuilder {
        this.userId = userId
        return this
    }
    
    fun setSessionId(sessionId: String): RevokeSessionRequestBuilder {
        this.sessionId = sessionId
        return this
    }
    
    fun build(): RevokeSessionRequest {
        return RevokeSessionRequest(userId = userId, sessionId = sessionId)
    }
}

// MARK: - Request/Response Models (Placeholder - replace with actual gRPC types)
data class GetActiveSessionsRequest(
    val userId: String,
    val includeInactive: Boolean
)

data class GetSecurityLogsRequest(
    val userId: String,
    val limit: Int
)

data class RevokeSessionRequest(
    val userId: String,
    val sessionId: String
)

data class GetActiveSessionsResponse(
    val success: Boolean,
    val sessions: List<GRPCSession>,
    val errorMessage: String
)

data class GetSecurityLogsResponse(
    val success: Boolean,
    val logs: List<GRPCSecurityLog>,
    val errorMessage: String
)

data class RevokeSessionResponse(
    val success: Boolean,
    val errorMessage: String
)

// MARK: - gRPC Models (Placeholder - replace with actual gRPC types)
data class GRPCSession(
    val sessionId: String,
    val deviceName: String,
    val deviceType: GRPCDeviceType,
    val locationInfo: String,
    val lastActivity: GRPCTimestamp,
    val ipAddress: String,
    val userAgent: String
)

data class GRPCSecurityLog(
    val logId: String,
    val eventType: String,
    val description: String,
    val timestamp: GRPCTimestamp,
    val requiresAttention: Boolean
)

enum class GRPCDeviceType {
    MOBILE, TABLET, DESKTOP, WEB
}

data class GRPCTimestamp(
    val date: Date
)