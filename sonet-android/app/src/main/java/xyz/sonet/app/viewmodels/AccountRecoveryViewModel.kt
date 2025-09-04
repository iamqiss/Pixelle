package xyz.sonet.app.viewmodels

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch

class AccountRecoveryViewModel(
    private val grpcClient: SonetGRPCClient = SonetGRPCClient(Environment.DEV)
) : ViewModel() {

    private val _recoveryEmail = MutableStateFlow("")
    val recoveryEmail: StateFlow<String> = _recoveryEmail

    private val _emailVerified = MutableStateFlow(false)
    val emailVerified: StateFlow<Boolean> = _emailVerified

    private val _recoveryPhone = MutableStateFlow("")
    val recoveryPhone: StateFlow<String> = _recoveryPhone

    private val _phoneVerified = MutableStateFlow(false)
    val phoneVerified: StateFlow<Boolean> = _phoneVerified

    private val _backupCodes = MutableStateFlow<List<String>>(emptyList())
    val backupCodes: StateFlow<List<String>> = _backupCodes

    private val _message = MutableStateFlow("")
    val message: StateFlow<String> = _message

    private val _error = MutableStateFlow("")
    val error: StateFlow<String> = _error

    fun load() = viewModelScope.launch {
        try {
            val res = grpcClient.getRecoveryState("current_user")
            if (res.success) {
                _recoveryEmail.value = res.state.email
                _emailVerified.value = res.state.emailVerified
                _recoveryPhone.value = res.state.phone
                _phoneVerified.value = res.state.phoneVerified
                _backupCodes.value = res.state.backupCodes
            }
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }

    fun setEmail(value: String) { _recoveryEmail.value = value }
    fun setPhone(value: String) { _recoveryPhone.value = value }

    fun verifyEmail() = viewModelScope.launch {
        try {
            val res = grpcClient.verifyRecoveryEmail("current_user", _recoveryEmail.value)
            if (res.success) { _emailVerified.value = true; _message.value = "Email verified" } else _error.value = res.errorMessage
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }

    fun verifyPhone() = viewModelScope.launch {
        try {
            val res = grpcClient.verifyRecoveryPhone("current_user", _recoveryPhone.value)
            if (res.success) { _phoneVerified.value = true; _message.value = "Phone verified" } else _error.value = res.errorMessage
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }

    fun generateBackupCodes() = viewModelScope.launch {
        try {
            val res = grpcClient.generateBackupCodes("current_user")
            if (res.success) { _backupCodes.value = res.codes; _message.value = "Backup codes generated" } else _error.value = res.errorMessage
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }

    fun resetPassword(newPassword: String) = viewModelScope.launch {
        try {
            val res = grpcClient.resetPassword("current_user", newPassword)
            if (res.success) _message.value = "Password updated" else _error.value = res.errorMessage
        } catch (t: Throwable) { _error.value = t.message ?: "Unknown error" }
    }
}