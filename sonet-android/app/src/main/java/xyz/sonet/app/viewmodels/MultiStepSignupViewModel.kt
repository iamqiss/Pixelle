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
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class MultiStepSignupViewModel(application: Application) : AndroidViewModel(application) {
    
    // MARK: - StateFlow Properties
    private val _displayName = MutableStateFlow("")
    val displayName: StateFlow<String> = _displayName.asStateFlow()
    
    private val _username = MutableStateFlow("")
    val username: StateFlow<String> = _username.asStateFlow()
    
    private val _password = MutableStateFlow("")
    val password: StateFlow<String> = _password.asStateFlow()
    
    private val _passwordConfirmation = MutableStateFlow("")
    val passwordConfirmation: StateFlow<String> = _passwordConfirmation.asStateFlow()
    
    private val _birthday = MutableStateFlow(LocalDate.now().minusYears(18))
    val birthday: StateFlow<LocalDate> = _birthday.asStateFlow()
    
    private val _email = MutableStateFlow("")
    val email: StateFlow<String> = _email.asStateFlow()
    
    private val _phoneNumber = MutableStateFlow("")
    val phoneNumber: StateFlow<String> = _phoneNumber.asStateFlow()
    
    private val _bio = MutableStateFlow("")
    val bio: StateFlow<String> = _bio.asStateFlow()
    
    private val _avatarImage = MutableStateFlow<String?>(null)
    val avatarImage: StateFlow<String?> = _avatarImage.asStateFlow()
    
    private val _selectedInterests = MutableStateFlow<Set<String>>(emptySet())
    val selectedInterests: StateFlow<Set<String>> = _selectedInterests.asStateFlow()
    
    private val _contactsAccess = MutableStateFlow(false)
    val contactsAccess: StateFlow<Boolean> = _contactsAccess.asStateFlow()
    
    private val _locationAccess = MutableStateFlow(false)
    val locationAccess: StateFlow<Boolean> = _locationAccess.asStateFlow()
    
    // MARK: - UI State
    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()
    
    private val _isSignupComplete = MutableStateFlow(false)
    val isSignupComplete: StateFlow<Boolean> = _isSignupComplete.asStateFlow()
    
    private val _errorMessage = MutableStateFlow<String?>(null)
    val errorMessage: StateFlow<String?> = _errorMessage.asStateFlow()
    
    // MARK: - Private Properties
    private val grpcClient = SonetGRPCClient(getApplication(), SonetConfiguration.development)
    
    // MARK: - Public Methods
    fun updateDisplayName(name: String) {
        _displayName.value = name
    }
    
    fun updateUsername(name: String) {
        _username.value = name
    }
    
    fun updatePassword(pwd: String) {
        _password.value = pwd
    }
    
    fun updatePasswordConfirmation(pwd: String) {
        _passwordConfirmation.value = pwd
    }
    
    fun updateBirthday(date: LocalDate) {
        _birthday.value = date
    }
    
    fun updateEmail(email: String) {
        _email.value = email
    }
    
    fun updatePhoneNumber(phone: String) {
        _phoneNumber.value = phone
    }
    
    fun updateBio(bio: String) {
        _bio.value = bio
    }
    
    fun updateAvatarImage(imagePath: String?) {
        _avatarImage.value = imagePath
    }
    
    fun updateSelectedInterests(interests: Set<String>) {
        _selectedInterests.value = interests
    }
    
    fun updateContactsAccess(access: Boolean) {
        _contactsAccess.value = access
    }
    
    fun updateLocationAccess(access: Boolean) {
        _locationAccess.value = access
    }
    
    fun completeSignup() {
        viewModelScope.launch {
            try {
                _isLoading.value = true
                _errorMessage.value = null
                
                // Validate all required fields
                if (!validateAllFields()) {
                    _errorMessage.value = "Please fill in all required fields"
                    return@launch
                }
                
                // Create user account
                val userProfile = grpcClient.registerUser(
                    username = username.value,
                    email = email.value,
                    password = password.value
                )
                
                // Update user profile with additional information
                updateUserProfile(userProfile)
                
                // Handle permissions
                handlePermissions()
                
                // Mark signup as complete
                _isSignupComplete.value = true
                
            } catch (error: Exception) {
                _errorMessage.value = "Failed to create account: ${error.localizedMessage}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    // MARK: - Private Methods
    private fun validateAllFields(): Boolean {
        // Basic validation
        if (displayName.value.trim().isEmpty()) return false
        if (username.value.trim().isEmpty()) return false
        if (password.value.length < 8) return false
        if (password.value != passwordConfirmation.value) return false
        
        // Age validation
        val age = LocalDate.now().year - birthday.value.year
        if (age < 13) return false
        
        // Email validation
        if (email.value.trim().isEmpty()) return false
        if (!isValidEmail(email.value)) return false
        
        // Interests validation
        if (selectedInterests.value.size < 3) return false
        
        return true
    }
    
    private fun isValidEmail(email: String): Boolean {
        val emailRegex = "[A-Z0-9a-z._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,64}".toRegex()
        return emailRegex.matches(email)
    }
    
    private suspend fun updateUserProfile(userProfile: UserProfile) {
        // Create update request with all collected information
        val request = UpdateUserProfileRequest.newBuilder()
            .setUserId(userProfile.userId)
            .setDisplayName(displayName.value)
            .setBio(bio.value)
            .addAllInterests(selectedInterests.value.toList())
            .build()
        
        // Update profile
        grpcClient.updateUserProfile(request)
        
        // Upload avatar if selected
        avatarImage.value?.let { imagePath ->
            uploadAvatar(imagePath, userProfile.userId)
        }
    }
    
    private suspend fun uploadAvatar(imagePath: String, userId: String) {
        try {
            // Read image file and convert to bytes
            val imageBytes = getApplication<Application>().assets.open(imagePath).readBytes()
            
            // Create upload request
            val request = UploadMediaRequest.newBuilder()
                .setUserId(userId)
                .setMediaType(MediaType.IMAGE)
                .setMediaData(com.google.protobuf.ByteString.copyFrom(imageBytes))
                .setFileName("avatar_$userId.jpg")
                .build()
            
            // Upload avatar
            grpcClient.uploadMedia(request)
            
        } catch (error: Exception) {
            // Handle avatar upload error gracefully
            // For now, just log the error
            error.printStackTrace()
        }
    }
    
    private suspend fun handlePermissions() {
        // Request contacts access if enabled
        if (contactsAccess.value) {
            requestContactsAccess()
        }
        
        // Request location access if enabled
        if (locationAccess.value) {
            requestLocationAccess()
        }
    }
    
    private suspend fun requestContactsAccess() {
        // Request contacts permission
        val status = requestContactsPermission()
        
        if (status) {
            // Sync contacts with server
            syncContacts()
        }
    }
    
    private suspend fun requestLocationAccess() {
        // Request location permission
        val status = requestLocationPermission()
        
        if (status) {
            // Get current location and send to server
            sendLocationToServer()
        }
    }
    
    private suspend fun requestContactsPermission(): Boolean {
        // This would integrate with Android permissions system
        // For now, return true as placeholder
        return true
    }
    
    private suspend fun requestLocationPermission(): Boolean {
        // This would integrate with Android permissions system
        // For now, return true as placeholder
        return true
    }
    
    private suspend fun syncContacts() {
        // This would sync contacts with the server
        // Implementation depends on your contacts sync service
    }
    
    private suspend fun sendLocationToServer() {
        // This would send location to the server
        // Implementation depends on your location service
    }
    
    // MARK: - Helper Methods
    fun createUserProfileRequest(): RegisterUserRequest {
        return RegisterUserRequest.newBuilder()
            .setUsername(username.value)
            .setEmail(email.value)
            .setPassword(password.value)
            .setDisplayName(displayName.value)
            .setBirthday(birthday.value.format(DateTimeFormatter.ISO_LOCAL_DATE))
            .setPhoneNumber(phoneNumber.value.ifEmpty { "" })
            .setBio(bio.value)
            .addAllInterests(selectedInterests.value.toList())
            .build()
    }
    
    suspend fun validateUsername(): Boolean {
        // This would check with the server if username is available
        // For now, return true as placeholder
        return true
    }
    
    suspend fun validateEmail(): Boolean {
        // This would check with the server if email is available
        // For now, return true as placeholder
        return true
    }
    
    fun clearError() {
        _errorMessage.value = null
    }
    
    fun resetSignupState() {
        _isSignupComplete.value = false
        _errorMessage.value = null
        _isLoading.value = false
    }
}

// MARK: - Mock Data for Development
fun MultiStepSignupViewModel.populateWithMockData() {
    updateDisplayName("John Doe")
    updateUsername("johndoe")
    updatePassword("Password123!")
    updatePasswordConfirmation("Password123!")
    updateBirthday(LocalDate.now().minusYears(25))
    updateEmail("john.doe@example.com")
    updatePhoneNumber("+1234567890")
    updateBio("Passionate about technology and innovation. Always learning and growing!")
    updateSelectedInterests(setOf("Technology", "Science", "Business", "Education", "Health"))
    updateContactsAccess(true)
    updateLocationAccess(true)
}

fun MultiStepSignupViewModel.clearAllData() {
    updateDisplayName("")
    updateUsername("")
    updatePassword("")
    updatePasswordConfirmation("")
    updateBirthday(LocalDate.now().minusYears(18))
    updateEmail("")
    updatePhoneNumber("")
    updateBio("")
    updateAvatarImage(null)
    updateSelectedInterests(emptySet())
    updateContactsAccess(false)
    updateLocationAccess(false)
}