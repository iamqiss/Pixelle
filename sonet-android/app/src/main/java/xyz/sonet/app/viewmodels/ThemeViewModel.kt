package xyz.sonet.app.viewmodels

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import xyz.sonet.app.utils.PreferenceUtils

class ThemeViewModel(application: Application) : AndroidViewModel(application) {
    
    // State flows
    private val _isDarkMode = MutableStateFlow(false)
    val isDarkMode: StateFlow<Boolean> = _isDarkMode.asStateFlow()
    
    private val _accentColor = MutableStateFlow(0xFF1083FE.toInt())
    val accentColor: StateFlow<Int> = _accentColor.asStateFlow()
    
    private val _backgroundColor = MutableStateFlow(0xFFFFFFFF.toInt())
    val backgroundColor: StateFlow<Int> = _backgroundColor.asStateFlow()
    
    private val _textColor = MutableStateFlow(0xFF000000.toInt())
    val textColor: StateFlow<Int> = _textColor.asStateFlow()
    
    private val _secondaryTextColor = MutableStateFlow(0xFF666666.toInt())
    val secondaryTextColor: StateFlow<Int> = _secondaryTextColor.asStateFlow()
    
    // Private properties
    private val preferenceUtils = PreferenceUtils(application)
    
    init {
        loadSavedTheme()
    }
    
    fun initialize() {
        // Apply system theme if no saved preference
        if (preferenceUtils.getTheme() == null) {
            applySystemTheme()
        }
    }
    
    fun setTheme(theme: AppTheme) {
        when (theme) {
            AppTheme.LIGHT -> {
                _isDarkMode.value = false
                applyLightTheme()
            }
            AppTheme.DARK -> {
                _isDarkMode.value = true
                applyDarkTheme()
            }
            AppTheme.SYSTEM -> {
                applySystemTheme()
            }
        }
        
        preferenceUtils.saveTheme(theme.name.lowercase())
    }
    
    fun toggleTheme() {
        val newTheme = if (_isDarkMode.value) AppTheme.LIGHT else AppTheme.DARK
        setTheme(newTheme)
    }
    
    // Private methods
    private fun loadSavedTheme() {
        val themeName = preferenceUtils.getTheme()
        if (themeName != null) {
            val theme = AppTheme.valueOf(themeName.uppercase())
            setTheme(theme)
        } else {
            applySystemTheme()
        }
    }
    
    private fun applySystemTheme() {
        // For now, default to light theme
        // In a real implementation, this would check system theme
        _isDarkMode.value = false
        applyLightTheme()
    }
    
    private fun applyLightTheme() {
        _backgroundColor.value = 0xFFFFFFFF.toInt()
        _textColor.value = 0xFF000000.toInt()
        _secondaryTextColor.value = 0xFF666666.toInt()
        _accentColor.value = 0xFF1083FE.toInt()
    }
    
    private fun applyDarkTheme() {
        _backgroundColor.value = 0xFF121212.toInt()
        _textColor.value = 0xFFFFFFFF.toInt()
        _secondaryTextColor.value = 0xFFB3B3B3.toInt()
        _accentColor.value = 0xFF1083FE.toInt()
    }
}

// App Theme Enum
enum class AppTheme(val displayName: String) {
    LIGHT("Light"),
    DARK("Dark"),
    SYSTEM("System")
}