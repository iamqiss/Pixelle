package xyz.sonet.app.utils

import android.content.Context
import android.content.SharedPreferences
import com.google.gson.Gson
import xyz.sonet.app.models.SonetUser

class PreferenceUtils(context: Context) {
    
    private val sharedPreferences: SharedPreferences = context.getSharedPreferences(
        PREF_NAME, Context.MODE_PRIVATE
    )
    
    private val gson = Gson()
    
    companion object {
        private const val PREF_NAME = "sonet_preferences"
        private const val KEY_CURRENT_USER = "current_user"
        private const val KEY_THEME = "selected_theme"
        private const val KEY_NOTIFICATIONS_ENABLED = "notifications_enabled"
    }
    
    fun saveCurrentUser(user: SonetUser) {
        val userJson = gson.toJson(user)
        sharedPreferences.edit().putString(KEY_CURRENT_USER, userJson).apply()
    }
    
    fun getCurrentUser(): SonetUser? {
        val userJson = sharedPreferences.getString(KEY_CURRENT_USER, null)
        return if (userJson != null) {
            try {
                gson.fromJson(userJson, SonetUser::class.java)
            } catch (e: Exception) {
                null
            }
        } else {
            null
        }
    }
    
    fun clearCurrentUser() {
        sharedPreferences.edit().remove(KEY_CURRENT_USER).apply()
    }
    
    fun saveTheme(theme: String) {
        sharedPreferences.edit().putString(KEY_THEME, theme).apply()
    }
    
    fun getTheme(): String? {
        return sharedPreferences.getString(KEY_THEME, null)
    }
    
    fun saveNotificationsEnabled(enabled: Boolean) {
        sharedPreferences.edit().putBoolean(KEY_NOTIFICATIONS_ENABLED, enabled).apply()
    }
    
    fun getNotificationsEnabled(): Boolean {
        return sharedPreferences.getBoolean(KEY_NOTIFICATIONS_ENABLED, true)
    }
    
    fun clearAll() {
        sharedPreferences.edit().clear().apply()
    }
}