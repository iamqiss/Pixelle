package xyz.sonet.app.models

import android.os.Parcelable
import kotlinx.parcelize.Parcelize

@Parcelize
data class SonetUser(
    val id: String,
    val username: String,
    val displayName: String,
    val avatarUrl: String?,
    val isVerified: Boolean,
    val createdAt: Long
) : Parcelable {
    
    val handle: String
        get() = "@$username"
    
    companion object {
        fun createMockUser(id: String = "mock_user"): SonetUser {
            return SonetUser(
                id = id,
                username = "mockuser",
                displayName = "Mock User",
                avatarUrl = null,
                isVerified = false,
                createdAt = System.currentTimeMillis()
            )
        }
    }
}