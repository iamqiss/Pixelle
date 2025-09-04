package xyz.sonet.app.models

import android.os.Parcelable
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.ui.graphics.Color
import kotlinx.parcelize.Parcelize
import java.util.*

@Parcelize
data class SecurityLog(
    val id: String,
    val eventType: SecurityEventType,
    val description: String,
    val timestamp: Date,
    val requiresAttention: Boolean,
    val metadata: Map<String, Any>
) : Parcelable {
    
    fun getTimeAgoDisplay(): String {
        val now = System.currentTimeMillis()
        val timeDiff = now - timestamp.time
        
        val minute = 60 * 1000L
        val hour = minute * 60
        val day = hour * 24
        val week = day * 7
        val month = day * 30
        val year = day * 365
        
        return when {
            timeDiff < minute -> "Just now"
            timeDiff < hour -> "${timeDiff / minute}m ago"
            timeDiff < day -> "${timeDiff / hour}h ago"
            timeDiff < week -> "${timeDiff / day}d ago"
            timeDiff < month -> "${timeDiff / week}w ago"
            timeDiff < year -> "${timeDiff / month}mo ago"
            else -> "${timeDiff / year}y ago"
        }
    }
    
    companion object {
        fun from(grpcLog: GRPCSecurityLog): SecurityLog {
            return SecurityLog(
                id = grpcLog.logId,
                eventType = SecurityEventType.from(grpcLog.eventType),
                description = grpcLog.description,
                timestamp = grpcLog.timestamp.date,
                requiresAttention = grpcLog.requiresAttention,
                metadata = emptyMap() // Parse from grpcLog.metadata if available
            )
        }
    }
}

enum class SecurityEventType(
    val displayName: String,
    val icon: androidx.compose.ui.graphics.vector.ImageVector,
    val color: Color,
    val requiresAttention: Boolean = false
) {
    LOGIN("Login", Icons.Default.PersonAdd, Color(0xFF6B6B6B)),
    LOGOUT("Logout", Icons.Default.PersonRemove, Color(0xFF6B6B6B)),
    PASSWORD_CHANGED("Password Changed", Icons.Default.Key, Color(0xFF6B6B6B)),
    PASSWORD_REQUIREMENT_CHANGED("Password Requirement Changed", Icons.Default.KeyOff, Color(0xFF6B6B6B)),
    BIOMETRIC_ENABLED("Biometric Enabled", Icons.Default.Fingerprint, Color(0xFF6B6B6B)),
    BIOMETRIC_DISABLED("Biometric Disabled", Icons.Default.FingerprintOff, Color(0xFF6B6B6B)),
    SESSION_REVOKED("Session Revoked", Icons.Default.Block, Color(0xFF9E9E9E), true),
    SUSPICIOUS_ACTIVITY("Suspicious Activity", Icons.Default.Warning, Color(0xFF9E9E9E), true),
    TWO_FACTOR_ENABLED("2FA Enabled", Icons.Default.Security, Color(0xFF6B6B6B)),
    TWO_FACTOR_DISABLED("2FA Disabled", Icons.Default.SecurityOff, Color(0xFF6B6B6B));
    
    companion object {
        fun from(eventType: String): SecurityEventType {
            return try {
                valueOf(eventType.uppercase())
            } catch (e: IllegalArgumentException) {
                LOGIN // Default fallback
            }
        }
    }
}

// MARK: - gRPC Models (Placeholder - replace with actual gRPC types)
data class GRPCSecurityLog(
    val logId: String,
    val eventType: String,
    val description: String,
    val timestamp: GRPCTimestamp,
    val requiresAttention: Boolean
)

data class GRPCTimestamp(
    val date: Date
)