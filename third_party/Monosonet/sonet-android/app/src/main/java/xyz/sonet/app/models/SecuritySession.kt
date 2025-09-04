package xyz.sonet.app.models

import android.os.Parcelable
import kotlinx.parcelize.Parcelize
import java.util.*

@Parcelize
data class SecuritySession(
    val id: String,
    val deviceName: String,
    val deviceType: DeviceType,
    val location: String,
    val lastActivity: Date,
    val ipAddress: String,
    val userAgent: String
) : Parcelable {
    
    fun getTimeAgoDisplay(): String {
        val now = System.currentTimeMillis()
        val timeDiff = now - lastActivity.time
        
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
        fun from(grpcSession: GRPCSession): SecuritySession {
            return SecuritySession(
                id = grpcSession.sessionId,
                deviceName = grpcSession.deviceName,
                deviceType = DeviceType.from(grpcSession.deviceType),
                location = grpcSession.locationInfo,
                lastActivity = grpcSession.lastActivity.date,
                ipAddress = grpcSession.ipAddress,
                userAgent = grpcSession.userAgent
            )
        }
    }
}

enum class DeviceType(val displayName: String, val icon: androidx.compose.ui.graphics.vector.ImageVector) {
    MOBILE("Mobile", androidx.compose.material.icons.Icons.Default.Phone),
    TABLET("Tablet", androidx.compose.material.icons.Icons.Default.Tablet),
    DESKTOP("Desktop", androidx.compose.material.icons.Icons.Default.Computer),
    WEB("Web", androidx.compose.material.icons.Icons.Default.Language),
    NONE("None", androidx.compose.material.icons.Icons.Default.DeviceUnknown);
    
    companion object {
        fun from(grpcType: GRPCDeviceType): DeviceType {
            return when (grpcType) {
                GRPCDeviceType.MOBILE -> MOBILE
                GRPCDeviceType.TABLET -> TABLET
                GRPCDeviceType.DESKTOP -> DESKTOP
                GRPCDeviceType.WEB -> WEB
                else -> MOBILE
            }
        }
    }
}

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

enum class GRPCDeviceType {
    MOBILE, TABLET, DESKTOP, WEB
}

data class GRPCTimestamp(
    val date: Date
)