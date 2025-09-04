package xyz.sonet.app.models

import android.os.Parcelable
import kotlinx.parcelize.Parcelize

@Parcelize
data class SonetNote(
    val id: String,
    val text: String,
    val author: SonetUser,
    val createdAt: Long,
    val likeCount: Int,
    val repostCount: Int,
    val replyCount: Int,
    val media: List<MediaItem>?,
    var isLiked: Boolean,
    var isReposted: Boolean
) : Parcelable {
    
    val timeAgo: String
        get() = getTimeAgoDisplay(createdAt)
    
    companion object {
        private fun getTimeAgoDisplay(timestamp: Long): String {
            val now = System.currentTimeMillis()
            val timeDiff = now - timestamp
            
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
    }
}