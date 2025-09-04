package xyz.sonet.app.models

import android.os.Parcelable
import kotlinx.parcelize.Parcelize

@Parcelize
data class MediaItem(
    val id: String,
    val url: String,
    val type: MediaType,
    val altText: String?
) : Parcelable

enum class MediaType {
    IMAGE, VIDEO, GIF
}