package xyz.sonet.app.ui

import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.ui.graphics.vector.ImageVector

// Central registry of Material icons used across the Android app.
// Reference icons via AppIcons to make refactors/theming easier.
object AppIcons {
    // Core navigation
    val Home: ImageVector = Icons.Default.Home
    val Video: ImageVector = Icons.Default.VideoLibrary
    val Messages: ImageVector = Icons.Default.Message
    val Notifications: ImageVector = Icons.Default.Notifications
    val Profile: ImageVector = Icons.Default.Person

    // Actions
    val Add: ImageVector = Icons.Default.Add
    val Search: ImageVector = Icons.Default.Search
    val Share: ImageVector = Icons.Default.Share
    val Like: ImageVector = Icons.Default.Favorite
    val LikeBorder: ImageVector = Icons.Default.FavoriteBorder
    val Reply: ImageVector = Icons.Default.Reply
    val Comment: ImageVector = Icons.Default.ChatBubbleOutline
    val More: ImageVector = Icons.Default.MoreVert
    val Close: ImageVector = Icons.Default.Close

    // People
    val Person: ImageVector = Icons.Default.Person
    val Verified: ImageVector = Icons.Default.Verified

    // Utility
    val Error: ImageVector = Icons.Default.Error
    val Warning: ImageVector = Icons.Default.Warning
}
