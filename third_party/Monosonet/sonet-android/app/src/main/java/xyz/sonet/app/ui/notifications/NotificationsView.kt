package xyz.sonet.app.ui.notifications

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInVertically
import androidx.compose.animation.slideOutVertically
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.scale
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewmodel.compose.viewModel
import coil.compose.AsyncImage
import coil.request.ImageRequest
import xyz.sonet.app.grpc.proto.*
import xyz.sonet.app.viewmodels.NotificationsViewModel
import xyz.sonet.app.viewmodels.NotificationFilter
import xyz.sonet.app.viewmodels.NotificationItem
import xyz.sonet.app.viewmodels.NotificationType
import xyz.sonet.app.viewmodels.AppUpdateInfo
import xyz.sonet.app.viewmodels.NotificationPreferences
import java.text.SimpleDateFormat
import java.util.*

@Composable
fun NotificationsView(
    modifier: Modifier = Modifier,
    viewModel: NotificationsViewModel = viewModel()
) {
    val context = LocalContext.current
    val notifications by viewModel.notifications.collectAsState()
    val filteredNotifications by viewModel.filteredNotifications.collectAsState()
    val selectedFilter by viewModel.selectedFilter.collectAsState()
    val isLoading by viewModel.isLoading.collectAsState()
    val error by viewModel.error.collectAsState()
    val unreadCount by viewModel.unreadCount.collectAsState()
    val showAppUpdate by viewModel.showAppUpdate.collectAsState()
    val appUpdateInfo by viewModel.appUpdateInfo.collectAsState()
    val notificationPreferences by viewModel.notificationPreferences.collectAsState()
    
    var showingPreferences by remember { mutableStateOf(false) }
    
    // Load notifications when view is created
    LaunchedEffect(Unit) {
        viewModel.loadNotifications()
        viewModel.checkForAppUpdates()
    }
    
    Column(
        modifier = modifier.fillMaxSize()
    ) {
        // Header with filters
        NotificationHeader(
            selectedFilter = selectedFilter,
            unreadCount = unreadCount,
            onFilterSelected = { viewModel.selectFilter(it) },
            onPreferences = { showingPreferences = true },
            onMarkAllRead = { viewModel.markAllAsRead() }
        )
        
        // Notifications list
        when {
            isLoading -> LoadingView()
            error != null -> ErrorView(
                error = error!!,
                onRetry = { viewModel.refreshNotifications() }
            )
            filteredNotifications.isEmpty() -> EmptyNotificationsView(selectedFilter = selectedFilter)
            else -> NotificationsList(
                notifications = filteredNotifications,
                onNotificationTap = { notification ->
                    viewModel.markAsRead(notification)
                    // Navigate to relevant content
                },
                onDeleteNotification = { notification ->
                    viewModel.deleteNotification(notification)
                }
            )
        }
    }
    
    // Preferences sheet
    if (showingPreferences) {
        NotificationPreferencesSheet(
            preferences = notificationPreferences,
            onSave = {
                viewModel.updateNotificationPreferences()
                showingPreferences = false
            },
            onDismiss = { showingPreferences = false }
        )
    }
    
    // App update dialog
    if (showAppUpdate && appUpdateInfo != null) {
        AppUpdateDialog(
            appUpdate = appUpdateInfo!!,
            onDismiss = { viewModel.showAppUpdate.value = false }
        )
    }
}

// MARK: - Notification Header
@Composable
private fun NotificationHeader(
    selectedFilter: NotificationFilter,
    unreadCount: Int,
    onFilterSelected: (NotificationFilter) -> Unit,
    onPreferences: () -> Unit,
    onMarkAllRead: () -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surface)
    ) {
        // Main header
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp, vertical = 12.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Notifications",
                style = MaterialTheme.typography.headlineLarge,
                fontWeight = FontWeight.Bold
            )
            
            Spacer(modifier = Modifier.weight(1f))
            
            Row(horizontalArrangement = Arrangement.spacedBy(16.dp)) {
                // Unread count badge
                if (unreadCount > 0) {
                    Button(
                        onClick = onMarkAllRead,
                        colors = ButtonDefaults.buttonColors(
                            containerColor = MaterialTheme.colorScheme.onSurface
                        ),
                        shape = CircleShape,
                        modifier = Modifier.size(24.dp)
                    ) {
                        Text(
                            text = unreadCount.toString(),
                            fontSize = 12.sp,
                            fontWeight = FontWeight.SemiBold,
                            color = MaterialTheme.colorScheme.surface
                        )
                    }
                }
                
                // Preferences button
                IconButton(onClick = onPreferences) {
                    Icon(
                        imageVector = Icons.Default.Settings,
                        contentDescription = "Settings",
                        tint = MaterialTheme.colorScheme.onSurface
                    )
                }
            }
        }
        
        // Filter tabs
        NotificationFilterTabs(
            selectedFilter = selectedFilter,
            onFilterSelected = onFilterSelected
        )
        
        Divider()
    }
}

// MARK: - Notification Filter Tabs
@Composable
private fun NotificationFilterTabs(
    selectedFilter: NotificationFilter,
    onFilterSelected: (NotificationFilter) -> Unit
) {
    LazyRow(
        contentPadding = PaddingValues(horizontal = 16.dp, vertical = 8.dp),
        horizontalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        items(NotificationFilter.values()) { filter ->
            NotificationFilterTab(
                filter = filter,
                isSelected = selectedFilter == filter,
                onTap = { onFilterSelected(filter) }
            )
        }
    }
}

// MARK: - Notification Filter Tab
@Composable
private fun NotificationFilterTab(
    filter: NotificationFilter,
    isSelected: Boolean,
    onTap: () -> Unit
) {
    val color = MaterialTheme.colorScheme.onSurface
    
    Button(
        onClick = onTap,
        colors = ButtonDefaults.buttonColors(
            containerColor = if (isSelected) color.copy(alpha = 0.1f) else Color.Transparent
        ),
        shape = RoundedCornerShape(20.dp)
    ) {
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Icon(
                imageVector = getIconForFilter(filter.icon),
                contentDescription = null,
                tint = if (isSelected) color else MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(16.dp)
            )
            
            Text(
                text = filter.title,
                fontSize = 14.sp,
                fontWeight = FontWeight.Medium,
                color = if (isSelected) color else MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

// MARK: - Notifications List
@Composable
private fun NotificationsList(
    notifications: List<NotificationItem>,
    onNotificationTap: (NotificationItem) -> Unit,
    onDeleteNotification: (NotificationItem) -> Void
) {
    LazyColumn {
        items(notifications) { notification ->
            NotificationRow(
                notification = notification,
                onTap = { onNotificationTap(notification) },
                onDelete = { onDeleteNotification(notification) }
            )
            
            if (notification.id != notifications.last()?.id) {
                Divider(modifier = Modifier.padding(start = 72.dp))
            }
        }
    }
}

// MARK: - Notification Row
@Composable
private fun NotificationRow(
    notification: NotificationItem,
    onTap: () -> Unit,
    onDelete: (NotificationItem) -> Unit
) {
    var showingDeleteAlert by remember { mutableStateOf(false) }
    
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clickable { onTap() }
            .padding(horizontal = 16.dp, vertical = 12.dp),
        verticalAlignment = Alignment.Top
    ) {
        // Notification icon
        NotificationIcon(
            type = notification.type,
            isRead = notification.isRead
        )
        
        Spacer(modifier = Modifier.width(12.dp))
        
        // Notification content
        Column(
            modifier = Modifier.weight(1f),
            verticalArrangement = Arrangement.spacedBy(4.dp)
        ) {
            // Title and message
            Column(
                verticalArrangement = Arrangement.spacedBy(2.dp),
                horizontalAlignment = Alignment.Start
            ) {
                Text(
                    text = notification.title,
                    fontSize = 15.sp,
                    fontWeight = FontWeight.SemiBold,
                    color = MaterialTheme.colorScheme.onSurface,
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis
                )
                
                Text(
                    text = notification.message,
                    fontSize = 14.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    maxLines = 3,
                    overflow = TextOverflow.Ellipsis
                )
            }
            
            // Timestamp
            Text(
                text = formatTimeAgo(notification.timestamp),
                fontSize = 12.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        
        // More options
        IconButton(
            onClick = { showingDeleteAlert = true },
            modifier = Modifier.size(32.dp)
        ) {
            Icon(
                imageVector = Icons.Default.MoreVert,
                contentDescription = "More options",
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(16.dp)
            )
        }
    }
    .background(
        MaterialTheme.colorScheme.surface.copy(
            alpha = if (notification.isRead) 1.0f else 0.05f
        )
    )
    
    // Delete confirmation dialog
    if (showingDeleteAlert) {
        AlertDialog(
            onDismissRequest = { showingDeleteAlert = false },
            title = { Text("Delete Notification") },
            text = { Text("Are you sure you want to delete this notification?") },
            confirmButton = {
                TextButton(
                    onClick = {
                        onDelete(notification)
                        showingDeleteAlert = false
                    }
                ) {
                    Text("Delete")
                }
            },
            dismissButton = {
                TextButton(onClick = { showingDeleteAlert = false }) {
                    Text("Cancel")
                }
            }
        )
    }
}

// MARK: - Notification Icon
@Composable
private fun NotificationIcon(
    type: NotificationType,
    isRead: Boolean
) {
    val color = MaterialTheme.colorScheme.onSurface
    
    Box(
        modifier = Modifier.size(40.dp),
        contentAlignment = Alignment.Center
    ) {
        Box(
            modifier = Modifier
                .size(40.dp)
                .clip(CircleShape)
                .background(color.copy(alpha = 0.1f))
        )
        
        Icon(
            imageVector = getIconForNotificationType(type.icon),
            contentDescription = null,
            tint = color,
            modifier = Modifier.size(18.dp)
        )
        
        if (!isRead) {
            Box(
                modifier = Modifier
                    .size(40.dp)
                    .clip(CircleShape)
                    .background(Color.Transparent)
                    .border(2.dp, color, CircleShape)
            )
        }
    }
}

// MARK: - Empty Notifications View
@Composable
private fun EmptyNotificationsView(selectedFilter: NotificationFilter) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(32.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center
    ) {
        Icon(
            imageVector = getIconForFilter(getIconForEmptyState(selectedFilter)),
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(60.dp)
        )
        
        Spacer(modifier = Modifier.height(20.dp))
        
        Text(
            text = getTitleForFilter(selectedFilter),
            style = MaterialTheme.typography.headlineMedium,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colorScheme.onSurface
        )
        
        Spacer(modifier = Modifier.height(8.dp))
        
        Text(
            text = getMessageForFilter(selectedFilter),
            style = MaterialTheme.typography.bodyLarge,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            textAlign = TextAlign.Center
        )
    }
}

// MARK: - Loading View
@Composable
private fun LoadingView() {
    Column(
        modifier = Modifier.fillMaxSize(),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center
    ) {
        CircularProgressIndicator(
            modifier = Modifier.size(48.dp),
            color = MaterialTheme.colorScheme.primary
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Text(
            text = "Loading notifications...",
            fontSize = 16.sp,
            fontWeight = FontWeight.Medium,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
    }
}

// MARK: - Error View
@Composable
private fun ErrorView(
    error: String,
    onRetry: () -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .padding(32.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center
    ) {
        Icon(
            imageVector = Icons.Default.Warning,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.tertiary,
            modifier = Modifier.size(48.dp)
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Text(
            text = "Notifications Error",
            style = MaterialTheme.typography.headlineMedium,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colorScheme.onSurface
        )
        
        Spacer(modifier = Modifier.height(8.dp))
        
        Text(
            text = error,
            style = MaterialTheme.typography.bodyLarge,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            textAlign = TextAlign.Center
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Button(onClick = onRetry) {
            Text("Try Again")
        }
    }
}

// MARK: - Notification Preferences Sheet
@Composable
private fun NotificationPreferencesSheet(
    preferences: NotificationPreferences,
    onSave: () -> Unit,
    onDismiss: () -> Unit
) {
    var currentPreferences by remember { mutableStateOf(preferences) }
    
    ModalBottomSheet(
        onDismissRequest = onDismiss,
        sheetState = rememberModalBottomSheetState()
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(24.dp)
        ) {
            Text(
                text = "Notification Settings",
                style = MaterialTheme.typography.headlineSmall,
                fontWeight = FontWeight.Bold
            )
            
            Spacer(modifier = Modifier.height(24.dp))
            
            // Push notifications
            Text(
                text = "Push Notifications",
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.SemiBold
            )
            
            Spacer(modifier = Modifier.height(8.dp))
            
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text("Enable Push Notifications")
                Switch(
                    checked = currentPreferences.pushEnabled,
                    onCheckedChange = { currentPreferences = currentPreferences.copy(pushEnabled = it) }
                )
            }
            
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text("Enable Email Notifications")
                Switch(
                    checked = currentPreferences.emailEnabled,
                    onCheckedChange = { currentPreferences = currentPreferences.copy(emailEnabled = it) }
                )
            }
            
            Spacer(modifier = Modifier.height(16.dp))
            
            // Notification types
            Text(
                text = "Notification Types",
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.SemiBold
            )
            
            Spacer(modifier = Modifier.height(8.dp))
            
            NotificationTypeToggle(
                title = "Likes",
                checked = currentPreferences.likeNotifications,
                onCheckedChange = { currentPreferences = currentPreferences.copy(likeNotifications = it) }
            )
            
            NotificationTypeToggle(
                title = "Replies",
                checked = currentPreferences.replyNotifications,
                onCheckedChange = { currentPreferences = currentPreferences.copy(replyNotifications = it) }
            )
            
            NotificationTypeToggle(
                title = "Reposts",
                checked = currentPreferences.repostNotifications,
                onCheckedChange = { currentPreferences = currentPreferences.copy(repostNotifications = it) }
            )
            
            NotificationTypeToggle(
                title = "Follows",
                checked = currentPreferences.followNotifications,
                onCheckedChange = { currentPreferences = currentPreferences.copy(followNotifications = it) }
            )
            
            NotificationTypeToggle(
                title = "Mentions",
                checked = currentPreferences.mentionNotifications,
                onCheckedChange = { currentPreferences = currentPreferences.copy(mentionNotifications = it) }
            )
            
            NotificationTypeToggle(
                title = "Hashtags",
                checked = currentPreferences.hashtagNotifications,
                onCheckedChange = { currentPreferences = currentPreferences.copy(hashtagNotifications = it) }
            )
            
            NotificationTypeToggle(
                title = "App Updates",
                checked = currentPreferences.appUpdateNotifications,
                onCheckedChange = { currentPreferences = currentPreferences.copy(appUpdateNotifications = it) }
            )
            
            NotificationTypeToggle(
                title = "System",
                checked = currentPreferences.systemNotifications,
                onCheckedChange = { currentPreferences = currentPreferences.copy(systemNotifications = it) }
            )
            
            Spacer(modifier = Modifier.height(24.dp))
            
            // Action buttons
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                OutlinedButton(
                    onClick = onDismiss,
                    modifier = Modifier.weight(1f)
                ) {
                    Text("Cancel")
                }
                
                Button(
                    onClick = onSave,
                    modifier = Modifier.weight(1f)
                ) {
                    Text("Save")
                }
            }
            
            Spacer(modifier = Modifier.height(16.dp))
        }
    }
}

// MARK: - Notification Type Toggle
@Composable
private fun NotificationTypeToggle(
    title: String,
    checked: Boolean,
    onCheckedChange: (Boolean) -> Unit
) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically
    ) {
        Text(title)
        Switch(
            checked = checked,
            onCheckedChange = onCheckedChange
        )
    }
}

// MARK: - App Update Dialog
@Composable
private fun AppUpdateDialog(
    appUpdate: AppUpdateInfo,
    onDismiss: () -> Unit
) {
    AlertDialog(
        onDismissRequest = onDismiss,
        title = {
            Text("Update Available")
        },
        text = {
            Column {
                Text("Version ${appUpdate.version}")
                    .fontWeight = FontWeight.SemiBold
                
                Spacer(modifier = Modifier.height(8.dp))
                
                Text(appUpdate.title)
                    .fontWeight = FontWeight.SemiBold
                
                Spacer(modifier = Modifier.height(4.dp))
                
                Text(appUpdate.description)
                    .color = MaterialTheme.colorScheme.onSurfaceVariant
                
                if (appUpdate.changelog.isNotEmpty()) {
                    Spacer(modifier = Modifier.height(16.dp))
                    
                    Text("What's New")
                        .fontWeight = FontWeight.SemiBold
                    
                    Spacer(modifier = Modifier.height(8.dp))
                    
                    appUpdate.changelog.forEach { change ->
                        Text("• $change")
                            .color = MaterialTheme.colorScheme.onSurfaceVariant
                    }
                }
            }
        },
        confirmButton = {
            Button(
                onClick = {
                    // Open download URL
                    // This would be implemented to open the URL
                }
            ) {
                Text(if (appUpdate.isRequired) "Update Now" else "Update")
            }
        },
        dismissButton = {
            if (!appUpdate.isRequired) {
                TextButton(onClick = onDismiss) {
                    Text("Later")
                }
            }
        }
    )
}

// MARK: - Helper Functions
private fun getIconForFilter(iconName: String): ImageVector {
    return when (iconName) {
        "bell" -> Icons.Default.Notifications
        "at" -> Icons.Default.AlternateEmail
        "heart" -> Icons.Default.Favorite
        "arrow_2_squarepath" -> Icons.Default.Reply
        "person_badge_plus" -> Icons.Default.PersonAdd
        "bubble_left" -> Icons.Default.ChatBubble
        "number" -> Icons.Default.Tag
        "app_badge" -> Icons.Default.Apps
        else -> Icons.Default.Notifications
    }
}

private fun getIconForNotificationType(iconName: String): ImageVector {
    return when (iconName) {
        "heart_fill" -> Icons.Default.Favorite
        "bubble_left_fill" -> Icons.Default.ChatBubble
        "arrow_2_squarepath_fill" -> Icons.Default.Reply
        "person_badge_plus_fill" -> Icons.Default.PersonAdd
        "at" -> Icons.Default.AlternateEmail
        "number" -> Icons.Default.Tag
        "app_badge_fill" -> Icons.Default.Apps
        "bell_fill" -> Icons.Default.Notifications
        else -> Icons.Default.Notifications
    }
}

private fun getIconForEmptyState(filter: NotificationFilter): String {
    return when (filter) {
        NotificationFilter.ALL -> "bell"
        NotificationFilter.MENTIONS -> "at"
        NotificationFilter.LIKES -> "heart"
        NotificationFilter.REPOSTS -> "arrow_2_squarepath"
        NotificationFilter.FOLLOWS -> "person_badge_plus"
        NotificationFilter.REPLIES -> "bubble_left"
        NotificationFilter.HASHTAGS -> "number"
        NotificationFilter.APP_UPDATES -> "app_badge"
    }
}

private fun getTitleForFilter(filter: NotificationFilter): String {
    return when (filter) {
        NotificationFilter.ALL -> "No notifications yet"
        NotificationFilter.MENTIONS -> "No mentions yet"
        NotificationFilter.LIKES -> "No likes yet"
        NotificationFilter.REPOSTS -> "No reposts yet"
        NotificationFilter.FOLLOWS -> "No new followers"
        NotificationFilter.REPLIES -> "No replies yet"
        NotificationFilter.HASHTAGS -> "No hashtag activity"
        NotificationFilter.APP_UPDATES -> "App is up to date"
    }
}

private fun getMessageForFilter(filter: NotificationFilter): String {
    return when (filter) {
        NotificationFilter.ALL -> "When you get notifications, they'll show up here."
        NotificationFilter.MENTIONS -> "When someone mentions you, it'll show up here."
        NotificationFilter.LIKES -> "When someone likes your posts, it'll show up here."
        NotificationFilter.REPOSTS -> "When someone reposts your content, it'll show up here."
        NotificationFilter.FOLLOWS -> "When someone follows you, it'll show up here."
        NotificationFilter.REPLIES -> "When someone replies to your posts, it'll show up here."
        NotificationFilter.HASHTAGS -> "When hashtags you follow have activity, it'll show up here."
        NotificationFilter.APP_UPDATES -> "Your app is running the latest version."
    }
}

private fun formatTimeAgo(timestamp: Long): String {
    val now = System.currentTimeMillis()
    val timeInterval = now - timestamp
    
    return when {
        timeInterval < 60000 -> "now"
        timeInterval < 3600000 -> "${timeInterval / 60000}m"
        timeInterval < 86400000 -> "${timeInterval / 3600000}h"
        timeInterval < 2592000000 -> "${timeInterval / 86400000}d"
        else -> "${timeInterval / 2592000000}mo"
    }
}