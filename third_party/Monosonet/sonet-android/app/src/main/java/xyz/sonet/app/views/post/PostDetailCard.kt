package xyz.sonet.app.views.post

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.clickable
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.foundation.Image
import androidx.compose.ui.layout.ContentScale
import coil.compose.rememberAsyncImagePainter
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.window.Dialog
import androidx.compose.foundation.gestures.Orientation
import androidx.compose.foundation.pager.HorizontalPager
import androidx.compose.foundation.pager.rememberPagerState
import xyz.sonet.app.grpc.proto.Note
import xyz.sonet.app.ui.AppIcons

@Composable
fun PostDetailCard(
    note: Note,
    currentUserId: String?,
    onReply: () -> Unit,
    onLike: () -> Unit,
    onRepost: () -> Unit,
    onShare: () -> Unit,
    isReply: Boolean = false
) {
    Card(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = if (isReply) 32.dp else 16.dp),
        elevation = CardDefaults.cardElevation(defaultElevation = 0.dp),
        colors = CardDefaults.cardColors(
            containerColor = if (isReply) MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.3f) 
                           else MaterialTheme.colorScheme.surface
        )
    ) {
        Column(
            modifier = Modifier.padding(vertical = 12.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            // User Header
            Row(
                verticalAlignment = Alignment.CenterVertically
            ) {
                // Avatar placeholder
                Surface(
                    modifier = Modifier.size(40.dp),
                    shape = MaterialTheme.shapes.circular,
                    color = MaterialTheme.colorScheme.surfaceVariant
                ) {
                    Icon(
                        imageVector = Icons.Default.Person,
                        contentDescription = null,
                        modifier = Modifier.padding(8.dp)
                    )
                }
                
                Spacer(modifier = Modifier.width(12.dp))
                
                Column {
                    Row(
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(
                            text = note.author?.displayName ?: "Unknown User",
                            style = MaterialTheme.typography.bodyMedium,
                            fontWeight = FontWeight.Semibold
                        )
                        
                        if (note.author?.isVerified == true) {
                            Spacer(modifier = Modifier.width(4.dp))
                            Icon(
                                imageVector = AppIcons.Verified, 
                                contentDescription = "Verified", 
                                tint = MaterialTheme.colorScheme.primary, 
                                modifier = Modifier.size(16.dp)
                            )
                        }
                    }
                    
                    Text(
                        text = "@${note.author?.handle ?: "unknown"}",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                
                Spacer(modifier = Modifier.weight(1f))
                
                Row(
                    verticalAlignment = Alignment.CenterVertically, 
                    horizontalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    Text(
                        text = formatTimeAgo(note.createdAt?.seconds ?: 0),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    
                    var showMenu by remember { mutableStateOf(false) }
                    Box {
                        IconButton(onClick = { showMenu = true }) {
                            Icon(
                                imageVector = AppIcons.More, 
                                contentDescription = "More options", 
                                tint = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                            }
                        DropdownMenu(
                            expanded = showMenu, 
                            onDismissRequest = { showMenu = false }
                        ) {
                            val isOwn = currentUserId != null && currentUserId == note.author?.userId
                            if (isOwn) {
                                DropdownMenuItem(
                                    text = { Text("Edit") }, 
                                    onClick = { showMenu = false }
                                )
                                DropdownMenuItem(
                                    text = { Text("Delete", color = MaterialTheme.colorScheme.error) }, 
                                    onClick = { showMenu = false }
                                )
                            } else {
                                DropdownMenuItem(
                                    text = { Text("Follow @${note.author?.handle ?: "unknown"}") }, 
                                    onClick = { showMenu = false }
                                )
                                DropdownMenuItem(
                                    text = { Text("Mute @${note.author?.handle ?: "unknown"}") }, 
                                    onClick = { showMenu = false }
                                )
                                DropdownMenuItem(
                                    text = { Text("Block @${note.author?.handle ?: "unknown"}", color = MaterialTheme.colorScheme.error) }, 
                                    onClick = { showMenu = false }
                                )
                                DropdownMenuItem(
                                    text = { Text("Report", color = MaterialTheme.colorScheme.error) }, 
                                    onClick = { showMenu = false }
                                )
                            }
                        }
                    }
                }
            }
            
            // Note Content
            if (note.content.isNotEmpty()) {
                Text(
                    text = note.content,
                    style = MaterialTheme.typography.bodyLarge
                )
            }
            
            // Media carousel
            if (note.attachmentsList.isNotEmpty()) {
                var showLightbox by remember { mutableStateOf(false) }
                var startIndex by remember { mutableStateOf(0) }
                
                MediaCarousel(
                    media = note.attachmentsList,
                    onMediaTap = { tappedIndex ->
                        startIndex = tappedIndex
                        showLightbox = true
                    }
                )
                
                if (showLightbox) {
                    Dialog(onDismissRequest = { showLightbox = false }) {
                        Box(modifier = Modifier.fillMaxSize()) {
                            MediaLightbox(
                                media = note.attachmentsList,
                                startIndex = startIndex,
                                onDismiss = { showLightbox = false }
                            )
                        }
                    }
                }
            }
            
            // Action Buttons
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 12.dp),
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                // Reply Button
                IconButton(onClick = onReply) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Icon(
                            imageVector = AppIcons.Comment, 
                            contentDescription = "Reply"
                        )
                        Spacer(modifier = Modifier.width(4.dp))
                        Text("${note.replyCount}")
                    }
                }
                
                // Repost Button
                IconButton(onClick = onRepost) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Icon(
                            imageVector = AppIcons.Repost, 
                            contentDescription = "Repost",
                            tint = if (note.userState?.isReposted == true) 
                                   MaterialTheme.colorScheme.primary 
                                   else MaterialTheme.colorScheme.onSurfaceVariant
                        )
                        Spacer(modifier = Modifier.width(4.dp))
                        Text("${note.renoteCount}")
                    }
                }
                
                // Like Button
                IconButton(onClick = onLike) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Icon(
                            imageVector = if (note.userState?.isLiked == true) 
                                         AppIcons.Like 
                                         else AppIcons.LikeBorder, 
                            contentDescription = "Like",
                            tint = if (note.userState?.isLiked == true) 
                                   MaterialTheme.colorScheme.error 
                                   else MaterialTheme.colorScheme.onSurfaceVariant
                        )
                        Spacer(modifier = Modifier.width(4.dp))
                        Text("${note.likeCount}")
                    }
                }
                
                // Share Button
                IconButton(onClick = onShare) {
                    Icon(
                        imageVector = AppIcons.Share, 
                        contentDescription = "Share"
                    )
                }
            }
        }
    }
}

@Composable
fun ThreadSeparator() {
    Box(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp)
    ) {
        Divider(
            modifier = Modifier.padding(start = 40.dp),
            color = MaterialTheme.colorScheme.outline.copy(alpha = 0.3f)
        )
    }
}

@Composable
fun MediaCarousel(
    media: List<Note.Attachment>,
    onMediaTap: (Int) -> Unit
) {
    if (media.isEmpty()) return
    
    val pagerState = rememberPagerState(pageCount = { media.size })
    
    HorizontalPager(
        state = pagerState,
        modifier = Modifier
            .fillMaxWidth()
            .height(200.dp)
    ) { page ->
        val mediaItem = media[page]
        Box(
            modifier = Modifier
                .fillMaxSize()
                .clickable { onMediaTap(page) }
        ) {
            Image(
                painter = rememberAsyncImagePainter(mediaItem.url),
                contentDescription = mediaItem.altText.ifEmpty { "Media content" },
                modifier = Modifier.fillMaxSize(),
                contentScale = ContentScale.Crop
            )
        }
    }
}

@Composable
fun MediaLightbox(
    media: List<Note.Attachment>,
    startIndex: Int,
    onDismiss: () -> Unit
) {
    val pagerState = rememberPagerState(
        initialPage = startIndex,
        pageCount = { media.size }
    )
    
    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(Color.Black)
    ) {
        // Close button
        IconButton(
            onClick = onDismiss,
            modifier = Modifier
                .align(Alignment.TopEnd)
                .padding(16.dp)
        ) {
            Icon(
                imageVector = Icons.Default.Close,
                contentDescription = "Close",
                tint = Color.White
            )
        }
        
        // Media pager
        HorizontalPager(
            state = pagerState,
            modifier = Modifier.fillMaxSize()
        ) { page ->
            val mediaItem = media[page]
            Box(
                modifier = Modifier.fillMaxSize(),
                contentAlignment = Alignment.Center
            ) {
                Image(
                    painter = rememberAsyncImagePainter(mediaItem.url),
                    contentDescription = mediaItem.altText.ifEmpty { "Media content" },
                    modifier = Modifier.fillMaxSize(),
                    contentScale = ContentScale.Fit
                )
            }
        }
        
        // Page indicator
        if (media.size > 1) {
            Row(
                modifier = Modifier
                    .align(Alignment.BottomCenter)
                    .padding(bottom = 32.dp),
                horizontalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                repeat(media.size) { index ->
                    Surface(
                        modifier = Modifier.size(8.dp),
                        shape = MaterialTheme.shapes.circular,
                        color = if (pagerState.currentPage == index) 
                                Color.White 
                                else Color.White.copy(alpha = 0.5f)
                    ) {}
                }
            }
        }
    }
}

private fun formatTimeAgo(timestampSeconds: Long): String {
    val now = System.currentTimeMillis() / 1000
    val timeDiff = now - timestampSeconds
    
    return when {
        timeDiff < 60 -> "now"
        timeDiff < 3600 -> "${timeDiff / 60}m"
        timeDiff < 86400 -> "${timeDiff / 3600}h"
        timeDiff < 2592000 -> "${timeDiff / 86400}d"
        else -> "${timeDiff / 2592000}mo"
    }
}