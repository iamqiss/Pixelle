package xyz.sonet.app.ui.contentcreation

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
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.scale
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
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
import xyz.sonet.app.viewmodels.ContentCreationViewModel
import java.text.SimpleDateFormat
import java.util.*

@Composable
fun ContentCreationView(
    replyToNote: Note? = null,
    quoteNote: Note? = null,
    modifier: Modifier = Modifier,
    viewModel: ContentCreationViewModel = viewModel()
) {
    val context = LocalContext.current
    val noteContent by viewModel.noteContent.collectAsState()
    val selectedMedia by viewModel.selectedMedia.collectAsState()
    val replyToNoteState by viewModel.replyToNote.collectAsState()
    val quoteNoteState by viewModel.quoteNote.collectAsState()
    val selectedHashtags by viewModel.selectedHashtags.collectAsState()
    val selectedMentions by viewModel.selectedMentions.collectAsState()
    val isPosting by viewModel.isPosting.collectAsState()
    val postingError by viewModel.postingError.collectAsState()
    val showHashtagSuggestions by viewModel.showHashtagSuggestions.collectAsState()
    val showMentionSuggestions by viewModel.showMentionSuggestions.collectAsState()
    val hashtagSuggestions by viewModel.hashtagSuggestions.collectAsState()
    val mentionSuggestions by viewModel.mentionSuggestions.collectAsState()
    val currentHashtagQuery by viewModel.currentHashtagQuery.collectAsState()
    val currentMentionQuery by viewModel.currentMentionQuery.collectAsState()
    val scheduledDate by viewModel.scheduledDate.collectAsState()
    val showScheduling by viewModel.showScheduling.collectAsState()
    val isDraft by viewModel.isDraft.collectAsState()
    val draftId by viewModel.draftId.collectAsState()
    
    // Set reply/quote notes when view is created
    LaunchedEffect(replyToNote, quoteNote) {
        viewModel.setReplyToNote(replyToNote)
        viewModel.setQuoteNote(quoteNote)
    }
    
    Column(
        modifier = modifier.fillMaxSize()
    ) {
        // Header
        ContentCreationHeader(
            canPost = viewModel.canPost,
            isPosting = isPosting,
            onPost = { viewModel.postNote() },
            onCancel = { /* Navigate back */ }
        )
        
        // Content
        LazyColumn(
            modifier = Modifier.weight(1f),
            contentPadding = PaddingValues(16.dp)
        ) {
            // Reply/Quote context
            replyToNoteState?.let { note ->
                item {
                    ReplyContextView(note = note)
                }
            }
            
            quoteNoteState?.let { note ->
                item {
                    QuoteContextView(note = note)
                }
            }
            
            // Main content area
            item {
                ContentArea(
                    noteContent = noteContent,
                    selectedMedia = selectedMedia,
                    characterCount = viewModel.characterCount,
                    remainingCharacters = viewModel.remainingCharacters,
                    characterCountColor = viewModel.characterCountColor,
                    onContentChange = { viewModel.updateNoteContent(it) },
                    onAddMedia = { viewModel.addMedia(it) },
                    onRemoveMedia = { viewModel.removeMedia(it) }
                )
            }
            
            // Hashtags and mentions
            if (selectedHashtags.isNotEmpty() || selectedMentions.isNotEmpty()) {
                item {
                    TagsAndMentionsView(
                        hashtags = selectedHashtags,
                        mentions = selectedMentions,
                        onRemoveHashtag = { viewModel.removeHashtag(it) },
                        onRemoveMention = { viewModel.removeMention(it) }
                    )
                }
            }
            
            // Scheduling info
            scheduledDate?.let { date ->
                item {
                    SchedulingInfoView(
                        scheduledDate = date,
                        onCancel = { viewModel.cancelScheduling() }
                    )
                }
            }
            
            // Error message
            postingError?.let { error ->
                item {
                    ErrorMessageView(error = error)
                }
            }
        }
        
        // Bottom toolbar
        ContentCreationToolbar(
            showHashtagSuggestions = showHashtagSuggestions,
            showMentionSuggestions = showMentionSuggestions,
            hashtagSuggestions = hashtagSuggestions,
            mentionSuggestions = mentionSuggestions,
            currentHashtagQuery = currentHashtagQuery,
            currentMentionQuery = currentMentionQuery,
            onAddHashtag = { viewModel.addHashtag(it) },
            onAddMention = { viewModel.addMention(it) },
            onSchedule = { viewModel.showScheduling() },
            onSaveDraft = { viewModel.saveDraft() }
        )
        
        // Suggestions overlay
        if (showHashtagSuggestions || showMentionSuggestions) {
            SuggestionsOverlay(
                showHashtagSuggestions = showHashtagSuggestions,
                showMentionSuggestions = showMentionSuggestions,
                hashtagSuggestions = hashtagSuggestions,
                mentionSuggestions = mentionSuggestions,
                currentHashtagQuery = currentHashtagQuery,
                currentMentionQuery = currentMentionQuery,
                onAddHashtag = { viewModel.addHashtag(it) },
                onAddMention = { viewModel.addMention(it) }
            )
        }
    }
    
    // Scheduling dialog
    if (showScheduling) {
        SchedulingDialog(
            onSchedule = { viewModel.schedulePost(it) },
            onCancel = { viewModel.cancelScheduling() }
        )
    }
}

// MARK: - Content Creation Header
@Composable
private fun ContentCreationHeader(
    canPost: Boolean,
    isPosting: Boolean,
    onPost: () -> Unit,
    onCancel: () -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surface)
            .padding(horizontal = 16.dp, vertical = 12.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        TextButton(onClick = onCancel) {
            Text(
                text = "Cancel",
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        
        Spacer(modifier = Modifier.weight(1f))
        
        Button(
            onClick = onPost,
            enabled = canPost,
            colors = ButtonDefaults.buttonColors(
                containerColor = if (canPost) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.surfaceVariant
            ),
            shape = RoundedCornerShape(20.dp)
        ) {
            if (isPosting) {
                CircularProgressIndicator(
                    modifier = Modifier.size(16.dp),
                    color = MaterialTheme.colorScheme.onPrimary,
                    strokeWidth = 2.dp
                )
            } else {
                Text(
                    text = "Post",
                    fontWeight = FontWeight.SemiBold,
                    color = if (canPost) MaterialTheme.colorScheme.onPrimary else MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
    }
    
    Divider()
}

// MARK: - Reply Context View
@Composable
private fun ReplyContextView(note: Note) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.3f))
            .padding(12.dp),
        verticalAlignment = Alignment.Top
    ) {
        // Reply indicator
        Column(
            modifier = Modifier.width(8.dp),
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Box(
                modifier = Modifier
                    .size(8.dp)
                    .clip(CircleShape)
                    .background(MaterialTheme.colorScheme.onSurfaceVariant)
            )
            
            Box(
                modifier = Modifier
                    .width(2.dp)
                    .height(40.dp)
                    .background(MaterialTheme.colorScheme.onSurfaceVariant)
            )
        }
        
        Spacer(modifier = Modifier.width(12.dp))
        
        // Note preview
        Column {
            Text(
                text = "Replying to",
                fontSize = 12.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            
            Text(
                text = note.content,
                fontSize = 14.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis
            )
        }
    }
}

// MARK: - Quote Context View
@Composable
private fun QuoteContextView(note: Note) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.3f))
            .padding(12.dp),
        verticalAlignment = Alignment.Top
    ) {
        // Quote indicator
        Column(
            modifier = Modifier.width(8.dp),
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Box(
                modifier = Modifier
                    .size(8.dp)
                    .clip(CircleShape)
                    .background(MaterialTheme.colorScheme.onSurfaceVariant)
            )
            
            Box(
                modifier = Modifier
                    .width(2.dp)
                    .height(40.dp)
                    .background(MaterialTheme.colorScheme.onSurfaceVariant)
            )
        }
        
        Spacer(modifier = Modifier.width(12.dp))
        
        // Note preview
        Column {
            Text(
                text = "Quoting",
                fontSize = 12.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            
            Text(
                text = note.content,
                fontSize = 14.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis
            )
        }
    }
}

// MARK: - Content Area
@Composable
private fun ContentArea(
    noteContent: String,
    selectedMedia: List<xyz.sonet.app.viewmodels.MediaItem>,
    characterCount: Int,
    remainingCharacters: Int,
    characterCountColor: androidx.compose.ui.graphics.Color,
    onContentChange: (String) -> Unit,
    onAddMedia: (xyz.sonet.app.viewmodels.MediaItem) -> Unit,
    onRemoveMedia: (Int) -> Unit
) {
    Column(
        modifier = Modifier.fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        // Text input
        BasicTextField(
            value = noteContent,
            onValueChange = onContentChange,
            textStyle = MaterialTheme.typography.bodyLarge.copy(
                fontSize = 20.sp,
                color = MaterialTheme.colorScheme.onSurface
            ),
            modifier = Modifier
                .fillMaxWidth()
                .heightIn(min = 120.dp),
            decorationBox = { innerTextField ->
                Box {
                    if (noteContent.isEmpty()) {
                        Text(
                            text = "What's happening?",
                            fontSize = 20.sp,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                    innerTextField()
                }
            }
        )
        
        // Media grid
        if (selectedMedia.isNotEmpty()) {
            MediaGridView(
                media = selectedMedia,
                onRemove = onRemoveMedia
            )
        }
        
        // Character count
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.End
        ) {
            Text(
                text = "$characterCount/280",
                fontSize = 12.sp,
                color = characterCountColor
            )
        }
    }
}

// MARK: - Media Grid View
@Composable
private fun MediaGridView(
    media: List<xyz.sonet.app.viewmodels.MediaItem>,
    onRemove: (Int) -> Unit
) {
    LazyVerticalGrid(
        columns = GridCells.Fixed(2),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        items(media.size) { index ->
            MediaItemView(
                media = media[index],
                onRemove = { onRemove(index) }
            )
        }
    }
}

// MARK: - Media Item View
@Composable
private fun MediaItemView(
    media: xyz.sonet.app.viewmodels.MediaItem,
    onRemove: () -> Unit
) {
    Box(
        modifier = Modifier
            .aspectRatio(1f)
            .clip(RoundedCornerShape(8.dp))
    ) {
        // Media content
        if (media.localUri != null) {
            AsyncImage(
                model = ImageRequest.Builder(LocalContext.current)
                    .data(media.localUri)
                    .crossfade(true)
                    .build(),
                contentDescription = "Local media",
                modifier = Modifier.fillMaxSize(),
                contentScale = ContentScale.Crop
            )
        } else {
            AsyncImage(
                model = ImageRequest.Builder(LocalContext.current)
                    .data(media.url)
                    .crossfade(true)
                    .build(),
                contentDescription = "Media",
                modifier = Modifier.fillMaxSize(),
                contentScale = ContentScale.Crop
            )
        }
        
        // Remove button
        IconButton(
            onClick = onRemove,
            modifier = Modifier
                .align(Alignment.TopEnd)
                .padding(8.dp)
        ) {
            Icon(
                imageVector = Icons.Default.Close,
                contentDescription = "Remove",
                tint = MaterialTheme.colorScheme.onBackground,
                modifier = Modifier
                    .size(20.dp)
                    .background(
                        CircleShape,
                        MaterialTheme.colorScheme.background.copy(alpha = 0.6f)
                    )
            )
        }
    }
}

// MARK: - Tags and Mentions View
@Composable
private fun TagsAndMentionsView(
    hashtags: List<String>,
    mentions: List<String>,
    onRemoveHashtag: (String) -> Unit,
    onRemoveMention: (String) -> Unit
) {
    Column(
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        // Hashtags
        if (hashtags.isNotEmpty()) {
            Column {
                Text(
                    text = "Hashtags",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                
                Spacer(modifier = Modifier.height(8.dp))
                
                LazyRow(
                    horizontalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    items(hashtags) { hashtag ->
                        HashtagChip(
                            hashtag = hashtag,
                            onRemove = { onRemoveHashtag(hashtag) }
                        )
                    }
                }
            }
        }
        
        // Mentions
        if (mentions.isNotEmpty()) {
            Column {
                Text(
                    text = "Mentions",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                
                Spacer(modifier = Modifier.height(8.dp))
                
                LazyRow(
                    horizontalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    items(mentions) { mention ->
                        MentionChip(
                            mention = mention,
                            onRemove = { onRemoveMention(mention) }
                        )
                    }
                }
            }
        }
    }
}

// MARK: - Hashtag Chip
@Composable
private fun HashtagChip(
    hashtag: String,
    onRemove: () -> Unit
) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .clip(RoundedCornerShape(12.dp))
            .background(MaterialTheme.colorScheme.primary.copy(alpha = 0.1f))
            .padding(horizontal = 8.dp, vertical = 4.dp)
    ) {
        Text(
            text = hashtag,
            fontSize = 12.sp,
            color = MaterialTheme.colorScheme.primary
        )
        
        Spacer(modifier = Modifier.width(6.dp))
        
        IconButton(
            onClick = onRemove,
            modifier = Modifier.size(16.dp)
        ) {
            Icon(
                imageVector = Icons.Default.Close,
                contentDescription = "Remove hashtag",
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(12.dp)
            )
        }
    }
}

// MARK: - Mention Chip
@Composable
private fun MentionChip(
    mention: String,
    onRemove: () -> Unit
) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .clip(RoundedCornerShape(12.dp))
            .background(MaterialTheme.colorScheme.primary.copy(alpha = 0.1f))
            .padding(horizontal = 8.dp, vertical = 4.dp)
    ) {
        Text(
            text = mention,
            fontSize = 12.sp,
            color = MaterialTheme.colorScheme.primary
        )
        
        Spacer(modifier = Modifier.width(6.dp))
        
        IconButton(
            onClick = onRemove,
            modifier = Modifier.size(16.dp)
        ) {
            Icon(
                imageVector = Icons.Default.Close,
                contentDescription = "Remove mention",
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(12.dp)
            )
        }
    }
}

// MARK: - Scheduling Info View
@Composable
private fun SchedulingInfoView(
    scheduledDate: Long,
    onCancel: () -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surfaceVariant)
            .padding(12.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Icon(
            imageVector = Icons.Default.Schedule,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(16.dp)
        )
        
        Spacer(modifier = Modifier.width(8.dp))
        
        Text(
            text = "Scheduled for ${formatDate(scheduledDate)}",
            fontSize = 12.sp,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
        
        Spacer(modifier = Modifier.weight(1f))
        
        TextButton(
            onClick = onCancel,
            modifier = Modifier.padding(0.dp)
        ) {
            Text(
                text = "Cancel",
                fontSize = 12.sp,
                color = MaterialTheme.colorScheme.primary
            )
        }
    }
}

// MARK: - Error Message View
@Composable
private fun ErrorMessageView(error: String) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surfaceVariant)
            .padding(12.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Icon(
            imageVector = Icons.Default.Warning,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(16.dp)
        )
        
        Spacer(modifier = Modifier.width(8.dp))
        
        Text(
            text = error,
            fontSize = 12.sp,
            color = MaterialTheme.colorScheme.onSurface
        )
    }
}

// MARK: - Content Creation Toolbar
@Composable
private fun ContentCreationToolbar(
    showHashtagSuggestions: Boolean,
    showMentionSuggestions: Boolean,
    hashtagSuggestions: List<String>,
    mentionSuggestions: List<UserProfile>,
    currentHashtagQuery: String,
    currentMentionQuery: String,
    onAddHashtag: (String) -> Unit,
    onAddMention: (UserProfile) -> Unit,
    onSchedule: () -> Unit,
    onSaveDraft: () -> Unit
) {
    Column {
        // Main toolbar
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .background(MaterialTheme.colorScheme.surface)
                .padding(horizontal = 16.dp, vertical = 12.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            // Media button
            IconButton(onClick = { /* Show media picker */ }) {
                Icon(
                    imageVector = Icons.Default.Photo,
                    contentDescription = "Add media",
                    tint = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.size(20.dp)
                )
            }
            
            // Hashtag button
            IconButton(onClick = { /* Show hashtag picker */ }) {
                Icon(
                    imageVector = Icons.Default.Tag,
                    contentDescription = "Add hashtag",
                    tint = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.size(20.dp)
                )
            }
            
            // Mention button
            IconButton(onClick = { /* Show mention picker */ }) {
                Icon(
                    imageVector = Icons.Default.AlternateEmail,
                    contentDescription = "Add mention",
                    tint = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.size(20.dp)
                )
            }
            
            // Schedule button
            IconButton(onClick = onSchedule) {
                Icon(
                    imageVector = Icons.Default.Schedule,
                    contentDescription = "Schedule post",
                    tint = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.size(20.dp)
                )
            }
            
            Spacer(modifier = Modifier.weight(1f))
            
            // Draft button
            IconButton(onClick = onSaveDraft) {
                Icon(
                    imageVector = Icons.Default.Save,
                    contentDescription = "Save draft",
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(20.dp)
                )
            }
        }
        
        Divider()
    }
}

// MARK: - Suggestions Overlay
@Composable
private fun SuggestionsOverlay(
    showHashtagSuggestions: Boolean,
    showMentionSuggestions: Boolean,
    hashtagSuggestions: List<String>,
    mentionSuggestions: List<UserProfile>,
    currentHashtagQuery: String,
    currentMentionQuery: String,
    onAddHashtag: (String) -> Unit,
    onAddMention: (UserProfile) -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surface)
            .padding(horizontal = 16.dp)
    ) {
        if (showHashtagSuggestions) {
            HashtagSuggestionsView(
                suggestions = hashtagSuggestions,
                query = currentHashtagQuery,
                onSelect = onAddHashtag
            )
        }
        
        if (showMentionSuggestions) {
            MentionSuggestionsView(
                suggestions = mentionSuggestions,
                query = currentMentionQuery,
                onSelect = onAddMention
            )
        }
    }
}

// MARK: - Hashtag Suggestions View
@Composable
private fun HashtagSuggestionsView(
    suggestions: List<String>,
    query: String,
    onSelect: (String) -> Unit
) {
    Column {
        Text(
            text = "Hashtags",
            fontSize = 12.sp,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp)
        )
        
        suggestions.forEach { hashtag ->
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable { onSelect(hashtag) }
                    .padding(horizontal = 12.dp, vertical = 8.dp)
            ) {
                Text(
                    text = "#$hashtag",
                    color = MaterialTheme.colorScheme.onSurface
                )
            }
            
            if (hashtag != suggestions.last()) {
                Divider(modifier = Modifier.padding(start = 12.dp))
            }
        }
    }
}

// MARK: - Mention Suggestions View
@Composable
private fun MentionSuggestionsView(
    suggestions: List<UserProfile>,
    query: String,
    onSelect: (UserProfile) -> Unit
) {
    Column {
        Text(
            text = "Users",
            fontSize = 12.sp,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp)
        )
        
        suggestions.forEach { user ->
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable { onSelect(user) }
                    .padding(horizontal = 12.dp, vertical = 8.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                AsyncImage(
                    model = ImageRequest.Builder(LocalContext.current)
                        .data(user.avatarUrl)
                        .crossfade(true)
                        .build(),
                    contentDescription = "User avatar",
                    modifier = Modifier
                        .size(32.dp)
                        .clip(CircleShape),
                    contentScale = ContentScale.Crop
                )
                
                Spacer(modifier = Modifier.width(12.dp))
                
                Column {
                    Text(
                        text = user.displayName,
                        color = MaterialTheme.colorScheme.onSurface
                    )
                    
                    Text(
                        text = "@${user.username}",
                        fontSize = 12.sp,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
            
            if (user.userId != suggestions.last()?.userId) {
                Divider(modifier = Modifier.padding(start = 56.dp))
            }
        }
    }
}

// MARK: - Scheduling Dialog
@Composable
private fun SchedulingDialog(
    onSchedule: (Long) -> Unit,
    onCancel: () -> Unit
) {
    var selectedDate by remember { mutableStateOf(System.currentTimeMillis() + 3600000) } // 1 hour from now
    
    AlertDialog(
        onDismissRequest = onCancel,
        title = {
            Text("Schedule Post")
        },
        text = {
            Column {
                Text("Select when to post:")
                
                Spacer(modifier = Modifier.height(16.dp))
                
                // Date picker would go here
                // For now, just show a simple text input
                Text("Scheduled for: ${formatDate(selectedDate)}")
            }
        },
        confirmButton = {
            TextButton(onClick = { onSchedule(selectedDate) }) {
                Text("Schedule")
            }
        },
        dismissButton = {
            TextButton(onClick = onCancel) {
                Text("Cancel")
            }
        }
    )
}

// MARK: - Helper Functions
private fun formatDate(timestamp: Long): String {
    val date = Date(timestamp)
    val formatter = SimpleDateFormat("MMM dd, HH:mm", Locale.getDefault())
    return formatter.format(date)
}