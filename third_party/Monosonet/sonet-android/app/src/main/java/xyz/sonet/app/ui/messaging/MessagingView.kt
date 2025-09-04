package xyz.sonet.app.ui.messaging

import androidx.compose.animation.*
import androidx.compose.animation.core.*
import androidx.compose.foundation.*
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material.icons.outlined.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.scale
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewmodel.compose.viewModel
import coil.compose.AsyncImage
import coil.request.ImageRequest
import xyz.sonet.app.grpc.proto.*
import xyz.sonet.app.viewmodels.*
import java.text.SimpleDateFormat
import java.util.*

@Composable
fun MessagingView(
    modifier: Modifier = Modifier,
    viewModel: MessagingViewModel = viewModel()
) {
    val conversations by viewModel.filteredConversations.collectAsState()
    val currentConversation by viewModel.currentConversation.collectAsState()
    val messages by viewModel.messages.collectAsState()
    val messageText by viewModel.messageText.collectAsState()
    val isTyping by viewModel.isTyping.collectAsState()
    val isLoading by viewModel.isLoading.collectAsState()
    val error by viewModel.error.collectAsState()
    val selectedMedia by viewModel.selectedMedia.collectAsState()
    val isRecordingVoice by viewModel.isRecordingVoice.collectAsState()
    val voiceRecordingDuration by viewModel.voiceRecordingDuration.collectAsState()
    val showConversationList by viewModel.showConversationList.collectAsState()
    
    LaunchedEffect(Unit) {
        viewModel.loadConversations()
    }
    
    Box(modifier = modifier.fillMaxSize()) {
        when {
            showConversationList -> {
                ConversationListView(
                    conversations = conversations,
                    isLoading = isLoading,
                    error = error,
                    onConversationSelected = { viewModel.selectConversation(it) },
                    onRetry = { viewModel.loadConversations() }
                )
            }
            currentConversation != null -> {
                ChatView(
                    conversation = currentConversation!!,
                    messages = messages,
                    messageText = messageText,
                    isTyping = isTyping,
                    selectedMedia = selectedMedia,
                    isRecordingVoice = isRecordingVoice,
                    voiceRecordingDuration = voiceRecordingDuration,
                    onMessageTextChange = { viewModel.updateMessageText(it) },
                    onSendMessage = { viewModel.sendMessage() },
                    onSendMedia = { viewModel.sendMediaMessage() },
                    onStartVoiceRecording = { viewModel.startVoiceRecording() },
                    onStopVoiceRecording = { viewModel.stopVoiceRecording() },
                    onDeleteMessage = { viewModel.deleteMessage(it) },
                    onReportMessage = { viewModel.reportMessage(it) },
                    onBack = { viewModel.backToConversationList() },
                    onAddMedia = { viewModel.addMedia(it) },
                    onRemoveMedia = { viewModel.removeMedia(it) }
                )
            }
        }
        
        // Error Snackbar
        error?.let { errorMessage ->
            LaunchedEffect(errorMessage) {
                // Show error snackbar
            }
        }
    }
}

// MARK: - Conversation List View
@Composable
fun ConversationListView(
    conversations: List<Conversation>,
    isLoading: Boolean,
    error: String?,
    onConversationSelected: (Conversation) -> Unit,
    onRetry: () -> Unit
) {
    Column(modifier = Modifier.fillMaxSize()) {
        // Header
        ConversationListHeader()
        
        // Content
        when {
            isLoading -> LoadingView()
            error != null -> ErrorView(error = error, onRetry = onRetry)
            conversations.isEmpty() -> EmptyConversationsView()
            else -> {
                LazyColumn(
                    modifier = Modifier.fillMaxSize(),
                    contentPadding = PaddingValues(vertical = 8.dp)
                ) {
                    items(conversations) { conversation ->
                        ConversationRow(
                            conversation = conversation,
                            onTap = { onConversationSelected(conversation) }
                        )
                        
                        if (conversation.id != conversations.lastOrNull()?.id) {
                            Divider(
                                modifier = Modifier.padding(start = 72.dp, end = 16.dp),
                                color = MaterialTheme.colorScheme.outlineVariant
                            )
                        }
                    }
                }
            }
        }
    }
}

// MARK: - Conversation List Header
@Composable
fun ConversationListHeader() {
    Surface(
        modifier = Modifier.fillMaxWidth(),
        color = MaterialTheme.colorScheme.surface,
        shadowElevation = 2.dp
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp, vertical = 12.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Messages",
                style = MaterialTheme.typography.headlineMedium,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.onSurface
            )
            
            Spacer(modifier = Modifier.weight(1f))
            
            IconButton(onClick = { /* New conversation */ }) {
                Icon(
                    imageVector = Icons.Default.Edit,
                    contentDescription = "New conversation",
                    tint = MaterialTheme.colorScheme.primary
                )
            }
        }
    }
}

// MARK: - Conversation Row
@Composable
fun ConversationRow(
    conversation: Conversation,
    onTap: () -> Unit
) {
    Card(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp, vertical = 4.dp),
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.surface
        ),
        elevation = CardDefaults.cardElevation(defaultElevation = 0.dp)
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(12.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            // Avatar
            ConversationAvatar(conversation = conversation)
            
            Spacer(modifier = Modifier.width(12.dp))
            
            // Content
            Column(
                modifier = Modifier.weight(1f),
                verticalArrangement = Arrangement.spacedBy(4.dp)
            ) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Text(
                        text = conversation.name,
                        style = MaterialTheme.typography.bodyLarge,
                        fontWeight = FontWeight.SemiBold,
                        color = MaterialTheme.colorScheme.onSurface
                    )
                    
                    Text(
                        text = formatTimeAgo(conversation.lastMessageTime),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Text(
                        text = conversation.lastMessage?.content ?: "No messages yet",
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                        modifier = Modifier.weight(1f)
                    )
                    
                    if (conversation.unreadCount > 0) {
                        Badge(
                            containerColor = MaterialTheme.colorScheme.primary
                        ) {
                            Text(
                                text = conversation.unreadCount.toString(),
                                style = MaterialTheme.typography.labelSmall,
                                color = MaterialTheme.colorScheme.onPrimary
                            )
                        }
                    }
                }
            }
        }
    }
}

// MARK: - Conversation Avatar
@Composable
fun ConversationAvatar(conversation: Conversation) {
    if (conversation.isGroup) {
        // Group avatar
        Box(
            modifier = Modifier
                .size(50.dp)
                .clip(CircleShape)
                .background(MaterialTheme.colorScheme.primaryContainer),
            contentAlignment = Alignment.Center
        ) {
            Icon(
                imageVector = Icons.Outlined.Group,
                contentDescription = "Group",
                tint = MaterialTheme.colorScheme.onPrimaryContainer,
                modifier = Modifier.size(24.dp)
            )
        }
    } else {
        // Individual avatar
        conversation.participants.firstOrNull()?.let { participant ->
            AsyncImage(
                model = ImageRequest.Builder(LocalContext.current)
                    .data(participant.avatarUrl)
                    .crossfade(true)
                    .build(),
                contentDescription = "Avatar",
                modifier = Modifier
                    .size(50.dp)
                    .clip(CircleShape),
                contentScale = ContentScale.Crop,
                error = {
                    Box(
                        modifier = Modifier
                            .size(50.dp)
                            .clip(CircleShape)
                            .background(MaterialTheme.colorScheme.surfaceVariant),
                        contentAlignment = Alignment.Center
                    ) {
                        Icon(
                            imageVector = Icons.Default.Person,
                            contentDescription = "Avatar placeholder",
                            tint = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
            )
            
            // Online indicator
            if (conversation.isOnline) {
                Box(
                    modifier = Modifier
                        .size(14.dp)
                        .clip(CircleShape)
                        .background(MaterialTheme.colorScheme.primary)
                        .border(
                            width = 2.dp,
                            color = MaterialTheme.colorScheme.surface,
                            shape = CircleShape
                        ),
                    contentAlignment = Alignment.Center
                )
            }
        }
    }
}

// MARK: - Chat View
@Composable
fun ChatView(
    conversation: Conversation,
    messages: List<Message>,
    messageText: String,
    isTyping: Boolean,
    selectedMedia: List<MediaItem>,
    isRecordingVoice: Boolean,
    voiceRecordingDuration: Double,
    onMessageTextChange: (String) -> Unit,
    onSendMessage: () -> Unit,
    onSendMedia: () -> Unit,
    onStartVoiceRecording: () -> Unit,
    onStopVoiceRecording: () -> Unit,
    onDeleteMessage: (Message) -> Unit,
    onReportMessage: (Message) -> Unit = {},
    onBack: () -> Unit,
    onAddMedia: (MediaItem) -> Unit,
    onRemoveMedia: (Int) -> Unit
) {
    Column(modifier = Modifier.fillMaxSize()) {
        // Chat header
        ChatHeader(
            conversation = conversation,
            onBack = onBack
        )
        
        // Messages
        LazyColumn(
            modifier = Modifier
                .fillMaxWidth()
                .weight(1f),
            contentPadding = PaddingValues(horizontal = 16.dp, vertical = 8.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            items(messages) { message ->
                MessageBubble(
                    message = message,
                    onDelete = { onDeleteMessage(message) },
                    onReport = { onReportMessage(message) }
                )
            }
            
            // Typing indicator
            if (isTyping) {
                item {
                    TypingIndicator()
                }
            }
        }
        
        // Input area
        ChatInputArea(
            messageText = messageText,
            selectedMedia = selectedMedia,
            isRecordingVoice = isRecordingVoice,
            voiceRecordingDuration = voiceRecordingDuration,
            onMessageTextChange = onMessageTextChange,
            onSendMessage = onSendMessage,
            onSendMedia = onSendMedia,
            onStartVoiceRecording = onStartVoiceRecording,
            onStopVoiceRecording = onStopVoiceRecording,
            onAddMedia = onAddMedia,
            onRemoveMedia = onRemoveMedia
        )
    }
}

// MARK: - Chat Header
@Composable
fun ChatHeader(
    conversation: Conversation,
    onBack: () -> Unit
) {
    Surface(
        modifier = Modifier.fillMaxWidth(),
        color = MaterialTheme.colorScheme.surface,
        shadowElevation = 2.dp
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(12.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            IconButton(onClick = onBack) {
                Icon(
                    imageVector = Icons.Default.ArrowBack,
                    contentDescription = "Back",
                    tint = MaterialTheme.colorScheme.primary
                )
            }
            
            Spacer(modifier = Modifier.width(8.dp))
            
            // Avatar
            ConversationAvatar(conversation = conversation)
            
            Spacer(modifier = Modifier.width(12.dp))
            
            // Info
            Column(modifier = Modifier.weight(1f)) {
                Text(
                    text = conversation.name,
                    style = MaterialTheme.typography.titleMedium,
                    fontWeight = FontWeight.SemiBold,
                    color = MaterialTheme.colorScheme.onSurface
                )
                
                if (conversation.isGroup) {
                    Text(
                        text = "${conversation.participants.size} members",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                } else {
                    Text(
                        text = if (conversation.isOnline) "Online" else "Offline",
                        style = MaterialTheme.typography.bodySmall,
                        color = if (conversation.isOnline) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
            
            // Actions
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                IconButton(onClick = { /* Video call */ }) {
                    Icon(
                        imageVector = Icons.Default.Videocam,
                        contentDescription = "Video call",
                        tint = MaterialTheme.colorScheme.primary
                    )
                }
                
                IconButton(onClick = { /* Voice call */ }) {
                    Icon(
                        imageVector = Icons.Default.Call,
                        contentDescription = "Voice call",
                        tint = MaterialTheme.colorScheme.primary
                    )
                }
                
                IconButton(onClick = { /* More options */ }) {
                    Icon(
                        imageVector = Icons.Default.MoreVert,
                        contentDescription = "More options",
                        tint = MaterialTheme.colorScheme.primary
                    )
                }
            }
        }
    }
}

// MARK: - Message Bubble
@Composable
fun MessageBubble(
    message: Message,
    onDelete: () -> Unit,
    onReport: () -> Unit = {}
) {
    var showDeleteDialog by remember { mutableStateOf(false) }
    var showReportDialog by remember { mutableStateOf(false) }
    
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = if (message.isFromCurrentUser) Arrangement.End else Arrangement.Start
    ) {
        if (message.isFromCurrentUser) {
            Spacer(modifier = Modifier.width(60.dp))
        }
        
        Column(
            horizontalAlignment = if (message.isFromCurrentUser) Alignment.End else Alignment.Start,
            verticalArrangement = Arrangement.spacedBy(4.dp)
        ) {
            // Sender name (for group chats)
            if (!message.isFromCurrentUser && message.sender.displayName != "You") {
                Text(
                    text = message.sender.displayName,
                    style = MaterialTheme.typography.labelMedium,
                    fontWeight = FontWeight.Medium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(start = 4.dp)
                )
            }
            
            // Message content
            Card(
                colors = CardDefaults.cardColors(
                    containerColor = if (message.isFromCurrentUser) {
                        MaterialTheme.colorScheme.primary
                    } else {
                        MaterialTheme.colorScheme.surfaceVariant
                    }
                ),
                shape = RoundedCornerShape(
                    topStart = 20.dp,
                    topEnd = 20.dp,
                    bottomStart = if (message.isFromCurrentUser) 20.dp else 8.dp,
                    bottomEnd = if (message.isFromCurrentUser) 8.dp else 20.dp
                ),
                elevation = CardDefaults.cardElevation(defaultElevation = 1.dp)
            ) {
                MessageContent(message = message)
            }
            
            // Message info
            MessageInfo(message = message)
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                if (message.isFromCurrentUser) {
                    TextButton(onClick = { showDeleteDialog = true }) { Text("Delete", color = MaterialTheme.colorScheme.error) }
                } else {
                    TextButton(onClick = { showReportDialog = true }) { Text("Report", color = MaterialTheme.colorScheme.error) }
                }
            }
        }
        
        if (!message.isFromCurrentUser) {
            Spacer(modifier = Modifier.width(60.dp))
        }
    }
    
    // Delete confirmation dialog
    if (showDeleteDialog) {
        AlertDialog(
            onDismissRequest = { showDeleteDialog = false },
            title = { Text("Delete Message") },
            text = { Text("Are you sure you want to delete this message?") },
            confirmButton = {
                TextButton(
                    onClick = {
                        onDelete()
                        showDeleteDialog = false
                    }
                ) {
                    Text("Delete")
                }
            },
            dismissButton = {
                TextButton(onClick = { showDeleteDialog = false }) {
                    Text("Cancel")
                }
            }
        )
    }
    // Report confirmation dialog
    if (showReportDialog) {
        AlertDialog(
            onDismissRequest = { showReportDialog = false },
            title = { Text("Report Message") },
            text = { Text("Report this message for moderation?") },
            confirmButton = {
                TextButton(
                    onClick = {
                        onReport()
                        showReportDialog = false
                    }
                ) {
                    Text("Report")
                }
            },
            dismissButton = {
                TextButton(onClick = { showReportDialog = false }) {
                    Text("Cancel")
                }
            }
        )
    }
}

// MARK: - Message Content
@Composable
fun MessageContent(message: Message) {
    Column(
        modifier = Modifier.padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        // Reply to message (if any)
        message.replyTo?.let { replyTo ->
            ReplyPreview(message = replyTo)
        }
        
        // Main content
        when (message.type) {
            MessageType.TEXT -> {
                Text(
                    text = message.content,
                    style = MaterialTheme.typography.bodyLarge,
                    color = if (message.isFromCurrentUser) {
                        MaterialTheme.colorScheme.onPrimary
                    } else {
                        MaterialTheme.colorScheme.onSurfaceVariant
                    }
                )
            }
            MessageType.IMAGE -> {
                AsyncImage(
                    model = ImageRequest.Builder(LocalContext.current)
                        .data(message.media.firstOrNull()?.url ?: "")
                        .crossfade(true)
                        .build(),
                    contentDescription = "Image",
                    modifier = Modifier
                        .fillMaxWidth()
                        .heightIn(max = 200.dp)
                        .clip(RoundedCornerShape(12.dp)),
                    contentScale = ContentScale.Fit,
                    error = {
                        Box(
                            modifier = Modifier
                                .fillMaxWidth()
                                .height(200.dp)
                                .clip(RoundedCornerShape(12.dp))
                                .background(MaterialTheme.colorScheme.surfaceVariant),
                            contentAlignment = Alignment.Center
                        ) {
                            CircularProgressIndicator()
                        }
                    }
                )
            }
            MessageType.VIDEO -> {
                VideoMessageView(media = message.media.firstOrNull())
            }
            MessageType.VOICE -> {
                VoiceMessageView(
                    duration = message.duration ?: 0.0,
                    isFromCurrentUser = message.isFromCurrentUser
                )
            }
            MessageType.DOCUMENT -> {
                DocumentMessageView(media = message.media.firstOrNull())
            }
            MessageType.LOCATION -> {
                LocationMessageView()
            }
            MessageType.CONTACT -> {
                ContactMessageView()
            }
        }
    }
}

// MARK: - Reply Preview
@Composable
fun ReplyPreview(message: Message) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(
                color = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f),
                shape = RoundedCornerShape(8.dp)
            )
            .padding(12.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Box(
            modifier = Modifier
                .width(3.dp)
                .height(32.dp)
                .background(
                    color = MaterialTheme.colorScheme.primary,
                    shape = RoundedCornerShape(1.5.dp)
                )
        )
        
        Spacer(modifier = Modifier.width(8.dp))
        
        Column(modifier = Modifier.weight(1f)) {
            Text(
                text = message.sender.displayName,
                style = MaterialTheme.typography.labelSmall,
                fontWeight = FontWeight.SemiBold,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
            
            Text(
                text = message.content,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis
            )
        }
    }
}

// MARK: - Message Info
@Composable
fun MessageInfo(message: Message) {
    Row(
        horizontalArrangement = Arrangement.spacedBy(4.dp),
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier.padding(horizontal = 4.dp)
    ) {
        Text(
            text = formatTime(message.timestamp),
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
        
        if (message.isFromCurrentUser) {
            Icon(
                imageVector = if (message.isRead) Icons.Default.Done else Icons.Default.Schedule,
                contentDescription = if (message.isRead) "Read" else "Sent",
                modifier = Modifier.size(12.dp),
                tint = if (message.isRead) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

// MARK: - Video Message View
@Composable
fun VideoMessageView(media: MediaItem?) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .background(
                color = MaterialTheme.colorScheme.surfaceVariant,
                shape = RoundedCornerShape(12.dp)
            )
            .padding(12.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        AsyncImage(
            model = ImageRequest.Builder(LocalContext.current)
                .data(media?.thumbnail ?: "")
                .crossfade(true)
                .build(),
            contentDescription = "Video thumbnail",
            modifier = Modifier
                .fillMaxWidth()
                .height(150.dp)
                .clip(RoundedCornerShape(8.dp)),
            contentScale = ContentScale.Fit,
            error = {
                Box(
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(150.dp)
                        .clip(RoundedCornerShape(8.dp))
                        .background(MaterialTheme.colorScheme.surfaceVariant),
                    contentAlignment = Alignment.Center
                ) {
                    Icon(
                        imageVector = Icons.Default.PlayCircle,
                        contentDescription = "Play",
                        modifier = Modifier.size(40.dp),
                        tint = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
        )
        
        Row(
            modifier = Modifier.fillMaxWidth(),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Icon(
                imageVector = Icons.Default.Videocam,
                contentDescription = "Video",
                modifier = Modifier.size(16.dp),
                tint = MaterialTheme.colorScheme.onSurfaceVariant
            )
            
            Spacer(modifier = Modifier.width(8.dp))
            
            Text(
                text = media?.fileName ?: "Video",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.weight(1f)
            )
            
            media?.duration?.let { duration ->
                Text(
                    text = formatDuration(duration),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
    }
}

// MARK: - Voice Message View
@Composable
fun VoiceMessageView(
    duration: Double,
    isFromCurrentUser: Boolean
) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        IconButton(
            onClick = { /* Play/pause */ },
            modifier = Modifier.size(32.dp)
        ) {
            Icon(
                imageVector = Icons.Default.PlayArrow,
                contentDescription = "Play",
                tint = if (isFromCurrentUser) {
                    MaterialTheme.colorScheme.onPrimary
                } else {
                    MaterialTheme.colorScheme.primary
                }
            )
        }
        
        // Waveform visualization
        Row(
            horizontalArrangement = Arrangement.spacedBy(2.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            repeat((duration * 2).toInt()) {
                Box(
                    modifier = Modifier
                        .width(2.dp)
                        .height((8..20).random().dp)
                        .background(
                            color = if (isFromCurrentUser) {
                                MaterialTheme.colorScheme.onPrimary
                            } else {
                                MaterialTheme.colorScheme.primary
                            },
                            shape = RoundedCornerShape(1.dp)
                        )
                )
            }
        }
        
        Text(
            text = formatDuration(duration),
            style = MaterialTheme.typography.bodySmall,
            color = if (isFromCurrentUser) {
                MaterialTheme.colorScheme.onPrimary.copy(alpha = 0.8f)
            } else {
                MaterialTheme.colorScheme.onSurfaceVariant
            }
        )
    }
}

// MARK: - Document Message View
@Composable
fun DocumentMessageView(media: MediaItem?) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(
                color = MaterialTheme.colorScheme.surfaceVariant,
                shape = RoundedCornerShape(12.dp)
            )
            .padding(12.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Icon(
            imageVector = Icons.Default.Description,
            contentDescription = "Document",
            modifier = Modifier.size(24.dp),
            tint = MaterialTheme.colorScheme.primary
        )
        
        Spacer(modifier = Modifier.width(12.dp))
        
        Column(modifier = Modifier.weight(1f)) {
            Text(
                text = media?.fileName ?: "Document",
                style = MaterialTheme.typography.bodyMedium,
                fontWeight = FontWeight.Medium,
                color = MaterialTheme.colorScheme.onSurface
            )
            
            media?.size?.let { size ->
                Text(
                    text = formatFileSize(size),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        
        IconButton(onClick = { /* Download/open */ }) {
            Icon(
                imageVector = Icons.Default.Download,
                contentDescription = "Download",
                tint = MaterialTheme.colorScheme.primary
            )
        }
    }
}

// MARK: - Location Message View
@Composable
fun LocationMessageView() {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .background(
                color = MaterialTheme.colorScheme.surfaceVariant,
                shape = RoundedCornerShape(12.dp)
            )
            .padding(16.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Icon(
            imageVector = Icons.Default.LocationOn,
            contentDescription = "Location",
            modifier = Modifier.size(40.dp),
            tint = MaterialTheme.colorScheme.error
        )
        
        Text(
            text = "Location shared",
            style = MaterialTheme.typography.bodyMedium,
            fontWeight = FontWeight.Medium,
            color = MaterialTheme.colorScheme.onSurface
        )
        
        TextButton(onClick = { /* Open in Maps */ }) {
            Text("View on Map")
        }
    }
}

// MARK: - Contact Message View
@Composable
fun ContactMessageView() {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(
                color = MaterialTheme.colorScheme.surfaceVariant,
                shape = RoundedCornerShape(12.dp)
            )
            .padding(12.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Icon(
            imageVector = Icons.Default.Person,
            contentDescription = "Contact",
            modifier = Modifier.size(40.dp),
            tint = MaterialTheme.colorScheme.primary
        )
        
        Spacer(modifier = Modifier.width(12.dp))
        
        Column {
            Text(
                text = "Contact shared",
                style = MaterialTheme.typography.bodyMedium,
                fontWeight = FontWeight.Medium,
                color = MaterialTheme.colorScheme.onSurface
            )
            
            Text(
                text = "Tap to view contact",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

// MARK: - Typing Indicator
@Composable
fun TypingIndicator() {
    var animationOffset by remember { mutableStateOf(0) }
    
    LaunchedEffect(Unit) {
        while (true) {
            delay(600)
            animationOffset = (animationOffset + 1) % 3
        }
    }
    
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.Start
    ) {
        Card(
            colors = CardDefaults.cardColors(
                containerColor = MaterialTheme.colorScheme.surfaceVariant
            ),
            shape = RoundedCornerShape(20.dp),
            elevation = CardDefaults.cardElevation(defaultElevation = 1.dp)
        ) {
            Row(
                modifier = Modifier.padding(horizontal = 16.dp, vertical = 10.dp),
                horizontalArrangement = Arrangement.spacedBy(4.dp)
            ) {
                repeat(3) { index ->
                    Box(
                        modifier = Modifier
                            .size(8.dp)
                            .scale(
                                if (animationOffset == index) 1.2f else 1.0f
                            )
                            .clip(CircleShape)
                            .background(MaterialTheme.colorScheme.onSurfaceVariant)
                    )
                }
            }
        }
        
        Spacer(modifier = Modifier.weight(1f))
    }
}

// MARK: - Chat Input Area
@Composable
fun ChatInputArea(
    messageText: String,
    selectedMedia: List<MediaItem>,
    isRecordingVoice: Boolean,
    voiceRecordingDuration: Double,
    onMessageTextChange: (String) -> Unit,
    onSendMessage: () -> Unit,
    onSendMedia: () -> Unit,
    onStartVoiceRecording: () -> Unit,
    onStopVoiceRecording: () -> Unit,
    onAddMedia: (MediaItem) -> Unit,
    onRemoveMedia: (Int) -> Unit
) {
    Column {
        // Media preview
        if (selectedMedia.isNotEmpty()) {
            MediaPreviewGrid(
                media = selectedMedia,
                onRemove = onRemoveMedia
            )
        }
        
        // Input bar
        Surface(
            modifier = Modifier.fillMaxWidth(),
            color = MaterialTheme.colorScheme.surface,
            shadowElevation = 8.dp
        ) {
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 16.dp, vertical = 12.dp),
                verticalAlignment = Alignment.Bottom
            ) {
                // Media button
                IconButton(onClick = { /* Show media picker */ }) {
                    Icon(
                        imageVector = Icons.Default.Add,
                        contentDescription = "Add media",
                        tint = MaterialTheme.colorScheme.primary
                    )
                }
                
                Spacer(modifier = Modifier.width(8.dp))
                
                // Text input
                BasicTextField(
                    value = messageText,
                    onValueChange = onMessageTextChange,
                    textStyle = MaterialTheme.typography.bodyLarge.copy(
                        color = MaterialTheme.colorScheme.onSurface
                    ),
                    modifier = Modifier
                        .weight(1f)
                        .background(
                            color = MaterialTheme.colorScheme.surfaceVariant,
                            shape = RoundedCornerShape(20.dp)
                        )
                        .padding(horizontal = 16.dp, vertical = 12.dp),
                    maxLines = 4,
                    decorationBox = { innerTextField ->
                        if (messageText.isEmpty()) {
                            Text(
                                text = "Message",
                                style = MaterialTheme.typography.bodyLarge,
                                color = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                        innerTextField()
                    }
                )
                
                Spacer(modifier = Modifier.width(8.dp))
                
                // Voice recording or send button
                if (messageText.isEmpty() && selectedMedia.isEmpty()) {
                    IconButton(
                        onClick = if (isRecordingVoice) onStopVoiceRecording else onStartVoiceRecording,
                        modifier = Modifier.size(48.dp)
                    ) {
                        Icon(
                            imageVector = if (isRecordingVoice) Icons.Default.Stop else Icons.Default.Mic,
                            contentDescription = if (isRecordingVoice) "Stop recording" else "Record voice",
                            tint = if (isRecordingVoice) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.primary,
                            modifier = Modifier.scale(
                                if (isRecordingVoice) 1.2f else 1.0f
                            )
                        )
                    }
                    
                    // Recording duration overlay
                    if (isRecordingVoice) {
                        Text(
                            text = formatDuration(voiceRecordingDuration),
                            style = MaterialTheme.typography.labelSmall,
                            fontWeight = FontWeight.SemiBold,
                            color = MaterialTheme.colorScheme.error,
                            modifier = Modifier.padding(top = 32.dp)
                        )
                    }
                } else {
                    IconButton(
                        onClick = onSendMessage,
                        modifier = Modifier.size(48.dp)
                    ) {
                        Icon(
                            imageVector = Icons.Default.Send,
                            contentDescription = "Send",
                            tint = MaterialTheme.colorScheme.primary
                        )
                    }
                }
            }
        }
    }
}

// MARK: - Media Preview Grid
@Composable
fun MediaPreviewGrid(
    media: List<MediaItem>,
    onRemove: (Int) -> Unit
) {
    LazyRow(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 16.dp, vertical = 8.dp),
        horizontalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        items(media.size) { index ->
            val mediaItem = media[index]
            Box {
                AsyncImage(
                    model = ImageRequest.Builder(LocalContext.current)
                        .data(mediaItem.url)
                        .crossfade(true)
                        .build(),
                    contentDescription = "Media preview",
                    modifier = Modifier
                        .size(80.dp)
                        .clip(RoundedCornerShape(8.dp)),
                    contentScale = ContentScale.Crop,
                    error = {
                        Box(
                            modifier = Modifier
                                .size(80.dp)
                                .clip(RoundedCornerShape(8.dp))
                                .background(MaterialTheme.colorScheme.surfaceVariant),
                            contentAlignment = Alignment.Center
                        ) {
                            Icon(
                                imageVector = Icons.Default.Image,
                                contentDescription = "Media placeholder",
                                tint = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                    }
                )
                
                // Remove button
                IconButton(
                    onClick = { onRemove(index) },
                    modifier = Modifier
                        .size(24.dp)
                        .align(Alignment.TopEnd)
                ) {
                    Icon(
                        imageVector = Icons.Default.Close,
                        contentDescription = "Remove",
                        tint = MaterialTheme.colorScheme.onBackground,
                        modifier = Modifier
                            .size(16.dp)
                            .background(
                                color = MaterialTheme.colorScheme.background.copy(alpha = 0.6f),
                                shape = CircleShape
                            )
                    )
                }
            }
        }
    }
}

// MARK: - Loading View
@Composable
fun LoadingView() {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(16.dp)
        ) {
            CircularProgressIndicator()
            
            Text(
                text = "Loading conversations...",
                style = MaterialTheme.typography.bodyLarge,
                fontWeight = FontWeight.Medium,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

// MARK: - Error View
@Composable
fun ErrorView(
    error: String,
    onRetry: () -> Unit
) {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(16.dp)
        ) {
            Icon(
                imageVector = Icons.Default.Warning,
                contentDescription = "Error",
                modifier = Modifier.size(48.dp),
                tint = MaterialTheme.colorScheme.error
            )
            
            Text(
                text = "Error",
                style = MaterialTheme.typography.headlineSmall,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.onSurface
            )
            
            Text(
                text = error,
                style = MaterialTheme.typography.bodyLarge,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                textAlign = androidx.compose.ui.text.style.TextAlign.Center,
                modifier = Modifier.padding(horizontal = 32.dp)
            )
            
            Button(onClick = onRetry) {
                Text("Try Again")
            }
        }
    }
}

// MARK: - Empty Conversations View
@Composable
fun EmptyConversationsView() {
    Box(
        modifier = Modifier.fillMaxSize(),
        contentAlignment = Alignment.Center
    ) {
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(20.dp)
        ) {
            Icon(
                imageVector = Icons.Default.Chat,
                contentDescription = "No conversations",
                modifier = Modifier.size(60.dp),
                tint = MaterialTheme.colorScheme.onSurfaceVariant
            )
            
            Text(
                text = "No conversations yet",
                style = MaterialTheme.typography.headlineSmall,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.onSurface
            )
            
            Text(
                text = "Start a conversation with someone to begin messaging",
                style = MaterialTheme.typography.bodyLarge,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                textAlign = androidx.compose.ui.text.style.TextAlign.Center,
                modifier = Modifier.padding(horizontal = 32.dp)
            )
        }
    }
}

// MARK: - Helper Functions
private fun formatTimeAgo(date: Date): String {
    val now = Date()
    val timeInterval = now.time - date.time
    
    return when {
        timeInterval < 60000 -> "now"
        timeInterval < 3600000 -> "${timeInterval / 60000}m"
        timeInterval < 86400000 -> "${timeInterval / 3600000}h"
        timeInterval < 2592000000 -> "${timeInterval / 86400000}d"
        else -> "${timeInterval / 2592000000}mo"
    }
}

private fun formatTime(date: Date): String {
    val formatter = SimpleDateFormat("HH:mm", Locale.getDefault())
    return formatter.format(date)
}

private fun formatDuration(duration: Double): String {
    val minutes = (duration / 60).toInt()
    val seconds = (duration % 60).toInt()
    return String.format("%d:%02d", minutes, seconds)
}

private fun formatFileSize(size: Long): String {
    return when {
        size < 1024 -> "$size B"
        size < 1024 * 1024 -> "${size / 1024} KB"
        size < 1024 * 1024 * 1024 -> "${size / (1024 * 1024)} MB"
        else -> "${size / (1024 * 1024 * 1024)} GB"
    }
}