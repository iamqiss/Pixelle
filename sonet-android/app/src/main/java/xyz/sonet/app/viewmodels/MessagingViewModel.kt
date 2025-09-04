package xyz.sonet.app.viewmodels

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import xyz.sonet.app.grpc.SonetGRPCClient
import xyz.sonet.app.grpc.SonetConfiguration
import xyz.sonet.app.grpc.proto.*
import java.util.*

class MessagingViewModel(application: Application) : AndroidViewModel(application) {
    
    // MARK: - State Flows
    private val _conversations = MutableStateFlow<List<Conversation>>(emptyList())
    val conversations: StateFlow<List<Conversation>> = _conversations.asStateFlow()
    
    private val _currentConversation = MutableStateFlow<Conversation?>(null)
    val currentConversation: StateFlow<Conversation?> = _currentConversation.asStateFlow()
    
    private val _messages = MutableStateFlow<List<Message>>(emptyList())
    val messages: StateFlow<List<Message>> = _messages.asStateFlow()
    
    private val _messageText = MutableStateFlow("")
    val messageText: StateFlow<String> = _messageText.asStateFlow()
    
    private val _isTyping = MutableStateFlow(false)
    val isTyping: StateFlow<Boolean> = _isTyping.asStateFlow()
    
    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()
    
    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()
    
    private val _showMediaPicker = MutableStateFlow(false)
    val showMediaPicker: StateFlow<Boolean> = _showMediaPicker.asStateFlow()
    
    private val _showVoiceRecorder = MutableStateFlow(false)
    val showVoiceRecorder: StateFlow<Boolean> = _showVoiceRecorder.asStateFlow()
    
    private val _isRecordingVoice = MutableStateFlow(false)
    val isRecordingVoice: StateFlow<Boolean> = _isRecordingVoice.asStateFlow()
    
    private val _voiceRecordingDuration = MutableStateFlow(0.0)
    val voiceRecordingDuration: StateFlow<Double> = _voiceRecordingDuration.asStateFlow()
    
    private val _selectedMedia = MutableStateFlow<List<MediaItem>>(emptyList())
    val selectedMedia: StateFlow<List<MediaItem>> = _selectedMedia.asStateFlow()
    
    private val _showConversationList = MutableStateFlow(true)
    val showConversationList: StateFlow<Boolean> = _showConversationList.asStateFlow()
    
    private val _searchQuery = MutableStateFlow("")
    val searchQuery: StateFlow<String> = _searchQuery.asStateFlow()
    
    private val _filteredConversations = MutableStateFlow<List<Conversation>>(emptyList())
    val filteredConversations: StateFlow<List<Conversation>> = _filteredConversations.asStateFlow()
    
    private val _onlineUsers = MutableStateFlow<Set<String>>(emptySet())
    val onlineUsers: StateFlow<Set<String>> = _onlineUsers.asStateFlow()
    
    private val _typingUsers = MutableStateFlow<Set<String>>(emptySet())
    val typingUsers: StateFlow<Set<String>> = _typingUsers.asStateFlow()
    
    // MARK: - Private Properties
    private val grpcClient = SonetGRPCClient(application, SonetConfiguration.development)
    private var messageStream: kotlinx.coroutines.Job? = null
    private var typingJob: kotlinx.coroutines.Job? = null
    private var voiceRecordingJob: kotlinx.coroutines.Job? = null
    private val currentUserId = SessionViewModel(getApplication()).currentUser.value?.id ?: ""
    
    // MARK: - Initialization
    init {
        setupMessageStream()
        loadConversations()
    }
    
    // MARK: - Public Methods
    fun loadConversations() {
        viewModelScope.launch {
            _isLoading.value = true
            _error.value = null
            
            try {
                val response = grpcClient.getConversations(page = 0, pageSize = 50)
                _conversations.value = response.conversationsList.map { Conversation.from(it, currentUserId) }
                applySearchFilter()
            } catch (e: Exception) {
                _error.value = "Failed to load conversations: ${e.localizedMessage}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    fun selectConversation(conversation: Conversation) {
        _currentConversation.value = conversation
        _showConversationList.value = false
        loadMessages(conversation.id)
        markConversationAsRead(conversation.id)
    }
    
    fun loadMessages(conversationId: String) {
        viewModelScope.launch {
            try {
                val response = grpcClient.getMessages(conversationId = conversationId, page = 0, pageSize = 100)
                _messages.value = response.messagesList.map { Message.from(it, currentUserId) }
            } catch (e: Exception) {
                _error.value = "Failed to load messages: ${e.localizedMessage}"
            }
        }
    }
    
    fun updateMessageText(text: String) {
        _messageText.value = text
    }
    
    fun sendMessage() {
        val content = _messageText.value.trim()
        if (content.isEmpty() || _currentConversation.value == null) return
        
        _messageText.value = ""
        
        viewModelScope.launch {
            try {
                val messageRequest = SendMessageRequest.newBuilder()
                    .setConversationId(_currentConversation.value!!.id)
                    .setContent(content)
                    .setType(MessageType.MESSAGE_TYPE_TEXT)
                    .build()
                
                val response = grpcClient.sendMessage(request = messageRequest)
                if (response.success) {
                    // Message will be added via the stream
                    sendTypingIndicator(false)
                } else {
                    _error.value = "Failed to send message: ${response.errorMessage}"
                }
            } catch (e: Exception) {
                _error.value = "Failed to send message: ${e.localizedMessage}"
            }
        }
    }
    
    fun sendMediaMessage() {
        if (_selectedMedia.value.isEmpty() || _currentConversation.value == null) return
        
        viewModelScope.launch {
            for (media in _selectedMedia.value) {
                try {
                    val messageRequest = SendMessageRequest.newBuilder()
                        .setConversationId(_currentConversation.value!!.id)
                        .addMediaList(media.toGRPC())
                        .setType(media.type.grpcType)
                        .build()
                    
                    val response = grpcClient.sendMessage(request = messageRequest)
                    if (!response.success) {
                        _error.value = "Failed to send media: ${response.errorMessage}"
                    }
                } catch (e: Exception) {
                    _error.value = "Failed to send media: ${e.localizedMessage}"
                }
            }
            
            _selectedMedia.value = emptyList()
        }
    }
    
    fun startVoiceRecording() {
        _isRecordingVoice.value = true
        _voiceRecordingDuration.value = 0.0
        
        voiceRecordingJob = viewModelScope.launch {
            while (_isRecordingVoice.value) {
                kotlinx.coroutines.delay(100)
                _voiceRecordingDuration.value += 0.1
            }
        }
        
        // Start actual voice recording here
        // This would integrate with MediaRecorder
    }
    
    fun stopVoiceRecording() {
        _isRecordingVoice.value = false
        voiceRecordingJob?.cancel()
        
        // Stop recording and send voice message
        if (_voiceRecordingDuration.value > 1.0) {
            sendVoiceMessage(_voiceRecordingDuration.value)
        }
        
        _voiceRecordingDuration.value = 0.0
    }
    
    fun sendTypingIndicator(isTyping: Boolean) {
        viewModelScope.launch {
            try {
                val typingRequest = TypingIndicatorRequest.newBuilder()
                    .setConversationId(_currentConversation.value?.id ?: return@launch)
                    .setIsTyping(isTyping)
                    .build()
                
                grpcClient.sendTypingIndicator(request = typingRequest)
            } catch (e: Exception) {
                // Handle error silently
            }
        }
    }
    
    fun markConversationAsRead(conversationId: String) {
        viewModelScope.launch {
            try {
                val response = grpcClient.markConversationAsRead(conversationId = conversationId)
                if (response.success) {
                    // Update local state
                    val updatedConversations = _conversations.value.toMutableList()
                    val index = updatedConversations.indexOfFirst { it.id == conversationId }
                    if (index != -1) {
                        updatedConversations[index] = updatedConversations[index].copy(unreadCount = 0)
                        _conversations.value = updatedConversations
                    }
                }
            } catch (e: Exception) {
                // Handle error silently
            }
        }
    }
    
    fun deleteMessage(message: Message) {
        viewModelScope.launch {
            try {
                val response = grpcClient.deleteMessage(messageId = message.id)
                if (response.success) {
                    _messages.value = _messages.value.filter { it.id != message.id }
                }
            } catch (e: Exception) {
                _error.value = "Failed to delete message: ${e.localizedMessage}"
            }
        }
    }
    
    fun reportMessage(message: Message) {
        viewModelScope.launch {
            try {
                // Replace with real moderation/report API when available
                grpcClient.reportContent(contentId = message.id, contentType = xyz.sonet.app.grpc.proto.ContentType.MESSAGE)
            } catch (_: Exception) {
                // Silent
            }
        }
    }
    
    fun createGroupChat(name: String, participants: List<String>) {
        viewModelScope.launch {
            try {
                val groupRequest = CreateGroupRequest.newBuilder()
                    .setName(name)
                    .addAllParticipantIds(participants)
                    .build()
                
                val response = grpcClient.createGroup(request = groupRequest)
                if (response.success) {
                    loadConversations()
                } else {
                    _error.value = "Failed to create group: ${response.errorMessage}"
                }
            } catch (e: Exception) {
                _error.value = "Failed to create group: ${e.localizedMessage}"
            }
        }
    }
    
    fun searchConversations(query: String) {
        _searchQuery.value = query
        applySearchFilter()
    }
    
    fun backToConversationList() {
        _showConversationList.value = true
        _currentConversation.value = null
        _messages.value = emptyList()
    }
    
    fun addMedia(media: MediaItem) {
        _selectedMedia.value = _selectedMedia.value + media
    }
    
    fun removeMedia(index: Int) {
        val updatedMedia = _selectedMedia.value.toMutableList()
        updatedMedia.removeAt(index)
        _selectedMedia.value = updatedMedia
    }
    
    fun clearError() {
        _error.value = null
    }
    
    // MARK: - Private Methods
    private fun setupMessageStream() {
        messageStream = viewModelScope.launch {
            try {
                grpcClient.messageStream().collect { message ->
                    handleNewMessage(message)
                }
            } catch (e: Exception) {
                _error.value = "Message stream error: ${e.localizedMessage}"
            }
        }
    }
    
    private fun handleNewMessage(message: xyz.sonet.app.grpc.proto.Message) {
        val newMessage = Message.from(message, currentUserId)
        
        // Add to current conversation if it matches
        if (_currentConversation.value?.id == message.conversationId) {
            _messages.value = _messages.value + newMessage
            
            // Mark as read if we're viewing the conversation
            if (!message.isRead) {
                markMessageAsRead(message.messageId)
            }
        }
        
        // Update conversation list
        val updatedConversations = _conversations.value.toMutableList()
        val index = updatedConversations.indexOfFirst { it.id == message.conversationId }
        if (index != -1) {
            val conversation = updatedConversations[index]
            updatedConversations[index] = conversation.copy(
                lastMessage = newMessage,
                lastMessageTime = Date(message.timestamp.seconds * 1000),
                unreadCount = conversation.unreadCount + if (message.isRead) 0 else 1
            )
            
            // Move to top
            val movedConversation = updatedConversations.removeAt(index)
            updatedConversations.add(0, movedConversation)
            
            _conversations.value = updatedConversations
        }
        
        applySearchFilter()

        // Post direct-reply notification for background state (placeholder)
        try {
            val context = getApplication<Application>().applicationContext
            val recent = _messages.value.takeLast(5).map { it.sender.displayName to it.content }
            val convo = _currentConversation.value ?: updatedConversations.firstOrNull { it.id == message.conversationId }
            if (convo != null) {
                val notif = xyz.sonet.app.notifications.MessagingNotificationHelper.buildMessageNotification(
                    context = context,
                    conversationId = convo.id,
                    conversationTitle = convo.name,
                    selfName = "You",
                    messages = recent
                )
                val nm = context.getSystemService(android.content.Context.NOTIFICATION_SERVICE) as android.app.NotificationManager
                nm.notify(convo.id.hashCode(), notif)
            }
        } catch (_: Exception) { }
    }
    
    private fun markMessageAsRead(messageId: String) {
        viewModelScope.launch {
            try {
                grpcClient.markMessageAsRead(messageId = messageId)
            } catch (e: Exception) {
                // Handle error silently
            }
        }
    }
    
    private fun sendVoiceMessage(duration: Double) {
        val conversation = _currentConversation.value ?: return
        
        viewModelScope.launch {
            try {
                val messageRequest = SendMessageRequest.newBuilder()
                    .setConversationId(conversation.id)
                    .setType(MessageType.MESSAGE_TYPE_VOICE)
                    .setDuration(duration.toInt())
                    .build()
                
                // Add voice file data here
                // messageRequest.setVoiceData(voiceData)
                
                val response = grpcClient.sendMessage(request = messageRequest)
                if (!response.success) {
                    _error.value = "Failed to send voice message: ${response.errorMessage}"
                }
            } catch (e: Exception) {
                _error.value = "Failed to send voice message: ${e.localizedMessage}"
            }
        }
    }
    
    private fun applySearchFilter() {
        val query = _searchQuery.value
        if (query.isEmpty()) {
            _filteredConversations.value = _conversations.value
        } else {
            _filteredConversations.value = _conversations.value.filter { conversation ->
                conversation.name.contains(query, ignoreCase = true) ||
                conversation.lastMessage?.content?.contains(query, ignoreCase = true) == true
            }
        }
    }
    
    override fun onCleared() {
        super.onCleared()
        messageStream?.cancel()
        typingJob?.cancel()
        voiceRecordingJob?.cancel()
    }
}

// MARK: - Data Models
data class Conversation(
    val id: String,
    val name: String,
    val type: ConversationType,
    val participants: List<UserProfile>,
    val lastMessage: Message?,
    val lastMessageTime: Date,
    val unreadCount: Int,
    val isGroup: Boolean,
    val groupAvatar: String?,
    val isOnline: Boolean
) {
    companion object {
        fun from(grpcConversation: xyz.sonet.app.grpc.proto.Conversation, currentUserId: String): Conversation {
            return Conversation(
                id = grpcConversation.conversationId,
                name = grpcConversation.name,
                type = ConversationType.from(grpcConversation.type),
                participants = grpcConversation.participantsList.map { UserProfile.from(it) },
                lastMessage = if (grpcConversation.hasLastMessage()) Message.from(grpcConversation.lastMessage, currentUserId) else null,
                lastMessageTime = Date(grpcConversation.lastMessageTime.seconds * 1000),
                unreadCount = grpcConversation.unreadCount,
                isGroup = grpcConversation.type == ConversationType.CONVERSATION_TYPE_GROUP,
                groupAvatar = if (grpcConversation.hasGroupAvatar()) grpcConversation.groupAvatar else null,
                isOnline = false // This would be updated via presence stream
            )
        }
    }
}

enum class ConversationType(val title: String, val icon: String) {
    DIRECT("Direct", "person"),
    GROUP("Group", "group");
    
    companion object {
        fun from(grpcType: xyz.sonet.app.grpc.proto.ConversationType): ConversationType {
            return when (grpcType) {
                ConversationType.CONVERSATION_TYPE_DIRECT -> DIRECT
                ConversationType.CONVERSATION_TYPE_GROUP -> GROUP
                else -> DIRECT
            }
        }
    }
}

data class Message(
    val id: String,
    val content: String,
    val type: MessageType,
    val timestamp: Date,
    val sender: UserProfile,
    val isFromCurrentUser: Boolean,
    val isRead: Boolean,
    val media: List<MediaItem>,
    val replyTo: Message?,
    val reactions: List<MessageReaction>,
    val duration: Double? // For voice messages
) {
    companion object {
        fun from(grpcMessage: xyz.sonet.app.grpc.proto.Message, currentUserId: String): Message {
            return Message(
                id = grpcMessage.messageId,
                content = grpcMessage.content,
                type = MessageType.from(grpcMessage.type),
                timestamp = Date(grpcMessage.timestamp.seconds * 1000),
                sender = UserProfile.from(grpcMessage.sender),
                isFromCurrentUser = grpcMessage.sender.userId == currentUserId,
                isRead = grpcMessage.isRead,
                media = grpcMessage.mediaList.map { MediaItem.from(it) },
                replyTo = if (grpcMessage.hasReplyTo()) Message.from(grpcMessage.replyTo, currentUserId) else null,
                reactions = grpcMessage.reactionsList.map { MessageReaction.from(it) },
                duration = if (grpcMessage.hasDuration()) grpcMessage.duration.toDouble() else null
            )
        }
    }
}

enum class MessageType(val icon: String) {
    TEXT("text"),
    IMAGE("image"),
    VIDEO("video"),
    VOICE("voice"),
    DOCUMENT("document"),
    LOCATION("location"),
    CONTACT("contact");
    
    companion object {
        fun from(grpcType: xyz.sonet.app.grpc.proto.MessageType): MessageType {
            return when (grpcType) {
                MessageType.MESSAGE_TYPE_TEXT -> TEXT
                MessageType.MESSAGE_TYPE_IMAGE -> IMAGE
                MessageType.MESSAGE_TYPE_VIDEO -> VIDEO
                MessageType.MESSAGE_TYPE_VOICE -> VOICE
                MessageType.MESSAGE_TYPE_DOCUMENT -> DOCUMENT
                MessageType.MESSAGE_TYPE_LOCATION -> LOCATION
                MessageType.MESSAGE_TYPE_CONTACT -> CONTACT
                else -> TEXT
            }
        }
    }
    
    val grpcType: xyz.sonet.app.grpc.proto.MessageType
        get() = when (this) {
            TEXT -> MessageType.MESSAGE_TYPE_TEXT
            IMAGE -> MessageType.MESSAGE_TYPE_IMAGE
            VIDEO -> MessageType.MESSAGE_TYPE_VIDEO
            VOICE -> MessageType.MESSAGE_TYPE_VOICE
            DOCUMENT -> MessageType.MESSAGE_TYPE_DOCUMENT
            LOCATION -> MessageType.MESSAGE_TYPE_LOCATION
            CONTACT -> MessageType.MESSAGE_TYPE_CONTACT
        }
}

data class MessageReaction(
    val id: String,
    val emoji: String,
    val user: UserProfile,
    val timestamp: Date
) {
    companion object {
        fun from(grpcReaction: xyz.sonet.app.grpc.proto.MessageReaction): MessageReaction {
            return MessageReaction(
                id = grpcReaction.reactionId,
                emoji = grpcReaction.emoji,
                user = UserProfile.from(grpcReaction.user),
                timestamp = Date(grpcReaction.timestamp.seconds * 1000)
            )
        }
    }
}

// MARK: - Media Types
enum class MediaType(val icon: String) {
    IMAGE("image"),
    VIDEO("video"),
    VOICE("voice"),
    DOCUMENT("document");
    
    val grpcType: xyz.sonet.app.grpc.proto.MediaType
        get() = when (this) {
            IMAGE -> MediaType.MEDIA_TYPE_IMAGE
            VIDEO -> MediaType.MEDIA_TYPE_VIDEO
            VOICE -> MediaType.MEDIA_TYPE_AUDIO
            DOCUMENT -> MediaType.MEDIA_TYPE_DOCUMENT
        }
}

data class MediaItem(
    val mediaId: String,
    val url: String,
    val type: MediaType,
    val thumbnail: String?,
    val duration: Double?,
    val size: Long?,
    val fileName: String?
) {
    companion object {
        fun from(grpcMedia: xyz.sonet.app.grpc.proto.MediaItem): MediaItem {
            return MediaItem(
                mediaId = grpcMedia.mediaId,
                url = grpcMedia.url,
                type = MediaType.from(grpcMedia.type),
                thumbnail = if (grpcMedia.hasThumbnail()) grpcMedia.thumbnail else null,
                duration = if (grpcMedia.hasDuration()) grpcMedia.duration.toDouble() else null,
                size = if (grpcMedia.hasSize()) grpcMedia.size else null,
                fileName = if (grpcMedia.hasFileName()) grpcMedia.fileName else null
            )
        }
    }
    
    fun toGRPC(): xyz.sonet.app.grpc.proto.MediaItem {
        return xyz.sonet.app.grpc.proto.MediaItem.newBuilder()
            .setMediaId(mediaId)
            .setUrl(url)
            .setType(type.grpcType)
            .apply {
                thumbnail?.let { setThumbnail(it) }
                duration?.let { setDuration(it.toInt()) }
                size?.let { setSize(it) }
                fileName?.let { setFileName(it) }
            }
            .build()
    }
}

// MARK: - Chat Theme (Future Implementation)
data class ChatTheme(
    val backgroundColor: androidx.compose.ui.graphics.Color,
    val bubbleColor: androidx.compose.ui.graphics.Color,
    val textColor: androidx.compose.ui.graphics.Color,
    val accentColor: androidx.compose.ui.graphics.Color,
    val customBackground: String? // URL or asset name
) {
    companion object {
        val default = ChatTheme(
            backgroundColor = androidx.compose.material3.MaterialTheme.colorScheme.surface,
            bubbleColor = androidx.compose.material3.MaterialTheme.colorScheme.primary,
            textColor = androidx.compose.material3.MaterialTheme.colorScheme.onPrimary,
            accentColor = androidx.compose.material3.MaterialTheme.colorScheme.primary,
            customBackground = null
        )
        
        val dark = ChatTheme(
            backgroundColor = androidx.compose.material3.MaterialTheme.colorScheme.surface,
            bubbleColor = androidx.compose.material3.MaterialTheme.colorScheme.primary,
            textColor = androidx.compose.material3.MaterialTheme.colorScheme.onPrimary,
            accentColor = androidx.compose.material3.MaterialTheme.colorScheme.primary,
            customBackground = null
        )
    }
}