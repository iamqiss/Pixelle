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

class ContentCreationViewModel(application: Application) : AndroidViewModel(application) {
    
    // MARK: - State Flows
    private val _noteContent = MutableStateFlow("")
    val noteContent: StateFlow<String> = _noteContent.asStateFlow()
    
    private val _selectedMedia = MutableStateFlow<List<MediaItem>>(emptyList())
    val selectedMedia: StateFlow<List<MediaItem>> = _selectedMedia.asStateFlow()
    
    private val _replyToNote = MutableStateFlow<Note?>(null)
    val replyToNote: StateFlow<Note?> = _replyToNote.asStateFlow()
    
    private val _quoteNote = MutableStateFlow<Note?>(null)
    val quoteNote: StateFlow<Note?> = _quoteNote.asStateFlow()
    
    private val _selectedHashtags = MutableStateFlow<List<String>>(emptyList())
    val selectedHashtags: StateFlow<List<String>> = _selectedHashtags.asStateFlow()
    
    private val _selectedMentions = MutableStateFlow<List<String>>(emptyList())
    val selectedMentions: StateFlow<List<String>> = _selectedMentions.asStateFlow()
    
    private val _isPosting = MutableStateFlow(false)
    val isPosting: StateFlow<Boolean> = _isPosting.asStateFlow()
    
    private val _postingError = MutableStateFlow<String?>(null)
    val postingError: StateFlow<String?> = _postingError.asStateFlow()
    
    private val _showHashtagSuggestions = MutableStateFlow(false)
    val showHashtagSuggestions: StateFlow<Boolean> = _showHashtagSuggestions.asStateFlow()
    
    private val _showMentionSuggestions = MutableStateFlow(false)
    val showMentionSuggestions: StateFlow<Boolean> = _showMentionSuggestions.asStateFlow()
    
    private val _hashtagSuggestions = MutableStateFlow<List<String>>(emptyList())
    val hashtagSuggestions: StateFlow<List<String>> = _hashtagSuggestions.asStateFlow()
    
    private val _mentionSuggestions = MutableStateFlow<List<UserProfile>>(emptyList())
    val mentionSuggestions: StateFlow<List<UserProfile>> = _mentionSuggestions.asStateFlow()
    
    private val _currentHashtagQuery = MutableStateFlow("")
    val currentHashtagQuery: StateFlow<String> = _currentHashtagQuery.asStateFlow()
    
    private val _currentMentionQuery = MutableStateFlow("")
    val currentMentionQuery: StateFlow<String> = _currentMentionQuery.asStateFlow()
    
    private val _scheduledDate = MutableStateFlow<Long?>(null)
    val scheduledDate: StateFlow<Long?> = _scheduledDate.asStateFlow()
    
    private val _showScheduling = MutableStateFlow(false)
    val showScheduling: StateFlow<Boolean> = _showScheduling.asStateFlow()
    
    private val _isDraft = MutableStateFlow(false)
    val isDraft: StateFlow<Boolean> = _isDraft.asStateFlow()
    
    private val _draftId = MutableStateFlow<String?>(null)
    val draftId: StateFlow<String?> = _draftId.asStateFlow()
    
    // MARK: - Private Properties
    private val grpcClient = SonetGRPCClient(application, SonetConfiguration.development)
    private var hashtagSearchJob: kotlinx.coroutines.Job? = null
    private var mentionSearchJob: kotlinx.coroutines.Job? = null
    
    // MARK: - Constants
    private val maxCharacterCount = 280
    private val maxMediaCount = 4
    
    // MARK: - Computed Properties
    val characterCount: Int
        get() = _noteContent.value.length
    
    val remainingCharacters: Int
        get() = maxCharacterCount - characterCount
    
    val canPost: Boolean
        get() = _noteContent.value.trim().isNotEmpty() && 
                !_isPosting.value && 
                remainingCharacters >= 0
    
    val characterCountColor: androidx.compose.ui.graphics.Color
        get() = when {
            remainingCharacters < 0 -> androidx.compose.ui.graphics.Color(0xFFDD3333)
            remainingCharacters <= 20 -> androidx.compose.ui.graphics.Color(0xFF888888)
            else -> androidx.compose.ui.graphics.Color(0xFF6B6B6B)
        }
    
    // MARK: - Initialization
    init {
        setupContentMonitoring()
        loadDraftIfNeeded()
    }
    
    // MARK: - Public Methods
    fun setReplyToNote(note: Note?) {
        _replyToNote.value = note
    }
    
    fun setQuoteNote(note: Note?) {
        _quoteNote.value = note
    }
    
    fun updateNoteContent(content: String) {
        _noteContent.value = content
        detectHashtagsAndMentions(content)
    }
    
    fun addMedia(media: MediaItem) {
        if (_selectedMedia.value.size < maxMediaCount) {
            val currentMedia = _selectedMedia.value.toMutableList()
            currentMedia.add(media)
            _selectedMedia.value = currentMedia
        }
    }
    
    fun removeMedia(index: Int) {
        if (index < _selectedMedia.value.size) {
            val currentMedia = _selectedMedia.value.toMutableList()
            currentMedia.removeAt(index)
            _selectedMedia.value = currentMedia
        }
    }
    
    fun addHashtag(hashtag: String) {
        val cleanHashtag = if (hashtag.startsWith("#")) hashtag else "#$hashtag"
        if (!_selectedHashtags.value.contains(cleanHashtag)) {
            val currentHashtags = _selectedHashtags.value.toMutableList()
            currentHashtags.add(cleanHashtag)
            _selectedHashtags.value = currentHashtags
        }
        _showHashtagSuggestions.value = false
        _currentHashtagQuery.value = ""
    }
    
    fun removeHashtag(hashtag: String) {
        val currentHashtags = _selectedHashtags.value.toMutableList()
        currentHashtags.remove(hashtag)
        _selectedHashtags.value = currentHashtags
    }
    
    fun addMention(user: UserProfile) {
        val mention = "@${user.username}"
        if (!_selectedMentions.value.contains(mention)) {
            val currentMentions = _selectedMentions.value.toMutableList()
            currentMentions.add(mention)
            _selectedMentions.value = currentMentions
        }
        _showMentionSuggestions.value = false
        _currentMentionQuery.value = ""
    }
    
    fun removeMention(mention: String) {
        val currentMentions = _selectedMentions.value.toMutableList()
        currentMentions.remove(mention)
        _selectedMentions.value = currentMentions
    }
    
    fun postNote() {
        if (!canPost) return
        
        viewModelScope.launch {
            _isPosting.value = true
            _postingError.value = null
            
            try {
                val content = _noteContent.value.trim()
                
                // Start foreground service and upload media, collect URLs
                try {
                    val ctx = getApplication<Application>()
                    val intent = android.content.Intent(ctx, xyz.sonet.app.notifications.UploadForegroundService::class.java)
                    ctx.startForegroundService(intent)
                } catch (_: Exception) {}
                
                // Upload media and collect URLs
                val uploadedUrls = mutableListOf<String>()
                _selectedMedia.value.forEachIndexed { index, media ->
                    if (media.localUri != null) {
                        try {
                            val uri = android.net.Uri.parse(media.localUri)
                            val bytes = getApplication<Application>().contentResolver.openInputStream(uri)?.use { it.readBytes() } ?: byteArrayOf()
                            val resp = grpcClient.uploadMedia(ownerUserId = SessionViewModel(getApplication()).currentUser.value?.id ?: "current_user", fileName = media.altText ?: "media", mimeType = when (media.type) {
                                MediaType.MEDIA_TYPE_IMAGE -> "image/jpeg"
                                MediaType.MEDIA_TYPE_VIDEO -> "video/mp4"
                                MediaType.MEDIA_TYPE_GIF -> "image/gif"
                                else -> "application/octet-stream"
                            }, bytes = bytes) { progress ->
                                try {
                                    val nm = getApplication<Application>().getSystemService(android.content.Context.NOTIFICATION_SERVICE) as android.app.NotificationManager
                                    val notif = xyz.sonet.app.notifications.MessagingNotificationHelper.buildUploadProgressNotification(
                                        getApplication(),
                                        "Uploading…",
                                        (progress * 100).toInt(),
                                        100,
                                        content = media.altText ?: "Media ${index + 1} of ${_selectedMedia.value.size}"
                                    )
                                    nm.notify(1001, notif)
                                } catch (_: Exception) {}
                            }
                            uploadedUrls.add(resp.url)
                        } catch (e: Exception) {
                            // Fallback to existing url
                            if (media.url.isNotEmpty()) uploadedUrls.add(media.url)
                        }
                    } else if (media.url.isNotEmpty()) {
                        uploadedUrls.add(media.url)
                    }
                }
                
                // Create note request with uploaded URLs
                val noteRequest = CreateNoteRequest.newBuilder()
                    .setContent(content)
                    .addAllMediaUrls(uploadedUrls)
                    .addAllHashtags(_selectedHashtags.value.map { it.removePrefix("#") })
                    .addAllMentions(_selectedMentions.value.map { it.removePrefix("@") })
                    .setVisibility(NoteVisibility.NOTE_VISIBILITY_PUBLIC)
                    .build()
                
                // Add reply info
                _replyToNote.value?.let { note ->
                    noteRequest.replyToNoteId = note.noteId
                }
                
                // Add quote info
                _quoteNote.value?.let { note ->
                    noteRequest.quoteNoteId = note.noteId
                }
                
                // Add scheduling
                _scheduledDate.value?.let { timestamp ->
                    val grpcTimestamp = Timestamp.newBuilder()
                        .setSeconds(timestamp / 1000)
                        .build()
                    noteRequest.scheduledAt = grpcTimestamp
                }
                
                // Post the note
                val response = grpcClient.createNote(noteRequest.build())
                
                if (response.success) {
                    try {
                        val nm = getApplication<Application>().getSystemService(android.content.Context.NOTIFICATION_SERVICE) as android.app.NotificationManager
                        nm.cancel(1001)
                    } catch (_: Exception) {}
                    // Clear form
                    clearForm()
                    
                    // Navigate back or show success
                    // This would be handled by the view
                } else {
                    _postingError.value = response.errorMessage
                }
                
            } catch (error: Exception) {
                _postingError.value = "Failed to post note: ${error.message}"
            } finally {
                _isPosting.value = false
            }
        }
    }
    
    fun saveDraft() {
        val draft = NoteDraft(
            id = _draftId.value ?: UUID.randomUUID().toString(),
            content = _noteContent.value,
            media = _selectedMedia.value,
            replyToNote = _replyToNote.value,
            quoteNote = _quoteNote.value,
            hashtags = _selectedHashtags.value,
            mentions = _selectedMentions.value,
            scheduledDate = _scheduledDate.value,
            createdAt = System.currentTimeMillis()
        )
        
        saveDraftToStorage(draft)
        _draftId.value = draft.id
        _isDraft.value = true
    }
    
    fun loadDraft(draft: NoteDraft) {
        _noteContent.value = draft.content
        _selectedMedia.value = draft.media
        _replyToNote.value = draft.replyToNote
        _quoteNote.value = draft.quoteNote
        _selectedHashtags.value = draft.hashtags
        _selectedMentions.value = draft.mentions
        _scheduledDate.value = draft.scheduledDate
        _draftId.value = draft.id
        _isDraft.value = true
    }
    
    fun clearForm() {
        _noteContent.value = ""
        _selectedMedia.value = emptyList()
        _selectedHashtags.value = emptyList()
        _selectedMentions.value = emptyList()
        _scheduledDate.value = null
        _draftId.value = null
        _isDraft.value = false
        _postingError.value = null
    }
    
    fun schedulePost(date: Long) {
        _scheduledDate.value = date
        _showScheduling.value = false
    }
    
    fun cancelScheduling() {
        _scheduledDate.value = null
        _showScheduling.value = false
    }
    
    fun showScheduling() {
        _showScheduling.value = true
    }
    
    // MARK: - Private Methods
    private fun setupContentMonitoring() {
        // Content monitoring is handled by the detectHashtagsAndMentions function
        // which is called whenever noteContent is updated
    }
    
    private fun detectHashtagsAndMentions(content: String) {
        // Detect hashtags
        val hashtagPattern = Regex("#(\\w+)")
        val hashtagMatches = hashtagPattern.findAll(content)
        
        hashtagMatches.lastOrNull()?.let { match ->
            val query = match.groupValues[1]
            if (query.length >= 2) {
                _currentHashtagQuery.value = query
                searchHashtags(query)
            } else {
                _showHashtagSuggestions.value = false
            }
        } ?: run {
            _showHashtagSuggestions.value = false
        }
        
        // Detect mentions
        val mentionPattern = Regex("@(\\w+)")
        val mentionMatches = mentionPattern.findAll(content)
        
        mentionMatches.lastOrNull()?.let { match ->
            val query = match.groupValues[1]
            if (query.length >= 2) {
                _currentMentionQuery.value = query
                searchUsers(query)
            } else {
                _showMentionSuggestions.value = false
            }
        } ?: run {
            _showMentionSuggestions.value = false
        }
    }
    
    private fun searchHashtags(query: String) {
        hashtagSearchJob?.cancel()
        
        hashtagSearchJob = viewModelScope.launch {
            try {
                val hashtags = grpcClient.searchHashtags(query, 0, 10)
                
                if (!isCancelled) {
                    _hashtagSuggestions.value = hashtags
                    _showHashtagSuggestions.value = true
                }
            } catch (error: Exception) {
                // Handle error silently for hashtag search
            }
        }
    }
    
    private fun searchUsers(query: String) {
        mentionSearchJob?.cancel()
        
        mentionSearchJob = viewModelScope.launch {
            try {
                val users = grpcClient.searchUsers(query, 0, 10)
                
                if (!isCancelled) {
                    _mentionSuggestions.value = users
                    _showMentionSuggestions.value = true
                }
            } catch (error: Exception) {
                // Handle error silently for user search
            }
        }
    }
    
    private fun loadDraftIfNeeded() {
        // Load draft if editing an existing draft
        // This would be implemented based on navigation context
    }
    
    private fun saveDraftToStorage(draft: NoteDraft) {
        // Save draft to SharedPreferences or Room database
        // This would be implemented for draft persistence
    }
}

// MARK: - Data Models
data class MediaItem(
    val id: String = UUID.randomUUID().toString(),
    val mediaId: String = "",
    val url: String = "",
    val type: xyz.sonet.app.grpc.proto.MediaType = xyz.sonet.app.grpc.proto.MediaType.MEDIA_TYPE_IMAGE,
    val altText: String? = null,
    val localUri: String? = null
)

data class NoteDraft(
    val id: String,
    val content: String,
    val media: List<MediaItem>,
    val replyToNote: Note?,
    val quoteNote: Note?,
    val hashtags: List<String>,
    val mentions: List<String>,
    val scheduledDate: Long?,
    val createdAt: Long
)