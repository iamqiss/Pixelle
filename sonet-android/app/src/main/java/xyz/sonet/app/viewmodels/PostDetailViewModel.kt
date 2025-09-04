package xyz.sonet.app.viewmodels

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import xyz.sonet.app.grpc.SonetGRPCClient
import xyz.sonet.app.grpc.proto.Note
import xyz.sonet.app.grpc.proto.CreateReplyRequest
import xyz.sonet.app.grpc.proto.GetThreadRequest
import xyz.sonet.app.grpc.proto.GetThreadResponse
import xyz.sonet.app.grpc.proto.LikeNoteRequest
import xyz.sonet.app.grpc.proto.RenoteNoteRequest
import xyz.sonet.app.grpc.proto.SortOrder

class PostDetailViewModel : ViewModel() {
    
    private val grpcClient = SonetGRPCClient.getInstance()
    
    // State
    private val _post = MutableStateFlow<Note?>(null)
    val post: StateFlow<Note?> = _post.asStateFlow()
    
    private val _thread = MutableStateFlow<List<Note>>(emptyList())
    val thread: StateFlow<List<Note>> = _thread.asStateFlow()
    
    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading.asStateFlow()
    
    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error.asStateFlow()
    
    private val _replyToNote = MutableStateFlow<Note?>(null)
    val replyToNote: StateFlow<Note?> = _replyToNote.asStateFlow()
    
    private val _isCreatingReply = MutableStateFlow(false)
    val isCreatingReply: StateFlow<Boolean> = _isCreatingReply.asStateFlow()
    
    // Actions
    fun loadPost(noteId: String) {
        viewModelScope.launch {
            try {
                _isLoading.value = true
                _error.value = null
                
                val note = grpcClient.getNote(noteId)
                _post.value = note
            } catch (e: Exception) {
                _error.value = "Failed to load post: ${e.message}"
            } finally {
                _isLoading.value = false
            }
        }
    }
    
    fun loadThread(noteId: String) {
        viewModelScope.launch {
            try {
                _error.value = null
                
                val request = GetThreadRequest.newBuilder()
                    .setNoteId(noteId)
                    .setUserId("") // Will be set by the server based on auth
                    .setSortOrder(SortOrder.CHRONOLOGICAL)
                    .build()
                
                val response = grpcClient.getThread(request)
                if (response.success) {
                    _thread.value = response.threadNotesList
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to load thread: ${e.message}"
            }
        }
    }
    
    fun setReplyToNote(note: Note?) {
        _replyToNote.value = note
    }
    
    fun createReply(content: String) {
        val replyTo = _replyToNote.value ?: _post.value ?: return
        
        viewModelScope.launch {
            try {
                _isCreatingReply.value = true
                _error.value = null
                
                val request = CreateReplyRequest.newBuilder()
                    .setParentNoteId(replyTo.noteId)
                    .setUserId("") // Will be set by the server based on auth
                    .setContent(content)
                    .build()
                
                val response = grpcClient.createReply(request)
                if (response.success) {
                    // Clear reply input and refresh thread
                    _replyToNote.value = null
                    loadThread(replyTo.noteId)
                } else {
                    _error.value = response.errorMessage
                }
            } catch (e: Exception) {
                _error.value = "Failed to create reply: ${e.message}"
            } finally {
                _isCreatingReply.value = false
            }
        }
    }
    
    fun toggleLike(noteId: String) {
        viewModelScope.launch {
            try {
                val request = LikeNoteRequest.newBuilder()
                    .setNoteId(noteId)
                    .setUserId("") // Will be set by the server based on auth
                    .build()
                
                val response = grpcClient.likeNote(request)
                if (response.success) {
                    // Update local state
                    _post.value?.let { note ->
                        if (note.noteId == noteId) {
                            val updatedNote = note.toBuilder()
                                .setLikeCount(note.likeCount + (if (note.userState?.isLiked == true) -1 else 1))
                                .build()
                            _post.value = updatedNote
                        }
                    }
                    
                    _thread.value = _thread.value.map { note ->
                        if (note.noteId == noteId) {
                            val updatedNote = note.toBuilder()
                                .setLikeCount(note.likeCount + (if (note.userState?.isLiked == true) -1 else 1))
                                .build()
                            updatedNote
                        } else {
                            note
                        }
                    }
                }
            } catch (e: Exception) {
                _error.value = "Failed to toggle like: ${e.message}"
            }
        }
    }
    
    fun toggleRepost(noteId: String) {
        viewModelScope.launch {
            try {
                val request = RenoteNoteRequest.newBuilder()
                    .setNoteId(noteId)
                    .setUserId("") // Will be set by the server based on auth
                    .build()
                
                val response = grpcClient.renoteNote(request)
                if (response.success) {
                    // Update local state
                    _post.value?.let { note ->
                        if (note.noteId == noteId) {
                            val updatedNote = note.toBuilder()
                                .setRenoteCount(note.renoteCount + (if (note.userState?.isReposted == true) -1 else 1))
                                .build()
                            _post.value = updatedNote
                        }
                    }
                    
                    _thread.value = _thread.value.map { note ->
                        if (note.noteId == noteId) {
                            val updatedNote = note.toBuilder()
                                .setRenoteCount(note.renoteCount + (if (note.userState?.isReposted == true) -1 else 1))
                                .build()
                            updatedNote
                        } else {
                            note
                        }
                    }
                }
            } catch (e: Exception) {
                _error.value = "Failed to toggle repost: ${e.message}"
            }
        }
    }
    
    fun clearError() {
        _error.value = null
    }
}