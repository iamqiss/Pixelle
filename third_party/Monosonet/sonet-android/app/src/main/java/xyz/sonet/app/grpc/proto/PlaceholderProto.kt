package xyz.sonet.app.grpc.proto

// This file is a placeholder for the generated protocol buffer classes
// In a real implementation, these would be generated from the .proto files using protoc

// MARK: - User Service
class UserServiceGrpc {
    class UserServiceBlockingStub(private val channel: Any) {
        fun authenticate(request: AuthRequest): AuthResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
        
        fun registerUser(request: RegisterUserRequest): RegisterUserResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
        
        fun refreshSession(request: RefreshSessionRequest): RefreshSessionResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
        
        fun ping(request: PingRequest): PingResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
    }
    
    companion object {
        fun newBlockingStub(channel: Any): UserServiceBlockingStub {
            return UserServiceBlockingStub(channel)
        }
    }
}

// MARK: - Note Service
class NoteServiceGrpc {
    class NoteServiceBlockingStub(private val channel: Any) {
        fun getNotesBatch(request: GetNotesBatchRequest): GetNotesBatchResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
        
        fun likeNote(request: LikeNoteRequest): LikeNoteResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
        
        fun unlikeNote(request: UnlikeNoteRequest): UnlikeNoteResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
    }
    
    companion object {
        fun newBlockingStub(channel: Any): NoteServiceBlockingStub {
            return NoteServiceBlockingStub(channel)
        }
    }
}

// MARK: - Timeline Service
class TimelineServiceGrpc {
    class TimelineServiceBlockingStub(private val channel: Any) {
        fun getHomeTimeline(request: GetTimelineRequest): GetTimelineResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
    }
    
    companion object {
        fun newBlockingStub(channel: Any): TimelineServiceBlockingStub {
            return TimelineServiceBlockingStub(channel)
        }
    }
}

// MARK: - Search Service
class SearchServiceGrpc {
    class SearchServiceBlockingStub(private val channel: Any) {
        fun searchUsers(request: SearchUsersRequest): SearchUsersResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
        
        fun searchNotes(request: SearchNotesRequest): SearchNotesResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
    }
    
    companion object {
        fun newBlockingStub(channel: Any): SearchServiceBlockingStub {
            return SearchServiceBlockingStub(channel)
        }
    }
}

// MARK: - Messaging Service
class MessagingServiceGrpc {
    class MessagingServiceBlockingStub(private val channel: Any) {
        fun getConversations(request: GetConversationsRequest): GetConversationsResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
        
        fun getMessages(request: GetMessagesRequest): GetMessagesResponse {
            // Placeholder implementation
            throw UnsupportedOperationException("Proto classes not generated yet")
        }
    }
    
    companion object {
        fun newBlockingStub(channel: Any): MessagingServiceBlockingStub {
            return MessagingServiceBlockingStub(channel)
        }
    }
}

// MARK: - Request/Response Classes
data class AuthRequest(
    val email: String,
    val password: String,
    val sessionType: SessionType
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private var email: String = ""
        private var password: String = ""
        private var sessionType: SessionType = SessionType.MOBILE
        
        fun setEmail(email: String): Builder {
            this.email = email
            return this
        }
        
        fun setPassword(password: String): Builder {
            this.password = password
            return this
        }
        
        fun setSessionType(sessionType: SessionType): Builder {
            this.sessionType = sessionType
            return this
        }
        
        fun build(): AuthRequest = AuthRequest(email, password, sessionType)
    }
}

data class AuthResponse(
    val userProfile: UserProfile,
    val session: Session,
    val accessToken: String,
    val refreshToken: String
)

data class RegisterUserRequest(
    val username: String,
    val email: String,
    val password: String
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private var username: String = ""
        private var email: String = ""
        private var password: String = ""
        
        fun setUsername(username: String): Builder {
            this.username = username
            return this
        }
        
        fun setEmail(email: String): Builder {
            this.email = email
            return this
        }
        
        fun setPassword(password: String): Builder {
            this.password = password
            return this
        }
        
        fun build(): RegisterUserRequest = RegisterUserRequest(username, email, password)
    }
}

data class RegisterUserResponse(
    val userProfile: UserProfile,
    val session: Session,
    val accessToken: String,
    val refreshToken: String
)

data class RefreshSessionRequest(
    val sessionID: String
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private var sessionID: String = ""
        
        fun setSessionID(sessionID: String): Builder {
            this.sessionID = sessionID
            return this
        }
        
        fun build(): RefreshSessionRequest = RefreshSessionRequest(sessionID)
    }
}

data class RefreshSessionResponse(
    val session: Session,
    val accessToken: String
)

data class GetTimelineRequest(
    val algorithm: TimelineAlgorithm,
    val page: Long,
    val pageSize: Long
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private var algorithm: TimelineAlgorithm = TimelineAlgorithm.HYBRID
        private var page: Long = 0
        private var pageSize: Long = 20
        
        fun setAlgorithm(algorithm: TimelineAlgorithm): Builder {
            this.algorithm = algorithm
            return this
        }
        
        fun setPage(page: Long): Builder {
            this.page = page
            return this
        }
        
        fun setPageSize(pageSize: Long): Builder {
            this.pageSize = pageSize
            return this
        }
        
        fun build(): GetTimelineRequest = GetTimelineRequest(algorithm, page, pageSize)
    }
}

data class GetTimelineResponse(
    val items: List<TimelineItem>,
    val pagination: Pagination,
    val metadata: TimelineMetadata
)

data class GetNotesBatchRequest(
    val noteIds: List<String>
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private val noteIds = mutableListOf<String>()
        
        fun addAllNoteIds(noteIds: List<String>): Builder {
            this.noteIds.addAll(noteIds)
            return this
        }
        
        fun build(): GetNotesBatchRequest = GetNotesBatchRequest(noteIds)
    }
}

data class GetNotesBatchResponse(
    val notes: List<Note>,
    val notFoundIds: List<String>
)

data class LikeNoteRequest(
    val noteID: String,
    val userID: String
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private var noteID: String = ""
        private var userID: String = ""
        
        fun setNoteID(noteID: String): Builder {
            this.noteID = noteID
            return this
        }
        
        fun setUserID(userID: String): Builder {
            this.userID = userID
            return this
        }
        
        fun build(): LikeNoteRequest = LikeNoteRequest(noteID, userID)
    }
}

data class LikeNoteResponse(
    val success: Boolean,
    val newLikeCount: Long
)

data class UnlikeNoteRequest(
    val noteID: String,
    val userID: String
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private var noteID: String = ""
        private var userID: String = ""
        
        fun setNoteID(noteID: String): Builder {
            this.noteID = noteID
            return this
        }
        
        fun setUserID(userID: String): Builder {
            this.userID = userID
            return this
        }
        
        fun build(): UnlikeNoteRequest = UnlikeNoteRequest(noteID, userID)
    }
}

data class UnlikeNoteResponse(
    val success: Boolean,
    val newLikeCount: Long
)

data class SearchUsersRequest(
    val query: String,
    val page: Long,
    val pageSize: Long
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private var query: String = ""
        private var page: Long = 0
        private var pageSize: Long = 20
        
        fun setQuery(query: String): Builder {
            this.query = query
            return this
        }
        
        fun setPage(page: Long): Builder {
            this.page = page
            return this
        }
        
        fun setPageSize(pageSize: Long): Builder {
            this.pageSize = pageSize
            return this
        }
        
        fun build(): SearchUsersRequest = SearchUsersRequest(query, page, pageSize)
    }
}

data class SearchUsersResponse(
    val users: List<UserProfile>,
    val pagination: Pagination
)

data class SearchNotesRequest(
    val query: String,
    val page: Long,
    val pageSize: Long
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private var query: String = ""
        private var page: Long = 0
        private var pageSize: Long = 20
        
        fun setQuery(query: String): Builder {
            this.query = query
            return this
        }
        
        fun setPage(page: Long): Builder {
            this.page = page
            return this
        }
        
        fun setPageSize(pageSize: Long): Builder {
            this.pageSize = pageSize
            return this
        }
        
        fun build(): SearchNotesRequest = SearchNotesRequest(query, page, pageSize)
    }
}

data class SearchNotesResponse(
    val notes: List<Note>,
    val pagination: Pagination
)

data class GetConversationsRequest(
    val userID: String
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private var userID: String = ""
        
        fun setUserID(userID: String): Builder {
            this.userID = userID
            return this
        }
        
        fun build(): GetConversationsRequest = GetConversationsRequest(userID)
    }
}

data class GetConversationsResponse(
    val conversations: List<Conversation>,
    val pagination: Pagination
)

data class GetMessagesRequest(
    val conversationID: String,
    val page: Long,
    val pageSize: Long
) {
    companion object {
        fun newBuilder(): Builder = Builder()
    }
    
    class Builder {
        private var conversationID: String = ""
        private var page: Long = 0
        private var pageSize: Long = 20
        
        fun setConversationID(conversationID: String): Builder {
            this.conversationID = conversationID
            return this
        }
        
        fun setPage(page: Long): Builder {
            this.page = page
            return this
        }
        
        fun setPageSize(pageSize: Long): Builder {
            this.pageSize = pageSize
            return this
        }
        
        fun build(): GetMessagesRequest = GetMessagesRequest(conversationID, page, pageSize)
    }
}

data class GetMessagesResponse(
    val messages: List<Message>,
    val pagination: Pagination
)

data class PingRequest

data class PingResponse(
    val timestamp: Timestamp,
    val serverVersion: String,
    val uptime: Long
)

// MARK: - Data Classes
data class UserProfile(
    val userId: String,
    val username: String,
    val email: String,
    val displayName: String,
    val bio: String,
    val avatarUrl: String,
    val location: String,
    val website: String,
    val status: UserStatus,
    val isVerified: Boolean,
    val isPrivate: Boolean,
    val createdAt: Timestamp,
    val updatedAt: Timestamp,
    val lastLogin: Timestamp,
    val followerCount: Long,
    val followingCount: Long,
    val noteCount: Long,
    val settings: Map<String, String>,
    val privacySettings: Map<String, String>
)

data class Session(
    val sessionId: String,
    val userId: String,
    val deviceId: String,
    val deviceName: String,
    val ipAddress: String,
    val userAgent: String,
    val type: SessionType,
    val createdAt: Timestamp,
    val lastActivity: Timestamp,
    val expiresAt: Timestamp,
    val isActive: Boolean,
    val isSuspicious: Boolean,
    val locationInfo: String
)

data class Note(
    val noteId: String,
    val authorId: String,
    val content: String,
    val media: List<MediaItem>,
    val replyToNoteId: String?,
    val renoteOfNoteId: String?,
    val quoteNoteId: String?,
    val createdAt: Timestamp,
    val updatedAt: Timestamp,
    val isDeleted: Boolean,
    val isSensitive: Boolean,
    val language: String,
    val engagement: NoteEngagement,
    val visibility: NoteVisibility,
    val mentions: List<String>,
    val hashtags: List<String>,
    val urls: List<String>
)

data class MediaItem(
    val mediaId: String,
    val url: String,
    val type: MediaType,
    val altText: String?,
    val width: Int,
    val height: Int,
    val duration: Float?,
    val thumbnailUrl: String?
)

data class NoteEngagement(
    val likeCount: Long,
    val repostCount: Long,
    val replyCount: Long,
    val bookmarkCount: Long,
    val viewCount: Long,
    val isLiked: Boolean,
    val isReposted: Boolean,
    val isBookmarked: Boolean
)

data class TimelineItem(
    val note: Note,
    val source: ContentSource,
    val rankingSignals: RankingSignals,
    val injectedAt: Timestamp,
    val finalScore: Double,
    val injectionReason: String,
    val positionInTimeline: Int
)

data class RankingSignals(
    val authorAffinityScore: Double,
    val contentQualityScore: Double,
    val engagementVelocity: Double,
    val recencyScore: Double,
    val personalizationScore: Double,
    val diversityScore: Double,
    val isReplyToFollowing: Boolean,
    val mutualFollowerInteractions: Int
)

data class Conversation(
    val conversationId: String,
    val participants: List<String>,
    val lastMessage: Message?,
    val unreadCount: Int,
    val updatedAt: Timestamp,
    val isGroupChat: Boolean,
    val groupName: String?,
    val groupAvatarUrl: String?
)

data class Message(
    val messageId: String,
    val conversationId: String,
    val senderId: String,
    val content: String,
    val media: List<MediaItem>?,
    val createdAt: Timestamp,
    val isEdited: Boolean,
    val editedAt: Timestamp?,
    val isDeleted: Boolean,
    val replyToMessageId: String?,
    val reactions: List<MessageReaction>
)

data class MessageReaction(
    val emoji: String,
    val userId: String,
    val createdAt: Timestamp
)

data class Pagination(
    val page: Int,
    val pageSize: Int,
    val totalItems: Long,
    val hasMore: Boolean
)

data class TimelineMetadata(
    val totalItems: Int,
    val newItemsSinceLastFetch: Int,
    val lastUpdated: Timestamp,
    val lastUserRead: Timestamp,
    val algorithmUsed: TimelineAlgorithm,
    val timelineVersion: String,
    val algorithmParams: Map<String, Double>
)

// MARK: - Enums
enum class UserStatus {
    USER_STATUS_UNSPECIFIED,
    USER_STATUS_ACTIVE,
    USER_STATUS_SUSPENDED,
    USER_STATUS_PENDING_VERIFICATION,
    USER_STATUS_DEACTIVATED,
    USER_STATUS_BANNED
}

enum class SessionType {
    SESSION_TYPE_UNSPECIFIED,
    SESSION_TYPE_WEB,
    SESSION_TYPE_MOBILE,
    SESSION_TYPE_API,
    SESSION_TYPE_ADMIN
}

enum class MediaType {
    MEDIA_TYPE_UNSPECIFIED,
    MEDIA_TYPE_IMAGE,
    MEDIA_TYPE_VIDEO,
    MEDIA_TYPE_GIF,
    MEDIA_TYPE_AUDIO
}

enum class NoteVisibility {
    NOTE_VISIBILITY_UNSPECIFIED,
    NOTE_VISIBILITY_PUBLIC,
    NOTE_VISIBILITY_FOLLOWERS,
    NOTE_VISIBILITY_PRIVATE,
    NOTE_VISIBILITY_UNLISTED
}

enum class ContentSource {
    CONTENT_SOURCE_UNKNOWN,
    CONTENT_SOURCE_FOLLOWING,
    CONTENT_SOURCE_TRENDING,
    CONTENT_SOURCE_RECOMMENDED,
    CONTENT_SOURCE_PROMOTED,
    CONTENT_SOURCE_LISTS
}

enum class TimelineAlgorithm {
    TIMELINE_ALGORITHM_UNKNOWN,
    TIMELINE_ALGORITHM_CHRONOLOGICAL,
    TIMELINE_ALGORITHM_ALGORITHMIC,
    TIMELINE_ALGORITHM_HYBRID
}

// MARK: - Timestamp
data class Timestamp(
    val seconds: Long,
    val nanos: Int
)