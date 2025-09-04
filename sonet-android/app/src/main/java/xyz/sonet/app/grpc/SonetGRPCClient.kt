package xyz.sonet.app.grpc

import android.content.Context
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import io.grpc.ClientInterceptors
import io.grpc.ForwardingClientCall
import io.grpc.Metadata
import io.grpc.StatusRuntimeException
import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import xyz.sonet.app.grpc.proto.*
import xyz.sonet.app.grpc.proto.UserServiceGrpc
import xyz.sonet.app.grpc.proto.NoteServiceGrpc
import xyz.sonet.app.grpc.proto.TimelineServiceGrpc
import xyz.sonet.app.grpc.proto.SearchServiceGrpc
import xyz.sonet.app.grpc.proto.MessagingServiceGrpc
import xyz.sonet.app.grpc.proto.VideoServiceGrpc
import xyz.sonet.app.grpc.proto.NotificationServiceGrpc
import xyz.sonet.app.grpc.proto.FollowServiceGrpc
import xyz.sonet.app.grpc.proto.MediaServiceGrpc
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

// Coordinator to serialize token refresh operations. Multiple callers will await the same refresh.
private class TokenRefreshCoordinator {
    private val mutex = Mutex()
    private var ongoing: CompletableDeferred<RefreshTokenResponseWrapper>? = null

    suspend fun refresh(op: suspend () -> RefreshTokenResponseWrapper): RefreshTokenResponseWrapper {
        // Fast path: if there's an ongoing refresh, wait for it
        ongoing?.let { return it.await() }

        return mutex.withLock {
            // Double-check inside mutex
            ongoing?.let { return it.await() }

            val deferred = CompletableDeferred<RefreshTokenResponseWrapper>()
            ongoing = deferred
            try {
                val res = op()
                deferred.complete(res)
                ongoing = null
                return res
            } catch (e: Throwable) {
                deferred.completeExceptionally(e)
                ongoing = null
                throw e
            }
        }
    }
}

class SonetGRPCClient(
    private val context: Context,
    private val configuration: SonetConfiguration
) {

    // Simple wrapper to return auth tokens alongside the user profile
    data class LoginUserResponseWrapper(
        val userProfile: UserProfile,
        val accessToken: String,
        val refreshToken: String,
        val expiresIn: Int,
        val session: Session
    )

    
    // MARK: - Properties
    private var channel: ManagedChannel? = null
    private var userServiceClient: UserServiceGrpc.UserServiceBlockingStub? = null
    private var noteServiceClient: NoteServiceGrpc.NoteServiceBlockingStub? = null
    private var timelineServiceClient: TimelineServiceGrpc.TimelineServiceBlockingStub? = null
    private var searchServiceClient: SearchServiceGrpc.SearchServiceBlockingStub? = null
    private var messagingServiceClient: MessagingServiceGrpc.MessagingServiceBlockingStub? = null
    private var videoServiceClient: VideoServiceGrpc.VideoServiceBlockingStub? = null
    private var notificationServiceClient: NotificationServiceGrpc.NotificationServiceBlockingStub? = null
    private var followServiceClient: FollowServiceGrpc.FollowServiceBlockingStub? = null
    private var mediaServiceClient: MediaServiceGrpc.MediaServiceBlockingStub? = null
    // Coordinator to serialize token refresh operations
    private val tokenRefresher = TokenRefreshCoordinator()
    
    // MARK: - Initialization
    init {
        setupConnection()
    }
    
    // MARK: - Connection Setup
    private fun setupConnection() {
        try {
            val channelBuilder = ManagedChannelBuilder
                .forAddress(configuration.host, configuration.port)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .maxInboundMessageSize(1024 * 1024) // 1MB
            
            if (configuration.useTLS) {
                channelBuilder.useTransportSecurity()
            } else {
                channelBuilder.usePlaintext()
            }
            
            channel = channelBuilder.build()
            setupServiceClients()
            testConnection()
            
        } catch (error: Exception) {
            println("gRPC Connection Error: $error")
        }
    }
    
    private fun setupServiceClients() {
        channel?.let { channel ->
            // Attach an interceptor that injects Authorization header from secure storage
            val interceptedChannel = ClientInterceptors.intercept(channel, AuthInterceptor())

            userServiceClient = UserServiceGrpc.newBlockingStub(interceptedChannel)
            noteServiceClient = NoteServiceGrpc.newBlockingStub(interceptedChannel)
            timelineServiceClient = TimelineServiceGrpc.newBlockingStub(interceptedChannel)
            searchServiceClient = SearchServiceGrpc.newBlockingStub(interceptedChannel)
            messagingServiceClient = MessagingServiceGrpc.newBlockingStub(interceptedChannel)
            videoServiceClient = VideoServiceGrpc.newBlockingStub(interceptedChannel)
            notificationServiceClient = NotificationServiceGrpc.newBlockingStub(interceptedChannel)
            followServiceClient = FollowServiceGrpc.newBlockingStub(interceptedChannel)
            mediaServiceClient = MediaServiceGrpc.newBlockingStub(interceptedChannel)
        }
    }

    // Client interceptor to add Authorization header from KeychainUtils
    private inner class AuthInterceptor : io.grpc.ClientInterceptor {
        private val AUTH_KEY: Metadata.Key<String> = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)

        override fun <ReqT, RespT> interceptCall(
            method: io.grpc.MethodDescriptor<ReqT, RespT>,
            callOptions: io.grpc.CallOptions,
            next: io.grpc.Channel
        ): io.grpc.ClientCall<ReqT, RespT> {
            val call = next.newCall(method, callOptions)
            return object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(call) {
                override fun start(responseListener: io.grpc.ClientCall.Listener<RespT>, headers: Metadata) {
                    try {
                        val token = KeychainUtils(context).getAuthToken()
                        if (!token.isNullOrEmpty()) {
                            headers.put(AUTH_KEY, "Bearer $token")
                        }
                    } catch (e: Exception) {
                        // ignore keychain issues; proceed without auth header
                    }
                    super.start(responseListener, headers)
                }
            }
        }
    }

    // Helper to execute a suspend RPC and retry once after refreshing tokens if UNAUTHENTICATED
    private suspend fun <T> callWithAuthRetry(block: suspend () -> T): T {
        try {
            return block()
        } catch (e: StatusRuntimeException) {
            if (e.status.code == Status.UNAUTHENTICATED.code) {
                // Attempt refresh
                val refreshToken = try { KeychainUtils(context).getAuthRefreshToken() } catch (_: Exception) { null }
                if (refreshToken.isNullOrEmpty()) throw e

                try {
                    val refreshResp = tokenRefresher.refresh {
                        refreshAccessToken(refreshToken)
                    }
                    // Persist new tokens
                    try { KeychainUtils(context).storeAuthToken(refreshResp.accessToken) } catch (_: Exception) {}
                    if (refreshResp.refreshToken.isNotEmpty()) {
                        try { KeychainUtils(context).storeAuthRefreshToken(refreshResp.refreshToken) } catch (_: Exception) {}
                    }
                    // Retry once
                    return block()
                } catch (re: Exception) {
                    throw re
                }
            }
            throw e
        }
    }
    
    private fun testConnection() {
        // Test connection with a simple ping
        try {
            val request = PingRequest.newBuilder().build()
            val response = userServiceClient?.ping(request)
            println("gRPC connection established successfully")
        } catch (error: Exception) {
            println("gRPC connection test failed: $error")
        }
    }

    // MARK: - Media Upload (streaming)
    suspend fun uploadMedia(ownerUserId: String, fileName: String, mimeType: String, bytes: ByteArray, onProgress: ((Double) -> Unit)? = null): UploadResponse {
        return suspendCancellableCoroutine { continuation ->
            try {
                val asyncClient = MediaServiceGrpc.newStub(channel)
                val responses = arrayListOf<UploadResponse>()
                val requestObserver = asyncClient.upload(object : StreamObserver<UploadResponse> {
                    override fun onNext(value: UploadResponse) { responses.add(value) }
                    override fun onError(t: Throwable) { continuation.resumeWithException(t) }
                    override fun onCompleted() {
                        continuation.resume(responses.last())
                    }
                })

                // init
                val init = UploadInit.newBuilder()
                    .setOwnerUserId(ownerUserId)
                    .setType(MediaType.MEDIA_TYPE_IMAGE)
                    .setOriginalFilename(fileName)
                    .setMimeType(mimeType)
                    .build()
                val initReq = UploadRequest.newBuilder().setInit(init).build()
                requestObserver.onNext(initReq)

                val chunkSize = 64 * 1024
                var sent = 0
                while (sent < bytes.size) {
                    val end = kotlin.math.min(sent + chunkSize, bytes.size)
                    val chunk = UploadChunk.newBuilder().setContent(com.google.protobuf.ByteString.copyFrom(bytes, sent, end - sent)).build()
                    val req = UploadRequest.newBuilder().setChunk(chunk).build()
                    requestObserver.onNext(req)
                    sent = end
                    onProgress?.invoke(sent.toDouble() / bytes.size.toDouble())
                }
                requestObserver.onCompleted()
            } catch (e: Exception) {
                continuation.resumeWithException(e)
            }
        }
    }
    
    // MARK: - Authentication Methods
    /**
     * Authenticate and return the full Login response (user profile + tokens + session).
     * Keeps backward-compatible conversion points in callers, but exposes tokens so
     * clients can persist access/refresh tokens securely.
     */
    suspend fun authenticate(email: String, password: String): LoginUserResponseWrapper {
        return suspendCancellableCoroutine { continuation ->
            try {
                val request = LoginUserRequest.newBuilder()
                    .setCredentials(AuthCredentials.newBuilder().setEmail(email).setPassword(password).build())
                    .setDeviceName("mobile")
                    .build()

                val response = userServiceClient?.loginUser(request)
                    ?: throw Exception("User service client not available")

                // Wrap the important pieces for callers
                val wrapper = LoginUserResponseWrapper(
                    userProfile = response.user,
                    accessToken = response.accessToken,
                    refreshToken = response.refreshToken,
                    expiresIn = response.expiresIn,
                    session = response.session
                )

                continuation.resume(wrapper)

            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }
    
    suspend fun registerUser(username: String, email: String, password: String): UserProfile {
        return suspendCancellableCoroutine { continuation ->
            try {
                val request = RegisterUserRequest.newBuilder()
                    .setUsername(username)
                    .setEmail(email)
                    .setPassword(password)
                    .build()
                
                val response = userServiceClient?.registerUser(request)
                    ?: throw Exception("User service client not available")
                
                continuation.resume(response.userProfile)
                
            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }
    
    suspend fun refreshSession(sessionId: String): Session {
        return suspendCancellableCoroutine { continuation ->
            try {
                val request = RefreshSessionRequest.newBuilder()
                    .setSessionID(sessionId)
                    .build()
                
                val response = userServiceClient?.refreshSession(request)
                    ?: throw Exception("User service client not available")
                
                continuation.resume(response.session)
                
            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }

    // Refresh access token using a refresh token
    data class RefreshTokenResponseWrapper(
        val accessToken: String,
        val refreshToken: String,
        val expiresIn: Int
    )

    suspend fun refreshAccessToken(refreshToken: String): RefreshTokenResponseWrapper {
        return suspendCancellableCoroutine { continuation ->
            try {
                val request = RefreshTokenRequest.newBuilder()
                    .setRefreshToken(refreshToken)
                    .build()

                val response = userServiceClient?.refreshToken(request)
                    ?: throw Exception("User service client not available")

                val wrapper = RefreshTokenResponseWrapper(
                    accessToken = response.accessToken,
                    refreshToken = response.refreshToken,
                    expiresIn = response.expiresIn
                )

                continuation.resume(wrapper)

            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }
    
    // MARK: - Video Service Methods
    suspend fun getVideos(tab: VideoTab, page: Int, pageSize: Int): GetVideosResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetVideosRequest.newBuilder()
                        .setTab(tab.grpcType)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = videoServiceClient?.getVideos(request)
                        ?: throw Exception("Video service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun getVideoFeed(feedType: String, algorithm: String, page: Int, pageSize: Int): VideoFeedResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val paginationRequest = PaginationRequest.newBuilder()
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val request = VideoFeedRequest.newBuilder()
                        .setFeedType(feedType)
                        .setAlgorithm(algorithm)
                        .setPagination(paginationRequest)
                        .build()

                    val response = videoServiceClient?.getVideoFeed(request)
                        ?: throw Exception("Video service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun getPersonalizedFeed(userId: String, feedType: String, page: Int, pageSize: Int): VideoFeedResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val paginationRequest = PaginationRequest.newBuilder()
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val baseRequest = VideoFeedRequest.newBuilder()
                        .setFeedType(feedType)
                        .setAlgorithm("ml_ranking")
                        .setPagination(paginationRequest)
                        .build()

                    val request = PersonalizedFeedRequest.newBuilder()
                        .setUserId(userId)
                        .setBaseRequest(baseRequest)
                        .build()

                    val response = videoServiceClient?.getPersonalizedFeed(request)
                        ?: throw Exception("Video service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun trackVideoEngagement(userId: String, videoId: String, eventType: String, durationMs: Long? = null, completionRate: Double? = null): EngagementResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val requestBuilder = EngagementEvent.newBuilder()
                        .setUserId(userId)
                        .setVideoId(videoId)
                        .setEventType(eventType)
                        .setTimestamp(java.time.Instant.now().toString())

                    durationMs?.let { requestBuilder.setDurationMs(it) }
                    completionRate?.let { requestBuilder.setCompletionRate(it) }

                    val request = requestBuilder.build()

                    val response = videoServiceClient?.trackEngagement(request)
                        ?: throw Exception("Video service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    fun streamVideoFeed(feedType: String, algorithm: String): Flow<VideoFeedUpdate> = flow {
        try {
            val request = VideoFeedRequest.newBuilder()
                .setFeedType(feedType)
                .setAlgorithm(algorithm)
                .build()

            val stream = callWithAuthRetry {
                suspendCancellableCoroutine { continuation ->
                    try {
                        val s = videoServiceClient?.streamVideoFeed(request)
                            ?: throw Exception("Video service client not available")
                        continuation.resume(s)
                    } catch (error: Exception) {
                        continuation.resumeWithException(error)
                    }
                }
            }

            // Note: This is a simplified implementation. In production, you'd want to handle
            // the streaming response properly with proper lifecycle management
            stream.forEach { update ->
                emit(update)
            }
        } catch (error: Exception) {
            throw error
        }
    }
    
    // MARK: - Video Interaction Methods
    suspend fun toggleVideoLike(videoId: String, userId: String, isLiked: Boolean): ToggleVideoLikeResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = ToggleVideoLikeRequest.newBuilder()
                        .setVideoId(videoId)
                        .setUserId(userId)
                        .setIsLiked(isLiked)
                        .build()

                    val response = videoServiceClient?.toggleVideoLike(request)
                        ?: throw Exception("Video service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun toggleFollow(userId: String, targetUserId: String, isFollowing: Boolean): ToggleFollowResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = ToggleFollowRequest.newBuilder()
                        .setUserId(userId)
                        .setTargetUserId(targetUserId)
                        .setIsFollowing(isFollowing)
                        .build()

                    val response = followServiceClient?.toggleFollow(request)
                        ?: throw Exception("Follow service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    // MARK: - Note Methods
    suspend fun getHomeTimeline(page: Int, pageSize: Int): List<TimelineItem> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetTimelineRequest.newBuilder()
                        .setAlgorithm(TimelineAlgorithm.HYBRID)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = timelineServiceClient?.getHomeTimeline(request)
                        ?: throw Exception("Timeline service client not available")

                    continuation.resume(response.itemsList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }

    // Removed HTTP fallback now that gRPC is available

    suspend fun toggleMediaLike(mediaId: String, userId: String, isLiked: Boolean): ToggleMediaLikeResponse {
        return suspendCancellableCoroutine { continuation ->
            try {
                val request = ToggleMediaLikeRequest.newBuilder()
                    .setMediaId(mediaId)
                    .setUserId(userId)
                    .setIsLiked(isLiked)
                    .build()
                val response = mediaServiceClient?.toggleMediaLike(request)
                    ?: throw Exception("Media service client not available")
                continuation.resume(response)
            } catch (e: Exception) {
                continuation.resumeWithException(e)
            }
        }
    }
    
    suspend fun getNotesBatch(noteIds: List<String>): List<Note> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetNotesBatchRequest.newBuilder()
                        .addAllNoteIds(noteIds)
                        .build()

                    val response = noteServiceClient?.getNotesBatch(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response.notesList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun createNote(content: String, authorId: String, mediaUrls: List<String> = emptyList(), hashtags: List<String> = emptyList()): CreateNoteResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = CreateNoteRequest.newBuilder()
                        .setContent(content)
                        .setAuthorId(authorId)
                        .addAllMediaUrls(mediaUrls)
                        .addAllHashtags(hashtags)
                        .build()

                    val response = noteServiceClient?.createNote(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun likeNote(noteId: String, userId: String): LikeNoteResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = LikeNoteRequest.newBuilder()
                        .setNoteID(noteId)
                        .setUserID(userId)
                        .build()

                    val response = noteServiceClient?.likeNote(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun unlikeNote(noteId: String, userId: String): UnlikeNoteResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = UnlikeNoteRequest.newBuilder()
                        .setNoteID(noteId)
                        .setUserID(userId)
                        .build()

                    val response = noteServiceClient?.unlikeNote(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun getNote(noteId: String): Note {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetNoteRequest.newBuilder()
                        .setNoteId(noteId)
                        .build()

                    val response = noteServiceClient?.getNote(request)
                        ?: throw Exception("Note service client not available")

                    if (!response.success) {
                        throw Exception(response.errorMessage)
                    }

                    continuation.resume(response.note)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun getThread(request: GetThreadRequest): GetThreadResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val response = noteServiceClient?.getThread(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun createReply(request: CreateReplyRequest): CreateNoteResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val response = noteServiceClient?.createReply(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun likeNote(request: LikeNoteRequest): LikeNoteResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val response = noteServiceClient?.likeNote(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun renoteNote(request: RenoteNoteRequest): RenoteNoteResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val response = noteServiceClient?.renoteNote(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    // MARK: - Search Methods
    suspend fun searchUsers(query: String, page: Int, pageSize: Int): List<UserProfile> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = SearchUsersRequest.newBuilder()
                        .setQuery(query)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = searchServiceClient?.searchUsers(request)
                        ?: throw Exception("Search service client not available")

                    continuation.resume(response.usersList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun searchNotes(query: String, page: Int, pageSize: Int): List<Note> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = SearchNotesRequest.newBuilder()
                        .setQuery(query)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = searchServiceClient?.searchNotes(request)
                        ?: throw Exception("Search service client not available")

                    continuation.resume(response.notesList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun searchHashtags(query: String, page: Int, pageSize: Int): List<String> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = SearchHashtagsRequest.newBuilder()
                        .setQuery(query)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = searchServiceClient?.searchHashtags(request)
                        ?: throw Exception("Search service client not available")

                    continuation.resume(response.hashtagsList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    // MARK: - Messaging Methods
    suspend fun getConversations(userId: String, page: Int, pageSize: Int): List<Conversation> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetConversationsRequest.newBuilder()
                        .setUserID(userId)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = messagingServiceClient?.getConversations(request)
                        ?: throw Exception("Messaging service client not available")

                    continuation.resume(response.conversationsList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun getMessages(conversationId: String, page: Int, pageSize: Int): List<Message> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetMessagesRequest.newBuilder()
                        .setConversationID(conversationId)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = messagingServiceClient?.getMessages(request)
                        ?: throw Exception("Messaging service client not available")

                    continuation.resume(response.messagesList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun sendMessage(conversationId: String, senderId: String, content: String, messageType: MessageType = MessageType.TEXT): SendMessageResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = SendMessageRequest.newBuilder()
                        .setConversationID(conversationId)
                        .setSenderID(senderId)
                        .setContent(content)
                        .setMessageType(messageType)
                        .build()

                    val response = messagingServiceClient?.sendMessage(request)
                        ?: throw Exception("Messaging service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun sendTypingIndicator(conversationId: String, userId: String, isTyping: Boolean): SendTypingIndicatorResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = TypingIndicatorRequest.newBuilder()
                        .setConversationID(conversationId)
                        .setUserID(userId)
                        .setIsTyping(isTyping)
                        .build()

                    val response = messagingServiceClient?.sendTypingIndicator(request)
                        ?: throw Exception("Messaging service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun markConversationAsRead(conversationId: String, userId: String): MarkConversationAsReadResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = MarkConversationAsReadRequest.newBuilder()
                        .setConversationID(conversationId)
                        .setUserID(userId)
                        .build()

                    val response = messagingServiceClient?.markConversationAsRead(request)
                        ?: throw Exception("Messaging service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun deleteMessage(messageId: String): DeleteMessageResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = DeleteMessageRequest.newBuilder()
                        .setMessageID(messageId)
                        .build()

                    val response = messagingServiceClient?.deleteMessage(request)
                        ?: throw Exception("Messaging service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun createGroup(name: String, creatorId: String, memberIds: List<String>): CreateGroupResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = CreateGroupRequest.newBuilder()
                        .setName(name)
                        .setCreatorID(creatorId)
                        .addAllMemberIDs(memberIds)
                        .build()

                    val response = messagingServiceClient?.createGroup(request)
                        ?: throw Exception("Messaging service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    fun messageStream(): Flow<Message> = flow {
        try {
            val request = MessageStreamRequest.newBuilder()
                .setUserID("current_user") // This should be passed from the caller
                .build()

            val stream = callWithAuthRetry {
                suspendCancellableCoroutine { continuation ->
                    try {
                        val s = messagingServiceClient?.messageStream(request)
                            ?: throw Exception("Messaging service client not available")
                        continuation.resume(s)
                    } catch (error: Exception) {
                        continuation.resumeWithException(error)
                    }
                }
            }

            // Note: This is a simplified implementation. In production, you'd want to handle
            // the streaming response properly with proper lifecycle management
            stream.forEach { message ->
                emit(message)
            }
        } catch (error: Exception) {
            throw error
        }
    }
    
    // MARK: - Notification Methods
    suspend fun getNotifications(page: Int, pageSize: Int): List<NotificationItem> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetNotificationsRequest.newBuilder()
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = notificationServiceClient?.getNotifications(request)
                        ?: throw Exception("Notification service client not available")

                    continuation.resume(response.notificationsList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun markNotificationAsRead(notificationId: String): MarkNotificationAsReadResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = MarkNotificationAsReadRequest.newBuilder()
                        .setNotificationID(notificationId)
                        .build()

                    val response = notificationServiceClient?.markNotificationAsRead(request)
                        ?: throw Exception("Notification service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun markAllNotificationsAsRead(): MarkAllNotificationsAsReadResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = MarkAllNotificationsAsReadRequest.newBuilder().build()

                    val response = notificationServiceClient?.markAllNotificationsAsRead(request)
                        ?: throw Exception("Notification service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun deleteNotification(notificationId: String): DeleteNotificationResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = DeleteNotificationRequest.newBuilder()
                        .setNotificationID(notificationId)
                        .build()

                    val response = notificationServiceClient?.deleteNotification(request)
                        ?: throw Exception("Notification service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun clearAllNotifications(): ClearAllNotificationsResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = ClearAllNotificationsRequest.newBuilder().build()

                    val response = notificationServiceClient?.clearAllNotifications(request)
                        ?: throw Exception("Notification service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun checkForAppUpdates(): AppUpdateInfo {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = CheckForUpdatesRequest.newBuilder().build()

                    val response = notificationServiceClient?.checkForUpdates(request)
                        ?: throw Exception("Notification service client not available")

                    continuation.resume(response.updateInfo)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun updateNotificationPreferences(preferences: NotificationPreferences): UpdateNotificationPreferencesResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = UpdateNotificationPreferencesRequest.newBuilder()
                        .setPreferences(preferences)
                        .build()

                    val response = notificationServiceClient?.updateNotificationPreferences(request)
                        ?: throw Exception("Notification service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    fun notificationStream(): Flow<NotificationItem> = flow {
        try {
            val request = NotificationStreamRequest.newBuilder()
                .setUserID("current_user") // This should be passed from the caller
                .build()
            
            val stream = notificationServiceClient?.notificationStream(request)
                ?: throw Exception("Notification service client not available")
            
            // Note: This is a simplified implementation. In production, you'd want to handle
            // the streaming response properly with proper lifecycle management
            stream.forEach { notification ->
                emit(notification)
            }
        } catch (error: Exception) {
            throw error
        }
    }
    
    // MARK: - Profile Methods
    suspend fun getUserProfile(userId: String): UserProfile {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetUserProfileRequest.newBuilder()
                        .setUserID(userId)
                        .build()

                    val response = userServiceClient?.getUserProfile(request)
                        ?: throw Exception("User service client not available")

                    continuation.resume(response.userProfile)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun getUserPosts(userId: String, page: Int, pageSize: Int): List<Note> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetUserPostsRequest.newBuilder()
                        .setUserID(userId)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = noteServiceClient?.getUserPosts(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response.notesList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun getUserReplies(userId: String, page: Int, pageSize: Int): List<Note> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetUserRepliesRequest.newBuilder()
                        .setUserID(userId)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = noteServiceClient?.getUserReplies(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response.notesList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun getUserMedia(userId: String, page: Int, pageSize: Int): List<Note> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetUserMediaRequest.newBuilder()
                        .setUserID(userId)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = noteServiceClient?.getUserMedia(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response.notesList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun getUserLikes(userId: String, page: Int, pageSize: Int): List<Note> {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = GetUserLikesRequest.newBuilder()
                        .setUserID(userId)
                        .setPage(page.toLong())
                        .setPageSize(pageSize.toLong())
                        .build()

                    val response = noteServiceClient?.getUserLikes(request)
                        ?: throw Exception("Note service client not available")

                    continuation.resume(response.notesList)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun blockUser(userId: String, targetUserId: String): BlockUserResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = BlockUserRequest.newBuilder()
                        .setUserID(userId)
                        .setTargetUserID(targetUserId)
                        .build()

                    val response = userServiceClient?.blockUser(request)
                        ?: throw Exception("User service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    suspend fun unblockUser(userId: String, targetUserId: String): UnblockUserResponse {
        return callWithAuthRetry {
            suspendCancellableCoroutine { continuation ->
                try {
                    val request = UnblockUserRequest.newBuilder()
                        .setUserID(userId)
                        .setTargetUserID(targetUserId)
                        .build()

                    val response = userServiceClient?.unblockUser(request)
                        ?: throw Exception("User service client not available")

                    continuation.resume(response)

                } catch (error: Exception) {
                    continuation.resumeWithException(error)
                }
            }
        }
    }
    
    // MARK: - Settings Methods
    suspend fun updateNotificationPreferences(request: UpdateNotificationPreferencesRequest): UpdateNotificationPreferencesResponse {
        return suspendCancellableCoroutine { continuation ->
            try {
                val response = notificationServiceClient?.updateNotificationPreferences(request)
                    ?: throw Exception("Notification service client not available")
                
                continuation.resume(response)
                
            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }
    
    suspend fun updatePrivacySettings(request: UpdatePrivacySettingsRequest): UpdatePrivacySettingsResponse {
        return suspendCancellableCoroutine { continuation ->
            try {
                val response = userServiceClient?.updatePrivacySettings(request)
                    ?: throw Exception("User service client not available")
                
                continuation.resume(response)
                
            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }
    
    suspend fun updateContentPreferences(request: UpdateContentPreferencesRequest): UpdateContentPreferencesResponse {
        return suspendCancellableCoroutine { continuation ->
            try {
                val response = userServiceClient?.updateContentPreferences(request)
                    ?: throw Exception("User service client not available")
                
                continuation.resume(response)
                
            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }
    
    suspend fun getStorageUsage(): StorageUsage {
        return suspendCancellableCoroutine { continuation ->
            try {
                val request = GetStorageUsageRequest.newBuilder().build()
                
                val response = mediaServiceClient?.getStorageUsage(request)
                    ?: throw Exception("Media service client not available")
                
                continuation.resume(response.storageUsage)
                
            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }
    
    suspend fun clearCache(): ClearCacheResponse {
        return suspendCancellableCoroutine { continuation ->
            try {
                val request = ClearCacheRequest.newBuilder().build()
                
                val response = mediaServiceClient?.clearCache(request)
                    ?: throw Exception("Media service client not available")
                
                continuation.resume(response)
                
            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }
    
    suspend fun exportUserData(): ExportUserDataResponse {
        return suspendCancellableCoroutine { continuation ->
            try {
                val request = ExportUserDataRequest.newBuilder().build()
                
                val response = userServiceClient?.exportUserData(request)
                    ?: throw Exception("User service client not available")
                
                continuation.resume(response)
                
            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }
    
    suspend fun deleteAccount(): DeleteAccountResponse {
        return suspendCancellableCoroutine { continuation ->
            try {
                val request = DeleteAccountRequest.newBuilder().build()
                
                val response = userServiceClient?.deleteAccount(request)
                    ?: throw Exception("User service client not available")
                
                continuation.resume(response)
                
            } catch (error: Exception) {
                continuation.resumeWithException(error)
            }
        }
    }
    
    // MARK: - Cleanup
    fun shutdown() {
        try {
            channel?.shutdown()?.awaitTermination(5, TimeUnit.SECONDS)
        } catch (error: Exception) {
            println("Error shutting down gRPC channel: $error")
        }
    }
}

// MARK: - Configuration
data class SonetConfiguration(
    val host: String,
    val port: Int,
    val useTLS: Boolean,
    val timeout: Long
) {
    companion object {
        val development = SonetConfiguration(
            host = "localhost",
            port = 50051,
            useTLS = false,
            timeout = 30L
        )
        
        val staging = SonetConfiguration(
            host = "staging.sonet.app",
            port = 443,
            useTLS = true,
            timeout = 30L
        )
        
        val production = SonetConfiguration(
            host = "api.sonet.app",
            port = 443,
            useTLS = true,
            timeout = 30L
        )
    }
}

// MARK: - Error Types
sealed class SonetError : Exception() {
    object ServiceUnavailable : SonetError() {
        override val message: String = "Service is currently unavailable"
    }
    
    object AuthenticationFailed : SonetError() {
        override val message: String = "Authentication failed"
    }
    
    object NetworkError : SonetError() {
        override val message: String = "Network connection error"
    }
    
    object InvalidResponse : SonetError() {
        override val message: String = "Invalid response from server"
    }
    
    object RateLimited : SonetError() {
        override val message: String = "Too many requests, please try again later"
    }
    
    object VideoNotFound : SonetError() {
        override val message: String = "Video not found"
    }
    
    object UserNotFound : SonetError() {
        override val message: String = "User not found"
    }
    
    object PermissionDenied : SonetError() {
        override val message: String = "Permission denied"
    }
    
    object InvalidRequest : SonetError() {
        override val message: String = "Invalid request"
    }
    
    object ServerError : SonetError() {
        override val message: String = "Server error occurred"
    }
}

// MARK: - Video Tab Enum
enum class VideoTab(
    val value: String,
    val displayName: String
) {
    FOR_YOU("for_you", "For You"),
    TRENDING("trending", "Trending"),
    FOLLOWING("following", "Following"),
    DISCOVER("discover", "Discover");
    
    val grpcType: xyz.sonet.app.grpc.proto.VideoTab
        get() = when (this) {
            FOR_YOU -> xyz.sonet.app.grpc.proto.VideoTab.VIDEO_TAB_FOR_YOU
            TRENDING -> xyz.sonet.app.grpc.proto.VideoTab.VIDEO_TAB_TRENDING
            FOLLOWING -> xyz.sonet.app.grpc.proto.VideoTab.VIDEO_TAB_FOLLOWING
            DISCOVER -> xyz.sonet.app.grpc.proto.VideoTab.VIDEO_TAB_DISCOVER
        }
}