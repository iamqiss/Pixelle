import Foundation

// MARK: - Common Types
struct Timestamp {
    let seconds: Int64
    let nanos: Int32
    
    var date: Date {
        return Date(timeIntervalSince1970: TimeInterval(seconds))
    }
}

struct Pagination {
    let page: UInt32
    let pageSize: UInt32
    let totalItems: UInt64
    let hasMore: Bool
}

// MARK: - User Service Types
struct UserProfile {
    let userId: String
    let username: String
    let email: String
    let displayName: String
    let bio: String
    let avatarUrl: String
    let location: String
    let website: String
    let status: UserStatus
    let isVerified: Bool
    let isPrivate: Bool
    let createdAt: Timestamp
    let updatedAt: Timestamp
    let lastLogin: Timestamp
    let followerCount: UInt64
    let followingCount: UInt64
    let noteCount: UInt64
    let settings: [String: String]
    let privacySettings: [String: String]
}

enum UserStatus: Int32 {
    case unspecified = 0
    case active = 1
    case suspended = 2
    case pendingVerification = 3
    case deactivated = 4
    case banned = 5
}

enum SessionType: Int32 {
    case unspecified = 0
    case web = 1
    case mobile = 2
    case api = 3
    case admin = 4
}

struct Session {
    let sessionId: String
    let userId: String
    let deviceId: String
    let deviceName: String
    let ipAddress: String
    let userAgent: String
    let type: SessionType
    let createdAt: Timestamp
    let lastActivity: Timestamp
    let expiresAt: Timestamp
    let isActive: Bool
    let isSuspicious: Bool
    let locationInfo: String
}

// MARK: - Note Service Types
struct Note {
    let noteId: String
    let authorId: String
    let content: String
    let media: [MediaItem]
    let replyToNoteId: String?
    let renoteOfNoteId: String?
    let quoteNoteId: String?
    let createdAt: Timestamp
    let updatedAt: Timestamp
    let isDeleted: Bool
    let isSensitive: Bool
    let language: String
    let engagement: NoteEngagement
    let visibility: NoteVisibility
    let mentions: [String]
    let hashtags: [String]
    let urls: [String]
}

struct MediaItem {
    let mediaId: String
    let url: String
    let type: MediaType
    let altText: String?
    let width: UInt32
    let height: UInt32
    let duration: Float?
    let thumbnailUrl: String?
}

enum MediaType: Int32 {
    case unspecified = 0
    case image = 1
    case video = 2
    case gif = 3
    case audio = 4
}

struct NoteEngagement {
    let likeCount: UInt64
    let repostCount: UInt64
    let replyCount: UInt64
    let bookmarkCount: UInt64
    let viewCount: UInt64
    let isLiked: Bool
    let isReposted: Bool
    let isBookmarked: Bool
}

enum NoteVisibility: Int32 {
    case unspecified = 0
    case public = 1
    case followers = 2
    case private = 3
    case unlisted = 4
}

// MARK: - Timeline Service Types
struct TimelineItem {
    let note: Note
    let source: ContentSource
    let rankingSignals: RankingSignals
    let injectedAt: Timestamp
    let finalScore: Double
    let injectionReason: String
    let positionInTimeline: Int32
}

enum ContentSource: Int32 {
    case unknown = 0
    case following = 1
    case trending = 2
    case recommended = 3
    case promoted = 4
    case lists = 5
}

struct RankingSignals {
    let authorAffinityScore: Double
    let contentQualityScore: Double
    let engagementVelocity: Double
    let recencyScore: Double
    let personalizationScore: Double
    let diversityScore: Double
    let isReplyToFollowing: Bool
    let mutualFollowerInteractions: Int32
}

enum TimelineAlgorithm: Int32 {
    case unknown = 0
    case chronological = 1
    case algorithmic = 2
    case hybrid = 3
}

// MARK: - Search Service Types
struct SearchResult {
    let query: String
    let totalResults: UInt64
    let page: UInt32
    let pageSize: UInt32
    let hasMore: Bool
}

struct UserSearchResult: SearchResult {
    let users: [UserProfile]
}

struct NoteSearchResult: SearchResult {
    let notes: [Note]
}

// MARK: - Messaging Service Types
struct Conversation {
    let conversationId: String
    let participants: [String]
    let lastMessage: Message?
    let unreadCount: UInt32
    let updatedAt: Timestamp
    let isGroupChat: Bool
    let groupName: String?
    let groupAvatarUrl: String?
}

struct Message {
    let messageId: String
    let conversationId: String
    let senderId: String
    let content: String
    let media: [MediaItem]?
    let createdAt: Timestamp
    let isEdited: Bool
    let editedAt: Timestamp?
    let isDeleted: Bool
    let replyToMessageId: String?
    let reactions: [MessageReaction]
}

struct MessageReaction {
    let emoji: String
    let userId: String
    let createdAt: Timestamp
}

// MARK: - Request/Response Types
struct AuthRequest {
    let email: String
    let password: String
    let twoFactorCode: String?
    let sessionType: SessionType
}

struct AuthResponse {
    let userProfile: UserProfile
    let session: Session
    let accessToken: String
    let refreshToken: String
}

struct RegisterUserRequest {
    let username: String
    let email: String
    let password: String
    let displayName: String?
    let bio: String?
}

struct RegisterUserResponse {
    let userProfile: UserProfile
    let session: Session
    let accessToken: String
    let refreshToken: String
}

struct RefreshSessionRequest {
    let sessionId: String
}

struct RefreshSessionResponse {
    let session: Session
    let accessToken: String
}

struct GetTimelineRequest {
    let algorithm: TimelineAlgorithm
    let page: UInt32
    let pageSize: UInt32
    let userId: String?
    let filters: TimelineFilters?
}

struct GetTimelineResponse {
    let items: [TimelineItem]
    let pagination: Pagination
    let metadata: TimelineMetadata
}

struct TimelineFilters {
    let showReplies: Bool
    let showRenotes: Bool
    let showRecommendedContent: Bool
    let showTrendingContent: Bool
    let mutedKeywords: [String]
    let mutedUsers: [String]
    let preferredLanguages: [String]
}

struct TimelineMetadata {
    let totalItems: Int32
    let newItemsSinceLastFetch: Int32
    let lastUpdated: Timestamp
    let lastUserRead: Timestamp
    let algorithmUsed: TimelineAlgorithm
    let timelineVersion: String
    let algorithmParams: [String: Double]
}

struct GetNotesBatchRequest {
    let noteIds: [String]
}

struct GetNotesBatchResponse {
    let notes: [Note]
    let notFoundIds: [String]
}

struct LikeNoteRequest {
    let noteId: String
    let userId: String
}

struct LikeNoteResponse {
    let success: Bool
    let newLikeCount: UInt64
}

struct UnlikeNoteRequest {
    let noteId: String
    let userId: String
}

struct UnlikeNoteResponse {
    let success: Bool
    let newLikeCount: UInt64
}

struct SearchUsersRequest {
    let query: String
    let page: UInt32
    let pageSize: UInt32
    let filters: UserSearchFilters?
}

struct UserSearchFilters {
    let verifiedOnly: Bool
    let hasAvatar: Bool
    let minFollowers: UInt64
    let maxFollowers: UInt64
    let languages: [String]
}

struct SearchUsersResponse {
    let users: [UserProfile]
    let pagination: Pagination
}

struct SearchNotesRequest {
    let query: String
    let page: UInt32
    let pageSize: UInt32
    let filters: NoteSearchFilters?
}

struct NoteSearchFilters {
    let authorId: String?
    let hasMedia: Bool
    let mediaType: MediaType?
    let language: String?
    let dateRange: DateRange?
    let engagementThreshold: UInt64?
}

struct DateRange {
    let startDate: Timestamp
    let endDate: Timestamp
}

struct SearchNotesResponse {
    let notes: [Note]
    let pagination: Pagination
}

struct GetConversationsRequest {
    let userId: String
    let page: UInt32
    let pageSize: UInt32
}

struct GetConversationsResponse {
    let conversations: [Conversation]
    let pagination: Pagination
}

struct GetMessagesRequest {
    let conversationId: String
    let page: UInt32
    let pageSize: UInt32
}

struct GetMessagesResponse {
    let messages: [Message]
    let pagination: Pagination
}

// MARK: - Ping Request/Response
struct PingRequest {
    // Empty ping request
}

struct PingResponse {
    let timestamp: Timestamp
    let serverVersion: String
    let uptime: UInt64
}