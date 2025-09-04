import Foundation
import Combine
import SwiftUI

@MainActor
class VideoFeedViewModel: ObservableObject {
    
    // MARK: - Published Properties
    @Published var videos: [VideoItem] = []
    @Published var currentVideoIndex = 0
    @Published var selectedTab: VideoTab = .forYou
    @Published var isLoading = false
    @Published var error: String?
    @Published var isLiked = false
    @Published var isFollowing = false
    @Published var showComments = false
    @Published var showShare = false
    @Published var currentVideo: VideoItem?
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private var cancellables = Set<AnyCancellable>()
    private var currentPage = 0
    private let pageSize = 10
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient) {
        self.grpcClient = grpcClient
        loadVideos()
    }
    
    // MARK: - Public Methods
    func loadVideos() {
        Task {
            isLoading = true
            error = nil
            
            do {
                let response = try await grpcClient.getVideos(
                    tab: selectedTab.grpcType,
                    page: currentPage,
                    pageSize: pageSize
                )
                
                let newVideos = response.videos.map { VideoItem(from: $0) }
                
                if currentPage == 0 {
                    videos = newVideos
                } else {
                    videos.append(contentsOf: newVideos)
                }
                
                if !videos.isEmpty {
                    currentVideo = videos[0]
                }
                
                currentPage += 1
            } catch {
                self.error = "Failed to load videos: \(error.localizedDescription)"
            }
            
            isLoading = false
        }
    }
    
    func switchTab(_ tab: VideoTab) {
        selectedTab = tab
        currentPage = 0
        videos.removeAll()
        currentVideoIndex = 0
        loadVideos()
    }
    
    func nextVideo() {
        guard currentVideoIndex < videos.count - 1 else {
            // Load more videos if needed
            loadVideos()
            return
        }
        
        currentVideoIndex += 1
        currentVideo = videos[currentVideoIndex]
        updateVideoState()
    }
    
    func previousVideo() {
        guard currentVideoIndex > 0 else { return }
        
        currentVideoIndex -= 1
        currentVideo = videos[currentVideoIndex]
        updateVideoState()
    }
    
    func selectVideo(at index: Int) {
        guard index >= 0 && index < videos.count else { return }
        
        currentVideoIndex = index
        currentVideo = videos[index]
        updateVideoState()
    }
    
    func toggleLike() {
        guard let video = currentVideo else { return }
        
        isLiked.toggle()
        
        Task {
            do {
                let request = ToggleVideoLikeRequest()
                request.videoId = video.id
                request.isLiked = isLiked
                
                let response = try await grpcClient.toggleVideoLike(request: request)
                if !response.success {
                    // Revert if failed
                    isLiked.toggle()
                }
            } catch {
                // Revert if failed
                isLiked.toggle()
            }
        }
    }
    
    func toggleFollow() {
        guard let video = currentVideo else { return }
        
        isFollowing.toggle()
        
        Task {
            do {
                let request = ToggleFollowRequest()
                request.userId = video.author.userId
                request.isFollowing = isFollowing
                
                let response = try await grpcClient.toggleFollow(request: request)
                if !response.success {
                    // Revert if failed
                    isFollowing.toggle()
                }
            } catch {
                // Revert if failed
                isFollowing.toggle()
            }
        }
    }
    
    func showComments() {
        showComments = true
    }
    
    func showShare() {
        showShare = true
    }
    
    func refreshVideos() {
        currentPage = 0
        videos.removeAll()
        currentVideoIndex = 0
        loadVideos()
    }
    
    // MARK: - Private Methods
    private func updateVideoState() {
        guard let video = currentVideo else { return }
        
        // Update like and follow state based on current video
        isLiked = video.isLiked
        isFollowing = video.author.isFollowing
    }
}

// MARK: - Data Models
enum VideoTab: String, CaseIterable {
    case forYou = "for_you"
    case trending = "trending"
    
    var displayName: String {
        switch self {
        case .forYou: return "For You"
        case .trending: return "Trending"
        }
    }
    
    var grpcType: xyz.sonet.app.grpc.proto.VideoTab {
        switch self {
        case .forYou: return .VIDEO_TAB_FOR_YOU
        case .trending: return .VIDEO_TAB_TRENDING
        }
    }
}

struct VideoItem: Identifiable {
    let id: String
    let videoUrl: String
    let thumbnailUrl: String
    let caption: String
    let author: UserProfile
    let likeCount: Int
    let commentCount: Int
    let shareCount: Int
    let viewCount: Int
    let duration: TimeInterval
    let isLiked: Bool
    let isFollowing: Bool
    let timestamp: Date
    let hashtags: [String]
    let music: VideoMusic?
    
    init(from grpcVideo: xyz.sonet.app.grpc.proto.VideoItem) {
        self.id = grpcVideo.videoId
        self.videoUrl = grpcVideo.videoUrl
        self.thumbnailUrl = grpcVideo.thumbnailUrl
        self.caption = grpcVideo.caption
        self.author = UserProfile(from: grpcVideo.author)
        self.likeCount = Int(grpcVideo.likeCount)
        self.commentCount = Int(grpcVideo.commentCount)
        self.shareCount = Int(grpcVideo.shareCount)
        self.viewCount = Int(grpcVideo.viewCount)
        self.duration = TimeInterval(grpcVideo.duration)
        self.isLiked = grpcVideo.isLiked
        self.isFollowing = grpcVideo.author.isFollowing
        self.timestamp = Date(timeIntervalSince1970: TimeInterval(grpcVideo.timestamp.seconds))
        self.hashtags = grpcVideo.hashtagsList
        self.music = grpcVideo.hasMusic ? VideoMusic(from: grpcVideo.music) : nil
    }
}

struct VideoMusic {
    let id: String
    let title: String
    let artist: String
    let audioUrl: String
    let duration: TimeInterval
    
    init(from grpcMusic: xyz.sonet.app.grpc.proto.VideoMusic) {
        self.id = grpcMusic.musicId
        self.title = grpcMusic.title
        self.artist = grpcMusic.artist
        self.audioUrl = grpcMusic.audioUrl
        self.duration = TimeInterval(grpcMusic.duration)
    }
}

struct VideoComment: Identifiable {
    let id: String
    let text: String
    let author: UserProfile
    let likeCount: Int
    let isLiked: Bool
    let timestamp: Date
    let replies: [VideoComment]
    
    init(from grpcComment: xyz.sonet.app.grpc.proto.VideoComment) {
        self.id = grpcComment.commentId
        self.text = grpcComment.text
        self.author = UserProfile(from: grpcComment.author)
        self.likeCount = Int(grpcComment.likeCount)
        self.isLiked = grpcComment.isLiked
        self.timestamp = Date(timeIntervalSince1970: TimeInterval(grpcComment.timestamp.seconds))
        self.replies = grpcComment.repliesList.map { VideoComment(from: $0) }
    }
}