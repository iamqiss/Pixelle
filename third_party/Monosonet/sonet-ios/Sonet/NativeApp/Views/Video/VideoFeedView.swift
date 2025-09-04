import SwiftUI
import AVKit

struct VideoFeedView: View {
    @StateObject private var viewModel: VideoFeedViewModel
    @State private var currentVideoIndex = 0
    
    init(grpcClient: SonetGRPCClient) {
        _viewModel = StateObject(wrappedValue: VideoFeedViewModel(grpcClient: grpcClient))
    }
    
    var body: some View {
        GeometryReader { geometry in
            ZStack {
                Color.black.ignoresSafeArea()
                
                VStack(spacing: 0) {
                    // Top tabs
                    VideoTabsView(
                        selectedTab: $viewModel.selectedTab,
                        onTabSelected: { viewModel.switchTab($0) }
                    )
                    
                    // Video feed
                    if viewModel.isLoading && viewModel.videos.isEmpty {
                        LoadingView()
                    } else if let error = viewModel.error {
                        ErrorView(error: error) {
                            viewModel.refreshVideos()
                        }
                    } else if viewModel.videos.isEmpty {
                        EmptyVideoView()
                    } else {
                        VideoFeedContent(
                            videos: viewModel.videos,
                            currentVideoIndex: $currentVideoIndex,
                            currentVideo: $viewModel.currentVideo,
                            isLiked: $viewModel.isLiked,
                            isFollowing: $viewModel.isFollowing,
                            onVideoChanged: { index in
                                viewModel.selectVideo(at: index)
                            },
                            onLike: { viewModel.toggleLike() },
                            onFollow: { viewModel.toggleFollow() },
                            onComment: { viewModel.showComments() },
                            onShare: { viewModel.showShare() }
                        )
                    }
                }
            }
        }
        .onAppear {
            viewModel.loadVideos()
        }
    }
}

// MARK: - Video Tabs View
struct VideoTabsView: View {
    @Binding var selectedTab: VideoTab
    let onTabSelected: (VideoTab) -> Void
    
    var body: some View {
        HStack(spacing: 0) {
            ForEach(VideoTab.allCases, id: \.self) { tab in
                Button(action: {
                    onTabSelected(tab)
                }) {
                    VStack(spacing: 4) {
                        Text(tab.displayName)
                            .font(.system(size: 16, weight: selectedTab == tab ? .semibold : .medium))
                            .foregroundColor(selectedTab == tab ? .white : .white.opacity(0.7))
                        
                        // Underline indicator
                        Rectangle()
                            .fill(selectedTab == tab ? Color.white : Color.clear)
                            .frame(height: 2)
                            .animation(.easeInOut(duration: 0.2), value: selectedTab)
                    }
                }
                .frame(maxWidth: .infinity)
            }
        }
        .padding(.horizontal, 20)
        .padding(.top, 8)
        .padding(.bottom, 12)
        .background(Color.black)
    }
}

// MARK: - Video Feed Content
struct VideoFeedContent: View {
    let videos: [VideoItem]
    @Binding var currentVideoIndex: Int
    @Binding var currentVideo: VideoItem?
    @Binding var isLiked: Bool
    @Binding var isFollowing: Bool
    let onVideoChanged: (Int) -> Void
    let onLike: () -> Void
    let onFollow: () -> Void
    let onComment: () -> Void
    let onShare: () -> Void
    
    var body: some View {
        TabView(selection: $currentVideoIndex) {
            ForEach(Array(videos.enumerated()), id: \.element.id) { index, video in
                VideoPlayerView(
                    video: video,
                    isLiked: index == currentVideoIndex ? isLiked : video.isLiked,
                    isFollowing: index == currentVideoIndex ? isFollowing : video.author.isFollowing,
                    onLike: onLike,
                    onFollow: onFollow,
                    onComment: onComment,
                    onShare: onShare
                )
                .tag(index)
                .onAppear {
                    if index == currentVideoIndex {
                        currentVideo = video
                    }
                }
            }
        }
        .tabViewStyle(PageTabViewStyle(indexDisplayMode: .never))
        .onChange(of: currentVideoIndex) { newIndex in
            onVideoChanged(newIndex)
        }
    }
}

// MARK: - Video Player View
struct VideoPlayerView: View {
    let video: VideoItem
    let isLiked: Bool
    let isFollowing: Bool
    let onLike: () -> Void
    let onFollow: () -> Void
    let onComment: () -> Void
    let onShare: () -> Void
    
    @State private var player: AVPlayer?
    @State private var isPlaying = false
    
    var body: some View {
        GeometryReader { geometry in
            ZStack {
                // Video player
                if let player = player {
                    VideoPlayer(player: player)
                        .edgesIgnoringSafeArea(.all)
                } else {
                    // Thumbnail placeholder
                    AsyncImage(url: URL(string: video.thumbnailUrl)) { image in
                        image
                            .resizable()
                            .aspectRatio(contentMode: .fill)
                    } placeholder: {
                        Rectangle()
                            .fill(Color(.systemGray6))
                            .overlay(
                                ProgressView()
                                    .scaleEffect(1.5)
                                    .foregroundColor(.white)
                            )
                    }
                    .frame(width: geometry.size.width, height: geometry.size.height)
                    .clipped()
                }
                
                // Video overlay content
                VStack {
                    Spacer()
                    
                    HStack(alignment: .bottom, spacing: 0) {
                        // Left side - Caption and author
                        VStack(alignment: .leading, spacing: 12) {
                            // Author info
                            HStack(spacing: 12) {
                                AsyncImage(url: URL(string: video.author.avatarUrl)) { image in
                                    image
                                        .resizable()
                                        .aspectRatio(contentMode: .fill)
                                } placeholder: {
                                    Circle()
                                        .fill(Color(.systemGray4))
                                        .overlay(
                                            Image(systemName: "person.fill")
                                                .foregroundColor(.secondary)
                                        )
                                }
                                .frame(width: 50, height: 50)
                                .clipShape(Circle())
                                
                                VStack(alignment: .leading, spacing: 4) {
                                    Text(video.author.displayName)
                                        .font(.system(size: 16, weight: .semibold))
                                        .foregroundColor(.white)
                                    
                                    Text("@\(video.author.username)")
                                        .font(.system(size: 14))
                                        .foregroundColor(.white.opacity(0.8))
                                }
                                
                                Spacer()
                                
                                Button(action: onFollow) {
                                    Text(isFollowing ? "Following" : "Follow")
                                        .font(.system(size: 14, weight: .semibold))
                                        .foregroundColor(isFollowing ? .white : .black)
                                        .padding(.horizontal, 16)
                                        .padding(.vertical, 8)
                                        .background(
                                            RoundedRectangle(cornerRadius: 20)
                                                .fill(isFollowing ? Color.white.opacity(0.3) : Color.white)
                                        )
                                }
                            }
                            
                            // Caption
                            Text(video.caption)
                                .font(.system(size: 16))
                                .foregroundColor(.white)
                                .lineLimit(3)
                                .multilineTextAlignment(.leading)
                            
                            // Hashtags
                            if !video.hashtags.isEmpty {
                                HStack(spacing: 8) {
                                    ForEach(video.hashtags.prefix(3), id: \.self) { hashtag in
                                        Text("#\(hashtag)")
                                            .font(.system(size: 14))
                                            .foregroundColor(.white.opacity(0.8))
                                    }
                                }
                            }
                            
                            // Music info
                            if let music = video.music {
                                HStack(spacing: 8) {
                                    Image(systemName: "music.note")
                                        .font(.system(size: 14))
                                        .foregroundColor(.white.opacity(0.8))
                                    
                                    Text("\(music.title) - \(music.artist)")
                                        .font(.system(size: 14))
                                        .foregroundColor(.white.opacity(0.8))
                                        .lineLimit(1)
                                }
                            }
                        }
                        .frame(maxWidth: geometry.size.width * 0.7)
                        
                        Spacer()
                        
                        // Right side - Action buttons
                        VStack(spacing: 24) {
                            // Like button
                            VStack(spacing: 4) {
                                Button(action: onLike) {
                                    Image(systemName: isLiked ? "heart.fill" : "heart")
                                        .font(.system(size: 28))
                                        .foregroundColor(isLiked ? .red : .white)
                                        .scaleEffect(isLiked ? 1.2 : 1.0)
                                        .animation(.spring(response: 0.3, dampingFraction: 0.6), value: isLiked)
                                }
                                
                                Text("\(video.likeCount)")
                                    .font(.system(size: 12, weight: .medium))
                                    .foregroundColor(.white)
                            }
                            
                            // Comment button
                            VStack(spacing: 4) {
                                Button(action: onComment) {
                                    Image(systemName: "message")
                                        .font(.system(size: 28))
                                        .foregroundColor(.white)
                                }
                                
                                Text("\(video.commentCount)")
                                    .font(.system(size: 12, weight: .medium))
                                    .foregroundColor(.white)
                            }
                            
                            // Share button
                            VStack(spacing: 4) {
                                Button(action: onShare) {
                                    Image(systemName: "square.and.arrow.up")
                                        .font(.system(size: 28))
                                        .foregroundColor(.white)
                                }
                                
                                Text("\(video.shareCount)")
                                    .font(.system(size: 12, weight: .medium))
                                    .foregroundColor(.white)
                            }
                            
                            // Play/Pause button
                            Button(action: {
                                if isPlaying {
                                    player?.pause()
                                } else {
                                    player?.play()
                                }
                                isPlaying.toggle()
                            }) {
                                Image(systemName: isPlaying ? "pause.circle.fill" : "play.circle.fill")
                                    .font(.system(size: 28))
                                    .foregroundColor(.white)
                            }
                        }
                        .padding(.trailing, 20)
                    }
                    .padding(.bottom, 100) // Account for bottom tab bar
                }
            }
        }
        .onAppear {
            setupPlayer()
        }
        .onDisappear {
            player?.pause()
            player = nil
        }
    }
    
    private func setupPlayer() {
        guard let url = URL(string: video.videoUrl) else { return }
        
        player = AVPlayer(url: url)
        player?.play()
        isPlaying = true
        
        // Loop video
        NotificationCenter.default.addObserver(
            forName: .AVPlayerItemDidPlayToEndTime,
            object: player?.currentItem,
            queue: .main
        ) { _ in
            player?.seek(to: .zero)
            player?.play()
        }
    }
}

// MARK: - Loading View
struct LoadingView: View {
    var body: some View {
        VStack(spacing: 20) {
            ProgressView()
                .scaleEffect(1.5)
                .progressViewStyle(CircularProgressViewStyle(tint: .white))
            
            Text("Loading videos...")
                .font(.system(size: 16, weight: .medium))
                .foregroundColor(.white)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.black)
    }
}

// MARK: - Error View
struct ErrorView: View {
    let error: String
    let onRetry: () -> Void
    
    var body: some View {
        VStack(spacing: 20) {
            IconView(AppIcons.warning, size: 48, color: .white)
                .font(.system(size: 48))
                .foregroundColor(.white)
            
            Text("Error")
                .font(.system(size: 20, weight: .bold))
                .foregroundColor(.white)
            
            Text(error)
                .font(.system(size: 16))
                .foregroundColor(.white.opacity(0.8))
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
            
            Button("Try Again", action: onRetry)
                .foregroundColor(.black)
                .padding(.horizontal, 24)
                .padding(.vertical, 12)
                .background(
                    RoundedRectangle(cornerRadius: 20)
                        .fill(Color.white)
                )
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.black)
    }
}

// MARK: - Empty Video View
struct EmptyVideoView: View {
    var body: some View {
        VStack(spacing: 20) {
            IconView(AppIcons.video, size: 60, color: .white.opacity(0.6))
            
            Text("No videos yet")
                .font(.title2)
                .fontWeight(.bold)
                .foregroundColor(.white)
            
            Text("Videos will appear here based on your interests")
                .font(.body)
                .foregroundColor(.white.opacity(0.8))
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color.black)
    }
}

// MARK: - Preview
struct VideoFeedView_Previews: PreviewProvider {
    static var previews: some View {
        VideoFeedView(
            grpcClient: SonetGRPCClient(configuration: .development)
        )
    }
}