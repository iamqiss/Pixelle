import SwiftUI

struct StoriesView: View {
    @StateObject private var viewModel = StoriesViewModel()
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                // Stories Header
                StoriesHeader(
                    onCreateStory: { viewModel.showCreateStory = true },
                    onViewMyStories: { viewModel.showMyStories = true }
                )
                
                // Stories Content
                if viewModel.isLoading {
                    StoriesLoadingView()
                } else if viewModel.stories.isEmpty {
                    StoriesEmptyView(
                        onCreateStory: { viewModel.showCreateStory = true }
                    )
                } else {
                    StoriesContent(
                        stories: viewModel.stories,
                        onStoryTap: { story in
                            viewModel.selectedStory = story
                            viewModel.showStoryViewer = true
                        }
                    )
                }
                
                Spacer()
            }
            .navigationTitle("Stories")
            .navigationBarTitleDisplayMode(.large)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                }
            }
        }
        .onAppear {
            viewModel.loadStories()
        }
        .sheet(isPresented: $viewModel.showCreateStory) {
            CreateStoryView()
        }
        .sheet(isPresented: $viewModel.showMyStories) {
            MyStoriesView()
        }
        .fullScreenCover(isPresented: $viewModel.showStoryViewer) {
            if let story = viewModel.selectedStory {
                StoryViewer(story: story)
            }
        }
        .refreshable {
            await viewModel.refreshStories()
        }
    }
}

// MARK: - Stories Header
struct StoriesHeader: View {
    let onCreateStory: () -> Void
    let onViewMyStories: () -> Void
    
    var body: some View {
        VStack(spacing: 16) {
            // My Story Section
            HStack(spacing: 16) {
                Button(action: onCreateStory) {
                    VStack(spacing: 8) {
                        ZStack {
                            Circle()
                                .fill(Color.blue.opacity(0.1))
                                .frame(width: 60, height: 60)
                            
                            Image(systemName: "plus")
                                .font(.title2)
                                .foregroundColor(.blue)
                        }
                        
                        Text("Add Story")
                            .font(.caption)
                            .foregroundColor(.blue)
                    }
                }
                
                Button(action: onViewMyStories) {
                    VStack(spacing: 8) {
                        ZStack {
                            Circle()
                                .fill(Color.gray.opacity(0.1))
                                .frame(width: 60, height: 60)
                            
                            Image(systemName: "person.circle")
                                .font(.title2)
                                .foregroundColor(.gray)
                        }
                        
                        Text("My Stories")
                            .font(.caption)
                            .foregroundColor(.gray)
                    }
                }
                
                Spacer()
            }
            .padding(.horizontal)
            
            Divider()
        }
    }
}

// MARK: - Stories Content
struct StoriesContent: View {
    let stories: [Story]
    let onStoryTap: (Story) -> Void
    
    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            LazyHStack(spacing: 16) {
                ForEach(stories) { story in
                    StoryPreviewCard(
                        story: story,
                        onTap: { onStoryTap(story) }
                    )
                }
            }
            .padding(.horizontal)
        }
    }
}

// MARK: - Story Preview Card
struct StoryPreviewCard: View {
    let story: Story
    let onTap: () -> Void
    
    var body: some View {
        Button(action: onTap) {
            VStack(spacing: 8) {
                // Story Avatar
                ZStack {
                    if let avatarUrl = story.avatarUrl {
                        AsyncImage(url: URL(string: avatarUrl)) { image in
                            image
                                .resizable()
                                .aspectRatio(contentMode: .fill)
                        } placeholder: {
                            Circle()
                                .fill(Color.gray.opacity(0.3))
                        }
                        .frame(width: 60, height: 60)
                        .clipShape(Circle())
                    } else {
                        Circle()
                            .fill(Color.gray.opacity(0.3))
                            .frame(width: 60, height: 60)
                            .overlay(
                                Image(systemName: "person.fill")
                                    .foregroundColor(.gray)
                            )
                    }
                    
                    // Story Ring
                    Circle()
                        .stroke(
                            story.isViewed ? Color.gray : Color.blue,
                            lineWidth: 2
                        )
                        .frame(width: 68, height: 68)
                }
                
                // Username
                Text(story.displayName.isEmpty ? story.username : story.displayName)
                    .font(.caption)
                    .foregroundColor(.primary)
                    .lineLimit(1)
                    .frame(width: 60)
            }
        }
        .buttonStyle(PlainButtonStyle())
    }
}

// MARK: - Stories Loading View
struct StoriesLoadingView: View {
    var body: some View {
        VStack(spacing: 16) {
            ProgressView()
                .scaleEffect(1.5)
            
            Text("Loading stories...")
                .font(.body)
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Stories Empty View
struct StoriesEmptyView: View {
    let onCreateStory: () -> Void
    
    var body: some View {
        VStack(spacing: 24) {
            Image(systemName: "camera.circle")
                .font(.system(size: 80))
                .foregroundColor(.gray)
            
            VStack(spacing: 8) {
                Text("No Stories Yet")
                    .font(.title2)
                    .fontWeight(.semibold)
                
                Text("Be the first to share a story with your friends!")
                    .font(.body)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
            }
            
            Button("Create Your First Story") {
                onCreateStory()
            }
            .buttonStyle(.borderedProminent)
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Stories View Model
@MainActor
class StoriesViewModel: ObservableObject {
    @Published var stories: [Story] = []
    @Published var isLoading = false
    @Published var showCreateStory = false
    @Published var showMyStories = false
    @Published var showStoryViewer = false
    @Published var selectedStory: Story?
    
    private let grpcClient: SonetGRPCClient
    
    init(grpcClient: SonetGRPCClient = SonetGRPCClient(configuration: .development)) {
        self.grpcClient = grpcClient
    }
    
    func loadStories() {
        Task {
            isLoading = true
            
            do {
                let request = GetStoriesRequest.newBuilder()
                    .setUserId("current_user")
                    .setLimit(50)
                    .build()
                
                let response = try await grpcClient.getStories(request: request)
                
                if response.success {
                    await MainActor.run {
                        self.stories = response.stories.map { Story(from: $0) }
                    }
                }
            } catch {
                print("Failed to load stories: \(error)")
            }
            
            await MainActor.run {
                self.isLoading = false
            }
        }
    }
    
    func refreshStories() async {
        await MainActor.run {
            isLoading = true
        }
        
        do {
            let request = GetStoriesRequest.newBuilder()
                .setUserId("current_user")
                .setLimit(50)
                .build()
            
            let response = try await grpcClient.getStories(request: request)
            
            if response.success {
                await MainActor.run {
                    self.stories = response.stories.map { Story(from: $0) }
                }
            }
        } catch {
            print("Failed to refresh stories: \(error)")
        }
        
        await MainActor.run {
            self.isLoading = false
        }
    }
}

// MARK: - Story Extension
extension Story {
    init(from grpcStory: GRPCStory) {
        self.id = grpcStory.storyId
        self.userId = grpcStory.userId
        self.username = grpcStory.username
        self.displayName = grpcStory.displayName
        self.avatarUrl = grpcStory.avatarUrl
        self.mediaItems = grpcStory.mediaItems.map { StoryMediaItem(from: $0) }
        self.createdAt = grpcStory.createdAt.date
        self.expiresAt = grpcStory.expiresAt.date
        self.isViewed = grpcStory.isViewed
        self.viewCount = Int(grpcStory.viewCount)
        self.reactions = grpcStory.reactions.map { StoryReaction(from: $0) }
        self.mentions = grpcStory.mentions
        self.hashtags = grpcStory.hashtags
        self.location = grpcStory.location.map { StoryLocation(from: $0) }
        self.music = grpcStory.music.map { StoryMusic(from: $0) }
        self.filters = StoryFilters(from: grpcStory.filters)
        self.privacy = StoryPrivacy(rawValue: grpcStory.privacy) ?? .friends
    }
}

// MARK: - Story Media Item Extension
extension StoryMediaItem {
    init(from grpcMediaItem: GRPCStoryMediaItem) {
        self.id = grpcMediaItem.mediaId
        self.type = StoryMediaType(rawValue: grpcMediaItem.type) ?? .image
        self.url = grpcMediaItem.url
        self.thumbnailUrl = grpcMediaItem.thumbnailUrl
        self.duration = grpcMediaItem.duration
        self.order = Int(grpcMediaItem.order)
        self.filters = StoryFilters(from: grpcMediaItem.filters)
        self.text = grpcMediaItem.text.map { StoryText(from: $0) }
        self.stickers = grpcMediaItem.stickers.map { StorySticker(from: $0) }
        self.drawings = grpcMediaItem.drawings.map { StoryDrawing(from: $0) }
    }
}

// MARK: - Story Filters Extension
extension StoryFilters {
    init(from grpcFilters: GRPCStoryFilters) {
        self.brightness = grpcFilters.brightness
        self.contrast = grpcFilters.contrast
        self.saturation = grpcFilters.saturation
        self.warmth = grpcFilters.warmth
        self.sharpness = grpcFilters.sharpness
        self.vignette = grpcFilters.vignette
        self.grain = grpcFilters.grain
        self.preset = StoryFilterPreset(rawValue: grpcFilters.preset) ?? .none
    }
}

// MARK: - Story Text Extension
extension StoryText {
    init(from grpcText: GRPCStoryText) {
        self.content = grpcText.content
        self.font = StoryFont(rawValue: grpcText.font) ?? .system
        self.color = StoryColor(rawValue: grpcText.color) ?? .white
        self.size = grpcText.size
        self.position = StoryTextPosition(rawValue: grpcText.position) ?? .center
        self.alignment = StoryTextAlignment(rawValue: grpcText.alignment) ?? .center
        self.effects = grpcText.effects.map { StoryTextEffect(rawValue: $0) ?? .none }
    }
}

// MARK: - Story Sticker Extension
extension StorySticker {
    init(from grpcSticker: GRPCStorySticker) {
        self.id = grpcSticker.stickerId
        self.type = StoryStickerType(rawValue: grpcSticker.type) ?? .emoji
        self.url = grpcSticker.url
        self.position = CGPoint(x: grpcSticker.positionX, y: grpcSticker.positionY)
        self.scale = grpcSticker.scale
        self.rotation = grpcSticker.rotation
        self.isAnimated = grpcSticker.isAnimated
    }
}

// MARK: - Story Drawing Extension
extension StoryDrawing {
    init(from grpcDrawing: GRPCStoryDrawing) {
        self.id = grpcDrawing.drawingId
        self.points = grpcDrawing.points.map { CGPoint(x: $0.x, y: $0.y) }
        self.color = StoryColor(rawValue: grpcDrawing.color) ?? .black
        self.brushSize = grpcDrawing.brushSize
        self.opacity = grpcDrawing.opacity
    }
}

// MARK: - Story Reaction Extension
extension StoryReaction {
    init(from grpcReaction: GRPCStoryReaction) {
        self.id = grpcReaction.reactionId
        self.userId = grpcReaction.userId
        self.username = grpcReaction.username
        self.type = StoryReactionType(rawValue: grpcReaction.type) ?? .like
        self.timestamp = grpcReaction.timestamp.date
    }
}

// MARK: - Story Location Extension
extension StoryLocation {
    init(from grpcLocation: GRPCStoryLocation) {
        self.name = grpcLocation.name
        self.address = grpcLocation.address
        self.latitude = grpcLocation.latitude
        self.longitude = grpcLocation.longitude
        self.category = grpcLocation.category
    }
}

// MARK: - Story Music Extension
extension StoryMusic {
    init(from grpcMusic: GRPCStoryMusic) {
        self.title = grpcMusic.title
        self.artist = grpcMusic.artist
        self.album = grpcMusic.album
        self.duration = grpcMusic.duration
        self.url = grpcMusic.url
        self.startTime = grpcMusic.startTime
        self.isLooping = grpcMusic.isLooping
    }
}

// MARK: - gRPC Extensions (Placeholder - replace with actual gRPC types)
extension GetStoriesRequest {
    static func newBuilder() -> GetStoriesRequestBuilder {
        return GetStoriesRequestBuilder()
    }
}

// MARK: - Request Builders (Placeholder - replace with actual gRPC types)
class GetStoriesRequestBuilder {
    private var userId: String = ""
    private var limit: Int32 = 50
    
    func setUserId(_ userId: String) -> GetStoriesRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setLimit(_ limit: Int32) -> GetStoriesRequestBuilder {
        self.limit = limit
        return self
    }
    
    func build() -> GetStoriesRequest {
        return GetStoriesRequest(userId: userId, limit: limit)
    }
}

// MARK: - Request/Response Models (Placeholder - replace with actual gRPC types)
struct GetStoriesRequest {
    let userId: String
    let limit: Int32
}

struct GetStoriesResponse {
    let success: Bool
    let stories: [GRPCStory]
    let errorMessage: String
}

// MARK: - gRPC Models (Placeholder - replace with actual gRPC types)
struct GRPCStory {
    let storyId: String
    let userId: String
    let username: String
    let displayName: String
    let avatarUrl: String
    let mediaItems: [GRPCStoryMediaItem]
    let createdAt: GRPCTimestamp
    let expiresAt: GRPCTimestamp
    let isViewed: Bool
    let viewCount: UInt64
    let reactions: [GRPCStoryReaction]
    let mentions: [String]
    let hashtags: [String]
    let location: GRPCStoryLocation?
    let music: GRPCStoryMusic?
    let filters: GRPCStoryFilters
    let privacy: String
}

struct GRPCStoryMediaItem {
    let mediaId: String
    let type: String
    let url: String
    let thumbnailUrl: String
    let duration: TimeInterval
    let order: UInt32
    let filters: GRPCStoryFilters
    let text: GRPCStoryText?
    let stickers: [GRPCStorySticker]
    let drawings: [GRPCStoryDrawing]
}

struct GRPCStoryFilters {
    let brightness: Double
    let contrast: Double
    let saturation: Double
    let warmth: Double
    let sharpness: Double
    let vignette: Double
    let grain: Double
    let preset: String
}

struct GRPCStoryText {
    let content: String
    let font: String
    let color: String
    let size: CGFloat
    let position: String
    let alignment: String
    let effects: [String]
}

struct GRPCStorySticker {
    let stickerId: String
    let type: String
    let url: String
    let positionX: Double
    let positionY: Double
    let scale: CGFloat
    let rotation: CGFloat
    let isAnimated: Bool
}

struct GRPCStoryDrawing {
    let drawingId: String
    let points: [GRPCPoint]
    let color: String
    let brushSize: CGFloat
    let opacity: CGFloat
}

struct GRPCStoryReaction {
    let reactionId: String
    let userId: String
    let username: String
    let type: String
    let timestamp: GRPCTimestamp
}

struct GRPCStoryLocation {
    let name: String
    let address: String
    let latitude: Double
    let longitude: Double
    let category: String
}

struct GRPCStoryMusic {
    let title: String
    let artist: String
    let album: String
    let duration: TimeInterval
    let url: String
    let startTime: TimeInterval
    let isLooping: Bool
}

struct GRPCPoint {
    let x: Double
    let y: Double
}

struct GRPCTimestamp {
    let date: Date
}