import Foundation
import SwiftUI

// MARK: - Story Model
struct Story: Identifiable, Codable {
    let id: String
    let userId: String
    let username: String
    let displayName: String
    let avatarUrl: String?
    let mediaItems: [StoryMediaItem]
    let createdAt: Date
    let expiresAt: Date
    let isViewed: Bool
    let viewCount: Int
    let reactions: [StoryReaction]
    let mentions: [String]
    let hashtags: [String]
    let location: StoryLocation?
    let music: StoryMusic?
    let filters: StoryFilters
    let privacy: StoryPrivacy
    
    // Computed properties
    var isExpired: Bool {
        Date() > expiresAt
    }
    
    var timeRemaining: TimeInterval {
        max(0, expiresAt.timeIntervalSince(Date()))
    }
    
    var isOwnStory: Bool {
        // This will be evaluated by comparing against the current session user where used
        // Avoid hardcoded IDs here; call sites should supply the viewer's userId
        return false
    }
    
    var canReply: Bool {
        privacy.allowsReplies && !isExpired
    }
    
    var canReact: Bool {
        !isExpired
    }
}

// MARK: - Story Media Item
struct StoryMediaItem: Identifiable, Codable {
    let id: String
    let type: StoryMediaType
    let url: String
    let thumbnailUrl: String?
    let duration: TimeInterval
    let order: Int
    let filters: StoryFilters
    let text: StoryText?
    let stickers: [StorySticker]
    let drawings: [StoryDrawing]
    
    // Computed properties
    var isVideo: Bool {
        type == .video
    }
    
    var isImage: Bool {
        type == .image
    }
    
    var isText: Bool {
        type == .text
    }
}

// MARK: - Story Media Types
enum StoryMediaType: String, Codable, CaseIterable {
    case image = "image"
    case video = "video"
    case text = "text"
    case boomerang = "boomerang"
    case collage = "collage"
    
    var displayName: String {
        switch self {
        case .image: return "Photo"
        case .video: return "Video"
        case .text: return "Text"
        case .boomerang: return "Boomerang"
        case .collage: return "Collage"
        }
    }
    
    var icon: String {
        switch self {
        case .image: return "photo"
        case .video: return "video"
        case .text: return "textformat"
        case .boomerang: return "arrow.triangle.2.circlepath"
        case .collage: return "rectangle.stack"
        }
    }
}

// MARK: - Story Text
struct StoryText: Codable {
    let content: String
    let font: StoryFont
    let color: StoryColor
    let size: CGFloat
    let position: StoryTextPosition
    let alignment: StoryTextAlignment
    let effects: [StoryTextEffect]
}

// MARK: - Story Font
enum StoryFont: String, Codable, CaseIterable {
    case system = "system"
    case rounded = "rounded"
    case serif = "serif"
    case monospace = "monospace"
    case handwritten = "handwritten"
    case bold = "bold"
    case light = "light"
    
    var fontName: String {
        switch self {
        case .system: return "SF Pro Display"
        case .rounded: return "SF Pro Rounded"
        case .serif: return "New York"
        case .monospace: return "SF Mono"
        case .handwritten: return "Snell Roundhand"
        case .bold: return "SF Pro Display Bold"
        case .light: return "SF Pro Display Light"
        }
    }
}

// MARK: - Story Color
enum StoryColor: String, Codable, CaseIterable {
    case white = "white"
    case black = "black"
    case red = "red"
    case blue = "blue"
    case green = "green"
    case yellow = "yellow"
    case purple = "purple"
    case orange = "orange"
    case pink = "pink"
    case gradient = "gradient"
    
    var color: Color {
        switch self {
        case .white: return .white
        case .black: return .black
        case .red: return .red
        case .blue: return .blue
        case .green: return .green
        case .yellow: return .yellow
        case .purple: return .purple
        case .orange: return .orange
        case .pink: return .pink
        case .gradient: return .blue
        }
    }
}

// MARK: - Story Text Position
enum StoryTextPosition: String, Codable, CaseIterable {
    case top = "top"
    case center = "center"
    case bottom = "bottom"
    case custom = "custom"
}

// MARK: - Story Text Alignment
enum StoryTextAlignment: String, Codable, CaseIterable {
    case left = "left"
    case center = "center"
    case right = "right"
}

// MARK: - Story Text Effect
enum StoryTextEffect: String, Codable, CaseIterable {
    case none = "none"
    case shadow = "shadow"
    case outline = "outline"
    case glow = "glow"
    case neon = "neon"
    case rainbow = "rainbow"
}

// MARK: - Story Sticker
struct StorySticker: Identifiable, Codable {
    let id: String
    let type: StoryStickerType
    let url: String
    let position: CGPoint
    let scale: CGFloat
    let rotation: CGFloat
    let isAnimated: Bool
}

// MARK: - Story Sticker Types
enum StoryStickerType: String, Codable, CaseIterable {
    case emoji = "emoji"
    case gif = "gif"
    case custom = "custom"
    case trending = "trending"
    case brand = "brand"
}

// MARK: - Story Drawing
struct StoryDrawing: Identifiable, Codable {
    let id: String
    let points: [CGPoint]
    let color: StoryColor
    let brushSize: CGFloat
    let opacity: CGFloat
}

// MARK: - Story Filters
struct StoryFilters: Codable {
    let brightness: Double
    let contrast: Double
    let saturation: Double
    let warmth: Double
    let sharpness: Double
    let vignette: Double
    let grain: Double
    let preset: StoryFilterPreset
    
    static let `default` = StoryFilters(
        brightness: 0.0,
        contrast: 0.0,
        saturation: 0.0,
        warmth: 0.0,
        sharpness: 0.0,
        vignette: 0.0,
        grain: 0.0,
        preset: .none
    )
}

// MARK: - Story Filter Presets
enum StoryFilterPreset: String, Codable, CaseIterable {
    case none = "none"
    case vibrant = "vibrant"
    case dramatic = "dramatic"
    case vintage = "vintage"
    case noir = "noir"
    case warm = "warm"
    case cool = "cool"
    case bright = "bright"
    case moody = "moody"
    case cinematic = "cinematic"
    
    var displayName: String {
        switch self {
        case .none: return "Original"
        case .vibrant: return "Vibrant"
        case .dramatic: return "Dramatic"
        case .vintage: return "Vintage"
        case .noir: return "Noir"
        case .warm: return "Warm"
        case .cool: return "Cool"
        case .bright: return "Bright"
        case .moody: return "Moody"
        case .cinematic: return "Cinematic"
        }
    }
    
    var previewImage: String {
        return "filter_\(rawValue)"
    }
}

// MARK: - Story Reaction
struct StoryReaction: Identifiable, Codable {
    let id: String
    let userId: String
    let username: String
    let type: StoryReactionType
    let timestamp: Date
}

// MARK: - Story Reaction Types
enum StoryReactionType: String, Codable, CaseIterable {
    case like = "like"
    case love = "love"
    case laugh = "laugh"
    case wow = "wow"
    case sad = "sad"
    case angry = "angry"
    
    var emoji: String {
        switch self {
        case .like: return "👍"
        case .love: return "❤️"
        case .laugh: return "😂"
        case .wow: return "😮"
        case .sad: return "😢"
        case .angry: return "😠"
        }
    }
    
    var displayName: String {
        switch self {
        case .like: return "Like"
        case .love: return "Love"
        case .laugh: return "Laugh"
        case .wow: return "Wow"
        case .sad: return "Sad"
        case .angry: return "Angry"
        }
    }
}

// MARK: - Story Location
struct StoryLocation: Codable {
    let name: String
    let address: String
    let latitude: Double
    let longitude: Double
    let category: String?
}

// MARK: - Story Music
struct StoryMusic: Codable {
    let title: String
    let artist: String
    let album: String?
    let duration: TimeInterval
    let url: String
    let startTime: TimeInterval
    let isLooping: Bool
}

// MARK: - Story Privacy
enum StoryPrivacy: String, Codable, CaseIterable {
    case public = "public"
    case friends = "friends"
    case closeFriends = "close_friends"
    case custom = "custom"
    
    var displayName: String {
        switch self {
        case .public: return "Everyone"
        case .friends: return "Friends"
        case .closeFriends: return "Close Friends"
        case .custom: return "Custom"
        }
    }
    
    var allowsReplies: Bool {
        switch self {
        case .public: return true
        case .friends: return true
        case .closeFriends: return true
        case .custom: return false
        }
    }
}

// MARK: - Story Creation
struct StoryCreation {
    var mediaItems: [StoryMediaItem] = []
    var text: StoryText?
    var stickers: [StorySticker] = []
    var drawings: [StoryDrawing] = []
    var filters: StoryFilters = .default
    var privacy: StoryPrivacy = .friends
    var music: StoryMusic?
    var location: StoryLocation?
    var mentions: [String] = []
    var hashtags: [String] = []
    
    var isValid: Bool {
        !mediaItems.isEmpty || text != nil
    }
    
    var estimatedDuration: TimeInterval {
        if let text = text {
            return 5.0 // Text stories show for 5 seconds
        }
        
        return mediaItems.reduce(0) { total, item in
            total + item.duration
        }
    }
}

// MARK: - Story Analytics
struct StoryAnalytics {
    let views: Int
    let uniqueViews: Int
    let replies: Int
    let reactions: Int
    let shares: Int
    let saves: Int
    let completionRate: Double
    let averageViewTime: TimeInterval
    let topReactions: [StoryReactionType: Int]
    let viewerDemographics: StoryViewerDemographics
}

// MARK: - Story Viewer Demographics
struct StoryViewerDemographics: Codable {
    let ageGroups: [String: Int]
    let genders: [String: Int]
    let locations: [String: Int]
    let devices: [String: Int]
    let timeOfDay: [String: Int]
}

// MARK: - Story Reply
struct StoryReply: Identifiable, Codable {
    let id: String
    let storyId: String
    let userId: String
    let username: String
    let displayName: String
    let avatarUrl: String?
    let content: String
    let mediaUrl: String?
    let timestamp: Date
    let isPrivate: Bool
}

// MARK: - Story Collection
struct StoryCollection: Identifiable, Codable {
    let id: String
    let name: String
    let description: String?
    let coverImageUrl: String?
    let stories: [Story]
    let createdAt: Date
    let isPublic: Bool
    let ownerId: String
    let collaboratorIds: [String]
    let tags: [String]
    
    var storyCount: Int {
        stories.count
    }
    
    var totalDuration: TimeInterval {
        stories.reduce(0) { total, story in
            total + story.mediaItems.reduce(0) { itemTotal, item in
                itemTotal + item.duration
            }
        }
    }
}