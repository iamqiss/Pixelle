import Foundation
import Combine
import SwiftUI

@MainActor
class ProfileViewModel: ObservableObject {
    
    // MARK: - Published Properties
    @Published var userProfile: UserProfile?
    @Published var selectedTab: ProfileTab = .posts
    @Published var posts: [Note] = []
    @Published var replies: [Note] = []
    @Published var media: [Note] = []
    @Published var likes: [Note] = []
    @Published var isFollowing = false
    @Published var isBlocked = false
    @Published var isLoading = false
    @Published var error: String?
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private let userId: String
    private var cancellables = Set<AnyCancellable>()
    
    // MARK: - Initialization
    init(userId: String, grpcClient: SonetGRPCClient) {
        self.userId = userId
        self.grpcClient = grpcClient
        loadUserProfile()
        loadProfileContent()
    }
    
    // MARK: - Profile Tabs
    enum ProfileTab: String, CaseIterable {
        case posts = "Posts"
        case replies = "Replies"
        case media = "Media"
        case likes = "Likes"
        
        var icon: String {
            switch self {
            case .posts: return "bubble.left"
            case .replies: return "arrowshape.turn.up.left"
            case .media: return "photo"
            case .likes: return "heart"
            }
        }
    }
    
    // MARK: - Public Methods
    func selectTab(_ tab: ProfileTab) {
        selectedTab = tab
        loadContentForSelectedTab()
    }
    
    func toggleFollow() {
        guard let userProfile = userProfile else { return }
        
        Task {
            do {
                if isFollowing {
                    // Unfollow user
                    let response = try await grpcClient.unfollowUser(userId: userProfile.userId)
                    if response.success {
                        await MainActor.run {
                            self.isFollowing = false
                            self.userProfile?.followerCount -= 1
                        }
                    }
                } else {
                    // Follow user
                    let response = try await grpcClient.followUser(userId: userProfile.userId)
                    if response.success {
                        await MainActor.run {
                            self.isFollowing = true
                            self.userProfile?.followerCount += 1
                        }
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to \(isFollowing ? "unfollow" : "follow") user"
                }
            }
        }
    }
    
    func blockUser() {
        guard let userProfile = userProfile else { return }
        
        Task {
            do {
                let response = try await grpcClient.blockUser(userId: userProfile.userId)
                if response.success {
                    await MainActor.run {
                        self.isBlocked = true
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to block user"
                }
            }
        }
    }
    
    func unblockUser() {
        guard let userProfile = userProfile else { return }
        
        Task {
            do {
                let response = try await grpcClient.unblockUser(userId: userProfile.userId)
                if response.success {
                    await MainActor.run {
                        self.isBlocked = false
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to unblock user"
                }
            }
        }
    }
    
    func refreshProfile() {
        loadUserProfile()
        loadProfileContent()
    }
    
    // MARK: - Private Methods
    private func loadUserProfile() {
        Task {
            isLoading = true
            error = nil
            
            do {
                let profile = try await grpcClient.getUserProfile(userId: userId)
                await MainActor.run {
                    self.userProfile = profile
                    self.isFollowing = profile.isFollowing
                    self.isBlocked = profile.isBlocked
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to load profile: \(error.localizedDescription)"
                }
            }
            
            await MainActor.run {
                self.isLoading = false
            }
        }
    }
    
    private func loadProfileContent() {
        loadContentForSelectedTab()
    }
    
    private func loadContentForSelectedTab() {
        Task {
            do {
                switch selectedTab {
                case .posts:
                    let notes = try await grpcClient.getUserPosts(userId: userId, page: 0, pageSize: 20)
                    await MainActor.run {
                        self.posts = notes
                    }
                    
                case .replies:
                    let notes = try await grpcClient.getUserReplies(userId: userId, page: 0, pageSize: 20)
                    await MainActor.run {
                        self.replies = notes
                    }
                    
                case .media:
                    let notes = try await grpcClient.getUserMedia(userId: userId, page: 0, pageSize: 20)
                    await MainActor.run {
                        self.media = notes
                    }
                    
                case .likes:
                    let notes = try await grpcClient.getUserLikes(userId: userId, page: 0, pageSize: 20)
                    await MainActor.run {
                        self.likes = notes
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to load \(selectedTab.rawValue.lowercased()): \(error.localizedDescription)"
                }
            }
        }
    }
}

// MARK: - Profile Actions
struct ProfileActions {
    let canFollow: Bool
    let canMessage: Bool
    let canBlock: Bool
    let canReport: Bool
    let isOwnProfile: Bool
}

// MARK: - Profile Stats
struct ProfileStats {
    let posts: Int
    let followers: Int
    let following: Int
    let likes: Int
    let joinedDate: Date
    let location: String?
    let website: String?
}