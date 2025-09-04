import Foundation
import Combine
import SwiftUI

@MainActor
class SettingsViewModel: ObservableObject {
    
    // MARK: - Published Properties
    @Published var userProfile: UserProfile?
    @Published var isDarkMode: Bool = false
    @Published var notificationsEnabled: Bool = true
    @Published var pushNotificationsEnabled: Bool = true
    @Published var emailNotificationsEnabled: Bool = true
    @Published var inAppNotificationsEnabled: Bool = true
    @Published var accountVisibility: AccountVisibility = .public
    @Published var contentLanguage: String = "English"
    @Published var contentFiltering: ContentFiltering = .moderate
    @Published var autoPlayVideos: Bool = true
    @Published var dataUsage: DataUsage = .standard
    @Published var storageUsage: StorageUsage = StorageUsage()
    @Published var isLoading = false
    @Published var error: String?
    @Published var showLogoutAlert = false
    @Published var showDeleteAccountAlert = false
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private var cancellables = Set<AnyCancellable>()
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient) {
        self.grpcClient = grpcClient
        loadUserProfile()
        loadSettings()
    }
    
    // MARK: - Public Methods
    func loadUserProfile() {
        Task {
            isLoading = true
            error = nil
            
            do {
                let response = try await grpcClient.getUserProfile(userId: "current_user")
                await MainActor.run {
                    self.userProfile = UserProfile(from: response)
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
    
    func loadSettings() {
        // Load from UserDefaults
        isDarkMode = UserDefaults.standard.bool(forKey: "isDarkMode")
        notificationsEnabled = UserDefaults.standard.bool(forKey: "notificationsEnabled")
        pushNotificationsEnabled = UserDefaults.standard.bool(forKey: "pushNotificationsEnabled")
        emailNotificationsEnabled = UserDefaults.standard.bool(forKey: "emailNotificationsEnabled")
        inAppNotificationsEnabled = UserDefaults.standard.bool(forKey: "inAppNotificationsEnabled")
        
        if let visibilityString = UserDefaults.standard.string(forKey: "accountVisibility"),
           let visibility = AccountVisibility(rawValue: visibilityString) {
            accountVisibility = visibility
        }
        
        if let language = UserDefaults.standard.string(forKey: "contentLanguage") {
            contentLanguage = language
        }
        
        if let filteringString = UserDefaults.standard.string(forKey: "contentFiltering"),
           let filtering = ContentFiltering(rawValue: filteringString) {
            contentFiltering = filtering
        }
        
        autoPlayVideos = UserDefaults.standard.bool(forKey: "autoPlayVideos")
        
        if let dataUsageString = UserDefaults.standard.string(forKey: "dataUsage"),
           let usage = DataUsage(rawValue: dataUsageString) {
            dataUsage = usage
        }
        
        loadStorageUsage()
    }
    
    func toggleDarkMode() {
        isDarkMode.toggle()
        UserDefaults.standard.set(isDarkMode, forKey: "isDarkMode")
        
        // Update app appearance
        if let windowScene = UIApplication.shared.connectedScenes.first as? UIWindowScene {
            windowScene.windows.forEach { window in
                window.overrideUserInterfaceStyle = isDarkMode ? .dark : .light
            }
        }
    }
    
    func updateNotificationSettings() {
        UserDefaults.standard.set(notificationsEnabled, forKey: "notificationsEnabled")
        UserDefaults.standard.set(pushNotificationsEnabled, forKey: "pushNotificationsEnabled")
        UserDefaults.standard.set(emailNotificationsEnabled, forKey: "emailNotificationsEnabled")
        UserDefaults.standard.set(inAppNotificationsEnabled, forKey: "inAppNotificationsEnabled")
        
        // Update server settings
        Task {
            do {
                var request = UpdateNotificationPreferencesRequest()
                request.pushEnabled = pushNotificationsEnabled
                request.emailEnabled = emailNotificationsEnabled
                request.inAppEnabled = inAppNotificationsEnabled
                
                let response = try await grpcClient.updateNotificationPreferences(request: request)
                if !response.success {
                    await MainActor.run {
                        self.error = "Failed to update notification settings: \(response.errorMessage)"
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to update notification settings: \(error.localizedDescription)"
                }
            }
        }
    }
    
    func updateAccountVisibility(_ visibility: AccountVisibility) {
        accountVisibility = visibility
        UserDefaults.standard.set(visibility.rawValue, forKey: "accountVisibility")
        
        Task {
            do {
                var request = UpdatePrivacySettingsRequest()
                request.accountVisibility = visibility.grpcType
                
                let response = try await grpcClient.updatePrivacySettings(request: request)
                if !response.success {
                    await MainActor.run {
                        self.error = "Failed to update privacy settings: \(response.errorMessage)"
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to update privacy settings: \(error.localizedDescription)"
                }
            }
        }
    }
    
    func updateContentLanguage(_ language: String) {
        contentLanguage = language
        UserDefaults.standard.set(language, forKey: "contentLanguage")
        
        // Update app localization
        // This would typically involve restarting the app or updating the bundle
    }
    
    func updateContentFiltering(_ filtering: ContentFiltering) {
        contentFiltering = filtering
        UserDefaults.standard.set(filtering.rawValue, forKey: "contentFiltering")
        
        Task {
            do {
                var request = UpdateContentPreferencesRequest()
                request.contentFiltering = filtering.grpcType
                
                let response = try await grpcClient.updateContentPreferences(request: request)
                if !response.success {
                    await MainActor.run {
                        self.error = "Failed to update content preferences: \(response.errorMessage)"
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to update content preferences: \(error.localizedDescription)"
                }
            }
        }
    }
    
    func toggleAutoPlayVideos() {
        autoPlayVideos.toggle()
        UserDefaults.standard.set(autoPlayVideos, forKey: "autoPlayVideos")
    }
    
    func updateDataUsage(_ usage: DataUsage) {
        dataUsage = usage
        UserDefaults.standard.set(usage.rawValue, forKey: "dataUsage")
    }
    
    func loadStorageUsage() {
        Task {
            do {
                let response = try await grpcClient.getStorageUsage()
                await MainActor.run {
                    self.storageUsage = StorageUsage(from: response)
                }
            } catch {
                // Handle error silently
            }
        }
    }
    
    func clearCache() {
        Task {
            do {
                let response = try await grpcClient.clearCache()
                if response.success {
                    await loadStorageUsage()
                } else {
                    await MainActor.run {
                        self.error = "Failed to clear cache: \(response.errorMessage)"
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to clear cache: \(error.localizedDescription)"
                }
            }
        }
    }
    
    func downloadUserData() {
        Task {
            do {
                let response = try await grpcClient.exportUserData()
                if response.success {
                    // Handle data download
                    // This would typically save to Files app
                } else {
                    await MainActor.run {
                        self.error = "Failed to export data: \(response.errorMessage)"
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to export data: \(error.localizedDescription)"
                }
            }
        }
    }
    
    func deleteAccount() {
        Task {
            do {
                let response = try await grpcClient.deleteAccount()
                if response.success {
                    // Handle account deletion
                    // This would typically log out and return to login
                } else {
                    await MainActor.run {
                        self.error = "Failed to delete account: \(response.errorMessage)"
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to delete account: \(error.localizedDescription)"
                }
            }
        }
    }
    
    func logout() {
        // Clear local data
        UserDefaults.standard.removeObject(forKey: "authToken")
        UserDefaults.standard.removeObject(forKey: "userId")
        
        // Clear keychain
        // This would typically clear stored credentials
        
        // Navigate to login
        // This would be handled by the parent view
    }
    
    func clearError() {
        error = nil
    }
}

// MARK: - Data Models
enum AccountVisibility: String, CaseIterable {
    case `public` = "public"
    case followers = "followers"
    case private = "private"
    
    var displayName: String {
        switch self {
        case .public: return "Public"
        case .followers: return "Followers Only"
        case .private: return "Private"
        }
    }
    
    var description: String {
        switch self {
        case .public: return "Anyone can see your profile and posts"
        case .followers: return "Only your followers can see your posts"
        case .private: return "Only you can see your posts"
        }
    }
    
    var grpcType: xyz.sonet.app.grpc.proto.AccountVisibility {
        switch self {
        case .public: return .ACCOUNT_VISIBILITY_PUBLIC
        case .followers: return .ACCOUNT_VISIBILITY_FOLLOWERS
        case .private: return .ACCOUNT_VISIBILITY_PRIVATE
        }
    }
}

enum ContentFiltering: String, CaseIterable {
    case off = "off"
    case moderate = "moderate"
    case strict = "strict"
    
    var displayName: String {
        switch self {
        case .off: return "Off"
        case .moderate: return "Moderate"
        case .strict: return "Strict"
        }
    }
    
    var description: String {
        switch self {
        case .off: return "Show all content"
        case .moderate: return "Filter some sensitive content"
        case .strict: return "Filter most sensitive content"
        }
    }
    
    var grpcType: xyz.sonet.app.grpc.proto.ContentFiltering {
        switch self {
        case .off: return .CONTENT_FILTERING_OFF
        case .moderate: return .CONTENT_FILTERING_MODERATE
        case .strict: return .CONTENT_FILTERING_STRICT
        }
    }
}

enum DataUsage: String, CaseIterable {
    case low = "low"
    case standard = "standard"
    case high = "high"
    
    var displayName: String {
        switch self {
        case .low: return "Low"
        case .standard: return "Standard"
        case .high: return "High"
        }
    }
    
    var description: String {
        switch self {
        case .low: return "Minimize data usage"
        case .standard: return "Balanced data usage"
        case .high: return "Optimize for quality"
        }
    }
}

struct StorageUsage {
    let totalStorage: Int64
    let usedStorage: Int64
    let cacheSize: Int64
    let mediaSize: Int64
    let appSize: Int64
    
    var availableStorage: Int64 { totalStorage - usedStorage }
    var usagePercentage: Double { Double(usedStorage) / Double(totalStorage) * 100 }
    
    init() {
        self.totalStorage = 0
        self.usedStorage = 0
        self.cacheSize = 0
        self.mediaSize = 0
        self.appSize = 0
    }
    
    init(from grpcStorage: xyz.sonet.app.grpc.proto.StorageUsage) {
        self.totalStorage = grpcStorage.totalStorage
        self.usedStorage = grpcStorage.usedStorage
        self.cacheSize = grpcStorage.cacheSize
        self.mediaSize = grpcStorage.mediaSize
        self.appSize = grpcStorage.appSize
    }
}

struct NotificationPreferences {
    let pushEnabled: Bool
    let emailEnabled: Bool
    let inAppEnabled: Bool
    let mentionsEnabled: Bool
    let likesEnabled: Bool
    let repostsEnabled: Bool
    let followsEnabled: Bool
    let directMessagesEnabled: Bool
    
    init() {
        self.pushEnabled = true
        self.emailEnabled = true
        self.inAppEnabled = true
        self.mentionsEnabled = true
        self.likesEnabled = true
        self.repostsEnabled = true
        self.followsEnabled = true
        self.directMessagesEnabled = true
    }
}

struct PrivacySettings {
    let accountVisibility: AccountVisibility
    let showEmail: Bool
    let showPhone: Bool
    let allowMentions: Bool
    let allowDirectMessages: Bool
    let allowFollowRequests: Bool
    let showOnlineStatus: Bool
    let showLastSeen: Bool
    
    init() {
        self.accountVisibility = .public
        self.showEmail = false
        self.showPhone = false
        self.allowMentions = true
        self.allowDirectMessages = true
        self.allowFollowRequests = true
        self.showOnlineStatus = true
        self.showLastSeen = true
    }
}

struct ContentPreferences {
    let contentLanguage: String
    let contentFiltering: ContentFiltering
    let autoPlayVideos: Bool
    let showSensitiveContent: Bool
    let showTrendingTopics: Bool
    let showSuggestedAccounts: Bool
    
    init() {
        self.contentLanguage = "English"
        self.contentFiltering = .moderate
        self.autoPlayVideos = true
        self.showSensitiveContent = false
        self.showTrendingTopics = true
        self.showSuggestedAccounts = true
    }
}