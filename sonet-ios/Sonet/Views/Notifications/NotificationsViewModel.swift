import Foundation
import Combine
import SwiftUI
import UserNotifications

@MainActor
class NotificationsViewModel: ObservableObject {
    
    // MARK: - Published Properties
    @Published var notifications: [NotificationItem] = []
    @Published var filteredNotifications: [NotificationItem] = []
    @Published var selectedFilter: NotificationFilter = .all
    @Published var isLoading = false
    @Published var error: String?
    @Published var unreadCount = 0
    @Published var showAppUpdate = false
    @Published var appUpdateInfo: AppUpdateInfo?
    @Published var notificationPreferences: NotificationPreferences = NotificationPreferences()
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private var cancellables = Set<AnyCancellable>()
    private var notificationStream: AnyCancellable?
    private var refreshTimer: Timer?
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient) {
        self.grpcClient = grpcClient
        setupNotificationStream()
        setupRefreshTimer()
        loadNotificationPreferences()
        requestNotificationPermissions()
    }
    
    // MARK: - Notification Filters
    enum NotificationFilter: String, CaseIterable {
        case all = "All"
        case mentions = "Mentions"
        case likes = "Likes"
        case reposts = "Reposts"
        case follows = "Follows"
        case replies = "Replies"
        case hashtags = "Hashtags"
        case appUpdates = "App Updates"
        
        var icon: String {
            switch self {
            case .all: return "bell"
            case .mentions: return "at"
            case .likes: return "heart"
            case .reposts: return "arrow.2.squarepath"
            case .follows: return "person.badge.plus"
            case .replies: return "bubble.left"
            case .hashtags: return "number"
            case .appUpdates: return "app.badge"
            }
        }
        
        var color: Color { .primary }
    }
    
    // MARK: - Public Methods
    func loadNotifications() {
        Task {
            isLoading = true
            error = nil
            
            do {
                let response = try await grpcClient.getNotifications(page: 0, pageSize: 50)
                await MainActor.run {
                    self.notifications = response.notifications.map { NotificationItem(from: $0) }
                    self.applyFilter()
                    self.updateUnreadCount()
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to load notifications: \(error.localizedDescription)"
                }
            }
            
            await MainActor.run {
                self.isLoading = false
            }
        }
    }
    
    func refreshNotifications() {
        loadNotifications()
    }
    
    func selectFilter(_ filter: NotificationFilter) {
        selectedFilter = filter
        applyFilter()
    }
    
    func markAsRead(_ notification: NotificationItem) {
        guard !notification.isRead else { return }
        
        Task {
            do {
                let response = try await grpcClient.markNotificationAsRead(notificationId: notification.id)
                if response.success {
                    await MainActor.run {
                        if let index = self.notifications.firstIndex(where: { $0.id == notification.id }) {
                            self.notifications[index].isRead = true
                        }
                        self.updateUnreadCount()
                    }
                }
            } catch {
                // Handle error silently
            }
        }
    }
    
    func markAllAsRead() {
        Task {
            do {
                let response = try await grpcClient.markAllNotificationsAsRead()
                if response.success {
                    await MainActor.run {
                        for index in self.notifications.indices {
                            self.notifications[index].isRead = true
                        }
                        self.updateUnreadCount()
                    }
                }
            } catch {
                // Handle error silently
            }
        }
    }
    
    func deleteNotification(_ notification: NotificationItem) {
        Task {
            do {
                let response = try await grpcClient.deleteNotification(notificationId: notification.id)
                if response.success {
                    await MainActor.run {
                        self.notifications.removeAll { $0.id == notification.id }
                        self.applyFilter()
                        self.updateUnreadCount()
                    }
                }
            } catch {
                // Handle error silently
            }
        }
    }
    
    func clearAllNotifications() {
        Task {
            do {
                let response = try await grpcClient.clearAllNotifications()
                if response.success {
                    await MainActor.run {
                        self.notifications.removeAll()
                        self.applyFilter()
                        self.updateUnreadCount()
                    }
                }
            } catch {
                // Handle error silently
            }
        }
    }
    
    func checkForAppUpdates() {
        Task {
            do {
                let response = try await grpcClient.checkForAppUpdates()
                if response.hasUpdate {
                    await MainActor.run {
                        self.appUpdateInfo = AppUpdateInfo(from: response)
                        self.showAppUpdate = true
                    }
                }
            } catch {
                // Handle error silently
            }
        }
    }
    
    func updateNotificationPreferences() {
        Task {
            do {
                let response = try await grpcClient.updateNotificationPreferences(preferences: notificationPreferences.toGRPC())
                if response.success {
                    // Preferences updated successfully
                }
            } catch {
                // Handle error silently
            }
        }
    }
    
    // MARK: - Private Methods
    private func setupNotificationStream() {
        // Set up real-time notification stream
        notificationStream = grpcClient.notificationStream()
            .receive(on: DispatchQueue.main)
            .sink(
                receiveCompletion: { completion in
                    switch completion {
                    case .finished:
                        break
                    case .failure(let error):
                        self.error = "Notification stream error: \(error.localizedDescription)"
                    }
                },
                receiveValue: { notification in
                    self.handleNewNotification(notification)
                }
            )
    }
    
    private func setupRefreshTimer() {
        refreshTimer = Timer.scheduledTimer(withTimeInterval: 30.0, repeats: true) { _ in
            self.refreshNotifications()
        }
    }
    
    private func handleNewNotification(_ notification: xyz.sonet.app.grpc.proto.Notification) {
        let newNotification = NotificationItem(from: notification)
        
        // Add to beginning of list
        notifications.insert(newNotification, at: 0)
        
        // Apply current filter
        applyFilter()
        
        // Update unread count
        updateUnreadCount()
        
        // Show local notification if app is in background
        if UIApplication.shared.applicationState != .active {
            showLocalNotification(for: newNotification)
        }
    }
    
    private func applyFilter() {
        switch selectedFilter {
        case .all:
            filteredNotifications = notifications
        case .mentions:
            filteredNotifications = notifications.filter { $0.type == .mention }
        case .likes:
            filteredNotifications = notifications.filter { $0.type == .like }
        case .reposts:
            filteredNotifications = notifications.filter { $0.type == .repost }
        case .follows:
            filteredNotifications = notifications.filter { $0.type == .follow }
        case .replies:
            filteredNotifications = notifications.filter { $0.type == .reply }
        case .hashtags:
            filteredNotifications = notifications.filter { $0.type == .hashtag }
        case .appUpdates:
            filteredNotifications = notifications.filter { $0.type == .appUpdate }
        }
    }
    
    private func updateUnreadCount() {
        unreadCount = notifications.filter { !$0.isRead }.count
    }
    
    private func loadNotificationPreferences() {
        // Load from UserDefaults
        if let data = UserDefaults.standard.data(forKey: "NotificationPreferences"),
           let preferences = try? JSONDecoder().decode(NotificationPreferences.self, from: data) {
            notificationPreferences = preferences
        }
    }
    
    private func requestNotificationPermissions() {
        UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .badge, .sound]) { granted, error in
            if granted {
                DispatchQueue.main.async {
                    UIApplication.shared.registerForRemoteNotifications()
                }
            }
        }
    }
    
    private func showLocalNotification(for notification: NotificationItem) {
        let content = UNMutableNotificationContent()
        content.title = notification.title
        content.body = notification.message
        content.sound = .default
        
        let request = UNNotificationRequest(
            identifier: notification.id,
            content: content,
            trigger: nil
        )
        
        UNUserNotificationCenter.current().add(request)
    }
    
    deinit {
        notificationStream?.cancel()
        refreshTimer?.invalidate()
    }
}

// MARK: - Data Models
struct NotificationItem: Identifiable {
    let id: String
    let type: NotificationType
    let title: String
    let message: String
    let timestamp: Date
    let isRead: Bool
    let sender: UserProfile?
    let targetNote: Note?
    let targetUser: UserProfile?
    let metadata: [String: String]
    
    init(from grpcNotification: xyz.sonet.app.grpc.proto.Notification) {
        self.id = grpcNotification.notificationId
        self.type = NotificationType(from: grpcNotification.type)
        self.title = grpcNotification.title
        self.message = grpcNotification.message
        self.timestamp = Date(timeIntervalSince1970: TimeInterval(grpcNotification.timestamp.seconds))
        self.isRead = grpcNotification.isRead
        self.sender = grpcNotification.hasSender ? UserProfile(from: grpcNotification.sender) : nil
        self.targetNote = grpcNotification.hasTargetNote ? Note(from: grpcNotification.targetNote) : nil
        self.targetUser = grpcNotification.hasTargetUser ? UserProfile(from: grpcNotification.targetUser) : nil
        self.metadata = Dictionary(grpcNotification.metadataMap.mapKeys { $0.key }, uniquingKeysWith: { first, _ in first })
    }
}

enum NotificationType: String, CaseIterable {
    case like = "like"
    case reply = "reply"
    case repost = "repost"
    case follow = "follow"
    case mention = "mention"
    case hashtag = "hashtag"
    case appUpdate = "app_update"
    case system = "system"
    
    init(from grpcType: xyz.sonet.app.grpc.proto.NotificationType) {
        switch grpcType {
        case .NOTIFICATION_TYPE_LIKE: self = .like
        case .NOTIFICATION_TYPE_REPLY: self = .reply
        case .NOTIFICATION_TYPE_REPOST: self = .repost
        case .NOTIFICATION_TYPE_FOLLOW: self = .follow
        case .NOTIFICATION_TYPE_MENTION: self = .mention
        case .NOTIFICATION_TYPE_HASHTAG: self = .hashtag
        case .NOTIFICATION_TYPE_APP_UPDATE: self = .appUpdate
        case .NOTIFICATION_TYPE_SYSTEM: self = .system
        default: self = .system
        }
    }
    
    var icon: String {
        switch self {
        case .like: return "heart.fill"
        case .reply: return "bubble.left.fill"
        case .repost: return "arrow.2.squarepath.fill"
        case .follow: return "person.badge.plus.fill"
        case .mention: return "at"
        case .hashtag: return "number"
        case .appUpdate: return "app.badge.fill"
        case .system: return "bell.fill"
        }
    }
    
    var color: Color { .primary }
}

struct AppUpdateInfo: Identifiable {
    let id: String
    let version: String
    let title: String
    let description: String
    let changelog: [String]
    let isRequired: Bool
    let downloadUrl: String
    let releaseDate: Date
    
    init(from grpcUpdate: xyz.sonet.app.grpc.proto.AppUpdate) {
        self.id = grpcUpdate.updateId
        self.version = grpcUpdate.version
        self.title = grpcUpdate.title
        self.description = grpcUpdate.description
        self.changelog = grpcUpdate.changelogList
        self.isRequired = grpcUpdate.isRequired
        self.downloadUrl = grpcUpdate.downloadUrl
        self.releaseDate = Date(timeIntervalSince1970: TimeInterval(grpcUpdate.releaseDate.seconds))
    }
}

struct NotificationPreferences: Codable {
    var pushEnabled: Bool = true
    var emailEnabled: Bool = true
    var likeNotifications: Bool = true
    var replyNotifications: Bool = true
    var repostNotifications: Bool = true
    var followNotifications: Bool = true
    var mentionNotifications: Bool = true
    var hashtagNotifications: Bool = true
    var appUpdateNotifications: Bool = true
    var systemNotifications: Bool = true
    var quietHoursEnabled: Bool = false
    var quietHoursStart: Date = Calendar.current.date(from: DateComponents(hour: 22, minute: 0)) ?? Date()
    var quietHoursEnd: Date = Calendar.current.date(from: DateComponents(hour: 8, minute: 0)) ?? Date()
    
    func toGRPC() -> xyz.sonet.app.grpc.proto.NotificationPreferences {
        var preferences = xyz.sonet.app.grpc.proto.NotificationPreferences()
        preferences.pushEnabled = pushEnabled
        preferences.emailEnabled = emailEnabled
        preferences.likeNotifications = likeNotifications
        preferences.replyNotifications = replyNotifications
        preferences.repostNotifications = repostNotifications
        preferences.followNotifications = followNotifications
        preferences.mentionNotifications = mentionNotifications
        preferences.hashtagNotifications = hashtagNotifications
        preferences.appUpdateNotifications = appUpdateNotifications
        preferences.systemNotifications = systemNotifications
        preferences.quietHoursEnabled = quietHoursEnabled
        
        var startTimestamp = Timestamp()
        startTimestamp.seconds = Int64(quietHoursStart.timeIntervalSince1970)
        preferences.quietHoursStart = startTimestamp
        
        var endTimestamp = Timestamp()
        endTimestamp.seconds = Int64(quietHoursEnd.timeIntervalSince1970)
        preferences.quietHoursEnd = endTimestamp
        
        return preferences
    }
}