import SwiftUI

struct NotificationsView: View {
    @StateObject private var viewModel: NotificationsViewModel
    @Environment(\.colorScheme) var colorScheme
    @State private var showingPreferences = false
    @State private var showingAppUpdate = false
    
    init(grpcClient: SonetGRPCClient) {
        _viewModel = StateObject(wrappedValue: NotificationsViewModel(grpcClient: grpcClient))
    }
    
    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                // Header with filters
                NotificationHeader(
                    selectedFilter: viewModel.selectedFilter,
                    unreadCount: viewModel.unreadCount,
                    onFilterSelected: { viewModel.selectFilter($0) },
                    onPreferences: { showingPreferences = true },
                    onMarkAllRead: { viewModel.markAllAsRead() }
                )
                
                // Notifications list
                if viewModel.isLoading {
                    LoadingView()
                } else if let error = viewModel.error {
                    ErrorView(error: error, onRetry: { viewModel.refreshNotifications() })
                } else if viewModel.filteredNotifications.isEmpty {
                    EmptyNotificationsView(selectedFilter: viewModel.selectedFilter)
                } else {
                    NotificationsList(
                        notifications: viewModel.filteredNotifications,
                        onNotificationTap: { notification in
                            viewModel.markAsRead(notification)
                            // Navigate to relevant content
                        },
                        onDeleteNotification: { notification in
                            viewModel.deleteNotification(notification)
                        }
                    )
                }
            }
            .navigationBarHidden(true)
            .refreshable {
                viewModel.refreshNotifications()
            }
            .onAppear {
                viewModel.loadNotifications()
                viewModel.checkForAppUpdates()
            }
            .sheet(isPresented: $showingPreferences) {
                NotificationPreferencesView(
                    preferences: $viewModel.notificationPreferences,
                    onSave: {
                        viewModel.updateNotificationPreferences()
                        showingPreferences = false
                    }
                )
            }
            .sheet(isPresented: $viewModel.showAppUpdate) {
                if let appUpdate = viewModel.appUpdateInfo {
                    AppUpdateView(
                        appUpdate: appUpdate,
                        onDismiss: { viewModel.showAppUpdate = false }
                    )
                }
            }
        }
    }
}

// MARK: - Notification Header
struct NotificationHeader: View {
    let selectedFilter: NotificationsViewModel.NotificationFilter
    let unreadCount: Int
    let onFilterSelected: (NotificationsViewModel.NotificationFilter) -> Void
    let onPreferences: () -> Void
    let onMarkAllRead: () -> Void
    
    var body: some View {
        VStack(spacing: 0) {
            // Main header
            HStack {
                Text("Notifications")
                    .font(.largeTitle)
                    .fontWeight(.bold)
                
                Spacer()
                
                HStack(spacing: 16) {
                    // Unread count badge
                    if unreadCount > 0 {
                        Button(action: onMarkAllRead) {
                            Text("\(unreadCount)")
                                .font(.caption)
                                .fontWeight(.semibold)
                                .foregroundColor(.background)
                                .frame(width: 20, height: 20)
                                .background(Circle().fill(Color.accentColor))
                        }
                    }
                    
                    // Preferences button
                    Button(action: onPreferences) { IconView(AppIcons.gear, size: 20) }
                }
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 12)
            
            // Filter tabs
            NotificationFilterTabs(
                selectedFilter: selectedFilter,
                onFilterSelected: onFilterSelected
            )
        }
        .background(Color(.systemBackground))
        .overlay(
            Rectangle()
                .fill(Color(.separator))
                .frame(height: 0.5),
            alignment: .bottom
        )
    }
}

// MARK: - Notification Filter Tabs
struct NotificationFilterTabs: View {
    let selectedFilter: NotificationsViewModel.NotificationFilter
    let onFilterSelected: (NotificationsViewModel.NotificationFilter) -> Void
    
    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 0) {
                ForEach(NotificationsViewModel.NotificationFilter.allCases, id: \.self) { filter in
                    NotificationFilterTab(
                        filter: filter,
                        isSelected: selectedFilter == filter,
                        onTap: { onFilterSelected(filter) }
                    )
                }
            }
            .padding(.horizontal, 16)
        }
        .padding(.vertical, 8)
    }
}

// MARK: - Notification Filter Tab
struct NotificationFilterTab: View {
    let filter: NotificationsViewModel.NotificationFilter
    let isSelected: Bool
    let onTap: () -> Void
    
    var body: some View {
        Button(action: onTap) {
            HStack(spacing: 8) {
                IconView(filter.icon, size: 16)
                
                Text(filter.rawValue)
                    .font(.system(size: 14, weight: .medium))
            }
            .foregroundColor(isSelected ? .primary : .secondary)
            .padding(.horizontal, 16)
            .padding(.vertical, 8)
            .background(
                RoundedRectangle(cornerRadius: 20)
                    .fill(isSelected ? Color.primary.opacity(0.1) : Color.clear)
            )
        }
        .buttonStyle(PlainButtonStyle())
    }
}

// MARK: - Notifications List
struct NotificationsList: View {
    let notifications: [NotificationItem]
    let onNotificationTap: (NotificationItem) -> Void
    let onDeleteNotification: (NotificationItem) -> Void
    
    var body: some View {
        LazyVStack(spacing: 0) {
            ForEach(notifications) { notification in
                NotificationRow(
                    notification: notification,
                    onTap: { onNotificationTap(notification) },
                    onDelete: { onDeleteNotification(notification) }
                )
                
                if notification.id != notifications.last?.id {
                    Divider()
                        .padding(.leading, 72)
                }
            }
        }
    }
}

// MARK: - Notification Row
struct NotificationRow: View {
    let notification: NotificationItem
    let onTap: () -> Void
    let onDelete: () -> Void
    
    @State private var showingDeleteAlert = false
    
    var body: some View {
        Button(action: onTap) {
            HStack(alignment: .top, spacing: 12) {
                // Notification icon
                NotificationIcon(
                    type: notification.type,
                    isRead: notification.isRead
                )
                
                // Notification content
                VStack(alignment: .leading, spacing: 4) {
                    // Title and message
                    VStack(alignment: .leading, spacing: 2) {
                        Text(notification.title)
                            .font(.system(size: 15, weight: .semibold))
                            .foregroundColor(.primary)
                            .lineLimit(2)
                        
                        Text(notification.message)
                            .font(.system(size: 14))
                            .foregroundColor(.secondary)
                            .lineLimit(3)
                    }
                    
                    // Timestamp
                    Text(timeAgoString)
                        .font(.system(size: 12))
                        .foregroundColor(.secondary)
                }
                
                Spacer()
                
                // More options
                Button(action: { showingDeleteAlert = true }) { IconView(AppIcons.more, size: 14, color: .secondary) }
                .buttonStyle(PlainButtonStyle())
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 12)
            .background(
                Color(.systemBackground)
                    .opacity(notification.isRead ? 1.0 : 0.05)
            )
        }
        .buttonStyle(PlainButtonStyle())
        .alert("Delete Notification", isPresented: $showingDeleteAlert) {
            Button("Delete", role: .destructive, action: onDelete)
            Button("Cancel", role: .cancel) { }
        } message: {
            Text("Are you sure you want to delete this notification?")
        }
    }
    
    private var timeAgoString: String {
        let now = Date()
        let timeInterval = now.timeIntervalSince(notification.timestamp)
        
        if timeInterval < 60 {
            return "now"
        } else if timeInterval < 3600 {
            let minutes = Int(timeInterval / 60)
            return "\(minutes)m"
        } else if timeInterval < 86400 {
            let hours = Int(timeInterval / 3600)
            return "\(hours)h"
        } else if timeInterval < 2592000 {
            let days = Int(timeInterval / 86400)
            return "\(days)d"
        } else {
            let months = Int(timeInterval / 2592000)
            return "\(months)mo"
        }
    }
}

// MARK: - Notification Icon
struct NotificationIcon: View {
    let type: NotificationType
    let isRead: Bool
    
    var body: some View {
        ZStack {
            Circle()
                .fill(type.color.opacity(0.1))
                .frame(width: 40, height: 40)
            
            Image(systemName: type.icon)
                .font(.system(size: 18, weight: .medium))
                .foregroundColor(type.color)
        }
        .overlay(
            Circle()
                .stroke(type.color, lineWidth: isRead ? 0 : 2)
        )
    }
}

// MARK: - Empty Notifications View
struct EmptyNotificationsView: View {
    let selectedFilter: NotificationsViewModel.NotificationFilter
    
    var body: some View {
        VStack(spacing: 20) {
            IconView(iconForFilter, size: 60, color: .secondary)
            
            Text(titleForFilter)
                .font(.title2)
                .fontWeight(.bold)
                .foregroundColor(.primary)
            
            Text(messageForFilter)
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(.systemBackground))
    }
    
    private var iconForFilter: String {
        switch selectedFilter {
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
    
    private var titleForFilter: String {
        switch selectedFilter {
        case .all: return "No notifications yet"
        case .mentions: return "No mentions yet"
        case .likes: return "No likes yet"
        case .reposts: return "No reposts yet"
        case .follows: return "No new followers"
        case .replies: return "No replies yet"
        case .hashtags: return "No hashtag activity"
        case .appUpdates: return "App is up to date"
        }
    }
    
    private var messageForFilter: String {
        switch selectedFilter {
        case .all: return "When you get notifications, they'll show up here."
        case .mentions: return "When someone mentions you, it'll show up here."
        case .likes: return "When someone likes your posts, it'll show up here."
        case .reposts: return "When someone reposts your content, it'll show up here."
        case .follows: return "When someone follows you, it'll show up here."
        case .replies: return "When someone replies to your posts, it'll show up here."
        case .hashtags: return "When hashtags you follow have activity, it'll show up here."
        case .appUpdates: return "Your app is running the latest version."
        }
    }
}

// MARK: - Loading View
struct LoadingView: View {
    var body: some View {
        VStack(spacing: 16) {
            ProgressView()
                .scaleEffect(1.2)
            
            Text("Loading notifications...")
                .font(.system(size: 16, weight: .medium))
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(.systemBackground))
    }
}

// MARK: - Error View
struct ErrorView: View {
    let error: String
    let onRetry: () -> Void
    
    var body: some View {
        VStack(spacing: 16) {
            IconView(AppIcons.warning, size: 48, color: .orange)
            
            Text("Notifications Error")
                .font(.system(size: 20, weight: .bold))
                .foregroundColor(.primary)
            
            Text(error)
                .font(.system(size: 16))
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
            
            Button("Try Again", action: onRetry)
                .foregroundColor(.white)
                .padding(.horizontal, 24)
                .padding(.vertical, 12)
                .background(
                    RoundedRectangle(cornerRadius: 20)
                        .fill(Color.accentColor)
                )
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(.systemBackground))
    }
}

// MARK: - Notification Preferences View
struct NotificationPreferencesView: View {
    @Binding var preferences: NotificationPreferences
    let onSave: () -> Void
    
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        NavigationView {
            Form {
                Section("Push Notifications") {
                    Toggle("Enable Push Notifications", isOn: $preferences.pushEnabled)
                    Toggle("Enable Email Notifications", isOn: $preferences.emailEnabled)
                }
                
                Section("Notification Types") {
                    Toggle("Likes", isOn: $preferences.likeNotifications)
                    Toggle("Replies", isOn: $preferences.replyNotifications)
                    Toggle("Reposts", isOn: $preferences.repostNotifications)
                    Toggle("Follows", isOn: $preferences.followNotifications)
                    Toggle("Mentions", isOn: $preferences.mentionNotifications)
                    Toggle("Hashtags", isOn: $preferences.hashtagNotifications)
                    Toggle("App Updates", isOn: $preferences.appUpdateNotifications)
                    Toggle("System", isOn: $preferences.systemNotifications)
                }
                
                Section("Quiet Hours") {
                    Toggle("Enable Quiet Hours", isOn: $preferences.quietHoursEnabled)
                    
                    if preferences.quietHoursEnabled {
                        DatePicker("Start Time", selection: $preferences.quietHoursStart, displayedComponents: .hourAndMinute)
                        DatePicker("End Time", selection: $preferences.quietHoursEnd, displayedComponents: .hourAndMinute)
                    }
                }
            }
            .navigationTitle("Notification Settings")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        dismiss()
                    }
                }
                
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Save") {
                        onSave()
                    }
                }
            }
        }
    }
}

// MARK: - App Update View
struct AppUpdateView: View {
    let appUpdate: AppUpdateInfo
    let onDismiss: () -> Void
    
    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 24) {
                    // App icon and version
                    VStack(spacing: 16) {
                        IconView("app.badge.fill", size: 80, color: .indigo)
                        
                        Text("Update Available")
                            .font(.title)
                            .fontWeight(.bold)
                        
                        Text("Version \(appUpdate.version)")
                            .font(.title3)
                            .foregroundColor(.secondary)
                    }
                    
                    // Update details
                    VStack(alignment: .leading, spacing: 16) {
                        Text(appUpdate.title)
                            .font(.title2)
                            .fontWeight(.semibold)
                        
                        Text(appUpdate.description)
                            .font(.body)
                            .foregroundColor(.secondary)
                        
                        // Changelog
                        if !appUpdate.changelog.isEmpty {
                            VStack(alignment: .leading, spacing: 8) {
                                Text("What's New")
                                    .font(.headline)
                                    .fontWeight(.semibold)
                                
                                ForEach(appUpdate.changelog, id: \.self) { change in
                                    HStack(alignment: .top, spacing: 8) {
                                        Text("•")
                                            .foregroundColor(.accentColor)
                                        
                                        Text(change)
                                            .font(.body)
                                    }
                                }
                            }
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                    
                    // Action buttons
                    VStack(spacing: 12) {
                        Button(action: {
                            // Open App Store or download URL
                            if let url = URL(string: appUpdate.downloadUrl) {
                                UIApplication.shared.open(url)
                            }
                        }) {
                            Text(appUpdate.isRequired ? "Update Now" : "Update")
                                .font(.headline)
                                .fontWeight(.semibold)
                                .foregroundColor(.white)
                                .frame(maxWidth: .infinity)
                                .padding(.vertical, 16)
                                .background(
                                    RoundedRectangle(cornerRadius: 12)
                                        .fill(Color.accentColor)
                                )
                        }
                        
                        if !appUpdate.isRequired {
                            Button("Later", action: onDismiss)
                                .font(.body)
                                .foregroundColor(.secondary)
                        }
                    }
                }
                .padding(24)
            }
            .navigationBarHidden(true)
        }
    }
}

// MARK: - Preview
struct NotificationsView_Previews: PreviewProvider {
    static var previews: some View {
        NotificationsView(
            grpcClient: SonetGRPCClient(configuration: .development)
        )
    }
}