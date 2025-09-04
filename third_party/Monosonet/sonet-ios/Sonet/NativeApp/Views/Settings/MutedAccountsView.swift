import SwiftUI

struct MutedAccountsView: View {
    @StateObject private var viewModel = MutedAccountsViewModel()
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                // Search Bar
                SearchBar(
                    text: $viewModel.searchText,
                    placeholder: "Search muted accounts..."
                )
                .padding()
                
                // Filter Options
                MuteFilterOptions(
                    selectedFilter: $viewModel.selectedFilter,
                    onFilterChanged: { filter in
                        viewModel.updateFilter(filter)
                    }
                )
                .padding(.horizontal)
                
                // Content
                if viewModel.isLoading {
                    LoadingView()
                } else if viewModel.mutedAccounts.isEmpty {
                    EmptyMutedAccountsView()
                } else {
                    MutedAccountsList(
                        accounts: viewModel.filteredAccounts,
                        onUnmute: { account in
                            viewModel.unmuteAccount(account)
                        },
                        onViewProfile: { account in
                            viewModel.viewProfile(account)
                        },
                        onMuteSettings: { account in
                            viewModel.showMuteSettings(account)
                        }
                    )
                }
            }
            .navigationTitle("Muted Accounts")
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
            viewModel.loadMutedAccounts()
        }
        .alert("Unmute Account", isPresented: $viewModel.showUnmuteAlert) {
            Button("Cancel", role: .cancel) { }
            Button("Unmute") {
                viewModel.confirmUnmute()
            }
        } message: {
            if let accountToUnmute = viewModel.accountToUnmute {
                Text("Are you sure you want to unmute \(accountToUnmute.displayName)? You'll start seeing their posts again.")
            }
        }
        .alert("Error", isPresented: $viewModel.showErrorAlert) {
            Button("OK") { viewModel.clearError() }
        } message: {
            Text(viewModel.errorMessage)
        }
        .alert("Success", isPresented: $viewModel.showSuccessAlert) {
            Button("OK") { }
        } message: {
            Text(viewModel.successMessage)
        }
        .sheet(isPresented: $viewModel.showMuteSettingsSheet) {
            if let account = viewModel.selectedAccountForSettings {
                MuteSettingsSheet(
                    account: account,
                    viewModel: viewModel
                )
            }
        }
    }
}

// MARK: - Mute Filter Options
struct MuteFilterOptions: View {
    @Binding var selectedFilter: MuteFilter
    let onFilterChanged: (MuteFilter) -> Void
    
    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 12) {
                ForEach(MuteFilter.allCases, id: \.self) { filter in
                    FilterChip(
                        title: filter.displayName,
                        isSelected: selectedFilter == filter,
                        onTap: {
                            selectedFilter = filter
                            onFilterChanged(filter)
                        }
                    )
                }
            }
            .padding(.horizontal)
        }
    }
}

// MARK: - Filter Chip
struct FilterChip: View {
    let title: String
    let isSelected: Bool
    let onTap: () -> Void
    
    var body: some View {
        Button(action: onTap) {
            Text(title)
                .font(.caption)
                .fontWeight(.medium)
                .padding(.horizontal, 16)
                .padding(.vertical, 8)
                .background(
                    RoundedRectangle(cornerRadius: 20)
                        .fill(isSelected ? Color.blue : Color(.systemGray6))
                )
                .foregroundColor(isSelected ? .white : .primary)
        }
        .buttonStyle(PlainButtonStyle())
    }
}

// MARK: - Muted Accounts List
struct MutedAccountsList: View {
    let accounts: [MutedAccount]
    let onUnmute: (MutedAccount) -> Void
    let onViewProfile: (MutedAccount) -> Void
    let onMuteSettings: (MutedAccount) -> Void
    
    var body: some View {
        List {
            ForEach(accounts) { account in
                MutedAccountRow(
                    account: account,
                    onUnmute: { onUnmute(account) },
                    onViewProfile: { onViewProfile(account) },
                    onMuteSettings: { onMuteSettings(account) }
                )
            }
        }
    }
}

// MARK: - Muted Account Row
struct MutedAccountRow: View {
    let account: MutedAccount
    let onUnmute: () -> Void
    let onViewProfile: () -> Void
    let onMuteSettings: () -> Void
    
    var body: some View {
        HStack(spacing: 12) {
            // Avatar
            Button(action: onViewProfile) {
                AsyncImage(url: URL(string: account.avatarUrl)) { image in
                    image
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                } placeholder: {
                    Circle()
                        .fill(Color.gray.opacity(0.3))
                        .overlay(
                            Image(systemName: "person.fill")
                                .foregroundColor(.gray)
                        )
                }
                .frame(width: 50, height: 50)
                .clipShape(Circle())
            }
            .buttonStyle(PlainButtonStyle())
            
            // Account Info
            VStack(alignment: .leading, spacing: 4) {
                HStack {
                    Text(account.displayName.isEmpty ? account.username : account.displayName)
                        .font(.headline)
                        .foregroundColor(.primary)
                    
                    if account.isVerified {
                        Image(systemName: "checkmark.seal.fill")
                            .foregroundColor(.blue)
                            .font(.caption)
                    }
                }
                
                Text("@\(account.username)")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                
                HStack(spacing: 8) {
                    Text("Muted on \(account.mutedDate.formatted(date: .abbreviated, time: .omitted))")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    
                    if account.muteDuration != .indefinite {
                        Text("• \(account.muteDuration.displayName)")
                            .font(.caption)
                            .foregroundColor(.orange)
                    }
                }
                
                if !account.muteReason.isEmpty {
                    Text("Reason: \(account.muteReason)")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }
            
            Spacer()
            
            // Action Buttons
            VStack(spacing: 8) {
                Button("Settings") {
                    onMuteSettings()
                }
                .buttonStyle(.bordered)
                .foregroundColor(.blue)
                .font(.caption)
                
                Button("Unmute") {
                    onUnmute()
                }
                .buttonStyle(.bordered)
                .foregroundColor(.green)
                .font(.caption)
            }
        }
        .padding(.vertical, 8)
    }
}

// MARK: - Mute Settings Sheet
struct MuteSettingsSheet: View {
    let account: MutedAccount
    @ObservedObject var viewModel: MutedAccountsViewModel
    @Environment(\.dismiss) private var dismiss
    
    @State private var muteDuration: MuteDuration = .indefinite
    @State private var muteReason = ""
    @State private var mutePosts = true
    @State private var muteStories = true
    @State private var muteReplies = true
    @State private var muteMentions = true
    
    var body: some View {
        NavigationView {
            Form {
                Section {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Mute Settings for \(account.displayName.isEmpty ? account.username : account.displayName)")
                            .font(.headline)
                        
                        Text("Customize what content you want to hide from this account")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                }
                
                Section("Mute Duration") {
                    Picker("Duration", selection: $muteDuration) {
                        ForEach(MuteDuration.allCases, id: \.self) { duration in
                            Text(duration.displayName).tag(duration)
                        }
                    }
                    .pickerStyle(.navigationLink)
                }
                
                Section("Mute Reason") {
                    TextField("Optional reason for muting", text: $muteReason)
                }
                
                Section("Content to Mute") {
                    Toggle("Posts", isOn: $mutePosts)
                    Toggle("Stories", isOn: $muteStories)
                    Toggle("Replies", isOn: $muteReplies)
                    Toggle("Mentions", isOn: $muteMentions)
                }
                
                Section {
                    Button("Update Mute Settings") {
                        viewModel.updateMuteSettings(
                            account: account,
                            duration: muteDuration,
                            reason: muteReason,
                            mutePosts: mutePosts,
                            muteStories: muteStories,
                            muteReplies: muteReplies,
                            muteMentions: muteMentions
                        )
                        dismiss()
                    }
                    .buttonStyle(.borderedProminent)
                    .frame(maxWidth: .infinity)
                }
            }
            .navigationTitle("Mute Settings")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                }
            }
        }
        .onAppear {
            muteDuration = account.muteDuration
            muteReason = account.muteReason
            mutePosts = account.mutePosts
            muteStories = account.muteStories
            muteReplies = account.muteReplies
            muteMentions = account.muteMentions
        }
    }
}

// MARK: - Empty State
struct EmptyMutedAccountsView: View {
    var body: some View {
        VStack(spacing: 24) {
            Image(systemName: "speaker.slash")
                .font(.system(size: 80))
                .foregroundColor(.gray)
            
            VStack(spacing: 8) {
                Text("No Muted Accounts")
                    .font(.title2)
                    .fontWeight(.semibold)
                
                Text("You haven't muted any accounts yet. Muted accounts' posts won't appear in your timeline, but they can still see your content.")
                    .font(.body)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 32)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Loading View
struct LoadingView: View {
    var body: some View {
        VStack(spacing: 16) {
            ProgressView()
                .scaleEffect(1.5)
            
            Text("Loading muted accounts...")
                .font(.body)
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Muted Account Model
struct MutedAccount: Identifiable, Codable {
    let id: String
    let username: String
    let displayName: String
    let avatarUrl: String
    let isVerified: Bool
    let muteReason: String
    let mutedDate: Date
    let muteDuration: MuteDuration
    let mutePosts: Bool
    let muteStories: Bool
    let muteReplies: Bool
    let muteMentions: Bool
    let mutualFriends: Int
    let mutualGroups: Int
    
    var hasMutualConnections: Bool {
        mutualFriends > 0 || mutualGroups > 0
    }
    
    var isExpired: Bool {
        muteDuration != .indefinite && Date() > mutedDate.addingTimeInterval(muteDuration.duration)
    }
}

// MARK: - Mute Duration Enum
enum MuteDuration: String, CaseIterable, Codable {
    case oneHour = "1h"
    case sixHours = "6h"
    case oneDay = "1d"
    case threeDays = "3d"
    case oneWeek = "1w"
    case oneMonth = "1m"
    case indefinite = "indefinite"
    
    var displayName: String {
        switch self {
        case .oneHour: return "1 Hour"
        case .sixHours: return "6 Hours"
        case .oneDay: return "1 Day"
        case .threeDays: return "3 Days"
        case .oneWeek: return "1 Week"
        case .oneMonth: return "1 Month"
        case .indefinite: return "Indefinite"
        }
    }
    
    var duration: TimeInterval {
        switch self {
        case .oneHour: return 3600
        case .sixHours: return 21600
        case .oneDay: return 86400
        case .threeDays: return 259200
        case .oneWeek: return 604800
        case .oneMonth: return 2592000
        case .indefinite: return 0
        }
    }
}

// MARK: - Mute Filter Enum
enum MuteFilter: String, CaseIterable {
    case all = "all"
    case posts = "posts"
    case stories = "stories"
    case replies = "replies"
    case mentions = "mentions"
    case expired = "expired"
    
    var displayName: String {
        switch self {
        case .all: return "All"
        case .posts: return "Posts"
        case .stories: return "Stories"
        case .replies: return "Replies"
        case .mentions: return "Mentions"
        case .expired: return "Expired"
        }
    }
}

// MARK: - Muted Accounts View Model
@MainActor
class MutedAccountsViewModel: ObservableObject {
    // MARK: - Published Properties
    @Published var mutedAccounts: [MutedAccount] = []
    @Published var searchText = ""
    @Published var selectedFilter: MuteFilter = .all
    @Published var isLoading = false
    @Published var showUnmuteAlert = false
    @Published var showErrorAlert = false
    @Published var showSuccessAlert = false
    @Published var showMuteSettingsSheet = false
    @Published var accountToUnmute: MutedAccount?
    @Published var selectedAccountForSettings: MutedAccount?
    @Published var errorMessage = ""
    @Published var successMessage = ""
    
    // MARK: - Computed Properties
    var filteredAccounts: [MutedAccount] {
        var filtered = mutedAccounts
        
        // Apply search filter
        if !searchText.isEmpty {
            filtered = filtered.filter { account in
                account.username.localizedCaseInsensitiveContains(searchText) ||
                account.displayName.localizedCaseInsensitiveContains(searchText)
            }
        }
        
        // Apply content filter
        switch selectedFilter {
        case .posts:
            filtered = filtered.filter { $0.mutePosts }
        case .stories:
            filtered = filtered.filter { $0.muteStories }
        case .replies:
            filtered = filtered.filter { $0.muteReplies }
        case .mentions:
            filtered = filtered.filter { $0.muteMentions }
        case .expired:
            filtered = filtered.filter { $0.isExpired }
        case .all:
            break
        }
        
        return filtered
    }
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private let userDefaults = UserDefaults.standard
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient = SonetGRPCClient(configuration: .development)) {
        self.grpcClient = grpcClient
    }
    
    // MARK: - Public Methods
    func loadMutedAccounts() {
        Task {
            await loadMutedAccountsFromServer()
        }
    }
    
    func updateFilter(_ filter: MuteFilter) {
        selectedFilter = filter
    }
    
    func unmuteAccount(_ account: MutedAccount) {
        accountToUnmute = account
        showUnmuteAlert = true
    }
    
    func confirmUnmute() {
        guard let account = accountToUnmute else { return }
        
        Task {
            await unmuteAccountFromServer(account)
        }
        
        accountToUnmute = nil
        showUnmuteAlert = false
    }
    
    func viewProfile(_ account: MutedAccount) {
        // Navigate to profile view
        print("Viewing profile for: \(account.username)")
    }
    
    func showMuteSettings(_ account: MutedAccount) {
        selectedAccountForSettings = account
        showMuteSettingsSheet = true
    }
    
    func updateMuteSettings(
        account: MutedAccount,
        duration: MuteDuration,
        reason: String,
        mutePosts: Bool,
        muteStories: Bool,
        muteReplies: Bool,
        muteMentions: Bool
    ) {
        Task {
            await updateMuteSettingsOnServer(
                account: account,
                duration: duration,
                reason: reason,
                mutePosts: mutePosts,
                muteStories: muteStories,
                muteReplies: muteReplies,
                muteMentions: muteMentions
            )
        }
    }
    
    func clearError() {
        errorMessage = ""
        showErrorAlert = false
    }
    
    // MARK: - Private Methods
    private func loadMutedAccountsFromServer() async {
        isLoading = true
        
        do {
            let request = GetMutedAccountsRequest.newBuilder()
                .setUserId("current_user")
                .build()
            
            let response = try await grpcClient.getMutedAccounts(request: request)
            
            if response.success {
                await MainActor.run {
                    self.mutedAccounts = response.accounts.map { MutedAccount(from: $0) }
                }
            } else {
                await MainActor.run {
                    self.errorMessage = response.errorMessage
                    self.showErrorAlert = true
                }
            }
        } catch {
            await MainActor.run {
                self.errorMessage = "Failed to load muted accounts: \(error.localizedDescription)"
                self.showErrorAlert = true
            }
        }
        
        await MainActor.run {
            self.isLoading = false
        }
    }
    
    private func unmuteAccountFromServer(_ account: MutedAccount) async {
        do {
            let request = UnmuteAccountRequest.newBuilder()
                .setUserId("current_user")
                .setMutedUserId(account.id)
                .build()
            
            let response = try await grpcClient.unmuteAccount(request: request)
            
            if response.success {
                await MainActor.run {
                    // Remove from local list
                    self.mutedAccounts.removeAll { $0.id == account.id }
                    self.successMessage = "Successfully unmuted \(account.displayName.isEmpty ? account.username : account.displayName)"
                    self.showSuccessAlert = true
                }
            } else {
                await MainActor.run {
                    self.errorMessage = response.errorMessage
                    self.showErrorAlert = true
                }
            }
        } catch {
            await MainActor.run {
                self.errorMessage = "Failed to unmute account: \(error.localizedDescription)"
                self.showErrorAlert = true
            }
        }
    }
    
    private func updateMuteSettingsOnServer(
        account: MutedAccount,
        duration: MuteDuration,
        reason: String,
        mutePosts: Bool,
        muteStories: Bool,
        muteReplies: Bool,
        muteMentions: Bool
    ) async {
        do {
            let request = UpdateMuteSettingsRequest.newBuilder()
                .setUserId("current_user")
                .setMutedUserId(account.id)
                .setDuration(duration.rawValue)
                .setReason(reason)
                .setMutePosts(mutePosts)
                .setMuteStories(muteStories)
                .setMuteReplies(muteReplies)
                .setMuteMentions(muteMentions)
                .build()
            
            let response = try await grpcClient.updateMuteSettings(request: request)
            
            if response.success {
                await MainActor.run {
                    // Update local account
                    if let index = self.mutedAccounts.firstIndex(where: { $0.id == account.id }) {
                        let updatedAccount = MutedAccount(
                            id: account.id,
                            username: account.username,
                            displayName: account.displayName,
                            avatarUrl: account.avatarUrl,
                            isVerified: account.isVerified,
                            muteReason: reason,
                            mutedDate: account.mutedDate,
                            muteDuration: duration,
                            mutePosts: mutePosts,
                            muteStories: muteStories,
                            muteReplies: muteReplies,
                            muteMentions: muteMentions,
                            mutualFriends: account.mutualFriends,
                            mutualGroups: account.mutualGroups
                        )
                        self.mutedAccounts[index] = updatedAccount
                    }
                    
                    self.successMessage = "Mute settings updated successfully"
                    self.showSuccessAlert = true
                }
            } else {
                await MainActor.run {
                    self.errorMessage = response.errorMessage
                    self.showErrorAlert = true
                }
            }
        } catch {
            await MainActor.run {
                self.errorMessage = "Failed to update mute settings: \(error.localizedDescription)"
                self.showErrorAlert = true
            }
        }
    }
}

// MARK: - Muted Account Extension
extension MutedAccount {
    init(from grpcAccount: GRPCMutedAccount) {
        self.id = grpcAccount.userId
        self.username = grpcAccount.username
        self.displayName = grpcAccount.username
        self.avatarUrl = grpcAccount.avatarUrl
        self.isVerified = grpcAccount.isVerified
        self.muteReason = grpcAccount.muteReason
        self.mutedDate = grpcAccount.mutedDate.date
        self.muteDuration = MuteDuration(rawValue: grpcAccount.muteDuration) ?? .indefinite
        self.mutePosts = grpcAccount.mutePosts
        self.muteStories = grpcAccount.muteStories
        self.muteReplies = grpcAccount.muteReplies
        self.muteMentions = grpcAccount.muteMentions
        self.mutualFriends = Int(grpcAccount.mutualFriends)
        self.mutualGroups = Int(grpcAccount.mutualGroups)
    }
}

// MARK: - gRPC Request/Response Models (Placeholder - replace with actual gRPC types)
struct GetMutedAccountsRequest {
    let userId: String
    
    static func newBuilder() -> GetMutedAccountsRequestBuilder {
        return GetMutedAccountsRequestBuilder()
    }
}

class GetMutedAccountsRequestBuilder {
    private var userId: String = ""
    
    func setUserId(_ userId: String) -> GetMutedAccountsRequestBuilder {
        self.userId = userId
        return self
    }
    
    func build() -> GetMutedAccountsRequest {
        return GetMutedAccountsRequest(userId: userId)
    }
}

struct GetMutedAccountsResponse {
    let success: Bool
    let accounts: [GRPCMutedAccount]
    let errorMessage: String
}

struct UnmuteAccountRequest {
    let userId: String
    let mutedUserId: String
    
    static func newBuilder() -> UnmuteAccountRequestBuilder {
        return UnmuteAccountRequestBuilder()
    }
}

class UnmuteAccountRequestBuilder {
    private var userId: String = ""
    private var mutedUserId: String = ""
    
    func setUserId(_ userId: String) -> UnmuteAccountRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setMutedUserId(_ mutedUserId: String) -> UnmuteAccountRequestBuilder {
        self.mutedUserId = mutedUserId
        return self
    }
    
    func build() -> UnmuteAccountRequest {
        return UnmuteAccountRequest(userId: userId, mutedUserId: mutedUserId)
    }
}

struct UnmuteAccountResponse {
    let success: Bool
    let errorMessage: String
}

struct UpdateMuteSettingsRequest {
    let userId: String
    let mutedUserId: String
    let duration: String
    let reason: String
    let mutePosts: Bool
    let muteStories: Bool
    let muteReplies: Bool
    let muteMentions: Bool
    
    static func newBuilder() -> UpdateMuteSettingsRequestBuilder {
        return UpdateMuteSettingsRequestBuilder()
    }
}

class UpdateMuteSettingsRequestBuilder {
    private var userId: String = ""
    private var mutedUserId: String = ""
    private var duration: String = ""
    private var reason: String = ""
    private var mutePosts: Bool = true
    private var muteStories: Bool = true
    private var muteReplies: Bool = true
    private var muteMentions: Bool = true
    
    func setUserId(_ userId: String) -> UpdateMuteSettingsRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setMutedUserId(_ mutedUserId: String) -> UpdateMuteSettingsRequestBuilder {
        self.mutedUserId = mutedUserId
        return self
    }
    
    func setDuration(_ duration: String) -> UpdateMuteSettingsRequestBuilder {
        self.duration = duration
        return self
    }
    
    func setReason(_ reason: String) -> UpdateMuteSettingsRequestBuilder {
        self.reason = reason
        return self
    }
    
    func setMutePosts(_ mutePosts: Bool) -> UpdateMuteSettingsRequestBuilder {
        self.mutePosts = mutePosts
        return self
    }
    
    func setMuteStories(_ muteStories: Bool) -> UpdateMuteSettingsRequestBuilder {
        self.muteStories = muteStories
        return self
    }
    
    func setMuteReplies(_ muteReplies: Bool) -> UpdateMuteSettingsRequestBuilder {
        self.muteReplies = muteReplies
        return self
    }
    
    func setMuteMentions(_ muteMentions: Bool) -> UpdateMuteSettingsRequestBuilder {
        self.muteMentions = muteMentions
        return self
    }
    
    func build() -> UpdateMuteSettingsRequest {
        return UpdateMuteSettingsRequest(
            userId: userId,
            mutedUserId: mutedUserId,
            duration: duration,
            reason: reason,
            mutePosts: mutePosts,
            muteStories: muteStories,
            muteReplies: muteReplies,
            muteMentions: muteMentions
        )
    }
}

struct UpdateMuteSettingsResponse {
    let success: Bool
    let errorMessage: String
}

struct GRPCMutedAccount {
    let userId: String
    let username: String
    let displayName: String
    let avatarUrl: String
    let isVerified: Bool
    let muteReason: String
    let mutedDate: GRPCTimestamp
    let muteDuration: String
    let mutePosts: Bool
    let muteStories: Bool
    let muteReplies: Bool
    let muteMentions: Bool
    let mutualFriends: UInt32
    let mutualGroups: UInt32
}

struct GRPCTimestamp {
    let date: Date
}