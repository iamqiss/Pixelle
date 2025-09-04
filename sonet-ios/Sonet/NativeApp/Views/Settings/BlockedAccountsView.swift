import SwiftUI

struct BlockedAccountsView: View {
    @StateObject private var viewModel = BlockedAccountsViewModel()
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                // Search Bar
                SearchBar(
                    text: $viewModel.searchText,
                    placeholder: "Search blocked accounts..."
                )
                .padding()
                
                // Content
                if viewModel.isLoading {
                    LoadingView()
                } else if viewModel.blockedAccounts.isEmpty {
                    EmptyBlockedAccountsView()
                } else {
                    BlockedAccountsList(
                        accounts: viewModel.filteredAccounts,
                        onUnblock: { account in
                            viewModel.unblockAccount(account)
                        },
                        onViewProfile: { account in
                            viewModel.viewProfile(account)
                        }
                    )
                }
            }
            .navigationTitle("Blocked Accounts")
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
            viewModel.loadBlockedAccounts()
        }
        .alert("Unblock Account", isPresented: $viewModel.showUnblockAlert) {
            Button("Cancel", role: .cancel) { }
            Button("Unblock") {
                viewModel.confirmUnblock()
            }
        } message: {
            if let accountToUnblock = viewModel.accountToUnblock {
                Text("Are you sure you want to unblock \(accountToUnblock.displayName)? They will be able to see your posts and contact you again.")
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
    }
}

// MARK: - Search Bar
struct SearchBar: View {
    @Binding var text: String
    let placeholder: String
    
    var body: some View {
        HStack {
            Image(systemName: "magnifyingglass")
                .foregroundColor(.secondary)
            
            TextField(placeholder, text: $text)
                .textFieldStyle(PlainTextFieldStyle())
            
            if !text.isEmpty {
                Button("Clear") {
                    text = ""
                }
                .foregroundColor(.blue)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(Color(.systemGray6))
        .cornerRadius(10)
    }
}

// MARK: - Blocked Accounts List
struct BlockedAccountsList: View {
    let accounts: [BlockedAccount]
    let onUnblock: (BlockedAccount) -> Void
    let onViewProfile: (BlockedAccount) -> Void
    
    var body: some View {
        List {
            ForEach(accounts) { account in
                BlockedAccountRow(
                    account: account,
                    onUnblock: { onUnblock(account) },
                    onViewProfile: { onViewProfile(account) }
                )
            }
        }
    }
}

// MARK: - Blocked Account Row
struct BlockedAccountRow: View {
    let account: BlockedAccount
    let onUnblock: () -> Void
    let onViewProfile: () -> Void
    
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
                
                if !account.blockReason.isEmpty {
                    Text("Blocked for: \(account.blockReason)")
                        .font(.caption)
                        .foregroundColor(.red)
                }
                
                Text("Blocked on \(account.blockedDate.formatted(date: .abbreviated, time: .omitted))")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            
            Spacer()
            
            // Unblock Button
            Button("Unblock") {
                onUnblock()
            }
            .buttonStyle(.bordered)
            .foregroundColor(.blue)
        }
        .padding(.vertical, 8)
    }
}

// MARK: - Empty State
struct EmptyBlockedAccountsView: View {
    var body: some View {
        VStack(spacing: 24) {
            Image(systemName: "person.slash")
                .font(.system(size: 80))
                .foregroundColor(.gray)
            
            VStack(spacing: 8) {
                Text("No Blocked Accounts")
                    .font(.title2)
                    .fontWeight(.semibold)
                
                Text("You haven't blocked any accounts yet. Blocked accounts won't be able to see your posts or contact you.")
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
            
            Text("Loading blocked accounts...")
                .font(.body)
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Blocked Account Model
struct BlockedAccount: Identifiable, Codable {
    let id: String
    let username: String
    let displayName: String
    let avatarUrl: String
    let isVerified: Bool
    let blockReason: String
    let blockedDate: Date
    let mutualFriends: Int
    let mutualGroups: Int
    
    var hasMutualConnections: Bool {
        mutualFriends > 0 || mutualGroups > 0
    }
}

// MARK: - Blocked Accounts View Model
@MainActor
class BlockedAccountsViewModel: ObservableObject {
    // MARK: - Published Properties
    @Published var blockedAccounts: [BlockedAccount] = []
    @Published var searchText = ""
    @Published var isLoading = false
    @Published var showUnblockAlert = false
    @Published var showErrorAlert = false
    @Published var showSuccessAlert = false
    @Published var accountToUnblock: BlockedAccount?
    @Published var errorMessage = ""
    @Published var successMessage = ""
    
    // MARK: - Computed Properties
    var filteredAccounts: [BlockedAccount] {
        if searchText.isEmpty {
            return blockedAccounts
        } else {
            return blockedAccounts.filter { account in
                account.username.localizedCaseInsensitiveContains(searchText) ||
                account.displayName.localizedCaseInsensitiveContains(searchText)
            }
        }
    }
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private let userDefaults = UserDefaults.standard
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient = SonetGRPCClient(configuration: .development)) {
        self.grpcClient = grpcClient
    }
    
    // MARK: - Public Methods
    func loadBlockedAccounts() {
        Task {
            await loadBlockedAccountsFromServer()
        }
    }
    
    func unblockAccount(_ account: BlockedAccount) {
        accountToUnblock = account
        showUnblockAlert = true
    }
    
    func confirmUnblock() {
        guard let account = accountToUnblock else { return }
        
        Task {
            await unblockAccountFromServer(account)
        }
        
        accountToUnblock = nil
        showUnblockAlert = false
    }
    
    func viewProfile(_ account: BlockedAccount) {
        // Navigate to profile view
        // This would typically be handled by navigation
        print("Viewing profile for: \(account.username)")
    }
    
    func clearError() {
        errorMessage = ""
        showErrorAlert = false
    }
    
    // MARK: - Private Methods
    private func loadBlockedAccountsFromServer() async {
        isLoading = true
        
        do {
            let request = GetBlockedAccountsRequest.newBuilder()
                .setUserId("current_user")
                .build()
            
            let response = try await grpcClient.getBlockedAccounts(request: request)
            
            if response.success {
                await MainActor.run {
                    self.blockedAccounts = response.accounts.map { BlockedAccount(from: $0) }
                }
            } else {
                await MainActor.run {
                    self.errorMessage = response.errorMessage
                    self.showErrorAlert = true
                }
            }
        } catch {
            await MainActor.run {
                self.errorMessage = "Failed to load blocked accounts: \(error.localizedDescription)"
                self.showErrorAlert = true
            }
        }
        
        await MainActor.run {
            self.isLoading = false
        }
    }
    
    private func unblockAccountFromServer(_ account: BlockedAccount) async {
        do {
            let request = UnblockAccountRequest.newBuilder()
                .setUserId("current_user")
                .setBlockedUserId(account.id)
                .build()
            
            let response = try await grpcClient.unblockAccount(request: request)
            
            if response.success {
                await MainActor.run {
                    // Remove from local list
                    self.blockedAccounts.removeAll { $0.id == account.id }
                    self.successMessage = "Successfully unblocked \(account.displayName.isEmpty ? account.username : account.displayName)"
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
                self.errorMessage = "Failed to unblock account: \(error.localizedDescription)"
                self.showErrorAlert = true
            }
        }
    }
}

// MARK: - Blocked Account Extension
extension BlockedAccount {
    init(from grpcAccount: GRPCBlockedAccount) {
        self.id = grpcAccount.userId
        self.username = grpcAccount.username
        self.displayName = grpcAccount.displayName
        self.avatarUrl = grpcAccount.avatarUrl
        self.isVerified = grpcAccount.isVerified
        self.blockReason = grpcAccount.blockReason
        self.blockedDate = grpcAccount.blockedDate.date
        self.mutualFriends = Int(grpcAccount.mutualFriends)
        self.mutualGroups = Int(grpcAccount.mutualGroups)
    }
}

// MARK: - gRPC Request/Response Models (Placeholder - replace with actual gRPC types)
struct GetBlockedAccountsRequest {
    let userId: String
    
    static func newBuilder() -> GetBlockedAccountsRequestBuilder {
        return GetBlockedAccountsRequestBuilder()
    }
}

class GetBlockedAccountsRequestBuilder {
    private var userId: String = ""
    
    func setUserId(_ userId: String) -> GetBlockedAccountsRequestBuilder {
        self.userId = userId
        return self
    }
    
    func build() -> GetBlockedAccountsRequest {
        return GetBlockedAccountsRequest(userId: userId)
    }
}

struct GetBlockedAccountsResponse {
    let success: Bool
    let accounts: [GRPCBlockedAccount]
    let errorMessage: String
}

struct UnblockAccountRequest {
    let userId: String
    let blockedUserId: String
    
    static func newBuilder() -> UnblockAccountRequestBuilder {
        return UnblockAccountRequestBuilder()
    }
}

class UnblockAccountRequestBuilder {
    private var userId: String = ""
    private var blockedUserId: String = ""
    
    func setUserId(_ userId: String) -> UnblockAccountRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setBlockedUserId(_ blockedUserId: String) -> UnblockAccountRequestBuilder {
        self.blockedUserId = blockedUserId
        return self
    }
    
    func build() -> UnblockAccountRequest {
        return UnblockAccountRequest(userId: userId, blockedUserId: blockedUserId)
    }
}

struct UnblockAccountResponse {
    let success: Bool
    let errorMessage: String
}

struct GRPCBlockedAccount {
    let userId: String
    let username: String
    let displayName: String
    let avatarUrl: String
    let isVerified: Bool
    let blockReason: String
    let blockedDate: GRPCTimestamp
    let mutualFriends: UInt32
    let mutualGroups: UInt32
}

struct GRPCTimestamp {
    let date: Date
}