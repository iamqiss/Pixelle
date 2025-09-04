import SwiftUI

struct SafetyView: View {
    @StateObject private var viewModel = SafetyViewModel()
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        NavigationView {
            List {
                // Content Safety Section
                Section {
                    NavigationLink("Content Filtering") {
                        ContentFilteringView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Muted Words") {
                        MutedWordsView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Hidden Content") {
                        HiddenContentView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Sensitive Content") {
                        SensitiveContentView(viewModel: viewModel)
                    }
                } header: {
                    Text("Content Safety")
                } footer: {
                    Text("Control what content you see and how it's filtered")
                }
                
                // Account Safety Section
                Section {
                    NavigationLink("Blocked Accounts") {
                        BlockedAccountsView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Muted Accounts") {
                        MutedAccountsView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Restricted Accounts") {
                        RestrictedAccountsView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Account Limits") {
                        AccountLimitsView(viewModel: viewModel)
                    }
                } header: {
                    Text("Account Safety")
                } footer: {
                    Text("Manage who can interact with you and your account")
                }
                
                // Reporting & Moderation Section
                Section {
                    NavigationLink("Report Settings") {
                        ReportSettingsView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Moderation History") {
                        ModerationHistoryView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Appeal Decisions") {
                        AppealDecisionsView(viewModel: viewModel)
                    }
                } header: {
                    Text("Reporting & Moderation")
                } footer: {
                    Text("Manage your reporting preferences and moderation history")
                }
                
                // Advanced Safety Section
                Section {
                    NavigationLink("Two-Factor Authentication") {
                        TwoFactorView()
                    }
                    
                    NavigationLink("Login Alerts") {
                        LoginAlertsView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Device Management") {
                        DeviceManagementView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Session Security") {
                        SessionSecurityView(viewModel: viewModel)
                    }
                } header: {
                    Text("Advanced Safety")
                } footer: {
                    Text("Enhanced security features and account protection")
                }
                
                // Emergency Section
                Section {
                    NavigationLink("Emergency Contacts") {
                        EmergencyContactsView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Account Recovery") {
                        AccountRecoveryView(viewModel: viewModel)
                    }
                    
                    Button("Lock Account Temporarily") {
                        viewModel.lockAccountTemporarily()
                    }
                    .foregroundColor(.orange)
                } header: {
                    Text("Emergency Actions")
                } footer: {
                    Text("Quick actions for emergency situations")
                }
            }
            .navigationTitle("Safety")
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
            viewModel.loadSafetySettings()
        }
        .alert("Safety Update", isPresented: $viewModel.showUpdateAlert) {
            Button("OK") { }
        } message: {
            Text(viewModel.updateMessage)
        }
        .alert("Error", isPresented: $viewModel.showErrorAlert) {
            Button("OK") { viewModel.clearError() }
        } message: {
            Text(viewModel.errorMessage)
        }
        .alert("Account Locked", isPresented: $viewModel.showLockAlert) {
            Button("OK") { }
        } message: {
            Text("Your account has been temporarily locked for safety. Contact support if this was done in error.")
        }
    }
}

// MARK: - Content Filtering View
struct ContentFilteringView: View {
    @ObservedObject var viewModel: SafetyViewModel
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        List {
            Section {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Content Filtering Level")
                        .font(.headline)
                    
                    Text("Choose how strictly content is filtered for you")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                
                Picker("Filtering Level", selection: $viewModel.contentFilteringLevel) {
                    ForEach(ContentFilteringLevel.allCases, id: \.self) { level in
                        VStack(alignment: .leading) {
                            Text(level.displayName)
                            Text(level.description)
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                        .tag(level)
                    }
                }
                .pickerStyle(.navigationLink)
                .onChange(of: viewModel.contentFilteringLevel) { _ in
                    viewModel.updateContentFilteringLevel()
                }
            } header: {
                Text("General Filtering")
            }
            
            Section {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Sensitive Content")
                        .font(.headline)
                    
                    Text("Control how sensitive content is handled")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                
                Toggle("Show Sensitive Content", isOn: $viewModel.showSensitiveContent)
                    .onChange(of: viewModel.showSensitiveContent) { _ in
                        viewModel.updateSensitiveContentSetting()
                    }
                
                Toggle("Warn Before Showing", isOn: $viewModel.warnBeforeSensitiveContent)
                    .onChange(of: viewModel.warnBeforeSensitiveContent) { _ in
                        viewModel.updateSensitiveContentWarning()
                    }
            } header: {
                Text("Sensitive Content")
            }
            
            Section {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Language Filtering")
                        .font(.headline)
                    
                    Text("Filter content based on language preferences")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                
                Toggle("Filter Profanity", isOn: $viewModel.filterProfanity)
                    .onChange(of: viewModel.filterProfanity) { _ in
                        viewModel.updateProfanityFilter()
                    }
                
                Toggle("Filter Hate Speech", isOn: $viewModel.filterHateSpeech)
                    .onChange(of: viewModel.filterHateSpeech) { _ in
                        viewModel.updateHateSpeechFilter()
                    }
                
                Toggle("Filter Violence", isOn: $viewModel.filterViolence)
                    .onChange(of: viewModel.filterViolence) { _ in
                        viewModel.updateViolenceFilter()
                    }
            } header: {
                Text("Language & Content")
            }
        }
        .navigationTitle("Content Filtering")
        .navigationBarTitleDisplayMode(.large)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button("Done") { dismiss() }
            }
        }
    }
}

// MARK: - Muted Words View
struct MutedWordsView: View {
    @ObservedObject var viewModel: SafetyViewModel
    @Environment(\.dismiss) private var dismiss
    @State private var newWord = ""
    
    var body: some View {
        List {
            Section {
                HStack {
                    TextField("Add muted word", text: $newWord)
                        .textFieldStyle(RoundedBorderTextFieldStyle())
                    
                    Button("Add") {
                        if !newWord.isEmpty {
                            viewModel.addMutedWord(newWord)
                            newWord = ""
                        }
                    }
                    .disabled(newWord.isEmpty)
                }
            } header: {
                Text("Add Muted Word")
            }
            
            Section {
                if viewModel.mutedWords.isEmpty {
                    Text("No muted words")
                        .foregroundColor(.secondary)
                        .italic()
                } else {
                    ForEach(viewModel.mutedWords, id: \.self) { word in
                        HStack {
                            Text(word)
                            Spacer()
                            Button("Remove") {
                                viewModel.removeMutedWord(word)
                            }
                            .foregroundColor(.red)
                        }
                    }
                }
            } header: {
                Text("Muted Words")
            } footer: {
                Text("Posts containing these words will be hidden from your timeline")
            }
        }
        .navigationTitle("Muted Words")
        .navigationBarTitleDisplayMode(.large)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button("Done") { dismiss() }
            }
        }
    }
}

// MARK: - Hidden Content View
struct HiddenContentView: View {
    @ObservedObject var viewModel: SafetyViewModel
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        List {
            Section {
                Toggle("Hide Muted Users' Content", isOn: $viewModel.hideMutedUsersContent)
                    .onChange(of: viewModel.hideMutedUsersContent) { _ in
                        viewModel.updateHideMutedUsersContent()
                    }
                
                Toggle("Hide Blocked Users' Content", isOn: $viewModel.hideBlockedUsersContent)
                    .onChange(of: viewModel.hideBlockedUsersContent) { _ in
                        viewModel.updateHideBlockedUsersContent()
                    }
                
                Toggle("Hide Sensitive Media", isOn: $viewModel.hideSensitiveMedia)
                    .onChange(of: viewModel.hideSensitiveMedia) { _ in
                        viewModel.updateHideSensitiveMedia()
                    }
            } header: {
                Text("Content Hiding")
            } footer: {
                Text("Control what content is automatically hidden from your view")
            }
            
            Section {
                NavigationLink("Hidden Posts") {
                    HiddenPostsView(viewModel: viewModel)
                }
                
                NavigationLink("Hidden Users") {
                    HiddenUsersView(viewModel: viewModel)
                }
                
                NavigationLink("Hidden Hashtags") {
                    HiddenHashtagsView(viewModel: viewModel)
                }
            } header: {
                Text("Hidden Items")
            } footer: {
                Text("View and manage content you've manually hidden")
            }
        }
        .navigationTitle("Hidden Content")
        .navigationBarTitleDisplayMode(.large)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button("Done") { dismiss() }
            }
        }
    }
}

// MARK: - Sensitive Content View
struct SensitiveContentView: View {
    @ObservedObject var viewModel: SafetyViewModel
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        List {
            Section {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Sensitive Content Handling")
                        .font(.headline)
                    
                    Text("Choose how sensitive content is displayed")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                
                Picker("Display Mode", selection: $viewModel.sensitiveContentDisplayMode) {
                    ForEach(SensitiveContentDisplayMode.allCases, id: \.self) { mode in
                        VStack(alignment: .leading) {
                            Text(mode.displayName)
                            Text(mode.description)
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                        .tag(mode)
                    }
                }
                .pickerStyle(.navigationLink)
                .onChange(of: viewModel.sensitiveContentDisplayMode) { _ in
                    viewModel.updateSensitiveContentDisplayMode()
                }
            } header: {
                Text("Display Settings")
            }
            
            Section {
                Toggle("Blur Sensitive Images", isOn: $viewModel.blurSensitiveImages)
                    .onChange(of: viewModel.blurSensitiveImages) { _ in
                        viewModel.updateBlurSensitiveImages()
                    }
                
                Toggle("Mute Sensitive Videos", isOn: $viewModel.muteSensitiveVideos)
                    .onChange(of: viewModel.muteSensitiveVideos) { _ in
                        viewModel.updateMuteSensitiveVideos()
                    }
                
                Toggle("Show Content Warnings", isOn: $viewModel.showContentWarnings)
                    .onChange(of: viewModel.showContentWarnings) { _ in
                        viewModel.updateShowContentWarnings()
                    }
            } header: {
                Text("Media Handling")
            }
        }
        .navigationTitle("Sensitive Content")
        .navigationBarTitleDisplayMode(.large)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button("Done") { dismiss() }
            }
        }
    }
}

// MARK: - Safety Enums
enum ContentFilteringLevel: String, CaseIterable {
    case off = "off"
    case low = "low"
    case medium = "medium"
    case high = "high"
    
    var displayName: String {
        switch self {
        case .off: return "Off"
        case .low: return "Low"
        case .medium: return "Medium"
        case .high: return "High"
        }
    }
    
    var description: String {
        switch self {
        case .off: return "No content filtering"
        case .low: return "Minimal filtering for obvious violations"
        case .medium: return "Standard filtering for most content"
        case .high: return "Aggressive filtering for maximum safety"
        }
    }
}

enum SensitiveContentDisplayMode: String, CaseIterable {
    case always = "always"
    case withWarning = "with_warning"
    case blurred = "blurred"
    case hidden = "hidden"
    
    var displayName: String {
        switch self {
        case .always: return "Always Show"
        case .withWarning: return "Show with Warning"
        case .blurred: return "Show Blurred"
        case .hidden: return "Hide Completely"
        }
    }
    
    var description: String {
        switch self {
        case .always: return "Show all content without restrictions"
        case .withWarning: return "Show content with a warning message"
        case .blurred: return "Show content but blur sensitive parts"
        case .hidden: return "Hide sensitive content completely"
        }
    }
}

// MARK: - Placeholder Views (to be implemented)
struct BlockedAccountsView: View {
    @ObservedObject var viewModel: SafetyViewModel
    
    var body: some View {
        Text("Blocked Accounts")
            .navigationTitle("Blocked Accounts")
    }
}

struct MutedAccountsView: View {
    @ObservedObject var viewModel: SafetyViewModel
    
    var body: some View {
        Text("Muted Accounts")
            .navigationTitle("Muted Accounts")
    }
}

struct RestrictedAccountsView: View {
    var body: some View {
        Text("Restricted Accounts")
            .navigationTitle("Restricted Accounts")
    }
}

struct AccountLimitsView: View {
    var body: some View {
        Text("Account Limits")
            .navigationTitle("Account Limits")
    }
}

struct ReportSettingsView: View {
    var body: some View {
        Text("Report Settings")
            .navigationTitle("Report Settings")
    }
}

struct ModerationHistoryView: View {
    var body: some View {
        Text("Moderation History")
            .navigationTitle("Moderation History")
    }
}

struct AppealDecisionsView: View {
    var body: some View {
        Text("Appeal Decisions")
            .navigationTitle("Appeal Decisions")
    }
}

struct LoginAlertsView: View {
    var body: some View {
        Text("Login Alerts")
            .navigationTitle("Login Alerts")
    }
}

struct DeviceManagementView: View {
    var body: some View {
        Text("Device Management")
            .navigationTitle("Device Management")
    }
}

struct SessionSecurityView: View {
    var body: some View {
        Text("Session Security")
            .navigationTitle("Session Security")
    }
}

struct EmergencyContactsView: View {
    var body: some View {
        Text("Emergency Contacts")
            .navigationTitle("Emergency Contacts")
    }
}

struct AccountRecoveryView: View {
    @StateObject private var vm = AccountRecoveryViewModel()
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        Form {
            Section("Recovery Email") {
                TextField("Email address", text: $vm.recoveryEmail)
                    .keyboardType(.emailAddress)
                    .autocapitalization(.none)
                Button("Verify Email") { vm.verifyEmail() }.disabled(!vm.canVerifyEmail)
                if vm.emailVerified { Label("Verified", systemImage: "checkmark.seal.fill").foregroundColor(.green) }
            }
            Section("Recovery Phone") {
                TextField("Phone number", text: $vm.recoveryPhone)
                    .keyboardType(.phonePad)
                Button("Verify Phone") { vm.verifyPhone() }.disabled(!vm.canVerifyPhone)
                if vm.phoneVerified { Label("Verified", systemImage: "checkmark.seal.fill").foregroundColor(.green) }
            }
            Section("Backup Codes") {
                if vm.backupCodes.isEmpty {
                    Button("Generate Backup Codes") { vm.generateBackupCodes() }
                } else {
                    ForEach(vm.backupCodes, id: \.self) { code in HStack { Text(code); Spacer(); Image(systemName: "doc.on.doc") } }
                    Button("Regenerate Codes", role: .destructive) { vm.regenerateBackupCodes() }
                }
            }
            Section("Password Reset") {
                SecureField("New Password", text: $vm.newPassword)
                SecureField("Confirm Password", text: $vm.confirmPassword)
                Button("Set New Password") { vm.resetPassword() }.disabled(!vm.canResetPassword)
            }
        }
        .navigationTitle("Account Recovery")
        .toolbar { ToolbarItem(placement: .navigationBarTrailing) { Button("Done") { dismiss() } } }
        .onAppear { vm.loadRecoveryState() }
        .alert("Success", isPresented: $vm.showSuccess) { Button("OK") {} } message: { Text(vm.successMessage) }
        .alert("Error", isPresented: $vm.showError) { Button("OK") {} } message: { Text(vm.errorMessage) }
    }
}

final class AccountRecoveryViewModel: ObservableObject {
    @Published var recoveryEmail = ""
    @Published var emailVerified = false
    @Published var recoveryPhone = ""
    @Published var phoneVerified = false
    @Published var backupCodes: [String] = []
    @Published var newPassword = ""
    @Published var confirmPassword = ""

    @Published var showSuccess = false
    @Published var showError = false
    @Published var successMessage = ""
    @Published var errorMessage = ""

    private let grpcClient: SonetGRPCClient = SonetGRPCClient(configuration: .development)

    var canVerifyEmail: Bool { recoveryEmail.contains("@") && recoveryEmail.contains(".") }
    var canVerifyPhone: Bool { recoveryPhone.count >= 7 }
    var canResetPassword: Bool { newPassword.count >= 8 && newPassword == confirmPassword }

    func loadRecoveryState() {
        Task {
            do {
                let req = GetRecoveryStateRequest.newBuilder().setUserId("current_user").build()
                let res = try await grpcClient.getRecoveryState(request: req)
                if res.success {
                    await MainActor.run {
                        recoveryEmail = res.state.email
                        emailVerified = res.state.emailVerified
                        recoveryPhone = res.state.phone
                        phoneVerified = res.state.phoneVerified
                        backupCodes = res.state.backupCodes
                    }
                }
            } catch {
                await MainActor.run { errorMessage = error.localizedDescription; showError = true }
            }
        }
    }

    func verifyEmail() {
        Task {
            do {
                let req = VerifyRecoveryEmailRequest.newBuilder().setUserId("current_user").setEmail(recoveryEmail).build()
                let res = try await grpcClient.verifyRecoveryEmail(request: req)
                await MainActor.run {
                    if res.success { emailVerified = true; successMessage = "Email verified"; showSuccess = true }
                    else { errorMessage = res.errorMessage; showError = true }
                }
            } catch { await MainActor.run { errorMessage = error.localizedDescription; showError = true } }
        }
    }

    func verifyPhone() {
        Task {
            do {
                let req = VerifyRecoveryPhoneRequest.newBuilder().setUserId("current_user").setPhone(recoveryPhone).build()
                let res = try await grpcClient.verifyRecoveryPhone(request: req)
                await MainActor.run {
                    if res.success { phoneVerified = true; successMessage = "Phone verified"; showSuccess = true }
                    else { errorMessage = res.errorMessage; showError = true }
                }
            } catch { await MainActor.run { errorMessage = error.localizedDescription; showError = true } }
        }
    }

    func generateBackupCodes() {
        Task {
            do {
                let req = GenerateBackupCodesRequest.newBuilder().setUserId("current_user").build()
                let res = try await grpcClient.generateBackupCodes(request: req)
                await MainActor.run {
                    if res.success { backupCodes = res.codes; successMessage = "Backup codes generated"; showSuccess = true }
                    else { errorMessage = res.errorMessage; showError = true }
                }
            } catch { await MainActor.run { errorMessage = error.localizedDescription; showError = true } }
        }
    }

    func regenerateBackupCodes() {
        generateBackupCodes()
    }

    func resetPassword() {
        Task {
            do {
                let req = ResetPasswordRequest.newBuilder().setUserId("current_user").setNewPassword(newPassword).build()
                let res = try await grpcClient.resetPassword(request: req)
                await MainActor.run {
                    if res.success { successMessage = "Password updated"; showSuccess = true; newPassword = ""; confirmPassword = "" }
                    else { errorMessage = res.errorMessage; showError = true }
                }
            } catch { await MainActor.run { errorMessage = error.localizedDescription; showError = true } }
        }
    }
}

// gRPC placeholders
struct GetRecoveryStateRequest { let userId: String; static func newBuilder() -> GetRecoveryStateRequestBuilder { GetRecoveryStateRequestBuilder() } }
class GetRecoveryStateRequestBuilder { private var userId: String = ""; func setUserId(_ userId: String) -> GetRecoveryStateRequestBuilder { self.userId = userId; return self } ; func build() -> GetRecoveryStateRequest { GetRecoveryStateRequest(userId: userId) } }
struct GetRecoveryStateResponse { let success: Bool; let state: GRPCRecoveryState; let errorMessage: String }
struct VerifyRecoveryEmailRequest { let userId: String; let email: String; static func newBuilder() -> VerifyRecoveryEmailRequestBuilder { VerifyRecoveryEmailRequestBuilder() } }
class VerifyRecoveryEmailRequestBuilder { private var userId: String = ""; private var email: String = ""; func setUserId(_ userId: String) -> VerifyRecoveryEmailRequestBuilder { self.userId = userId; return self } ; func setEmail(_ email: String) -> VerifyRecoveryEmailRequestBuilder { self.email = email; return self } ; func build() -> VerifyRecoveryEmailRequest { VerifyRecoveryEmailRequest(userId: userId, email: email) } }
struct VerifyRecoveryEmailResponse { let success: Bool; let errorMessage: String }
struct VerifyRecoveryPhoneRequest { let userId: String; let phone: String; static func newBuilder() -> VerifyRecoveryPhoneRequestBuilder { VerifyRecoveryPhoneRequestBuilder() } }
class VerifyRecoveryPhoneRequestBuilder { private var userId: String = ""; private var phone: String = ""; func setUserId(_ userId: String) -> VerifyRecoveryPhoneRequestBuilder { self.userId = userId; return self } ; func setPhone(_ phone: String) -> VerifyRecoveryPhoneRequestBuilder { self.phone = phone; return self } ; func build() -> VerifyRecoveryPhoneRequest { VerifyRecoveryPhoneRequest(userId: userId, phone: phone) } }
struct VerifyRecoveryPhoneResponse { let success: Bool; let errorMessage: String }
struct GenerateBackupCodesRequest { let userId: String; static func newBuilder() -> GenerateBackupCodesRequestBuilder { GenerateBackupCodesRequestBuilder() } }
class GenerateBackupCodesRequestBuilder { private var userId: String = ""; func setUserId(_ userId: String) -> GenerateBackupCodesRequestBuilder { self.userId = userId; return self } ; func build() -> GenerateBackupCodesRequest { GenerateBackupCodesRequest(userId: userId) } }
struct GenerateBackupCodesResponse { let success: Bool; let codes: [String]; let errorMessage: String }
struct ResetPasswordRequest { let userId: String; let newPassword: String; static func newBuilder() -> ResetPasswordRequestBuilder { ResetPasswordRequestBuilder() } }
class ResetPasswordRequestBuilder { private var userId: String = ""; private var newPassword: String = ""; func setUserId(_ userId: String) -> ResetPasswordRequestBuilder { self.userId = userId; return self } ; func setNewPassword(_ password: String) -> ResetPasswordRequestBuilder { self.newPassword = password; return self } ; func build() -> ResetPasswordRequest { ResetPasswordRequest(userId: userId, newPassword: newPassword) } }
struct ResetPasswordResponse { let success: Bool; let errorMessage: String }
struct GRPCRecoveryState { let email: String; let emailVerified: Bool; let phone: String; let phoneVerified: Bool; let backupCodes: [String] }

struct HiddenPostsView: View {
    var body: some View {
        Text("Hidden Posts")
            .navigationTitle("Hidden Posts")
    }
}

struct HiddenUsersView: View {
    var body: some View {
        Text("Hidden Users")
            .navigationTitle("Hidden Users")
    }
}

struct HiddenHashtagsView: View {
    var body: some View {
        Text("Hidden Hashtags")
            .navigationTitle("Hidden Hashtags")
    }
}