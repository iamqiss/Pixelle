import SwiftUI

struct SettingsView: View {
    @StateObject private var viewModel: SettingsViewModel
    @Environment(\.dismiss) private var dismiss
    
    init(grpcClient: SonetGRPCClient) {
        _viewModel = StateObject(wrappedValue: SettingsViewModel(grpcClient: grpcClient))
    }
    
    var body: some View {
        NavigationView {
            List {
                // Profile Section
                ProfileSection(userProfile: viewModel.userProfile)
                
                // Account Settings
                AccountSettingsSection(
                    accountVisibility: $viewModel.accountVisibility,
                    onUpdateVisibility: { viewModel.updateAccountVisibility($0) }
                )
                
                // Appearance
                AppearanceSection(
                    isDarkMode: $viewModel.isDarkMode,
                    onToggleDarkMode: { viewModel.toggleDarkMode() }
                )
                
                // Notifications
                NotificationsSection(
                    notificationsEnabled: $viewModel.notificationsEnabled,
                    pushNotificationsEnabled: $viewModel.pushNotificationsEnabled,
                    emailNotificationsEnabled: $viewModel.emailNotificationsEnabled,
                    inAppNotificationsEnabled: $viewModel.inAppNotificationsEnabled,
                    onUpdateNotifications: { viewModel.updateNotificationSettings() }
                )
                
                // Content Preferences
                ContentPreferencesSection(
                    contentLanguage: $viewModel.contentLanguage,
                    contentFiltering: $viewModel.contentFiltering,
                    autoPlayVideos: $viewModel.autoPlayVideos,
                    onUpdateLanguage: { viewModel.updateContentLanguage($0) },
                    onUpdateFiltering: { viewModel.updateContentFiltering($0) },
                    onToggleAutoPlay: { viewModel.toggleAutoPlayVideos() }
                )
                
                // Data & Storage
                DataStorageSection(
                    dataUsage: $viewModel.dataUsage,
                    storageUsage: viewModel.storageUsage,
                    onUpdateDataUsage: { viewModel.updateDataUsage($0) },
                    onClearCache: { viewModel.clearCache() },
                    onDownloadData: { viewModel.downloadUserData() }
                )
                
                // Privacy & Safety
                PrivacySafetySection()
                
                // Help & Support
                HelpSupportSection()
                
                // Account Actions
                AccountActionsSection(
                    onLogout: { viewModel.showLogoutAlert = true },
                    onDeleteAccount: { viewModel.showDeleteAccountAlert = true }
                )
            }
            .navigationTitle("Settings")
            .navigationBarTitleDisplayMode(.large)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                }
            }
        }
        .alert("Log Out", isPresented: $viewModel.showLogoutAlert) {
            Button("Cancel", role: .cancel) { }
            Button("Log Out", role: .destructive) {
                viewModel.logout()
                dismiss()
            }
        } message: {
            Text("Are you sure you want to log out?")
        }
        .alert("Delete Account", isPresented: $viewModel.showDeleteAccountAlert) {
            Button("Cancel", role: .cancel) { }
            Button("Delete", role: .destructive) {
                viewModel.deleteAccount()
                dismiss()
            }
        } message: {
            Text("This action cannot be undone. All your data will be permanently deleted.")
        }
        .alert("Error", isPresented: .constant(viewModel.error != nil)) {
            Button("OK") {
                viewModel.clearError()
            }
        } message: {
            if let error = viewModel.error {
                Text(error)
            }
        }
    }
}

// MARK: - Profile Section
struct ProfileSection: View {
    let userProfile: UserProfile?
    
    var body: some View {
        Section {
            HStack(spacing: 16) {
                AsyncImage(url: URL(string: userProfile?.avatarUrl ?? "")) { image in
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
                .frame(width: 60, height: 60)
                .clipShape(Circle())
                
                VStack(alignment: .leading, spacing: 4) {
                    Text(userProfile?.displayName ?? "Loading...")
                        .font(.title3)
                        .fontWeight(.semibold)
                    
                    Text("@\(userProfile?.username ?? "username")")
                        .font(.subheadline)
                        .foregroundColor(.secondary)
                }
                
                Spacer()
                
                NavigationLink("Edit") {
                    Text("Edit Profile View")
                }
            }
            .padding(.vertical, 8)
        } header: {
            Text("Profile")
        }
    }
}

// MARK: - Account Settings Section
struct AccountSettingsSection: View {
    @Binding var accountVisibility: AccountVisibility
    let onUpdateVisibility: (AccountVisibility) -> Void
    
    var body: some View {
        Section {
            NavigationLink("Account Information") {
                AccountInformationView()
            }
            
            NavigationLink("Security") {
                SecurityView()
            }
            
            NavigationLink("Two-Factor Authentication") {
                TwoFactorView()
            }
            
            NavigationLink("Connected Accounts") {
                ConnectedAccountsView()
            }
            
            Picker("Account Visibility", selection: $accountVisibility) {
                ForEach(AccountVisibility.allCases, id: \.self) { visibility in
                    VStack(alignment: .leading) {
                        Text(visibility.displayName)
                        Text(visibility.description)
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                    .tag(visibility)
                }
            }
            .onChange(of: accountVisibility) { newValue in
                onUpdateVisibility(newValue)
            }
        } header: {
            Text("Account")
        } footer: {
            Text("Control who can see your profile and posts")
        }
    }
}

// MARK: - Appearance Section
struct AppearanceSection: View {
    @Binding var isDarkMode: Bool
    let onToggleDarkMode: () -> Void
    
    var body: some View {
        Section {
            Toggle("Dark Mode", isOn: $isDarkMode)
                .onChange(of: isDarkMode) { _ in
                    onToggleDarkMode()
                }
            
            NavigationLink("Theme") {
                ThemeView()
            }
            
            NavigationLink("Font Size") {
                FontSizeView()
            }
            
            NavigationLink("Color Scheme") {
                ColorSchemeView()
            }
        } header: {
            Text("Appearance")
        } footer: {
            Text("Customize how Sonet looks and feels")
        }
    }
}

// MARK: - Notifications Section
struct NotificationsSection: View {
    @Binding var notificationsEnabled: Bool
    @Binding var pushNotificationsEnabled: Bool
    @Binding var emailNotificationsEnabled: Bool
    @Binding var inAppNotificationsEnabled: Bool
    let onUpdateNotifications: () -> Void
    
    var body: some View {
        Section {
            Toggle("Notifications", isOn: $notificationsEnabled)
                .onChange(of: notificationsEnabled) { _ in
                    onUpdateNotifications()
                }
            
            if notificationsEnabled {
                Toggle("Push Notifications", isOn: $pushNotificationsEnabled)
                    .onChange(of: pushNotificationsEnabled) { _ in
                        onUpdateNotifications()
                    }
                
                Toggle("Email Notifications", isOn: $emailNotificationsEnabled)
                    .onChange(of: emailNotificationsEnabled) { _ in
                        onUpdateNotifications()
                    }
                
                Toggle("In-App Notifications", isOn: $inAppNotificationsEnabled)
                    .onChange(of: inAppNotificationsEnabled) { _ in
                        onUpdateNotifications()
                    }
                
                NavigationLink("Notification Preferences") {
                    NotificationPreferencesView()
                }
            }
        } header: {
            Text("Notifications")
        } footer: {
            Text("Choose how you want to be notified")
        }
    }
}

// MARK: - Content Preferences Section
struct ContentPreferencesSection: View {
    @Binding var contentLanguage: String
    @Binding var contentFiltering: ContentFiltering
    @Binding var autoPlayVideos: Bool
    let onUpdateLanguage: (String) -> Void
    let onUpdateFiltering: (ContentFiltering) -> Void
    let onToggleAutoPlay: () -> Void
    
    private let languages = ["English", "Spanish", "French", "German", "Italian", "Portuguese", "Russian", "Chinese", "Japanese", "Korean"]
    
    var body: some View {
        Section {
            Picker("Language", selection: $contentLanguage) {
                ForEach(languages, id: \.self) { language in
                    Text(language).tag(language)
                }
            }
            .onChange(of: contentLanguage) { newValue in
                onUpdateLanguage(newValue)
            }
            
            Picker("Content Filtering", selection: $contentFiltering) {
                ForEach(ContentFiltering.allCases, id: \.self) { filtering in
                    VStack(alignment: .leading) {
                        Text(filtering.displayName)
                        Text(filtering.description)
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                    .tag(filtering)
                }
            }
            .onChange(of: contentFiltering) { newValue in
                onUpdateFiltering(newValue)
            }
            
            Toggle("Auto-play Videos", isOn: $autoPlayVideos)
                .onChange(of: autoPlayVideos) { _ in
                    onToggleAutoPlay()
                }
            
            NavigationLink("Content Preferences") {
                ContentPreferencesDetailView()
            }
        } header: {
            Text("Content")
        } footer: {
            Text("Control what content you see and how it's displayed")
        }
    }
}

// MARK: - Data & Storage Section
struct DataStorageSection: View {
    @Binding var dataUsage: DataUsage
    let storageUsage: StorageUsage
    let onUpdateDataUsage: (DataUsage) -> Void
    let onClearCache: () -> Void
    let onDownloadData: () -> Void
    
    var body: some View {
        Section {
            Picker("Data Usage", selection: $dataUsage) {
                ForEach(DataUsage.allCases, id: \.self) { usage in
                    VStack(alignment: .leading) {
                        Text(usage.displayName)
                        Text(usage.description)
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                    .tag(usage)
                }
            }
            .onChange(of: dataUsage) { newValue in
                onUpdateDataUsage(newValue)
            }
            
            VStack(alignment: .leading, spacing: 8) {
                HStack {
                    Text("Storage")
                    Spacer()
                    Text("\(formatBytes(storageUsage.usedStorage)) of \(formatBytes(storageUsage.totalStorage))")
                        .foregroundColor(.secondary)
                }
                
                ProgressView(value: storageUsage.usagePercentage, total: 100)
                    .progressViewStyle(LinearProgressViewStyle())
                
                HStack {
                    Text("Cache: \(formatBytes(storageUsage.cacheSize))")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    
                    Spacer()
                    
                    Button("Clear") {
                        onClearCache()
                    }
                    .font(.caption)
                }
            }
            
            Button("Download Your Data") {
                onDownloadData()
            }
        } header: {
            Text("Data & Storage")
        } footer: {
            Text("Manage your data usage and storage")
        }
    }
    
    private func formatBytes(_ bytes: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB]
        formatter.countStyle = .file
        return formatter.string(fromByteCount: bytes)
    }
}

// MARK: - Privacy & Safety Section
struct PrivacySafetySection: View {
    var body: some View {
        Section {
            NavigationLink("Privacy") {
                PrivacyView()
            }
            
            NavigationLink("Safety") {
                SafetyView()
            }
            
            NavigationLink("Blocked Accounts") {
                BlockedAccountsView()
            }
            
            NavigationLink("Muted Accounts") {
                MutedAccountsView()
            }
            
            NavigationLink("Content You've Hidden") {
                HiddenContentView()
            }
        } header: {
            Text("Privacy & Safety")
        } footer: {
            Text("Control your privacy and safety settings")
        }
    }
}

// MARK: - Help & Support Section
struct HelpSupportSection: View {
    var body: some View {
        Section {
            NavigationLink("Help Center") {
                HelpCenterView()
            }
            
            NavigationLink("Contact Support") {
                ContactSupportView()
            }
            
            NavigationLink("Report a Problem") {
                ReportProblemView()
            }
            
            NavigationLink("Terms of Service") {
                TermsOfServiceView()
            }
            
            NavigationLink("Privacy Policy") {
                PrivacyPolicyView()
            }
            
            NavigationLink("About Sonet") {
                AboutSonetView()
            }
        } header: {
            Text("Help & Support")
        } footer: {
            Text("Get help and learn more about Sonet")
        }
    }
}

// MARK: - Account Actions Section
struct AccountActionsSection: View {
    let onLogout: () -> Void
    let onDeleteAccount: () -> Void
    
    var body: some View {
        Section {
            Button("Log Out") {
                onLogout()
            }
            .foregroundColor(.red)
            
            Button("Delete Account") {
                onDeleteAccount()
            }
            .foregroundColor(.red)
        } header: {
            Text("Account Actions")
        } footer: {
            Text("These actions cannot be undone")
        }
    }
}

// MARK: - Placeholder Views
struct AccountInformationView: View {
    var body: some View {
        Text("Account Information")
            .navigationTitle("Account Information")
    }
}

// MARK: - Security View
struct SecurityView: View {
    @StateObject private var viewModel = SecurityViewModel()
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        NavigationView {
            List {
                // Password Section
                Section {
                    NavigationLink("Change Password") {
                        ChangePasswordView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Password History") {
                        PasswordHistoryView(viewModel: viewModel)
                    }
                    
                    Toggle("Require Password for App Store", isOn: $viewModel.requirePasswordForPurchases)
                        .onChange(of: viewModel.requirePasswordForPurchases) { newValue in
                            viewModel.updatePasswordRequirement(for: .purchases, required: newValue)
                        }
                    
                    Toggle("Require Password for Settings", isOn: $viewModel.requirePasswordForSettings)
                        .onChange(of: viewModel.requirePasswordForSettings) { newValue in
                            viewModel.updatePasswordRequirement(for: .settings, required: newValue)
                        }
                } header: {
                    Text("Password")
                } footer: {
                    Text("Configure when your password is required for sensitive actions.")
                }
                
                // Biometric Authentication Section
                Section {
                    if viewModel.biometricType != .none {
                        Toggle("Use \(viewModel.biometricType.displayName)", isOn: $viewModel.biometricEnabled)
                            .onChange(of: viewModel.biometricEnabled) { newValue in
                                viewModel.updateBiometricAuthentication(enabled: newValue)
                            }
                        
                        if viewModel.biometricEnabled {
                            Text("You can use \(viewModel.biometricType.displayName) to unlock the app and authorize sensitive actions.")
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                    } else {
                        Text("Biometric authentication is not available on this device.")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                } header: {
                    Text("Biometric Authentication")
                } footer: {
                    Text("Use your fingerprint or face to quickly access the app.")
                }
                
                // Active Sessions Section
                Section {
                    if viewModel.activeSessions.isEmpty {
                        Text("No active sessions")
                            .foregroundColor(.secondary)
                    } else {
                        ForEach(viewModel.activeSessions) { session in
                            SessionRow(
                                session: session,
                                isCurrentSession: session.id == viewModel.currentSessionId,
                                onRevoke: {
                                    viewModel.revokeSession(session.id)
                                }
                            )
                        }
                    }
                    
                    Button("View All Sessions") {
                        viewModel.loadAllSessions()
                    }
                    .foregroundColor(.blue)
                } header: {
                    Text("Active Sessions")
                } footer: {
                    Text("Manage devices and browsers that are currently signed into your account.")
                }
                
                // Security Logs Section
                Section {
                    if viewModel.securityLogs.isEmpty {
                        Text("No security events")
                            .foregroundColor(.secondary)
                    } else {
                        ForEach(viewModel.securityLogs.prefix(5)) { log in
                            SecurityLogRow(log: log)
                        }
                        
                        if viewModel.securityLogs.count > 5 {
                            NavigationLink("View All Security Events (\(viewModel.securityLogs.count))") {
                                SecurityLogsView(viewModel: viewModel)
                            }
                        }
                    }
                } header: {
                    Text("Recent Security Events")
                } footer: {
                    Text("Track important security events like logins, password changes, and suspicious activity.")
                }
                
                // Security Alerts Section
                Section {
                    Toggle("Login Alerts", isOn: $viewModel.loginAlertsEnabled)
                        .onChange(of: viewModel.loginAlertsEnabled) { newValue in
                            viewModel.updateLoginAlerts(enabled: newValue)
                        }
                    
                    Toggle("Suspicious Activity Alerts", isOn: $viewModel.suspiciousActivityAlertsEnabled)
                        .onChange(of: viewModel.suspiciousActivityAlertsEnabled) { newValue in
                            viewModel.updateSuspiciousActivityAlerts(enabled: newValue)
                        }
                    
                    Toggle("Password Change Alerts", isOn: $viewModel.passwordChangeAlertsEnabled)
                        .onChange(of: viewModel.passwordChangeAlertsEnabled) { newValue in
                            viewModel.updatePasswordChangeAlerts(enabled: newValue)
                        }
                } header: {
                    Text("Security Alerts")
                } footer: {
                    Text("Get notified about important security events on your account.")
                }
                
                // Advanced Security Section
                Section {
                    NavigationLink("Trusted Devices") {
                        TrustedDevicesView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Login History") {
                        LoginHistoryView(viewModel: viewModel)
                    }
                    
                    NavigationLink("Security Questions") {
                        SecurityQuestionsView(viewModel: viewModel)
                    }
                } header: {
                    Text("Advanced Security")
                }
            }
            .navigationTitle("Security")
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
            viewModel.loadSecuritySettings()
            viewModel.loadActiveSessions()
            viewModel.loadSecurityLogs()
        }
        .alert("Error", isPresented: $viewModel.showErrorAlert) {
            Button("OK") {
                viewModel.clearError()
            }
        } message: {
            if let error = viewModel.error {
                Text(error)
            }
        }
    }
}

// MARK: - Session Row
struct SessionRow: View {
    let session: SecuritySession
    let isCurrentSession: Bool
    let onRevoke: () -> Void
    
    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: session.deviceType.iconName)
                .foregroundColor(.blue)
                .frame(width: 24, height: 24)
            
            VStack(alignment: .leading, spacing: 4) {
                HStack {
                    Text(session.deviceName)
                        .font(.subheadline)
                        .fontWeight(.medium)
                    
                    if isCurrentSession {
                        Text("(Current)")
                            .font(.caption)
                            .foregroundColor(.green)
                    }
                }
                
                Text(session.location)
                    .font(.caption)
                    .foregroundColor(.secondary)
                
                Text("Last active: \(session.lastActivity, style: .relative)")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            
            Spacer()
            
            if !isCurrentSession {
                Button("Revoke") {
                    onRevoke()
                }
                .buttonStyle(.bordered)
                .foregroundColor(.red)
            }
        }
        .padding(.vertical, 4)
    }
}

// MARK: - Security Log Row
struct SecurityLogRow: View {
    let log: SecurityLog
    
    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: log.eventType.iconName)
                .foregroundColor(log.eventType.color)
                .frame(width: 24, height: 24)
            
            VStack(alignment: .leading, spacing: 4) {
                Text(log.eventType.displayName)
                    .font(.subheadline)
                    .fontWeight(.medium)
                
                Text(log.description)
                    .font(.caption)
                    .foregroundColor(.secondary)
                
                Text(log.timestamp, style: .relative)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            
            Spacer()
            
            if log.requiresAttention {
                IconView(AppIcons.warning, size: 16, color: .orange)
            }
        }
        .padding(.vertical, 4)
    }
}

// MARK: - Two-Factor Authentication View
struct TwoFactorView: View {
    @StateObject private var viewModel = TwoFactorViewModel()
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        NavigationView {
            List {
                if viewModel.is2FAEnabled {
                    // 2FA Status Section
                    Section {
                        HStack {
                            IconView(AppIcons.shield, size: 16, color: .green)
                            Text("Two-Factor Authentication is enabled")
                                .fontWeight(.medium)
                        }
                        
                        if let lastUsed = viewModel.lastUsedDate {
                            HStack {
                                IconView(AppIcons.clock, size: 16, color: .secondary)
                                Text("Last used: \(lastUsed, style: .relative)")
                                    .foregroundColor(.secondary)
                            }
                        }
                    } header: {
                        Text("Status")
                    }
                    
                    // Backup Codes Section
                    Section {
                        if viewModel.backupCodes.isEmpty {
                            Button("Generate Backup Codes") {
                                viewModel.generateBackupCodes()
                            }
                            .foregroundColor(.blue)
                        } else {
                            ForEach(viewModel.backupCodes, id: \.self) { code in
                                HStack {
                                    Text(code)
                                        .font(.system(.body, design: .monospaced))
                                        .fontWeight(.medium)
                                    Spacer()
                                    if viewModel.usedBackupCodes.contains(code) {
                                        Text("Used")
                                            .font(.caption)
                                            .foregroundColor(.secondary)
                                    }
                                }
                            }
                            
                            Button("Regenerate Backup Codes") {
                                viewModel.showRegenerateAlert = true
                            }
                            .foregroundColor(.orange)
                        }
                    } header: {
                        Text("Backup Codes")
                    } footer: {
                        Text("Save these codes in a secure location. Each code can only be used once.")
                    }
                    
                    // Disable 2FA Section
                    Section {
                        Button("Disable Two-Factor Authentication") {
                            viewModel.showDisableAlert = true
                        }
                        .foregroundColor(.red)
                    } header: {
                        Text("Danger Zone")
                    } footer: {
                        Text("Disabling 2FA will make your account less secure.")
                    }
                } else {
                    // Setup 2FA Section
                    Section {
                        VStack(alignment: .leading, spacing: 16) {
                            Text("Two-Factor Authentication adds an extra layer of security to your account by requiring a verification code in addition to your password.")
                                .font(.body)
                                .foregroundColor(.secondary)
                            
                            Button("Set Up Two-Factor Authentication") {
                                viewModel.setup2FA()
                            }
                            .buttonStyle(.borderedProminent)
                        }
                        .padding(.vertical, 8)
                    } header: {
                        Text("Setup")
                    }
                }
                
                // Security Tips Section
                Section {
                    VStack(alignment: .leading, spacing: 12) {
                        SecurityTipRow(
                            icon: AppIcons.shield,
                            title: "Keep your backup codes safe",
                            description: "Store them in a password manager or secure location"
                        )
                        
                        SecurityTipRow(
                            icon: "iphone",
                            title: "Use a dedicated authenticator app",
                            description: "Apps like Google Authenticator or Authy are recommended"
                        )
                        
                        SecurityTipRow(
                            icon: AppIcons.warning,
                            title: "Never share your 2FA codes",
                            description: "These codes provide access to your account"
                        )
                    }
                    .padding(.vertical, 8)
                } header: {
                    Text("Security Tips")
                }
            }
            .navigationTitle("Two-Factor Authentication")
            .navigationBarTitleDisplayMode(.large)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                }
            }
        }
        .sheet(isPresented: $viewModel.showSetupSheet) {
            TwoFASetupSheet(viewModel: viewModel)
        }
        .alert("Regenerate Backup Codes", isPresented: $viewModel.showRegenerateAlert) {
            Button("Cancel", role: .cancel) { }
            Button("Regenerate", role: .destructive) {
                viewModel.generateBackupCodes()
            }
        } message: {
            Text("This will invalidate all existing backup codes. Are you sure?")
        }
        .alert("Disable 2FA", isPresented: $viewModel.showDisableAlert) {
            Button("Cancel", role: .cancel) { }
            Button("Disable", role: .destructive) {
                viewModel.disable2FA()
            }
        } message: {
            Text("This will make your account less secure. Are you sure?")
        }
        .alert("Error", isPresented: $viewModel.showErrorAlert) {
            Button("OK") {
                viewModel.clearError()
            }
        } message: {
            if let error = viewModel.error {
                Text(error)
            }
        }
    }
}

// MARK: - Security Tip Row
struct SecurityTipRow: View {
    let icon: String
    let title: String
    let description: String
    
    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: icon)
                .foregroundColor(.blue)
                .frame(width: 24, height: 24)
            
            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.subheadline)
                    .fontWeight(.medium)
                
                Text(description)
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            
            Spacer()
        }
    }
}

struct ThemeView: View {
    var body: some View {
        Text("Theme Settings")
            .navigationTitle("Theme")
    }
}

struct FontSizeView: View {
    var body: some View {
        Text("Font Size Settings")
            .navigationTitle("Font Size")
    }
}

struct ColorSchemeView: View {
    var body: some View {
        Text("Color Scheme Settings")
            .navigationTitle("Color Scheme")
    }
}

struct NotificationPreferencesView: View {
    var body: some View {
        Text("Notification Preferences")
            .navigationTitle("Notifications")
    }
}

struct ContentPreferencesDetailView: View {
    var body: some View {
        Text("Content Preferences")
            .navigationTitle("Content")
    }
}

struct PrivacyView: View {
    var body: some View {
        Text("Privacy Settings")
            .navigationTitle("Privacy")
    }
}

struct SafetyView: View {
    var body: some View {
        Text("Safety Settings")
            .navigationTitle("Safety")
    }
}

struct BlockedAccountsView: View {
    var body: some View {
        Text("Blocked Accounts")
            .navigationTitle("Blocked")
    }
}

struct MutedAccountsView: View {
    var body: some View {
        Text("Muted Accounts")
            .navigationTitle("Muted")
    }
}

struct HiddenContentView: View {
    var body: some View {
        Text("Hidden Content")
            .navigationTitle("Hidden")
    }
}

struct HelpCenterView: View {
    var body: some View {
        Text("Help Center")
            .navigationTitle("Help")
    }
}

struct ContactSupportView: View {
    var body: some View {
        Text("Contact Support")
            .navigationTitle("Support")
    }
}

struct ReportProblemView: View {
    var body: some View {
        Text("Report a Problem")
            .navigationTitle("Report")
    }
}

struct TermsOfServiceView: View {
    var body: some View {
        Text("Terms of Service")
            .navigationTitle("Terms")
    }
}

struct PrivacyPolicyView: View {
    var body: some View {
        Text("Privacy Policy")
            .navigationTitle("Privacy")
    }
}

struct AboutSonetView: View {
    var body: some View {
        VStack(spacing: 20) {
            IconView(AppIcons.network, size: 60, color: .accentColor)
            
            Text("Sonet")
                .font(.largeTitle)
                .fontWeight(.bold)
            
            Text("Version 1.0.0")
                .font(.subheadline)
                .foregroundColor(.secondary)
            
            Text("Connect, share, and discover with the world")
                .font(.body)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
        }
        .navigationTitle("About")
    }
}

// MARK: - Preview
struct SettingsView_Previews: PreviewProvider {
    static var previews: some View {
        SettingsView(
            grpcClient: SonetGRPCClient(configuration: .development)
        )
    }
}