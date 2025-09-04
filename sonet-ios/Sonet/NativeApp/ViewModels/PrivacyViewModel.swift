import Foundation
import Combine

@MainActor
class PrivacyViewModel: ObservableObject {
    // MARK: - Published Properties
    @Published var accountVisibility: AccountVisibility = .public
    @Published var emailVisibility: EmailVisibility = .private
    @Published var phoneVisibility: PhoneVisibility = .private
    @Published var profilePictureVisibility: ProfilePictureVisibility = .public
    @Published var bioVisibility: BioVisibility = .public
    @Published var followerCountVisibility: FollowerCountVisibility = .public
    @Published var defaultPostPrivacy: PostPrivacy = .public
    @Published var mediaVisibility: MediaVisibility = .public
    @Published var storyPrivacy: StoryPrivacy = .public
    
    // Location privacy
    @Published var locationServicesEnabled: Bool = false
    @Published var sharePreciseLocation: Bool = false
    @Published var allowLocationOnPosts: Bool = false
    @Published var defaultGeotagPrivacy: PostPrivacy = .friends
    @Published var locationHistory: [LocationHistoryItem] = []

    // Third-party apps
    @Published var connectedApps: [ConnectedApp] = []

    // Data export
    @Published var previousExports: [DataExportRecord] = []
    
    // MARK: - UI State
    @Published var isLoading = false
    @Published var showUpdateAlert = false
    @Published var showErrorAlert = false
    @Published var updateMessage = ""
    @Published var errorMessage = ""
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private let userDefaults = UserDefaults.standard
    private var cancellables = Set<AnyCancellable>()
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient = SonetGRPCClient(configuration: .development)) {
        self.grpcClient = grpcClient
        loadLocalSettings()
    }
    
    // MARK: - Public Methods
    func loadPrivacySettings() {
        Task { await loadPrivacySettingsFromServer() }
    }
    
    func updateAccountVisibility() { Task { await updatePrivacySetting(type: .accountVisibility, value: accountVisibility.rawValue) } }
    func updateEmailVisibility() { Task { await updatePrivacySetting(type: .emailVisibility, value: emailVisibility.rawValue) } }
    func updatePhoneVisibility() { Task { await updatePrivacySetting(type: .phoneVisibility, value: phoneVisibility.rawValue) } }
    func updateProfilePictureVisibility() { Task { await updatePrivacySetting(type: .profilePictureVisibility, value: profilePictureVisibility.rawValue) } }
    func updateBioVisibility() { Task { await updatePrivacySetting(type: .bioVisibility, value: bioVisibility.rawValue) } }
    func updateFollowerCountVisibility() { Task { await updatePrivacySetting(type: .followerCountVisibility, value: followerCountVisibility.rawValue) } }
    func updateDefaultPostPrivacy() { Task { await updatePrivacySetting(type: .defaultPostPrivacy, value: defaultPostPrivacy.rawValue) } }
    func updateMediaVisibility() { Task { await updatePrivacySetting(type: .mediaVisibility, value: mediaVisibility.rawValue) } }
    func updateStoryPrivacy() { Task { await updatePrivacySetting(type: .storyPrivacy, value: storyPrivacy.rawValue) } }

    // Location
    func loadLocationPrivacy() { Task { await fetchLocationPrivacy() } }
    func updateLocationServicesEnabled() { Task { await updatePrivacySetting(type: .locationServicesEnabled, value: locationServicesEnabled.description) } }
    func updateSharePreciseLocation() { Task { await updatePrivacySetting(type: .sharePreciseLocation, value: sharePreciseLocation.description) } }
    func updateAllowLocationOnPosts() { Task { await updatePrivacySetting(type: .allowLocationOnPosts, value: allowLocationOnPosts.description) } }
    func updateDefaultGeotagPrivacy() { Task { await updatePrivacySetting(type: .defaultGeotagPrivacy, value: defaultGeotagPrivacy.rawValue) } }
    func clearLocationHistory() { Task { await clearLocationHistoryOnServer() } }

    // Third-party apps
    func loadConnectedApps() { Task { await fetchConnectedApps() } }
    func revokeThirdPartyApp(appId: String) { Task { await revokeApp(appId: appId) } }

    // Data export
    func loadPreviousExports() { Task { await fetchPreviousExports() } }
    func requestDataExport(includeMedia: Bool, includeMessages: Bool, includeConnections: Bool, format: DataExportFormat, range: DataExportRange) {
        Task {
            await requestExport(includeMedia: includeMedia, includeMessages: includeMessages, includeConnections: includeConnections, format: format.rawValue, range: range.rawValue)
        }
    }
    func downloadExport(exportId: String) { Task { await downloadExportFromServer(exportId: exportId) } }
    
    func clearError() {
        errorMessage = ""
        showErrorAlert = false
    }
    
    // MARK: - Private Methods
    private func loadLocalSettings() {
        if let accountVisibilityString = userDefaults.string(forKey: "privacy_account_visibility"), let visibility = AccountVisibility(rawValue: accountVisibilityString) { accountVisibility = visibility }
        if let emailVisibilityString = userDefaults.string(forKey: "privacy_email_visibility"), let visibility = EmailVisibility(rawValue: emailVisibilityString) { emailVisibility = visibility }
        if let phoneVisibilityString = userDefaults.string(forKey: "privacy_phone_visibility"), let visibility = PhoneVisibility(rawValue: phoneVisibilityString) { phoneVisibility = visibility }
        if let profilePictureVisibilityString = userDefaults.string(forKey: "privacy_profile_picture_visibility"), let visibility = ProfilePictureVisibility(rawValue: profilePictureVisibilityString) { profilePictureVisibility = visibility }
        if let bioVisibilityString = userDefaults.string(forKey: "privacy_bio_visibility"), let visibility = BioVisibility(rawValue: bioVisibilityString) { bioVisibility = visibility }
        if let followerCountVisibilityString = userDefaults.string(forKey: "privacy_follower_count_visibility"), let visibility = FollowerCountVisibility(rawValue: followerCountVisibilityString) { followerCountVisibility = visibility }
        if let defaultPostPrivacyString = userDefaults.string(forKey: "privacy_default_post_privacy"), let privacy = PostPrivacy(rawValue: defaultPostPrivacyString) { defaultPostPrivacy = privacy }
        if let mediaVisibilityString = userDefaults.string(forKey: "privacy_media_visibility"), let visibility = MediaVisibility(rawValue: mediaVisibilityString) { mediaVisibility = visibility }
        if let storyPrivacyString = userDefaults.string(forKey: "privacy_story_privacy"), let privacy = StoryPrivacy(rawValue: storyPrivacyString) { storyPrivacy = privacy }
        locationServicesEnabled = userDefaults.bool(forKey: "privacy_location_services_enabled")
        sharePreciseLocation = userDefaults.bool(forKey: "privacy_share_precise_location")
        allowLocationOnPosts = userDefaults.bool(forKey: "privacy_allow_location_on_posts")
        if let geotag = userDefaults.string(forKey: "privacy_default_geotag_privacy"), let gp = PostPrivacy(rawValue: geotag) { defaultGeotagPrivacy = gp }
    }
    
    private func loadPrivacySettingsFromServer() async {
        isLoading = true
        do {
            let request = GetPrivacySettingsRequest.newBuilder().setUserId("current_user").build()
            let response = try await grpcClient.getPrivacySettings(request: request)
            if response.success {
                await MainActor.run {
                    updatePrivacySettingsFromResponse(response.settings)
                    showUpdateAlert = true
                    updateMessage = "Privacy settings loaded successfully"
                }
            } else {
                await MainActor.run { errorMessage = response.errorMessage; showErrorAlert = true }
            }
        } catch {
            await MainActor.run { errorMessage = "Failed to load privacy settings: \(error.localizedDescription)"; showErrorAlert = true }
        }
        await MainActor.run { isLoading = false }
    }
    
    private func updatePrivacySetting(type: PrivacySettingType, value: String) async {
        do {
            let request = UpdatePrivacySettingRequest.newBuilder().setUserId("current_user").setSettingType(type.rawValue).setValue(value).build()
            let response = try await grpcClient.updatePrivacySetting(request: request)
            if response.success {
                await MainActor.run { saveLocalSetting(type: type, value: value); showUpdateAlert = true; updateMessage = "Privacy setting updated successfully" }
            } else {
                await MainActor.run { errorMessage = response.errorMessage; showErrorAlert = true }
            }
        } catch {
            await MainActor.run { errorMessage = "Failed to update privacy setting: \(error.localizedDescription)"; showErrorAlert = true }
        }
    }
    
    private func updatePrivacySettingsFromResponse(_ settings: GRPCPrivacySettings) {
        if let v = AccountVisibility(rawValue: settings.accountVisibility) { accountVisibility = v }
        if let v = EmailVisibility(rawValue: settings.emailVisibility) { emailVisibility = v }
        if let v = PhoneVisibility(rawValue: settings.phoneVisibility) { phoneVisibility = v }
        if let v = ProfilePictureVisibility(rawValue: settings.profilePictureVisibility) { profilePictureVisibility = v }
        if let v = BioVisibility(rawValue: settings.bioVisibility) { bioVisibility = v }
        if let v = FollowerCountVisibility(rawValue: settings.followerCountVisibility) { followerCountVisibility = v }
        if let v = PostPrivacy(rawValue: settings.defaultPostPrivacy) { defaultPostPrivacy = v }
        if let v = MediaVisibility(rawValue: settings.mediaVisibility) { mediaVisibility = v }
        if let v = StoryPrivacy(rawValue: settings.storyPrivacy) { storyPrivacy = v }
        if let gp = PostPrivacy(rawValue: settings.defaultGeotagPrivacy) { defaultGeotagPrivacy = gp }
        locationServicesEnabled = settings.locationServicesEnabled
        sharePreciseLocation = settings.sharePreciseLocation
        allowLocationOnPosts = settings.allowLocationOnPosts
        locationHistory = settings.locationHistory.map { LocationHistoryItem(id: $0.id, name: $0.name, timestamp: $0.timestamp) }
        connectedApps = settings.connectedApps.map { ConnectedApp(id: $0.id, name: $0.name, scopes: $0.scopes, lastUsed: $0.lastUsed) }
        saveAllLocalSettings()
    }
    
    private func saveLocalSetting(type: PrivacySettingType, value: String) {
        let key = "privacy_\(type.rawValue)"
        userDefaults.set(value, forKey: key)
    }
    
    private func saveAllLocalSettings() {
        saveLocalSetting(type: .accountVisibility, value: accountVisibility.rawValue)
        saveLocalSetting(type: .emailVisibility, value: emailVisibility.rawValue)
        saveLocalSetting(type: .phoneVisibility, value: phoneVisibility.rawValue)
        saveLocalSetting(type: .profilePictureVisibility, value: profilePictureVisibility.rawValue)
        saveLocalSetting(type: .bioVisibility, value: bioVisibility.rawValue)
        saveLocalSetting(type: .followerCountVisibility, value: followerCountVisibility.rawValue)
        saveLocalSetting(type: .defaultPostPrivacy, value: defaultPostPrivacy.rawValue)
        saveLocalSetting(type: .mediaVisibility, value: mediaVisibility.rawValue)
        saveLocalSetting(type: .storyPrivacy, value: storyPrivacy.rawValue)
        saveLocalSetting(type: .defaultGeotagPrivacy, value: defaultGeotagPrivacy.rawValue)
        userDefaults.set(locationServicesEnabled, forKey: "privacy_location_services_enabled")
        userDefaults.set(sharePreciseLocation, forKey: "privacy_share_precise_location")
        userDefaults.set(allowLocationOnPosts, forKey: "privacy_allow_location_on_posts")
    }

    // MARK: - Location gRPC
    private func fetchLocationPrivacy() async {
        do {
            let request = GetLocationPrivacyRequest.newBuilder().setUserId("current_user").build()
            let response = try await grpcClient.getLocationPrivacy(request: request)
            if response.success {
                await MainActor.run {
                    locationServicesEnabled = response.settings.locationServicesEnabled
                    sharePreciseLocation = response.settings.sharePreciseLocation
                    allowLocationOnPosts = response.settings.allowLocationOnPosts
                    if let gp = PostPrivacy(rawValue: response.settings.defaultGeotagPrivacy) { defaultGeotagPrivacy = gp }
                    locationHistory = response.settings.locationHistory.map { LocationHistoryItem(id: $0.id, name: $0.name, timestamp: $0.timestamp) }
                }
            } else {
                await MainActor.run { errorMessage = response.errorMessage; showErrorAlert = true }
            }
        } catch {
            await MainActor.run { errorMessage = "Failed to load location privacy: \(error.localizedDescription)"; showErrorAlert = true }
        }
    }

    private func clearLocationHistoryOnServer() async {
        do {
            let request = ClearLocationHistoryRequest.newBuilder().setUserId("current_user").build()
            let response = try await grpcClient.clearLocationHistory(request: request)
            if response.success {
                await MainActor.run { locationHistory = []; showUpdateAlert = true; updateMessage = "Location history cleared" }
            } else {
                await MainActor.run { errorMessage = response.errorMessage; showErrorAlert = true }
            }
        } catch {
            await MainActor.run { errorMessage = "Failed to clear location history: \(error.localizedDescription)"; showErrorAlert = true }
        }
    }

    // MARK: - Third-party apps gRPC
    private func fetchConnectedApps() async {
        do {
            let request = GetConnectedAppsRequest.newBuilder().setUserId("current_user").build()
            let response = try await grpcClient.getConnectedApps(request: request)
            if response.success {
                await MainActor.run { connectedApps = response.apps.map { ConnectedApp(id: $0.id, name: $0.name, scopes: $0.scopes, lastUsed: $0.lastUsed) } }
            } else {
                await MainActor.run { errorMessage = response.errorMessage; showErrorAlert = true }
            }
        } catch {
            await MainActor.run { errorMessage = "Failed to load connected apps: \(error.localizedDescription)"; showErrorAlert = true }
        }
    }

    private func revokeApp(appId: String) async {
        do {
            let request = RevokeConnectedAppRequest.newBuilder().setUserId("current_user").setAppId(appId).build()
            let response = try await grpcClient.revokeConnectedApp(request: request)
            if response.success {
                await MainActor.run {
                    connectedApps.removeAll { $0.id == appId }
                    showUpdateAlert = true
                    updateMessage = "App access revoked"
                }
            } else {
                await MainActor.run { errorMessage = response.errorMessage; showErrorAlert = true }
            }
        } catch {
            await MainActor.run { errorMessage = "Failed to revoke app: \(error.localizedDescription)"; showErrorAlert = true }
        }
    }

    // MARK: - Data export gRPC
    private func fetchPreviousExports() async {
        do {
            let request = GetDataExportsRequest.newBuilder().setUserId("current_user").build()
            let response = try await grpcClient.getDataExports(request: request)
            if response.success {
                await MainActor.run { previousExports = response.exports.map { DataExportRecord(id: $0.id, requestedAt: $0.requestedAt, status: DataExportStatus(rawValue: $0.status) ?? .processing) } }
            }
        } catch {
            await MainActor.run { errorMessage = "Failed to load exports: \(error.localizedDescription)"; showErrorAlert = true }
        }
    }

    private func requestExport(includeMedia: Bool, includeMessages: Bool, includeConnections: Bool, format: String, range: String) async {
        do {
            let request = RequestDataExportRequest.newBuilder()
                .setUserId("current_user")
                .setIncludeMedia(includeMedia)
                .setIncludeMessages(includeMessages)
                .setIncludeConnections(includeConnections)
                .setFormat(format)
                .setRange(range)
                .build()
            let response = try await grpcClient.requestDataExport(request: request)
            if response.success {
                await MainActor.run { showUpdateAlert = true; updateMessage = "Export requested. You'll be notified when it's ready."; await fetchPreviousExports() }
            } else {
                await MainActor.run { errorMessage = response.errorMessage; showErrorAlert = true }
            }
        } catch {
            await MainActor.run { errorMessage = "Failed to request export: \(error.localizedDescription)"; showErrorAlert = true }
        }
    }

    private func downloadExportFromServer(exportId: String) async {
        do {
            let request = DownloadDataExportRequest.newBuilder().setUserId("current_user").setExportId(exportId).build()
            let response = try await grpcClient.downloadDataExport(request: request)
            if response.success {
                await MainActor.run { showUpdateAlert = true; updateMessage = "Download started" }
            } else {
                await MainActor.run { errorMessage = response.errorMessage; showErrorAlert = true }
            }
        } catch {
            await MainActor.run { errorMessage = "Failed to download export: \(error.localizedDescription)"; showErrorAlert = true }
        }
    }
}

// MARK: - Privacy Setting Types
enum PrivacySettingType: String, CaseIterable {
    case accountVisibility = "account_visibility"
    case emailVisibility = "email_visibility"
    case phoneVisibility = "phone_visibility"
    case profilePictureVisibility = "profile_picture_visibility"
    case bioVisibility = "bio_visibility"
    case followerCountVisibility = "follower_count_visibility"
    case defaultPostPrivacy = "default_post_privacy"
    case mediaVisibility = "media_visibility"
    case storyPrivacy = "story_privacy"
    case locationServicesEnabled = "location_services_enabled"
    case sharePreciseLocation = "share_precise_location"
    case allowLocationOnPosts = "allow_location_on_posts"
    case defaultGeotagPrivacy = "default_geotag_privacy"
}

// MARK: - Local Models
struct LocationHistoryItem: Identifiable, Codable { let id: String; let name: String; let timestamp: Date }
struct ConnectedApp: Identifiable, Codable { let id: String; let name: String; let scopes: [String]; let lastUsed: Date }
struct DataExportRecord: Identifiable, Codable { let id: String; let requestedAt: Date; let status: DataExportStatus }
enum DataExportStatus: String, Codable { case processing, ready, failed; var displayName: String { switch self { case .processing: return "Processing"; case .ready: return "Ready"; case .failed: return "Failed" } } }

// MARK: - gRPC Placeholders
struct GetPrivacySettingsRequest { let userId: String; static func newBuilder() -> GetPrivacySettingsRequestBuilder { GetPrivacySettingsRequestBuilder() } }
class GetPrivacySettingsRequestBuilder { private var userId: String = ""; func setUserId(_ userId: String) -> GetPrivacySettingsRequestBuilder { self.userId = userId; return self } ; func build() -> GetPrivacySettingsRequest { GetPrivacySettingsRequest(userId: userId) } }
struct GetPrivacySettingsResponse { let success: Bool; let settings: GRPCPrivacySettings; let errorMessage: String }
struct UpdatePrivacySettingRequest { let userId: String; let settingType: String; let value: String; static func newBuilder() -> UpdatePrivacySettingRequestBuilder { UpdatePrivacySettingRequestBuilder() } }
class UpdatePrivacySettingRequestBuilder { private var userId: String = ""; private var settingType: String = ""; private var value: String = ""; func setUserId(_ userId: String) -> UpdatePrivacySettingRequestBuilder { self.userId = userId; return self } ; func setSettingType(_ settingType: String) -> UpdatePrivacySettingRequestBuilder { self.settingType = settingType; return self } ; func setValue(_ value: String) -> UpdatePrivacySettingRequestBuilder { self.value = value; return self } ; func build() -> UpdatePrivacySettingRequest { UpdatePrivacySettingRequest(userId: userId, settingType: settingType, value: value) } }
struct UpdatePrivacySettingResponse { let success: Bool; let errorMessage: String }
struct GRPCPrivacySettings {
    let accountVisibility: String
    let emailVisibility: String
    let phoneVisibility: String
    let profilePictureVisibility: String
    let bioVisibility: String
    let followerCountVisibility: String
    let defaultPostPrivacy: String
    let mediaVisibility: String
    let storyPrivacy: String
    // New
    let locationServicesEnabled: Bool
    let sharePreciseLocation: Bool
    let allowLocationOnPosts: Bool
    let defaultGeotagPrivacy: String
    let locationHistory: [GRPCLocationHistoryItem]
    let connectedApps: [GRPCConnectedApp]
}
struct GRPCLocationPrivacySettings { let locationServicesEnabled: Bool; let sharePreciseLocation: Bool; let allowLocationOnPosts: Bool; let defaultGeotagPrivacy: String; let locationHistory: [GRPCLocationHistoryItem] }
struct GRPCLocationHistoryItem { let id: String; let name: String; let timestamp: Date }
struct GRPCConnectedApp { let id: String; let name: String; let scopes: [String]; let lastUsed: Date }

struct GetLocationPrivacyRequest { let userId: String; static func newBuilder() -> GetLocationPrivacyRequestBuilder { GetLocationPrivacyRequestBuilder() } }
class GetLocationPrivacyRequestBuilder { private var userId: String = ""; func setUserId(_ userId: String) -> GetLocationPrivacyRequestBuilder { self.userId = userId; return self } ; func build() -> GetLocationPrivacyRequest { GetLocationPrivacyRequest(userId: userId) } }
struct GetLocationPrivacyResponse { let success: Bool; let settings: GRPCLocationPrivacySettings; let errorMessage: String }
struct ClearLocationHistoryRequest { let userId: String; static func newBuilder() -> ClearLocationHistoryRequestBuilder { ClearLocationHistoryRequestBuilder() } }
class ClearLocationHistoryRequestBuilder { private var userId: String = ""; func setUserId(_ userId: String) -> ClearLocationHistoryRequestBuilder { self.userId = userId; return self } ; func build() -> ClearLocationHistoryRequest { ClearLocationHistoryRequest(userId: userId) } }
struct ClearLocationHistoryResponse { let success: Bool; let errorMessage: String }

struct GetConnectedAppsRequest { let userId: String; static func newBuilder() -> GetConnectedAppsRequestBuilder { GetConnectedAppsRequestBuilder() } }
class GetConnectedAppsRequestBuilder { private var userId: String = ""; func setUserId(_ userId: String) -> GetConnectedAppsRequestBuilder { self.userId = userId; return self } ; func build() -> GetConnectedAppsRequest { GetConnectedAppsRequest(userId: userId) } }
struct GetConnectedAppsResponse { let success: Bool; let apps: [GRPCConnectedApp]; let errorMessage: String }
struct RevokeConnectedAppRequest { let userId: String; let appId: String; static func newBuilder() -> RevokeConnectedAppRequestBuilder { RevokeConnectedAppRequestBuilder() } }
class RevokeConnectedAppRequestBuilder { private var userId: String = ""; private var appId: String = ""; func setUserId(_ userId: String) -> RevokeConnectedAppRequestBuilder { self.userId = userId; return self } ; func setAppId(_ appId: String) -> RevokeConnectedAppRequestBuilder { self.appId = appId; return self } ; func build() -> RevokeConnectedAppRequest { RevokeConnectedAppRequest(userId: userId, appId: appId) } }
struct RevokeConnectedAppResponse { let success: Bool; let errorMessage: String }

struct GetDataExportsRequest { let userId: String; static func newBuilder() -> GetDataExportsRequestBuilder { GetDataExportsRequestBuilder() } }
class GetDataExportsRequestBuilder { private var userId: String = ""; func setUserId(_ userId: String) -> GetDataExportsRequestBuilder { self.userId = userId; return self } ; func build() -> GetDataExportsRequest { GetDataExportsRequest(userId: userId) } }
struct GetDataExportsResponse { let success: Bool; let exports: [GRPCDataExportRecord]; let errorMessage: String }
struct RequestDataExportRequest { let userId: String; let includeMedia: Bool; let includeMessages: Bool; let includeConnections: Bool; let format: String; let range: String; static func newBuilder() -> RequestDataExportRequestBuilder { RequestDataExportRequestBuilder() } }
class RequestDataExportRequestBuilder { private var userId: String = ""; private var includeMedia: Bool = true; private var includeMessages: Bool = true; private var includeConnections: Bool = true; private var format: String = "json"; private var range: String = "all"; func setUserId(_ userId: String) -> RequestDataExportRequestBuilder { self.userId = userId; return self } ; func setIncludeMedia(_ includeMedia: Bool) -> RequestDataExportRequestBuilder { self.includeMedia = includeMedia; return self } ; func setIncludeMessages(_ includeMessages: Bool) -> RequestDataExportRequestBuilder { self.includeMessages = includeMessages; return self } ; func setIncludeConnections(_ includeConnections: Bool) -> RequestDataExportRequestBuilder { self.includeConnections = includeConnections; return self } ; func setFormat(_ format: String) -> RequestDataExportRequestBuilder { self.format = format; return self } ; func setRange(_ range: String) -> RequestDataExportRequestBuilder { self.range = range; return self } ; func build() -> RequestDataExportRequest { RequestDataExportRequest(userId: userId, includeMedia: includeMedia, includeMessages: includeMessages, includeConnections: includeConnections, format: format, range: range) } }
struct RequestDataExportResponse { let success: Bool; let errorMessage: String }
struct DownloadDataExportRequest { let userId: String; let exportId: String; static func newBuilder() -> DownloadDataExportRequestBuilder { DownloadDataExportRequestBuilder() } }
class DownloadDataExportRequestBuilder { private var userId: String = ""; private var exportId: String = ""; func setUserId(_ userId: String) -> DownloadDataExportRequestBuilder { self.userId = userId; return self } ; func setExportId(_ exportId: String) -> DownloadDataExportRequestBuilder { self.exportId = exportId; return self } ; func build() -> DownloadDataExportRequest { DownloadDataExportRequest(userId: userId, exportId: exportId) } }
struct DownloadDataExportResponse { let success: Bool; let errorMessage: String }