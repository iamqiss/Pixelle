import Foundation
import Combine

@MainActor
class SafetyViewModel: ObservableObject {
    // MARK: - Published Properties
    @Published var contentFilteringLevel: ContentFilteringLevel = .medium
    @Published var showSensitiveContent = false
    @Published var warnBeforeSensitiveContent = true
    @Published var filterProfanity = true
    @Published var filterHateSpeech = true
    @Published var filterViolence = true
    @Published var sensitiveContentDisplayMode: SensitiveContentDisplayMode = .withWarning
    @Published var blurSensitiveImages = true
    @Published var muteSensitiveVideos = true
    @Published var showContentWarnings = true
    @Published var hideMutedUsersContent = true
    @Published var hideBlockedUsersContent = true
    @Published var hideSensitiveMedia = true
    @Published var mutedWords: Set<String> = []
    
    // MARK: - UI State
    @Published var isLoading = false
    @Published var showUpdateAlert = false
    @Published var showErrorAlert = false
    @Published var showLockAlert = false
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
    func loadSafetySettings() {
        Task {
            await loadSafetySettingsFromServer()
        }
    }
    
    func updateContentFilteringLevel() {
        Task {
            await updateSafetySetting(type: .contentFilteringLevel, value: contentFilteringLevel.rawValue)
        }
    }
    
    func updateSensitiveContentSetting() {
        Task {
            await updateSafetySetting(type: .showSensitiveContent, value: showSensitiveContent.description)
        }
    }
    
    func updateSensitiveContentWarning() {
        Task {
            await updateSafetySetting(type: .warnBeforeSensitiveContent, value: warnBeforeSensitiveContent.description)
        }
    }
    
    func updateProfanityFilter() {
        Task {
            await updateSafetySetting(type: .filterProfanity, value: filterProfanity.description)
        }
    }
    
    func updateHateSpeechFilter() {
        Task {
            await updateSafetySetting(type: .filterHateSpeech, value: filterHateSpeech.description)
        }
    }
    
    func updateViolenceFilter() {
        Task {
            await updateSafetySetting(type: .filterViolence, value: filterViolence.description)
        }
    }
    
    func updateSensitiveContentDisplayMode() {
        Task {
            await updateSafetySetting(type: .sensitiveContentDisplayMode, value: sensitiveContentDisplayMode.rawValue)
        }
    }
    
    func updateBlurSensitiveImages() {
        Task {
            await updateSafetySetting(type: .blurSensitiveImages, value: blurSensitiveImages.description)
        }
    }
    
    func updateMuteSensitiveVideos() {
        Task {
            await updateSafetySetting(type: .muteSensitiveVideos, value: muteSensitiveVideos.description)
        }
    }
    
    func updateShowContentWarnings() {
        Task {
            await updateSafetySetting(type: .showContentWarnings, value: showContentWarnings.description)
        }
    }
    
    func updateHideMutedUsersContent() {
        Task {
            await updateSafetySetting(type: .hideMutedUsersContent, value: hideMutedUsersContent.description)
        }
    }
    
    func updateHideBlockedUsersContent() {
        Task {
            await updateSafetySetting(type: .hideBlockedUsersContent, value: hideBlockedUsersContent.description)
        }
    }
    
    func updateHideSensitiveMedia() {
        Task {
            await updateSafetySetting(type: .hideSensitiveMedia, value: hideSensitiveMedia.description)
        }
    }
    
    func addMutedWord(_ word: String) {
        mutedWords.insert(word.lowercased())
        saveMutedWords()
        Task {
            await addMutedWordToServer(word)
        }
    }
    
    func removeMutedWord(_ word: String) {
        mutedWords.remove(word.lowercased())
        saveMutedWords()
        Task {
            await removeMutedWordFromServer(word)
        }
    }
    
    func lockAccountTemporarily() {
        Task {
            await lockAccount()
        }
    }
    
    func clearError() {
        errorMessage = ""
        showErrorAlert = false
    }
    
    // MARK: - Private Methods
    private func loadLocalSettings() {
        // Load settings from UserDefaults as fallback
        if let filteringLevelString = userDefaults.string(forKey: "safety_content_filtering_level"),
           let level = ContentFilteringLevel(rawValue: filteringLevelString) {
            contentFilteringLevel = level
        }
        
        showSensitiveContent = userDefaults.bool(forKey: "safety_show_sensitive_content")
        warnBeforeSensitiveContent = userDefaults.bool(forKey: "safety_warn_before_sensitive_content")
        filterProfanity = userDefaults.bool(forKey: "safety_filter_profanity")
        filterHateSpeech = userDefaults.bool(forKey: "safety_filter_hate_speech")
        filterViolence = userDefaults.bool(forKey: "safety_filter_violence")
        
        if let displayModeString = userDefaults.string(forKey: "safety_sensitive_content_display_mode"),
           let mode = SensitiveContentDisplayMode(rawValue: displayModeString) {
            sensitiveContentDisplayMode = mode
        }
        
        blurSensitiveImages = userDefaults.bool(forKey: "safety_blur_sensitive_images")
        muteSensitiveVideos = userDefaults.bool(forKey: "safety_mute_sensitive_videos")
        showContentWarnings = userDefaults.bool(forKey: "safety_show_content_warnings")
        hideMutedUsersContent = userDefaults.bool(forKey: "safety_hide_muted_users_content")
        hideBlockedUsersContent = userDefaults.bool(forKey: "safety_hide_blocked_users_content")
        hideSensitiveMedia = userDefaults.bool(forKey: "safety_hide_sensitive_media")
        
        // Load muted words
        if let mutedWordsArray = userDefaults.array(forKey: "safety_muted_words") as? [String] {
            mutedWords = Set(mutedWordsArray)
        }
    }
    
    private func loadSafetySettingsFromServer() async {
        isLoading = true
        
        do {
            let request = GetSafetySettingsRequest.newBuilder()
                .setUserId("current_user")
                .build()
            
            let response = try await grpcClient.getSafetySettings(request: request)
            
            if response.success {
                await MainActor.run {
                    updateSafetySettingsFromResponse(response.settings)
                    showUpdateAlert = true
                    updateMessage = "Safety settings loaded successfully"
                }
            } else {
                await MainActor.run {
                    errorMessage = response.errorMessage
                    showErrorAlert = true
                }
            }
        } catch {
            await MainActor.run {
                errorMessage = "Failed to load safety settings: \(error.localizedDescription)"
                showErrorAlert = true
            }
        }
        
        await MainActor.run {
            isLoading = false
        }
    }
    
    private func updateSafetySetting(type: SafetySettingType, value: String) async {
        do {
            let request = UpdateSafetySettingRequest.newBuilder()
                .setUserId("current_user")
                .setSettingType(type.rawValue)
                .setValue(value)
                .build()
            
            let response = try await grpcClient.updateSafetySetting(request: request)
            
            if response.success {
                await MainActor.run {
                    saveLocalSafetySetting(type: type, value: value)
                    showUpdateAlert = true
                    updateMessage = "Safety setting updated successfully"
                }
            } else {
                await MainActor.run {
                    errorMessage = response.errorMessage
                    showErrorAlert = true
                }
            }
        } catch {
            await MainActor.run {
                errorMessage = "Failed to update safety setting: \(error.localizedDescription)"
                showErrorAlert = true
            }
        }
    }
    
    private func addMutedWordToServer(_ word: String) async {
        do {
            let request = AddMutedWordRequest.newBuilder()
                .setUserId("current_user")
                .setWord(word)
                .build()
            
            let response = try await grpcClient.addMutedWord(request: request)
            
            if !response.success {
                await MainActor.run {
                    errorMessage = response.errorMessage
                    showErrorAlert = true
                }
            }
        } catch {
            await MainActor.run {
                errorMessage = "Failed to add muted word: \(error.localizedDescription)"
                showErrorAlert = true
            }
        }
    }
    
    private func removeMutedWordFromServer(_ word: String) async {
        do {
            let request = RemoveMutedWordRequest.newBuilder()
                .setUserId("current_user")
                .setWord(word)
                .build()
            
            let response = try await grpcClient.removeMutedWord(request: request)
            
            if !response.success {
                await MainActor.run {
                    errorMessage = response.errorMessage
                    showErrorAlert = true
                }
            }
        } catch {
            await MainActor.run {
                errorMessage = "Failed to remove muted word: \(error.localizedDescription)"
                showErrorAlert = true
            }
        }
    }
    
    private func lockAccount() async {
        do {
            let request = LockAccountRequest.newBuilder()
                .setUserId("current_user")
                .setReason("User requested temporary lock")
                .setDuration(3600) // 1 hour
                .build()
            
            let response = try await grpcClient.lockAccount(request: request)
            
            if response.success {
                await MainActor.run {
                    showLockAlert = true
                }
            } else {
                await MainActor.run {
                    errorMessage = response.errorMessage
                    showErrorAlert = true
                }
            }
        } catch {
            await MainActor.run {
                errorMessage = "Failed to lock account: \(error.localizedDescription)"
                showErrorAlert = true
            }
        }
    }
    
    private func updateSafetySettingsFromResponse(_ settings: GRPCSafetySettings) {
        if let filteringLevel = ContentFilteringLevel(rawValue: settings.contentFilteringLevel) {
            contentFilteringLevel = filteringLevel
        }
        
        showSensitiveContent = settings.showSensitiveContent
        warnBeforeSensitiveContent = settings.warnBeforeSensitiveContent
        filterProfanity = settings.filterProfanity
        filterHateSpeech = settings.filterHateSpeech
        filterViolence = settings.filterViolence
        
        if let displayMode = SensitiveContentDisplayMode(rawValue: settings.sensitiveContentDisplayMode) {
            sensitiveContentDisplayMode = displayMode
        }
        
        blurSensitiveImages = settings.blurSensitiveImages
        muteSensitiveVideos = settings.muteSensitiveVideos
        showContentWarnings = settings.showContentWarnings
        hideMutedUsersContent = settings.hideMutedUsersContent
        hideBlockedUsersContent = settings.hideBlockedUsersContent
        hideSensitiveMedia = settings.hideSensitiveMedia
        
        // Update muted words
        mutedWords = Set(settings.mutedWords)
        
        // Save to local storage
        saveAllLocalSafetySettings()
    }
    
    private func saveLocalSafetySetting(type: SafetySettingType, value: String) {
        let key = "safety_\(type.rawValue)"
        userDefaults.set(value, forKey: key)
    }
    
    private func saveAllLocalSafetySettings() {
        saveLocalSafetySetting(type: .contentFilteringLevel, value: contentFilteringLevel.rawValue)
        saveLocalSafetySetting(type: .showSensitiveContent, value: showSensitiveContent.description)
        saveLocalSafetySetting(type: .warnBeforeSensitiveContent, value: warnBeforeSensitiveContent.description)
        saveLocalSafetySetting(type: .filterProfanity, value: filterProfanity.description)
        saveLocalSafetySetting(type: .filterHateSpeech, value: filterHateSpeech.description)
        saveLocalSafetySetting(type: .filterViolence, value: filterViolence.description)
        saveLocalSafetySetting(type: .sensitiveContentDisplayMode, value: sensitiveContentDisplayMode.rawValue)
        saveLocalSafetySetting(type: .blurSensitiveImages, value: blurSensitiveImages.description)
        saveLocalSafetySetting(type: .muteSensitiveVideos, value: muteSensitiveVideos.description)
        saveLocalSafetySetting(type: .showContentWarnings, value: showContentWarnings.description)
        saveLocalSafetySetting(type: .hideMutedUsersContent, value: hideMutedUsersContent.description)
        saveLocalSafetySetting(type: .hideBlockedUsersContent, value: hideBlockedUsersContent.description)
        saveLocalSafetySetting(type: .hideSensitiveMedia, value: hideSensitiveMedia.description)
        
        saveMutedWords()
    }
    
    private func saveMutedWords() {
        userDefaults.set(Array(mutedWords), forKey: "safety_muted_words")
    }
}

// MARK: - Safety Setting Types
enum SafetySettingType: String, CaseIterable {
    case contentFilteringLevel = "content_filtering_level"
    case showSensitiveContent = "show_sensitive_content"
    case warnBeforeSensitiveContent = "warn_before_sensitive_content"
    case filterProfanity = "filter_profanity"
    case filterHateSpeech = "filter_hate_speech"
    case filterViolence = "filter_violence"
    case sensitiveContentDisplayMode = "sensitive_content_display_mode"
    case blurSensitiveImages = "blur_sensitive_images"
    case muteSensitiveVideos = "mute_sensitive_videos"
    case showContentWarnings = "show_content_warnings"
    case hideMutedUsersContent = "hide_muted_users_content"
    case hideBlockedUsersContent = "hide_blocked_users_content"
    case hideSensitiveMedia = "hide_sensitive_media"
}

// MARK: - gRPC Request/Response Models (Placeholder - replace with actual gRPC types)
struct GetSafetySettingsRequest {
    let userId: String
    
    static func newBuilder() -> GetSafetySettingsRequestBuilder {
        return GetSafetySettingsRequestBuilder()
    }
}

class GetSafetySettingsRequestBuilder {
    private var userId: String = ""
    
    func setUserId(_ userId: String) -> GetSafetySettingsRequestBuilder {
        self.userId = userId
        return self
    }
    
    func build() -> GetSafetySettingsRequest {
        return GetSafetySettingsRequest(userId: userId)
    }
}

struct GetSafetySettingsResponse {
    let success: Bool
    let settings: GRPCSafetySettings
    let errorMessage: String
}

struct UpdateSafetySettingRequest {
    let userId: String
    let settingType: String
    let value: String
    
    static func newBuilder() -> UpdateSafetySettingRequestBuilder {
        return UpdateSafetySettingRequestBuilder()
    }
}

class UpdateSafetySettingRequestBuilder {
    private var userId: String = ""
    private var settingType: String = ""
    private var value: String = ""
    
    func setUserId(_ userId: String) -> UpdateSafetySettingRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setSettingType(_ settingType: String) -> UpdateSafetySettingRequestBuilder {
        self.settingType = settingType
        return self
    }
    
    func setValue(_ value: String) -> UpdateSafetySettingRequestBuilder {
        self.value = value
        return self
    }
    
    func build() -> UpdateSafetySettingRequest {
        return UpdateSafetySettingRequest(userId: userId, settingType: settingType, value: value)
    }
}

struct UpdateSafetySettingResponse {
    let success: Bool
    let errorMessage: String
}

struct AddMutedWordRequest {
    let userId: String
    let word: String
    
    static func newBuilder() -> AddMutedWordRequestBuilder {
        return AddMutedWordRequestBuilder()
    }
}

class AddMutedWordRequestBuilder {
    private var userId: String = ""
    private var word: String = ""
    
    func setUserId(_ userId: String) -> AddMutedWordRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setWord(_ word: String) -> AddMutedWordRequestBuilder {
        self.word = word
        return self
    }
    
    func build() -> AddMutedWordRequest {
        return AddMutedWordRequest(userId: userId, word: word)
    }
}

struct AddMutedWordResponse {
    let success: Bool
    let errorMessage: String
}

struct RemoveMutedWordRequest {
    let userId: String
    let word: String
    
    static func newBuilder() -> RemoveMutedWordRequestBuilder {
        return RemoveMutedWordRequestBuilder()
    }
}

class RemoveMutedWordRequestBuilder {
    private var userId: String = ""
    private var word: String = ""
    
    func setUserId(_ userId: String) -> RemoveMutedWordRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setWord(_ word: String) -> RemoveMutedWordRequestBuilder {
        self.word = word
        return self
    }
    
    func build() -> RemoveMutedWordRequest {
        return RemoveMutedWordRequest(userId: userId, word: word)
    }
}

struct RemoveMutedWordResponse {
    let success: Bool
    let errorMessage: String
}

struct LockAccountRequest {
    let userId: String
    let reason: String
    let duration: Int32
    
    static func newBuilder() -> LockAccountRequestBuilder {
        return LockAccountRequestBuilder()
    }
}

class LockAccountRequestBuilder {
    private var userId: String = ""
    private var reason: String = ""
    private var duration: Int32 = 0
    
    func setUserId(_ userId: String) -> LockAccountRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setReason(_ reason: String) -> LockAccountRequestBuilder {
        self.reason = reason
        return self
    }
    
    func setDuration(_ duration: Int32) -> LockAccountRequestBuilder {
        self.duration = duration
        return self
    }
    
    func build() -> LockAccountRequest {
        return LockAccountRequest(userId: userId, reason: reason, duration: duration)
    }
}

struct LockAccountResponse {
    let success: Bool
    let errorMessage: String
}

struct GRPCSafetySettings {
    let contentFilteringLevel: String
    let showSensitiveContent: Bool
    let warnBeforeSensitiveContent: Bool
    let filterProfanity: Bool
    let filterHateSpeech: Bool
    let filterViolence: Bool
    let sensitiveContentDisplayMode: String
    let blurSensitiveImages: Bool
    let muteSensitiveVideos: Bool
    let showContentWarnings: Bool
    let hideMutedUsersContent: Bool
    let hideBlockedUsersContent: Bool
    let hideSensitiveMedia: Bool
    let mutedWords: [String]
}