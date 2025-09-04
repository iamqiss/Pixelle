import Foundation
import Combine
import LocalAuthentication
import Security

@MainActor
class SecurityViewModel: ObservableObject {
    
    // MARK: - Published Properties
    @Published var requirePasswordForPurchases = false
    @Published var requirePasswordForSettings = false
    @Published var biometricEnabled = false
    @Published var biometricType: BiometricType = .none
    @Published var activeSessions: [SecuritySession] = []
    @Published var securityLogs: [SecurityLog] = []
    @Published var loginAlertsEnabled = true
    @Published var suspiciousActivityAlertsEnabled = true
    @Published var passwordChangeAlertsEnabled = true
    @Published var isLoading = false
    @Published var error: String?
    @Published var showErrorAlert = false
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private var cancellables = Set<AnyCancellable>()
    private let keychain = KeychainService()
    private let biometricContext = LAContext()
    
    // MARK: - Computed Properties
    var currentSessionId: String {
        return UserDefaults.standard.string(forKey: "currentSessionId") ?? ""
    }
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient) {
        self.grpcClient = grpcClient
        setupBiometricContext()
        loadSecuritySettings()
    }
    
    // MARK: - Public Methods
    func loadSecuritySettings() {
        // Load from UserDefaults
        requirePasswordForPurchases = UserDefaults.standard.bool(forKey: "requirePasswordForPurchases")
        requirePasswordForSettings = UserDefaults.standard.bool(forKey: "requirePasswordForSettings")
        biometricEnabled = UserDefaults.standard.bool(forKey: "biometricEnabled")
        loginAlertsEnabled = UserDefaults.standard.bool(forKey: "loginAlertsEnabled")
        suspiciousActivityAlertsEnabled = UserDefaults.standard.bool(forKey: "suspiciousActivityAlertsEnabled")
        passwordChangeAlertsEnabled = UserDefaults.standard.bool(forKey: "passwordChangeAlertsEnabled")
    }
    
    func loadActiveSessions() {
        Task {
            isLoading = true
            error = nil
            
            do {
                let request = GetActiveSessionsRequest.newBuilder()
                    .setUserId("current_user")
                    .build()
                
                let response = try await grpcClient.getActiveSessions(request: request)
                
                if response.success {
                    await MainActor.run {
                        self.activeSessions = response.sessions.map { SecuritySession(from: $0) }
                    }
                } else {
                    await MainActor.run {
                        self.error = response.errorMessage
                        self.showErrorAlert = true
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to load active sessions: \(error.localizedDescription)"
                    self.showErrorAlert = true
                }
            }
            
            await MainActor.run {
                self.isLoading = false
            }
        }
    }
    
    func loadSecurityLogs() {
        Task {
            isLoading = true
            error = nil
            
            do {
                let request = GetSecurityLogsRequest.newBuilder()
                    .setUserId("current_user")
                    .setLimit(50)
                    .build()
                
                let response = try await grpcClient.getSecurityLogs(request: request)
                
                if response.success {
                    await MainActor.run {
                        self.securityLogs = response.logs.map { SecurityLog(from: $0) }
                    }
                } else {
                    await MainActor.run {
                        self.error = response.errorMessage
                        self.showErrorAlert = true
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to load security logs: \(error.localizedDescription)"
                    self.showErrorAlert = true
                }
            }
            
            await MainActor.run {
                self.isLoading = false
            }
        }
    }
    
    func loadAllSessions() {
        // This would typically navigate to a detailed sessions view
        // For now, we'll just load more sessions
        Task {
            do {
                let request = GetActiveSessionsRequest.newBuilder()
                    .setUserId("current_user")
                    .setIncludeInactive(true)
                    .build()
                
                let response = try await grpcClient.getActiveSessions(request: request)
                
                if response.success {
                    await MainActor.run {
                        self.activeSessions = response.sessions.map { SecuritySession(from: $0) }
                    }
                }
            } catch {
                print("Failed to load all sessions: \(error)")
            }
        }
    }
    
    func revokeSession(_ sessionId: String) {
        Task {
            isLoading = true
            error = nil
            
            do {
                let request = RevokeSessionRequest.newBuilder()
                    .setUserId("current_user")
                    .setSessionId(sessionId)
                    .build()
                
                let response = try await grpcClient.revokeSession(request: request)
                
                if response.success {
                    await MainActor.run {
                        self.activeSessions.removeAll { $0.id == sessionId }
                        self.logSecurityEvent(.sessionRevoked, description: "Session revoked: \(sessionId)")
                    }
                } else {
                    await MainActor.run {
                        self.error = response.errorMessage
                        self.showErrorAlert = true
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to revoke session: \(error.localizedDescription)"
                    self.showErrorAlert = true
                }
            }
            
            await MainActor.run {
                self.isLoading = false
            }
        }
    }
    
    func updatePasswordRequirement(for action: PasswordRequirementAction, required: Bool) {
        switch action {
        case .purchases:
            requirePasswordForPurchases = required
            UserDefaults.standard.set(required, forKey: "requirePasswordForPurchases")
        case .settings:
            requirePasswordForSettings = required
            UserDefaults.standard.set(required, forKey: "requirePasswordForSettings")
        }
        
        logSecurityEvent(.passwordRequirementChanged, description: "Password requirement for \(action.displayName) changed to \(required)")
    }
    
    func updateBiometricAuthentication(enabled: Bool) {
        biometricEnabled = enabled
        UserDefaults.standard.set(enabled, forKey: "biometricEnabled")
        
        if enabled {
            logSecurityEvent(.biometricEnabled, description: "Biometric authentication enabled")
        } else {
            logSecurityEvent(.biometricDisabled, description: "Biometric authentication disabled")
        }
    }
    
    func updateLoginAlerts(enabled: Bool) {
        loginAlertsEnabled = enabled
        UserDefaults.standard.set(enabled, forKey: "loginAlertsEnabled")
    }
    
    func updateSuspiciousActivityAlerts(enabled: Bool) {
        suspiciousActivityAlertsEnabled = enabled
        UserDefaults.standard.set(enabled, forKey: "suspiciousActivityAlertsEnabled")
    }
    
    func updatePasswordChangeAlerts(enabled: Bool) {
        passwordChangeAlertsEnabled = enabled
        UserDefaults.standard.set(enabled, forKey: "passwordChangeAlertsEnabled")
    }
    
    func clearError() {
        error = nil
        showErrorAlert = false
    }
    
    // MARK: - Private Methods
    private func setupBiometricContext() {
        var error: NSError?
        if biometricContext.canEvaluatePolicy(.deviceOwnerAuthenticationWithBiometrics, error: &error) {
            switch biometricContext.biometryType {
            case .faceID:
                biometricType = .faceID
            case .touchID:
                biometricType = .touchID
            default:
                biometricType = .none
            }
        } else {
            biometricType = .none
        }
    }
    
    private func logSecurityEvent(_ eventType: SecurityEventType, description: String) {
        let log = SecurityLog(
            id: UUID().uuidString,
            eventType: eventType,
            description: description,
            timestamp: Date(),
            requiresAttention: eventType.requiresAttention,
            metadata: [:]
        )
        
        securityLogs.insert(log, at: 0)
        
        // Save to local storage and potentially sync with server
        saveSecurityLog(log)
    }
    
    private func saveSecurityLog(_ log: SecurityLog) {
        // Save to UserDefaults for now
        // In a real app, this would be saved to Core Data or similar
        var logs = UserDefaults.standard.array(forKey: "securityLogs") as? [[String: Any]] ?? []
        
        let logDict: [String: Any] = [
            "id": log.id,
            "eventType": log.eventType.rawValue,
            "description": log.description,
            "timestamp": log.timestamp.timeIntervalSince1970,
            "requiresAttention": log.requiresAttention
        ]
        
        logs.insert(logDict, at: 0)
        
        // Keep only last 100 logs
        if logs.count > 100 {
            logs = Array(logs.prefix(100))
        }
        
        UserDefaults.standard.set(logs, forKey: "securityLogs")
    }
}

// MARK: - Supporting Types
enum BiometricType: CaseIterable {
    case none, touchID, faceID
    
    var displayName: String {
        switch self {
        case .none:
            return "Biometric Authentication"
        case .touchID:
            return "Touch ID"
        case .faceID:
            return "Face ID"
        }
    }
}

enum PasswordRequirementAction: CaseIterable {
    case purchases, settings
    
    var displayName: String {
        switch self {
        case .purchases:
            return "App Store purchases"
        case .settings:
            return "Settings changes"
        }
    }
}

enum SecurityEventType: String, CaseIterable {
    case login = "login"
    case logout = "logout"
    case passwordChanged = "password_changed"
    case passwordRequirementChanged = "password_requirement_changed"
    case biometricEnabled = "biometric_enabled"
    case biometricDisabled = "biometric_disabled"
    case sessionRevoked = "session_revoked"
    case suspiciousActivity = "suspicious_activity"
    case twoFactorEnabled = "2fa_enabled"
    case twoFactorDisabled = "2fa_disabled"
    
    var displayName: String {
        switch self {
        case .login:
            return "Login"
        case .logout:
            return "Logout"
        case .passwordChanged:
            return "Password Changed"
        case .passwordRequirementChanged:
            return "Password Requirement Changed"
        case .biometricEnabled:
            return "Biometric Enabled"
        case .biometricDisabled:
            return "Biometric Disabled"
        case .sessionRevoked:
            return "Session Revoked"
        case .suspiciousActivity:
            return "Suspicious Activity"
        case .twoFactorEnabled:
            return "2FA Enabled"
        case .twoFactorDisabled:
            return "2FA Disabled"
        }
    }
    
    var iconName: String {
        switch self {
        case .login:
            return "person.badge.plus"
        case .logout:
            return "person.badge.minus"
        case .passwordChanged:
            return "key.fill"
        case .passwordRequirementChanged:
            return "key.slash"
        case .biometricEnabled:
            return "faceid"
        case .biometricDisabled:
            return "faceid.slash"
        case .sessionRevoked:
            return "xmark.shield"
        case .suspiciousActivity:
            return "exclamationmark.triangle.fill"
        case .twoFactorEnabled:
            return "shield.checkered"
        case .twoFactorDisabled:
            return "shield.slash"
        }
    }
    
    var color: Color {
        switch self {
        case .login, .passwordChanged, .biometricEnabled, .twoFactorEnabled:
            return .green
        case .logout, .passwordRequirementChanged, .biometricDisabled, .twoFactorDisabled:
            return .orange
        case .sessionRevoked:
            return .red
        case .suspiciousActivity:
            return .red
        }
    }
    
    var requiresAttention: Bool {
        switch self {
        case .suspiciousActivity, .sessionRevoked:
            return true
        default:
            return false
        }
    }
}

struct SecuritySession: Identifiable {
    let id: String
    let deviceName: String
    let deviceType: DeviceType
    let location: String
    let lastActivity: Date
    let ipAddress: String
    let userAgent: String
    
    init(from grpcSession: GRPCSession) {
        self.id = grpcSession.sessionId
        self.deviceName = grpcSession.deviceName
        self.deviceType = DeviceType(from: grpcSession.deviceType)
        self.location = grpcSession.locationInfo
        self.lastActivity = grpcSession.lastActivity.date
        self.ipAddress = grpcSession.ipAddress
        self.userAgent = grpcSession.userAgent
    }
}

enum DeviceType: CaseIterable {
    case mobile, tablet, desktop, web
    
    init(from grpcType: GRPCDeviceType) {
        switch grpcType {
        case .mobile:
            self = .mobile
        case .tablet:
            self = .tablet
        case .desktop:
            self = .desktop
        case .web:
            self = .web
        default:
            self = .mobile
        }
    }
    
    var iconName: String {
        switch self {
        case .mobile:
            return "iphone"
        case .tablet:
            return "ipad"
        case .desktop:
            return "desktopcomputer"
        case .web:
            return "globe"
        }
    }
}

struct SecurityLog: Identifiable {
    let id: String
    let eventType: SecurityEventType
    let description: String
    let timestamp: Date
    let requiresAttention: Bool
    let metadata: [String: Any]
    
    init(from grpcLog: GRPCSecurityLog) {
        self.id = grpcLog.logId
        self.eventType = SecurityEventType(rawValue: grpcLog.eventType) ?? .login
        self.description = grpcLog.description
        self.timestamp = grpcLog.timestamp.date
        self.requiresAttention = grpcLog.requiresAttention
        self.metadata = [:] // Parse from grpcLog.metadata if available
    }
}

// MARK: - gRPC Extensions (Placeholder - replace with actual gRPC types)
extension GetActiveSessionsRequest {
    static func newBuilder() -> GetActiveSessionsRequestBuilder {
        return GetActiveSessionsRequestBuilder()
    }
}

extension GetSecurityLogsRequest {
    static func newBuilder() -> GetSecurityLogsRequestBuilder {
        return GetSecurityLogsRequestBuilder()
    }
}

extension RevokeSessionRequest {
    static func newBuilder() -> RevokeSessionRequestBuilder {
        return RevokeSessionRequestBuilder()
    }
}

// MARK: - Request Builders (Placeholder - replace with actual gRPC types)
class GetActiveSessionsRequestBuilder {
    private var userId: String = ""
    private var includeInactive: Bool = false
    
    func setUserId(_ userId: String) -> GetActiveSessionsRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setIncludeInactive(_ include: Bool) -> GetActiveSessionsRequestBuilder {
        self.includeInactive = include
        return self
    }
    
    func build() -> GetActiveSessionsRequest {
        return GetActiveSessionsRequest(userId: userId, includeInactive: includeInactive)
    }
}

class GetSecurityLogsRequestBuilder {
    private var userId: String = ""
    private var limit: Int32 = 50
    
    func setUserId(_ userId: String) -> GetSecurityLogsRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setLimit(_ limit: Int32) -> GetSecurityLogsRequestBuilder {
        self.limit = limit
        return self
    }
    
    func build() -> GetSecurityLogsRequest {
        return GetSecurityLogsRequest(userId: userId, limit: limit)
    }
}

class RevokeSessionRequestBuilder {
    private var userId: String = ""
    private var sessionId: String = ""
    
    func setUserId(_ userId: String) -> RevokeSessionRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setSessionId(_ sessionId: String) -> RevokeSessionRequestBuilder {
        self.sessionId = sessionId
        return self
    }
    
    func build() -> RevokeSessionRequest {
        return RevokeSessionRequest(userId: userId, sessionId: sessionId)
    }
}

// MARK: - Request/Response Models (Placeholder - replace with actual gRPC types)
struct GetActiveSessionsRequest {
    let userId: String
    let includeInactive: Bool
}

struct GetSecurityLogsRequest {
    let userId: String
    let limit: Int32
}

struct RevokeSessionRequest {
    let userId: String
    let sessionId: String
}

struct GetActiveSessionsResponse {
    let success: Bool
    let sessions: [GRPCSession]
    let errorMessage: String
}

struct GetSecurityLogsResponse {
    let success: Bool
    let logs: [GRPCSecurityLog]
    let errorMessage: String
}

struct RevokeSessionResponse {
    let success: Bool
    let errorMessage: String
}

// MARK: - gRPC Models (Placeholder - replace with actual gRPC types)
struct GRPCSession {
    let sessionId: String
    let deviceName: String
    let deviceType: GRPCDeviceType
    let locationInfo: String
    let lastActivity: GRPCTimestamp
    let ipAddress: String
    let userAgent: String
}

struct GRPCSecurityLog {
    let logId: String
    let eventType: String
    let description: String
    let timestamp: GRPCTimestamp
    let requiresAttention: Bool
}

enum GRPCDeviceType {
    case mobile, tablet, desktop, web
}

struct GRPCTimestamp {
    let date: Date
}