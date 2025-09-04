import Foundation
import Combine
import CryptoKit
import LocalAuthentication

@MainActor
class TwoFactorViewModel: ObservableObject {
    
    // MARK: - Published Properties
    @Published var is2FAEnabled = false
    @Published var lastUsedDate: Date?
    @Published var backupCodes: [String] = []
    @Published var usedBackupCodes: Set<String> = []
    @Published var showSetupSheet = false
    @Published var showRegenerateAlert = false
    @Published var showDisableAlert = false
    @Published var showErrorAlert = false
    @Published var error: String?
    @Published var isLoading = false
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private var cancellables = Set<AnyCancellable>()
    private let keychain = KeychainService()
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient) {
        self.grpcClient = grpcClient
        load2FAStatus()
        loadBackupCodes()
    }
    
    // MARK: - Public Methods
    func setup2FA() {
        showSetupSheet = true
    }
    
    func generateBackupCodes() {
        Task {
            isLoading = true
            error = nil
            
            do {
                let request = GenerateBackupCodesRequest.newBuilder()
                    .setUserId("current_user")
                    .setCount(10)
                    .build()
                
                let response = try await grpcClient.generateBackupCodes(request: request)
                
                if response.success {
                    await MainActor.run {
                        self.backupCodes = Array(response.backupCodes)
                        self.saveBackupCodes()
                    }
                } else {
                    await MainActor.run {
                        self.error = response.errorMessage
                        self.showErrorAlert = true
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to generate backup codes: \(error.localizedDescription)"
                    self.showErrorAlert = true
                }
            }
            
            await MainActor.run {
                self.isLoading = false
            }
        }
    }
    
    func disable2FA() {
        Task {
            isLoading = true
            error = nil
            
            do {
                let request = Disable2FARequest.newBuilder()
                    .setUserId("current_user")
                    .build()
                
                let response = try await grpcClient.disable2FA(request: request)
                
                if response.success {
                    await MainActor.run {
                        self.is2FAEnabled = false
                        self.backupCodes.removeAll()
                        self.usedBackupCodes.removeAll()
                        self.save2FAStatus()
                        self.saveBackupCodes()
                    }
                } else {
                    await MainActor.run {
                        self.error = response.errorMessage
                        self.showErrorAlert = true
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to disable 2FA: \(error.localizedDescription)"
                    self.showErrorAlert = true
                }
            }
            
            await MainActor.run {
                self.isLoading = false
            }
        }
    }
    
    func clearError() {
        error = nil
        showErrorAlert = false
    }
    
    // MARK: - Private Methods
    private func load2FAStatus() {
        // Load from UserDefaults and Keychain
        is2FAEnabled = UserDefaults.standard.bool(forKey: "is2FAEnabled")
        
        if let lastUsed = UserDefaults.standard.object(forKey: "2FALastUsed") as? Date {
            lastUsedDate = lastUsed
        }
    }
    
    private func loadBackupCodes() {
        // Load backup codes from Keychain
        if let codes = keychain.retrieveBackupCodes() {
            backupCodes = codes
        }
        
        // Load used backup codes from UserDefaults
        if let usedCodes = UserDefaults.standard.array(forKey: "usedBackupCodes") as? [String] {
            usedBackupCodes = Set(usedCodes)
        }
    }
    
    private func save2FAStatus() {
        UserDefaults.standard.set(is2FAEnabled, forKey: "is2FAEnabled")
        if let lastUsed = lastUsedDate {
            UserDefaults.standard.set(lastUsed, forKey: "2FALastUsed")
        }
    }
    
    private func saveBackupCodes() {
        // Save backup codes to Keychain
        keychain.storeBackupCodes(backupCodes)
        
        // Save used backup codes to UserDefaults
        UserDefaults.standard.set(Array(usedBackupCodes), forKey: "usedBackupCodes")
    }
}

// MARK: - Keychain Service
class KeychainService {
    private let service = "com.sonet.2fa.backupcodes"
    private let account = "backupCodes"
    
    func storeBackupCodes(_ codes: [String]) {
        let codesData = codes.joined(separator: ",").data(using: .utf8)
        
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecValueData as String: codesData ?? Data()
        ]
        
        // Delete existing item
        SecItemDelete(query as CFDictionary)
        
        // Add new item
        let status = SecItemAdd(query as CFDictionary, nil)
        if status != errSecSuccess {
            print("Failed to save backup codes to keychain: \(status)")
        }
    }
    
    func retrieveBackupCodes() -> [String]? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne
        ]
        
        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        
        if status == errSecSuccess,
           let data = result as? Data,
           let codesString = String(data: data, encoding: .utf8) {
            return codesString.components(separatedBy: ",")
        }
        
        return nil
    }
}

// MARK: - gRPC Request/Response Extensions
extension GenerateBackupCodesRequest {
    static func newBuilder() -> GenerateBackupCodesRequestBuilder {
        return GenerateBackupCodesRequestBuilder()
    }
}

extension Disable2FARequest {
    static func newBuilder() -> Disable2FARequestBuilder {
        return Disable2FARequestBuilder()
    }
}

// MARK: - Request Builders (Placeholder - replace with actual gRPC types)
class GenerateBackupCodesRequestBuilder {
    private var userId: String = ""
    private var count: Int32 = 10
    
    func setUserId(_ userId: String) -> GenerateBackupCodesRequestBuilder {
        self.userId = userId
        return self
    }
    
    func setCount(_ count: Int32) -> GenerateBackupCodesRequestBuilder {
        self.count = count
        return self
    }
    
    func build() -> GenerateBackupCodesRequest {
        return GenerateBackupCodesRequest(userId: userId, count: count)
    }
}

class Disable2FARequestBuilder {
    private var userId: String = ""
    
    func setUserId(_ userId: String) -> Disable2FARequestBuilder {
        self.userId = userId
        return self
    }
    
    func build() -> Disable2FARequest {
        return Disable2FARequest(userId: userId)
    }
}

// MARK: - Request/Response Models (Placeholder - replace with actual gRPC types)
struct GenerateBackupCodesRequest {
    let userId: String
    let count: Int32
}

struct Disable2FARequest {
    let userId: String
}

struct GenerateBackupCodesResponse {
    let success: Bool
    let backupCodes: [String]
    let errorMessage: String
}

struct Disable2FAResponse {
    let success: Bool
    let errorMessage: String
}