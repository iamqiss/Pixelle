import Foundation
import SwiftUI
import Combine

@MainActor
class AppState: ObservableObject {
    // MARK: - Published Properties
    @Published var isInitialized = false
    @Published var isLoading = false
    @Published var currentError: AppError?
    @Published var appVersion: String = ""
    @Published var buildNumber: String = ""
    @Published var isDebugMode = false
    
    // MARK: - Private Properties
    private var cancellables = Set<AnyCancellable>()
    
    // MARK: - Initialization
    init() {
        setupAppInfo()
        setupErrorHandling()
    }
    
    // MARK: - Setup Methods
    private func setupAppInfo() {
        if let version = Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String {
            appVersion = version
        }
        
        if let build = Bundle.main.infoDictionary?["CFBundleVersion"] as? String {
            buildNumber = build
        }
        
        #if DEBUG
        isDebugMode = true
        #endif
    }
    
    private func setupErrorHandling() {
        $currentError
            .compactMap { $0 }
            .sink { [weak self] error in
                self?.handleError(error)
            }
            .store(in: &cancellables)
    }
    
    // MARK: - Public Methods
    func initialize() async {
        isLoading = true
        
        do {
            // Initialize core services
            try await initializeCoreServices()
            
            // Load user preferences
            try await loadUserPreferences()
            
            // Setup background services
            setupBackgroundServices()
            
            isInitialized = true
        } catch {
            currentError = AppError.initializationFailed(error)
        }
        
        isLoading = false
    }
    
    func reset() {
        isInitialized = false
        isLoading = false
        currentError = nil
    }
    
    // MARK: - Private Methods
    private func initializeCoreServices() async throws {
        // Initialize networking, caching, etc.
        try await Task.sleep(nanoseconds: 100_000_000) // Simulate initialization
    }
    
    private func loadUserPreferences() async throws {
        // Load user preferences from UserDefaults or other storage
        try await Task.sleep(nanoseconds: 50_000_000) // Simulate loading
    }
    
    private func setupBackgroundServices() {
        // Setup background refresh, notifications, etc.
    }
    
    private func handleError(_ error: AppError) {
        // Log error and potentially show user-facing error
        print("App Error: \(error.localizedDescription)")
    }
}

// MARK: - App Error Types
enum AppError: LocalizedError, Identifiable {
    case initializationFailed(Error)
    case networkError(Error)
    case authenticationFailed
    case dataCorruption
    case unknown
    
    var id: String {
        switch self {
        case .initializationFailed: return "initialization_failed"
        case .networkError: return "network_error"
        case .authenticationFailed: return "authentication_failed"
        case .dataCorruption: return "data_corruption"
        case .unknown: return "unknown"
        }
    }
    
    var errorDescription: String? {
        switch self {
        case .initializationFailed(let error):
            return "Failed to initialize app: \(error.localizedDescription)"
        case .networkError(let error):
            return "Network error: \(error.localizedDescription)"
        case .authenticationFailed:
            return "Authentication failed"
        case .dataCorruption:
            return "Data corruption detected"
        case .unknown:
            return "An unknown error occurred"
        }
    }
    
    var recoverySuggestion: String? {
        switch self {
        case .initializationFailed:
            return "Please restart the app"
        case .networkError:
            return "Please check your internet connection"
        case .authenticationFailed:
            return "Please log in again"
        case .dataCorruption:
            return "Please reinstall the app"
        case .unknown:
            return "Please try again later"
        }
    }
}