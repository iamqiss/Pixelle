import Foundation
import SwiftUI
import Combine

@MainActor
class MultiStepSignupViewModel: ObservableObject {
    
    // MARK: - Published Properties
    @Published var displayName = ""
    @Published var username = ""
    @Published var password = ""
    @Published var passwordConfirmation = ""
    @Published var birthday = Calendar.current.date(byAdding: .year, value: -18, to: Date()) ?? Date()
    @Published var email = ""
    @Published var phoneNumber = ""
    @Published var bio = ""
    @Published var avatarImage: UIImage?
    @Published var selectedInterests: Set<String> = []
    @Published var contactsAccess = false
    @Published var locationAccess = false
    
    // MARK: - Alert States
    @Published var showSuccessAlert = false
    @Published var showErrorAlert = false
    @Published var errorMessage: String?
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private var cancellables = Set<AnyCancellable>()
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient = SonetGRPCClient(configuration: .development)) {
        self.grpcClient = grpcClient
    }
    
    // MARK: - Public Methods
    func completeSignup() async {
        do {
            // Validate all required fields
            guard validateAllFields() else {
                await showError("Please fill in all required fields")
                return
            }
            
            // Create user account
            let userProfile = try await grpcClient.registerUser(
                username: username,
                email: email,
                password: password
            )
            
            // Update user profile with additional information
            try await updateUserProfile(userProfile: userProfile)
            
            // Handle permissions
            await handlePermissions()
            
            // Show success
            await showSuccess()
            
        } catch {
            await showError("Failed to create account: \(error.localizedDescription)")
        }
    }
    
    // MARK: - Private Methods
    private func validateAllFields() -> Bool {
        // Basic validation
        guard !displayName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return false }
        guard !username.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return false }
        guard password.count >= 8 else { return false }
        guard password == passwordConfirmation else { return false }
        
        // Age validation
        let calendar = Calendar.current
        let ageComponents = calendar.dateComponents([.year], from: birthday, to: Date())
        guard (ageComponents.year ?? 0) >= 13 else { return false }
        
        // Email validation
        guard !email.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return false }
        guard isValidEmail(email) else { return false }
        
        // Interests validation
        guard selectedInterests.count >= 3 else { return false }
        
        return true
    }
    
    private func isValidEmail(_ email: String) -> Bool {
        let emailRegex = "[A-Z0-9a-z._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,64}"
        let emailPredicate = NSPredicate(format: "SELF MATCHES %@", emailRegex)
        return emailPredicate.evaluate(with: email)
    }
    
    private func updateUserProfile(userProfile: UserProfile) async throws {
        // Create update request with all collected information
        let request = UpdateUserProfileRequest.with {
            $0.userID = userProfile.userID
            $0.displayName = displayName
            $0.bio = bio
            $0.interests = Array(selectedInterests)
            
            // Add phone number if provided
            if !phoneNumber.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                $0.phoneNumber = phoneNumber
            }
        }
        
        // Update profile
        _ = try await grpcClient.updateUserProfile(request: request)
        
        // Upload avatar if selected
        if let avatarImage = avatarImage {
            try await uploadAvatar(image: avatarImage, userId: userProfile.userID)
        }
    }
    
    private func uploadAvatar(image: UIImage, userId: String) async throws {
        // Convert UIImage to Data
        guard let imageData = image.jpegData(compressionQuality: 0.8) else {
            throw NSError(domain: "AvatarUpload", code: -1, userInfo: [NSLocalizedDescriptionKey: "Failed to convert image to data"])
        }
        
        // Create upload request
        let request = UploadMediaRequest.with {
            $0.userID = userId
            $0.mediaType = .image
            $0.mediaData = imageData
            $0.fileName = "avatar_\(userId).jpg"
        }
        
        // Upload avatar
        _ = try await grpcClient.uploadMedia(request: request)
    }
    
    private func handlePermissions() async {
        // Request contacts access if enabled
        if contactsAccess {
            await requestContactsAccess()
        }
        
        // Request location access if enabled
        if locationAccess {
            await requestLocationAccess()
        }
    }
    
    private func requestContactsAccess() async {
        // Request contacts permission
        let status = await requestContactsPermission()
        
        if status {
            // Sync contacts with server
            await syncContacts()
        }
    }
    
    private func requestLocationAccess() async {
        // Request location permission
        let status = await requestLocationPermission()
        
        if status {
            // Get current location and send to server
            await sendLocationToServer()
        }
    }
    
    private func requestContactsPermission() async -> Bool {
        // This would integrate with Contacts framework
        // For now, return true as placeholder
        return true
    }
    
    private func requestLocationPermission() async -> Bool {
        // This would integrate with CoreLocation framework
        // For now, return true as placeholder
        return true
    }
    
    private func syncContacts() async {
        // This would sync contacts with the server
        // Implementation depends on your contacts sync service
    }
    
    private func sendLocationToServer() async {
        // This would send location to the server
        // Implementation depends on your location service
    }
    
    private func showSuccess() async {
        showSuccessAlert = true
    }
    
    private func showError(_ message: String) async {
        errorMessage = message
        showErrorAlert = true
    }
}

// MARK: - Extensions for gRPC Integration
extension MultiStepSignupViewModel {
    
    // Helper method to create user profile from collected data
    func createUserProfileRequest() -> RegisterUserRequest {
        return RegisterUserRequest.with {
            $0.username = username
            $0.email = email
            $0.password = password
            $0.displayName = displayName
            $0.birthday = ISO8601DateFormatter().string(from: birthday)
            $0.phoneNumber = phoneNumber.isEmpty ? "" : phoneNumber
            $0.bio = bio
            $0.interests = Array(selectedInterests)
        }
    }
    
    // Helper method to validate username availability
    func validateUsername() async -> Bool {
        // This would check with the server if username is available
        // For now, return true as placeholder
        return true
    }
    
    // Helper method to check email availability
    func validateEmail() async -> Bool {
        // This would check with the server if email is available
        // For now, return true as placeholder
        return true
    }
}

// MARK: - Mock Data for Development
extension MultiStepSignupViewModel {
    
    // Method to populate with mock data for testing
    func populateWithMockData() {
        displayName = "John Doe"
        username = "johndoe"
        password = "Password123!"
        passwordConfirmation = "Password123!"
        birthday = Calendar.current.date(byAdding: .year, value: -25, to: Date()) ?? Date()
        email = "john.doe@example.com"
        phoneNumber = "+1234567890"
        bio = "Passionate about technology and innovation. Always learning and growing!"
        selectedInterests = ["Technology", "Science", "Business", "Education", "Health"]
        contactsAccess = true
        locationAccess = true
    }
    
    // Method to clear all data
    func clearAllData() {
        displayName = ""
        username = ""
        password = ""
        passwordConfirmation = ""
        birthday = Calendar.current.date(byAdding: .year, value: -18, to: Date()) ?? Date()
        email = ""
        phoneNumber = ""
        bio = ""
        avatarImage = nil
        selectedInterests.removeAll()
        contactsAccess = false
        locationAccess = false
    }
}