import SwiftUI

struct MultiStepSignupView: View {
    @StateObject private var viewModel = MultiStepSignupViewModel()
    @Environment(\.dismiss) private var dismiss
    @State private var currentStep = 0
    @State private var showPrivacyPolicy = false
    @State private var showTermsOfService = false
    
    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                // Progress indicator
                SignupProgressView(currentStep: currentStep, totalSteps: 5)
                
                // Step content
                TabView(selection: $currentStep) {
                    // Step 1: Basic Information
                    BasicInfoStepView(
                        displayName: $viewModel.displayName,
                        username: $viewModel.username,
                        password: $viewModel.password,
                        passwordConfirmation: $viewModel.passwordConfirmation,
                        onNext: { moveToNextStep() }
                    )
                    .tag(0)
                    
                    // Step 2: Compliance & Contact
                    ComplianceStepView(
                        birthday: $viewModel.birthday,
                        email: $viewModel.email,
                        phoneNumber: $viewModel.phoneNumber,
                        onNext: { moveToNextStep() },
                        onBack: { moveToPreviousStep() }
                    )
                    .tag(1)
                    
                    // Step 3: Profile Completion (Optional)
                    ProfileCompletionStepView(
                        bio: $viewModel.bio,
                        avatarImage: $viewModel.avatarImage,
                        onNext: { moveToNextStep() },
                        onBack: { moveToPreviousStep() },
                        onSkip: { moveToNextStep() }
                    )
                    .tag(2)
                    
                    // Step 4: Interests
                    InterestsStepView(
                        selectedInterests: $viewModel.selectedInterests,
                        onNext: { moveToNextStep() },
                        onBack: { moveToPreviousStep() }
                    )
                    .tag(3)
                    
                    // Step 5: Permissions
                    PermissionsStepView(
                        contactsAccess: $viewModel.contactsAccess,
                        locationAccess: $viewModel.locationAccess,
                        onComplete: { completeSignup() },
                        onBack: { moveToPreviousStep() }
                    )
                    .tag(4)
                }
                .tabViewStyle(PageTabViewStyle(indexDisplayMode: .never))
                .animation(.easeInOut(duration: 0.3), value: currentStep)
            }
            .navigationTitle("Join Sonet")
            .navigationBarTitleDisplayMode(.large)
            .navigationBarBackButtonHidden(true)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    if currentStep > 0 {
                        Button("Back") {
                            moveToPreviousStep()
                        }
                        .foregroundColor(.blue)
                    }
                }
                
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Cancel") {
                        dismiss()
                    }
                    .foregroundColor(.secondary)
                }
            }
        }
        .sheet(isPresented: $showPrivacyPolicy) {
            PrivacyPolicyView()
        }
        .sheet(isPresented: $showTermsOfService) {
            TermsOfServiceView()
        }
        .alert("Signup Complete!", isPresented: $viewModel.showSuccessAlert) {
            Button("Get Started") {
                dismiss()
            }
        } message: {
            Text("Welcome to Sonet! Your account has been created successfully.")
        }
        .alert("Error", isPresented: $viewModel.showErrorAlert) {
            Button("OK") { }
        } message: {
            Text(viewModel.errorMessage ?? "An error occurred during signup.")
        }
    }
    
    private func moveToNextStep() {
        if currentStep < 4 {
            withAnimation(.easeInOut(duration: 0.3)) {
                currentStep += 1
            }
        }
    }
    
    private func moveToPreviousStep() {
        if currentStep > 0 {
            withAnimation(.easeInOut(duration: 0.3)) {
                currentStep -= 1
            }
        }
    }
    
    private func completeSignup() {
        Task {
            await viewModel.completeSignup()
        }
    }
}

// MARK: - Progress Indicator
struct SignupProgressView: View {
    let currentStep: Int
    let totalSteps: Int
    
    var body: some View {
        VStack(spacing: 16) {
            HStack {
                Text("Step \(currentStep + 1) of \(totalSteps)")
                    .font(.subheadline)
                    .foregroundColor(.secondary)
                
                Spacer()
                
                Text("\(Int((Double(currentStep + 1) / Double(totalSteps)) * 100))%")
                    .font(.subheadline)
                    .fontWeight(.semibold)
                    .foregroundColor(.blue)
            }
            
            // Progress bar
            GeometryReader { geometry in
                ZStack(alignment: .leading) {
                    Rectangle()
                        .fill(Color(.systemGray5))
                        .frame(height: 4)
                        .cornerRadius(2)
                    
                    Rectangle()
                        .fill(Color.blue)
                        .frame(width: geometry.size.width * progress, height: 4)
                        .cornerRadius(2)
                        .animation(.easeInOut(duration: 0.3), value: currentStep)
                }
            }
            .frame(height: 4)
        }
        .padding(.horizontal, 20)
        .padding(.top, 20)
    }
    
    private var progress: Double {
        Double(currentStep + 1) / Double(totalSteps)
    }
}

// MARK: - Step 1: Basic Information
struct BasicInfoStepView: View {
    @Binding var displayName: String
    @Binding var username: String
    @Binding var password: String
    @Binding var passwordConfirmation: String
    let onNext: () -> Void
    
    @State private var showPassword = false
    @State private var showPasswordConfirmation = false
    
    var body: some View {
        ScrollView {
            VStack(spacing: 24) {
                // Header
                VStack(spacing: 12) {
                    Image(systemName: "person.badge.plus")
                        .font(.system(size: 48))
                        .foregroundColor(.blue)
                    
                    Text("Let's get you started!")
                        .font(.title2)
                        .fontWeight(.bold)
                    
                    Text("Create your Sonet account and join millions of people sharing amazing moments.")
                        .font(.body)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                }
                .padding(.top, 20)
                
                // Form fields
                VStack(spacing: 20) {
                    // Display Name
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Display Name")
                            .font(.headline)
                            .foregroundColor(.primary)
                        
                        TextField("Enter your display name", text: $displayName)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                            .autocapitalization(.words)
                    }
                    
                    // Username
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Username")
                            .font(.headline)
                            .foregroundColor(.primary)
                        
                        HStack {
                            Text("@")
                                .foregroundColor(.secondary)
                                .font(.title3)
                            
                            TextField("username", text: $username)
                                .textFieldStyle(RoundedBorderTextFieldStyle())
                                .autocapitalization(.none)
                                .disableAutocorrection(true)
                        }
                        
                        Text("This will be your unique identifier on Sonet")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                    
                    // Password
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Password")
                            .font(.headline)
                            .foregroundColor(.primary)
                        
                        HStack {
                            if showPassword {
                                TextField("Enter your password", text: $password)
                                    .textFieldStyle(RoundedBorderTextFieldStyle())
                            } else {
                                SecureField("Enter your password", text: $password)
                                    .textFieldStyle(RoundedBorderTextFieldStyle())
                            }
                            
                            Button(action: { showPassword.toggle() }) {
                                Image(systemName: showPassword ? "eye.slash" : "eye")
                                    .foregroundColor(.secondary)
                            }
                        }
                        
                        Text("Use at least 8 characters with a mix of letters, numbers, and symbols")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                    
                    // Password Confirmation
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Confirm Password")
                            .font(.headline)
                            .foregroundColor(.primary)
                        
                        HStack {
                            if showPasswordConfirmation {
                                TextField("Confirm your password", text: $passwordConfirmation)
                                    .textFieldStyle(RoundedBorderTextFieldStyle())
                            } else {
                                SecureField("Confirm your password", text: $passwordConfirmation)
                                    .textFieldStyle(RoundedBorderTextFieldStyle())
                            }
                            
                            Button(action: { showPasswordConfirmation.toggle() }) {
                                Image(systemName: showPasswordConfirmation ? "eye.slash" : "eye")
                                    .foregroundColor(.secondary)
                            }
                        }
                    }
                }
                .padding(.horizontal, 20)
                
                // Next button
                Button(action: onNext) {
                    Text("Continue")
                        .font(.headline)
                        .foregroundColor(.white)
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 16)
                        .background(
                            RoundedRectangle(cornerRadius: 12)
                                .fill(canProceed ? Color.blue : Color(.systemGray4))
                        )
                }
                .disabled(!canProceed)
                .padding(.horizontal, 20)
                .padding(.bottom, 40)
            }
        }
    }
    
    private var canProceed: Bool {
        !displayName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty &&
        !username.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty &&
        password.count >= 8 &&
        password == passwordConfirmation
    }
}

// MARK: - Step 2: Compliance & Contact
struct ComplianceStepView: View {
    @Binding var birthday: Date
    @Binding var email: String
    @Binding var phoneNumber: String
    let onNext: () -> Void
    let onBack: () -> Void
    
    @State private var isAgeValid = false
    
    var body: some View {
        ScrollView {
            VStack(spacing: 24) {
                // Header
                VStack(spacing: 12) {
                    Image(systemName: "shield.checkered")
                        .font(.system(size: 48))
                        .foregroundColor(.green)
                    
                    Text("Almost there!")
                        .font(.title2)
                        .fontWeight(.bold)
                    
                    Text("We need a few more details to keep Sonet safe and secure for everyone.")
                        .font(.body)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                }
                .padding(.top, 20)
                
                // Form fields
                VStack(spacing: 20) {
                    // Birthday
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Birthday")
                            .font(.headline)
                            .foregroundColor(.primary)
                        
                        DatePicker(
                            "Select your birthday",
                            selection: $birthday,
                            displayedComponents: .date
                        )
                        .datePickerStyle(WheelDatePickerStyle())
                        .onChange(of: birthday) { _ in
                            validateAge()
                        }
                        
                        if !isAgeValid {
                            Text("You must be at least 13 years old to use Sonet")
                                .font(.caption)
                                .foregroundColor(.red)
                        }
                    }
                    
                    // Email
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Email Address")
                            .font(.headline)
                            .foregroundColor(.primary)
                        
                        TextField("Enter your email address", text: $email)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                            .keyboardType(.emailAddress)
                            .autocapitalization(.none)
                        
                        Text("We'll use this to send you important updates and keep your account secure")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                    
                    // Phone Number
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Phone Number (Optional)")
                            .font(.headline)
                            .foregroundColor(.primary)
                        
                        TextField("Enter your phone number", text: $phoneNumber)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                            .keyboardType(.phonePad)
                        
                        Text("This helps us recommend friends and keep your account secure. You can skip this step.")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                }
                .padding(.horizontal, 20)
                
                // Navigation buttons
                HStack(spacing: 16) {
                    Button(action: onBack) {
                        Text("Back")
                            .font(.headline)
                            .foregroundColor(.blue)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 16)
                            .background(
                                RoundedRectangle(cornerRadius: 12)
                                    .stroke(Color.blue, lineWidth: 2)
                            )
                    }
                    
                    Button(action: onNext) {
                        Text("Continue")
                            .font(.headline)
                            .foregroundColor(.white)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 16)
                            .background(
                                RoundedRectangle(cornerRadius: 12)
                                    .fill(canProceed ? Color.blue : Color(.systemGray4))
                            )
                    }
                    .disabled(!canProceed)
                }
                .padding(.horizontal, 20)
                .padding(.bottom, 40)
            }
        }
        .onAppear {
            validateAge()
        }
    }
    
    private func validateAge() {
        let calendar = Calendar.current
        let ageComponents = calendar.dateComponents([.year], from: birthday, to: Date())
        isAgeValid = (ageComponents.year ?? 0) >= 13
    }
    
    private var canProceed: Bool {
        isAgeValid && !email.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }
}

// MARK: - Step 3: Profile Completion (Optional)
struct ProfileCompletionStepView: View {
    @Binding var bio: String
    @Binding var avatarImage: UIImage?
    let onNext: () -> Void
    let onBack: () -> Void
    let onSkip: () -> Void
    
    @State private var showImagePicker = false
    
    var body: some View {
        ScrollView {
            VStack(spacing: 24) {
                // Header
                VStack(spacing: 12) {
                    Image(systemName: "person.crop.circle.badge.plus")
                        .font(.system(size: 48))
                        .foregroundColor(.orange)
                    
                    Text("Tell us about yourself!")
                        .font(.title2)
                    .fontWeight(.bold)
                    
                    Text("This step is completely optional. You can always add these details later.")
                        .font(.body)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                }
                .padding(.top, 20)
                
                // Form fields
                VStack(spacing: 20) {
                    // Profile Picture
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Profile Picture")
                            .font(.headline)
                            .foregroundColor(.primary)
                        
                        Button(action: { showImagePicker = true }) {
                            if let avatarImage = avatarImage {
                                Image(uiImage: avatarImage)
                                    .resizable()
                                    .aspectRatio(contentMode: .fill)
                                    .frame(width: 100, height: 100)
                                    .clipShape(Circle())
                                    .overlay(
                                        Circle()
                                            .stroke(Color.blue, lineWidth: 2)
                                    )
                            } else {
                                ZStack {
                                    Circle()
                                        .fill(Color(.systemGray5))
                                        .frame(width: 100, height: 100)
                                    
                                    VStack(spacing: 4) {
                                        Image(systemName: "camera")
                                            .font(.system(size: 24))
                                            .foregroundColor(.secondary)
                                        
                                        Text("Add Photo")
                                            .font(.caption)
                                            .foregroundColor(.secondary)
                                    }
                                }
                            }
                        }
                        .buttonStyle(PlainButtonStyle())
                        
                        Text("Add a profile picture to help friends recognize you")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                    
                    // Bio
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Bio")
                            .font(.headline)
                            .foregroundColor(.primary)
                        
                        TextField("Tell us about yourself...", text: $bio, axis: .vertical)
                            .textFieldStyle(RoundedBorderTextFieldStyle())
                            .lineLimit(3...6)
                        
                        Text("Share a little about yourself (optional)")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                }
                .padding(.horizontal, 20)
                
                // Navigation buttons
                VStack(spacing: 16) {
                    Button(action: onNext) {
                        Text("Continue")
                            .font(.headline)
                            .foregroundColor(.white)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 16)
                            .background(
                                RoundedRectangle(cornerRadius: 12)
                                    .fill(Color.blue)
                            )
                    }
                    
                    Button(action: onSkip) {
                        Text("Skip for now")
                            .font(.subheadline)
                            .foregroundColor(.secondary)
                    }
                    
                    Button(action: onBack) {
                        Text("Back")
                            .font(.headline)
                            .foregroundColor(.blue)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 16)
                            .background(
                                RoundedRectangle(cornerRadius: 12)
                                    .stroke(Color.blue, lineWidth: 2)
                            )
                    }
                }
                .padding(.horizontal, 20)
                .padding(.bottom, 40)
            }
        }
        .sheet(isPresented: $showImagePicker) {
            ImagePicker(selectedImage: $avatarImage)
        }
    }
}

// MARK: - Step 4: Interests
struct InterestsStepView: View {
    @Binding var selectedInterests: Set<String>
    let onNext: () -> Void
    let onBack: () -> Void
    
    private let availableInterests = [
        "Technology", "Music", "Sports", "Travel", "Food", "Fashion",
        "Art", "Gaming", "Fitness", "Photography", "Books", "Movies",
        "Science", "Business", "Education", "Health", "Nature", "Cars",
        "Pets", "DIY", "Cooking", "Dance", "Comedy", "News"
    ]
    
    var body: some View {
        ScrollView {
            VStack(spacing: 24) {
                // Header
                VStack(spacing: 12) {
                    Image(systemName: "heart.fill")
                        .font(.system(size: 48))
                        .foregroundColor(.red)
                    
                    Text("What interests you?")
                        .font(.title2)
                        .fontWeight(.bold)
                    
                    Text("Help us personalize your experience and connect you with like-minded people.")
                        .font(.body)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                }
                .padding(.top, 20)
                
                // Interests grid
                LazyVGrid(columns: Array(repeating: GridItem(.flexible()), count: 2), spacing: 12) {
                    ForEach(availableInterests, id: \.self) { interest in
                        InterestChip(
                            title: interest,
                            isSelected: selectedInterests.contains(interest),
                            onTap: {
                                if selectedInterests.contains(interest) {
                                    selectedInterests.remove(interest)
                                } else {
                                    selectedInterests.insert(interest)
                                }
                            }
                        )
                    }
                }
                .padding(.horizontal, 20)
                
                // Navigation buttons
                HStack(spacing: 16) {
                    Button(action: onBack) {
                        Text("Back")
                            .font(.headline)
                            .foregroundColor(.blue)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 16)
                            .background(
                                RoundedRectangle(cornerRadius: 12)
                                    .stroke(Color.blue, lineWidth: 2)
                            )
                    }
                    
                    Button(action: onNext) {
                        Text("Continue")
                            .font(.headline)
                            .foregroundColor(.white)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 16)
                            .background(
                                RoundedRectangle(cornerRadius: 12)
                                    .fill(canProceed ? Color.blue : Color(.systemGray4))
                            )
                    }
                    .disabled(!canProceed)
                }
                .padding(.horizontal, 20)
                .padding(.bottom, 40)
            }
        }
    }
    
    private var canProceed: Bool {
        selectedInterests.count >= 3
    }
}

// MARK: - Step 5: Permissions
struct PermissionsStepView: View {
    @Binding var contactsAccess: Bool
    @Binding var locationAccess: Bool
    let onComplete: () -> Void
    let onBack: () -> Void
    
    var body: some View {
        ScrollView {
            VStack(spacing: 24) {
                // Header
                VStack(spacing: 12) {
                    Image(systemName: "hand.raised.fill")
                        .font(.system(size: 48))
                        .foregroundColor(.purple)
                    
                    Text("Last step!")
                        .font(.title2)
                        .fontWeight(.bold)
                    
                    Text("Help us make Sonet better for you by granting a few permissions.")
                        .font(.body)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                }
                .padding(.top, 20)
                
                // Permissions
                VStack(spacing: 20) {
                    // Contacts Access
                    PermissionCard(
                        icon: "person.2.fill",
                        title: "Find Friends",
                        description: "Connect with people you know by syncing your contacts. We'll help you discover friends who are already on Sonet!",
                        isEnabled: $contactsAccess,
                        permissionType: "Contacts"
                    )
                    
                    // Location Access
                    PermissionCard(
                        icon: "location.fill",
                        title: "Discover Nearby",
                        description: "Find interesting people and content near you. We'll recommend local creators and trending topics in your area.",
                        isEnabled: $locationAccess,
                        permissionType: "Location"
                    )
                }
                .padding(.horizontal, 20)
                
                // Trust message
                VStack(spacing: 12) {
                    Image(systemName: "lock.shield")
                        .font(.system(size: 24))
                        .foregroundColor(.green)
                    
                    Text("Your privacy is important to us")
                        .font(.headline)
                        .fontWeight(.semibold)
                    
                    Text("You can always change these permissions in Settings. We never share your personal data with third parties.")
                        .font(.caption)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                }
                .padding(.horizontal, 20)
                
                // Navigation buttons
                HStack(spacing: 16) {
                    Button(action: onBack) {
                        Text("Back")
                            .font(.headline)
                            .foregroundColor(.blue)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 16)
                            .background(
                                RoundedRectangle(cornerRadius: 12)
                                    .stroke(Color.blue, lineWidth: 2)
                            )
                    }
                    
                    Button(action: onComplete) {
                        Text("Complete Signup")
                            .font(.headline)
                            .foregroundColor(.white)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 16)
                            .background(
                                RoundedRectangle(cornerRadius: 12)
                                    .fill(Color.blue)
                            )
                    }
                }
                .padding(.horizontal, 20)
                .padding(.bottom, 40)
            }
        }
    }
}

// MARK: - Supporting Views
struct InterestChip: View {
    let title: String
    let isSelected: Bool
    let onTap: () -> Void
    
    var body: some View {
        Button(action: onTap) {
            Text(title)
                .font(.subheadline)
                .fontWeight(.medium)
                .foregroundColor(isSelected ? .white : .primary)
                .padding(.horizontal, 16)
                .padding(.vertical, 12)
                .background(
                    RoundedRectangle(cornerRadius: 20)
                        .fill(isSelected ? Color.blue : Color(.systemGray6))
                )
                .overlay(
                    RoundedRectangle(cornerRadius: 20)
                        .stroke(isSelected ? Color.blue : Color.clear, lineWidth: 2)
                )
        }
        .buttonStyle(PlainButtonStyle())
    }
}

struct PermissionCard: View {
    let icon: String
    let title: String
    let description: String
    @Binding var isEnabled: Bool
    let permissionType: String
    
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 12) {
                Image(systemName: icon)
                    .font(.system(size: 24))
                    .foregroundColor(.blue)
                    .frame(width: 32)
                
                VStack(alignment: .leading, spacing: 4) {
                    Text(title)
                        .font(.headline)
                        .foregroundColor(.primary)
                    
                    Text(description)
                        .font(.subheadline)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.leading)
                }
                
                Spacer()
                
                Toggle("", isOn: $isEnabled)
                    .labelsHidden()
            }
        }
        .padding(16)
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(Color(.systemGray6))
        )
    }
}

// MARK: - Image Picker
struct ImagePicker: UIViewControllerRepresentable {
    @Binding var selectedImage: UIImage?
    @Environment(\.dismiss) private var dismiss
    
    func makeUIViewController(context: Context) -> UIImagePickerController {
        let picker = UIImagePickerController()
        picker.delegate = context.coordinator
        picker.sourceType = .photoLibrary
        picker.allowsEditing = true
        return picker
    }
    
    func updateUIViewController(_ uiViewController: UIImagePickerController, context: Context) {}
    
    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }
    
    class Coordinator: NSObject, UIImagePickerControllerDelegate, UINavigationControllerDelegate {
        let parent: ImagePicker
        
        init(_ parent: ImagePicker) {
            self.parent = parent
        }
        
        func imagePickerController(_ picker: UIImagePickerController, didFinishPickingMediaWithInfo info: [UIImagePickerController.InfoKey : Any]) {
            if let editedImage = info[.editedImage] as? UIImage {
                parent.selectedImage = editedImage
            } else if let originalImage = info[.originalImage] as? UIImage {
                parent.selectedImage = originalImage
            }
            parent.dismiss()
        }
        
        func imagePickerControllerDidCancel(_ picker: UIImagePickerController) {
            parent.dismiss()
        }
    }
}

// MARK: - Privacy Policy & Terms Views
struct PrivacyPolicyView: View {
    var body: some View {
        NavigationView {
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    Text("Privacy Policy")
                        .font(.title)
                        .fontWeight(.bold)
                    
                    Text("Your privacy is important to us. This policy explains how we collect, use, and protect your information.")
                        .font(.body)
                    
                    // Add your privacy policy content here
                }
                .padding()
            }
            .navigationTitle("Privacy Policy")
            .navigationBarTitleDisplayMode(.inline)
        }
    }
}

struct TermsOfServiceView: View {
    var body: some View {
        NavigationView {
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    Text("Terms of Service")
                        .font(.title)
                        .fontWeight(.bold)
                    
                    Text("By using Sonet, you agree to these terms of service.")
                        .font(.body)
                    
                    // Add your terms of service content here
                }
                .padding()
            }
            .navigationTitle("Terms of Service")
            .navigationBarTitleDisplayMode(.inline)
        }
    }
}

// MARK: - Preview
struct MultiStepSignupView_Previews: PreviewProvider {
    static var previews: some View {
        MultiStepSignupView()
    }
}