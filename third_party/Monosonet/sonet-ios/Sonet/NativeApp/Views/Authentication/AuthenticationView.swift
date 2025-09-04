import SwiftUI

struct AuthenticationView: View {
    @EnvironmentObject var sessionManager: SessionManager
    @State private var isShowingSignUp = false
    @State private var username = ""
    @State private var password = ""
    @State private var isLoading = false
    @State private var showPassword = false
    
    var body: some View {
        NavigationView {
            VStack(spacing: 32) {
                // Header
                VStack(spacing: 16) {
                    Image(systemName: "network")
                        .font(.system(size: 80))
                        .foregroundColor(.blue)
                    
                    Text("Welcome to Sonet")
                        .font(.largeTitle)
                        .fontWeight(.bold)
                        .multilineTextAlignment(.center)
                    
                    Text("Connect with friends and discover amazing content")
                        .font(.body)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                        .padding(.horizontal, 32)
                }
                
                // Login Form
                VStack(spacing: 20) {
                    VStack(spacing: 16) {
                        // Username Field
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Username")
                                .font(.headline)
                                .foregroundColor(.primary)
                            
                            HStack {
                                Image(systemName: "at")
                                    .foregroundColor(.secondary)
                                    .frame(width: 20)
                                
                                TextField("Enter username", text: $username)
                                    .textFieldStyle(RoundedBorderTextFieldStyle())
                                    .autocapitalization(.none)
                                    .disableAutocorrection(true)
                            }
                        }
                        
                        // Password Field
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Password")
                                .font(.headline)
                                .foregroundColor(.primary)
                            
                            HStack {
                                Image(systemName: "lock")
                                    .foregroundColor(.secondary)
                                    .frame(width: 20)
                                
                                if showPassword {
                                    TextField("Enter password", text: $password)
                                        .textFieldStyle(RoundedBorderTextFieldStyle())
                                } else {
                                    SecureField("Enter password", text: $password)
                                        .textFieldStyle(RoundedBorderTextFieldStyle())
                                }
                                
                                Button(action: {
                                    showPassword.toggle()
                                }) {
                                    Image(systemName: showPassword ? "eye.slash" : "eye")
                                        .foregroundColor(.secondary)
                                }
                            }
                        }
                    }
                    
                    // Error Message
                    if let error = sessionManager.authenticationError {
                        Text(error.localizedDescription)
                            .foregroundColor(.red)
                            .font(.caption)
                            .multilineTextAlignment(.center)
                    }
                    
                    // Login Button
                    Button(action: {
                        Task {
                            await performLogin()
                        }
                    }) {
                        HStack {
                            if isLoading {
                                ProgressView()
                                    .progressViewStyle(CircularProgressViewStyle(tint: .white))
                                    .scaleEffect(0.8)
                            } else {
                                Text("Sign In")
                                    .fontWeight(.semibold)
                            }
                        }
                        .frame(maxWidth: .infinity)
                        .frame(height: 50)
                        .background(Color.blue)
                        .foregroundColor(.white)
                        .cornerRadius(12)
                    }
                    .disabled(username.isEmpty || password.isEmpty || isLoading)
                    .opacity((username.isEmpty || password.isEmpty || isLoading) ? 0.6 : 1.0)
                    
                    // Sign Up Link
                    HStack {
                        Text("Don't have an account?")
                            .foregroundColor(.secondary)
                        
                        Button("Sign Up") {
                            isShowingSignUp = true
                        }
                        .foregroundColor(.blue)
                        .fontWeight(.semibold)
                    }
                    .font(.subheadline)
                }
                .padding(.horizontal, 32)
                
                Spacer()
                
                // Footer
                VStack(spacing: 8) {
                    Text("By continuing, you agree to our")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    
                    HStack(spacing: 4) {
                        Button("Terms of Service") {
                            // Open terms
                        }
                        .font(.caption)
                        .foregroundColor(.blue)
                        
                        Text("and")
                            .font(.caption)
                            .foregroundColor(.secondary)
                        
                        Button("Privacy Policy") {
                            // Open privacy policy
                        }
                        .font(.caption)
                        .foregroundColor(.blue)
                    }
                }
            }
            .padding(.vertical, 32)
            .background(Color(.systemBackground))
            .sheet(isPresented: $isShowingSignUp) {
                SignUpView()
            }
        }
        .navigationViewStyle(StackNavigationViewStyle())
    }
    
    private func performLogin() async {
        isLoading = true
        
        do {
            try await sessionManager.authenticate(username: username, password: password)
        } catch {
            // Error is already handled by SessionManager
            print("Login failed: \(error)")
        }
        
        isLoading = false
    }
}

// MARK: - Sign Up View
struct SignUpView: View {
    @Environment(\.dismiss) private var dismiss
    @State private var username = ""
    @State private var email = ""
    @State private var password = ""
    @State private var confirmPassword = ""
    @State private var isLoading = false
    @State private var showPassword = false
    @State private var showConfirmPassword = false
    
    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 24) {
                    // Header
                    VStack(spacing: 8) {
                        Text("Create Account")
                            .font(.largeTitle)
                            .fontWeight(.bold)
                        
                        Text("Join Sonet and start sharing your world")
                            .font(.body)
                            .foregroundColor(.secondary)
                            .multilineTextAlignment(.center)
                    }
                    .padding(.top, 20)
                    
                    // Form Fields
                    VStack(spacing: 20) {
                        // Username Field
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Username")
                                .font(.headline)
                            
                            TextField("Choose username", text: $username)
                                .textFieldStyle(RoundedBorderTextFieldStyle())
                                .autocapitalization(.none)
                                .disableAutocorrection(true)
                        }
                        
                        // Email Field
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Email")
                                .font(.headline)
                            
                            TextField("Enter email", text: $email)
                                .textFieldStyle(RoundedBorderTextFieldStyle())
                                .keyboardType(.emailAddress)
                                .autocapitalization(.none)
                                .disableAutocorrection(true)
                        }
                        
                        // Password Field
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Password")
                                .font(.headline)
                            
                            HStack {
                                if showPassword {
                                    TextField("Create password", text: $password)
                                        .textFieldStyle(RoundedBorderTextFieldStyle())
                                } else {
                                    SecureField("Create password", text: $password)
                                        .textFieldStyle(RoundedBorderTextFieldStyle())
                                }
                                
                                Button(action: {
                                    showPassword.toggle()
                                }) {
                                    Image(systemName: showPassword ? "eye.slash" : "eye")
                                        .foregroundColor(.secondary)
                                }
                            }
                        }
                        
                        // Confirm Password Field
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Confirm Password")
                                .font(.headline)
                            
                            HStack {
                                if showConfirmPassword {
                                    TextField("Confirm password", text: $confirmPassword)
                                        .textFieldStyle(RoundedBorderTextFieldStyle())
                                } else {
                                    SecureField("Confirm password", text: $confirmPassword)
                                        .textFieldStyle(RoundedBorderTextFieldStyle())
                                }
                                
                                Button(action: {
                                    showConfirmPassword.toggle()
                                }) {
                                    Image(systemName: showConfirmPassword ? "eye.slash" : "eye")
                                        .foregroundColor(.secondary)
                                }
                            }
                        }
                    }
                    .padding(.horizontal, 32)
                    
                    // Sign Up Button
                    Button(action: {
                        Task {
                            await performSignUp()
                        }
                    }) {
                        HStack {
                            if isLoading {
                                ProgressView()
                                    .progressViewStyle(CircularProgressViewStyle(tint: .white))
                                    .scaleEffect(0.8)
                            } else {
                                Text("Create Account")
                                    .fontWeight(.semibold)
                            }
                        }
                        .frame(maxWidth: .infinity)
                        .frame(height: 50)
                        .background(Color.blue)
                        .foregroundColor(.white)
                        .cornerRadius(12)
                    }
                    .disabled(!isFormValid || isLoading)
                    .opacity((!isFormValid || isLoading) ? 0.6 : 1.0)
                    .padding(.horizontal, 32)
                    
                    Spacer(minLength: 50)
                }
            }
            .navigationTitle("Sign Up")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        dismiss()
                    }
                }
            }
        }
    }
    
    private var isFormValid: Bool {
        !username.isEmpty && !email.isEmpty && !password.isEmpty && 
        password == confirmPassword && password.count >= 8
    }
    
    private func performSignUp() async {
        isLoading = true
        
        // Simulate sign up process
        try? await Task.sleep(nanoseconds: 2_000_000_000)
        
        // For now, just dismiss the view
        // In real implementation, this would create the account
        dismiss()
        
        isLoading = false
    }
}

// MARK: - Preview
struct AuthenticationView_Previews: PreviewProvider {
    static var previews: some View {
        AuthenticationView()
            .environmentObject(SessionManager())
    }
}