import SwiftUI

struct WelcomeView: View {
    @State private var showSignup = false
    @State private var showLogin = false
    @State private var animateGradient = false
    
    var body: some View {
        GeometryReader { geometry in
            ZStack {
                // Animated gradient background
                LinearGradient(
                    colors: [
                        Color.blue.opacity(0.8),
                        Color.purple.opacity(0.6),
                        Color.orange.opacity(0.4)
                    ],
                    startPoint: animateGradient ? .topLeading : .bottomTrailing,
                    endPoint: animateGradient ? .bottomTrailing : .topLeading
                )
                .ignoresSafeArea()
                .onAppear {
                    withAnimation(.easeInOut(duration: 3.0).repeatForever(autoreverses: true)) {
                        animateGradient.toggle()
                    }
                }
                
                // Content
                VStack(spacing: 40) {
                    Spacer()
                    
                    // Sonet Logo & Branding
                    VStack(spacing: 24) {
                        // Logo
                        ZStack {
                            Circle()
                                .fill(Color.white.opacity(0.2))
                                .frame(width: 120, height: 120)
                            
                            Image(systemName: "network")
                                .font(.system(size: 60, weight: .light))
                                .foregroundColor(.white)
                        }
                        .scaleEffect(animateGradient ? 1.1 : 1.0)
                        .animation(.easeInOut(duration: 2.0).repeatForever(autoreverses: true), value: animateGradient)
                        
                        // App Name
                        Text("Sonet")
                            .font(.system(size: 48, weight: .bold, design: .rounded))
                            .foregroundColor(.white)
                        
                        // Tagline
                        Text("Connect. Share. Discover.")
                            .font(.title2)
                            .fontWeight(.medium)
                            .foregroundColor(.white.opacity(0.9))
                            .multilineTextAlignment(.center)
                    }
                    
                    // Features Preview
                    VStack(spacing: 20) {
                        FeatureRow(
                            icon: "video.fill",
                            title: "TikTok-style Videos",
                            description: "Create and discover amazing short-form content"
                        )
                        
                        FeatureRow(
                            icon: "message.fill",
                            title: "Smart Messaging",
                            description: "Connect with friends through intelligent conversations"
                        )
                        
                        FeatureRow(
                            icon: "person.2.fill",
                            title: "Social Discovery",
                            description: "Find people who share your interests and passions"
                        )
                    }
                    .padding(.horizontal, 40)
                    
                    Spacer()
                    
                    // Action Buttons
                    VStack(spacing: 16) {
                        // Get Started Button
                        Button(action: { showSignup = true }) {
                            HStack {
                                Text("Get Started")
                                    .font(.title3)
                                    .fontWeight(.semibold)
                                
                                Image(systemName: "arrow.right")
                                    .font(.title3)
                                    .fontWeight(.semibold)
                            }
                            .foregroundColor(.blue)
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 18)
                            .background(
                                RoundedRectangle(cornerRadius: 16)
                                    .fill(Color.white)
                                    .shadow(color: .black.opacity(0.1), radius: 10, x: 0, y: 5)
                            )
                        }
                        .buttonStyle(PlainButtonStyle())
                        
                        // Sign In Button
                        Button(action: { showLogin = true }) {
                            Text("Already have an account? Sign In")
                                .font(.body)
                                .fontWeight(.medium)
                                .foregroundColor(.white)
                                .padding(.vertical, 16)
                        }
                        .buttonStyle(PlainButtonStyle())
                    }
                    .padding(.horizontal, 40)
                    .padding(.bottom, 40)
                }
            }
        }
        .sheet(isPresented: $showSignup) {
            MultiStepSignupView()
        }
        .sheet(isPresented: $showLogin) {
            LoginView()
        }
    }
}

// MARK: - Feature Row
struct FeatureRow: View {
    let icon: String
    let title: String
    let description: String
    
    var body: some View {
        HStack(spacing: 16) {
            // Icon
            ZStack {
                Circle()
                    .fill(Color.white.opacity(0.2))
                    .frame(width: 50, height: 50)
                
                Image(systemName: icon)
                    .font(.system(size: 24, weight: .medium))
                    .foregroundColor(.white)
            }
            
            // Text
            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.headline)
                    .fontWeight(.semibold)
                    .foregroundColor(.white)
                
                Text(description)
                    .font(.subheadline)
                    .foregroundColor(.white.opacity(0.8))
                    .multilineTextAlignment(.leading)
            }
            
            Spacer()
        }
        .padding(.horizontal, 20)
        .padding(.vertical, 16)
        .background(
            RoundedRectangle(cornerRadius: 16)
                .fill(Color.white.opacity(0.1))
                .overlay(
                    RoundedRectangle(cornerRadius: 16)
                        .stroke(Color.white.opacity(0.2), lineWidth: 1)
                )
        )
    }
}

// MARK: - Login View (Placeholder)
struct LoginView: View {
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        NavigationView {
            VStack(spacing: 24) {
                Text("Sign In")
                    .font(.largeTitle)
                    .fontWeight(.bold)
                
                Text("Welcome back! Sign in to your Sonet account.")
                    .font(.body)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
                
                // Placeholder for login form
                VStack(spacing: 16) {
                    TextField("Email or Username", text: .constant(""))
                        .textFieldStyle(RoundedBorderTextFieldStyle())
                    
                    SecureField("Password", text: .constant(""))
                        .textFieldStyle(RoundedBorderTextFieldStyle())
                    
                    Button("Sign In") {
                        // TODO: Implement login
                    }
                    .buttonStyle(.borderedProminent)
                }
                .padding(.horizontal, 40)
                
                Spacer()
            }
            .padding(.top, 40)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Cancel") {
                        dismiss()
                    }
                }
            }
        }
    }
}

// MARK: - Preview
struct WelcomeView_Previews: PreviewProvider {
    static var previews: some View {
        WelcomeView()
    }
}