import SwiftUI

struct ProfileTabView: View {
    @EnvironmentObject var navigationManager: NavigationManager
    @EnvironmentObject var themeManager: ThemeManager
    @EnvironmentObject var sessionManager: SessionManager
    @StateObject private var grpcClient = SonetGRPCClient(configuration: .development)
    
    var body: some View {
        if let user = sessionManager.currentUser {
            ProfileView(userId: user.id, grpcClient: grpcClient)
        } else {
            // Show login prompt
            VStack(spacing: 20) {
                Image(systemName: "person.circle")
                    .font(.system(size: 60))
                    .foregroundColor(.secondary)
                
                Text("Not Logged In")
                    .font(.title2)
                    .fontWeight(.bold)
                
                Text("Please log in to view your profile")
                    .font(.body)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 32)
                
                Button("Log In") {
                    // Navigate to login
                }
                .buttonStyle(.borderedProminent)
            }
            .padding(.vertical, 32)
            .navigationTitle("Profile")
        }
    }
}

struct ProfileTabView_Previews: PreviewProvider {
    static var previews: some View {
        ProfileTabView()
            .environmentObject(NavigationManager())
            .environmentObject(ThemeManager())
            .environmentObject(SessionManager())
    }
}