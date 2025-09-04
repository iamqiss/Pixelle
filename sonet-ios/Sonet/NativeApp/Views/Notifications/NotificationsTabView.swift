import SwiftUI

struct NotificationsTabView: View {
    @EnvironmentObject var navigationManager: NavigationManager
    @EnvironmentObject var themeManager: ThemeManager
    
    var body: some View {
        NavigationView {
            VStack(spacing: 20) {
                // Notifications Header
                VStack(spacing: 16) {
                    IconView(AppIcons.notifications, size: 60, color: .blue)
                    
                    Text("Notifications")
                        .font(.largeTitle)
                        .fontWeight(.bold)
                    
                    Text("Activity notifications coming soon")
                        .font(.body)
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.center)
                        .padding(.horizontal, 32)
                }
                
                Spacer()
            }
            .padding(.vertical, 32)
            .navigationBarHidden(true)
        }
        .navigationViewStyle(StackNavigationViewStyle())
    }
}

struct NotificationsTabView_Previews: PreviewProvider {
    static var previews: some View {
        NotificationsTabView()
            .environmentObject(NavigationManager())
            .environmentObject(ThemeManager())
    }
}