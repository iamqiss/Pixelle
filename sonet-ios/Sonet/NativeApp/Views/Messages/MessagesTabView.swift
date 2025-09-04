import SwiftUI

struct MessagesTabView: View {
    @EnvironmentObject var navigationManager: NavigationManager
    @EnvironmentObject var themeManager: ThemeManager
    
    var body: some View {
        NavigationView {
            VStack(spacing: 20) {
                // Messages Header
                VStack(spacing: 16) {
                    IconView(AppIcons.messages, size: 60, color: .blue)
                    
                    Text("Messages")
                        .font(.largeTitle)
                        .fontWeight(.bold)
                    
                    Text("Direct messaging coming soon")
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

struct MessagesTabView_Previews: PreviewProvider {
    static var previews: some View {
        MessagesTabView()
            .environmentObject(NavigationManager())
            .environmentObject(ThemeManager())
    }
}