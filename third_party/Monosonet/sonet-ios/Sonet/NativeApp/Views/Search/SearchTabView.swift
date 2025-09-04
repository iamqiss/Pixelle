import SwiftUI

struct SearchTabView: View {
    @EnvironmentObject var navigationManager: NavigationManager
    @EnvironmentObject var themeManager: ThemeManager
    @StateObject private var grpcClient = SonetGRPCClient(configuration: .development)
    
    var body: some View {
        SearchView(grpcClient: grpcClient)
    }
}

struct SearchTabView_Previews: PreviewProvider {
    static var previews: some View {
        SearchTabView()
            .environmentObject(NavigationManager())
            .environmentObject(ThemeManager())
    }
}