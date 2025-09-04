import SwiftUI

struct MeatballMenu: View {
    @Binding var isPresented: Bool
    @State private var searchText = ""
    @State private var selectedTheme: ColorScheme = .light
    
    var body: some View {
        ZStack {
            // Background overlay
            if isPresented {
                Color.black.opacity(0.3)
                    .ignoresSafeArea()
                    .onTapGesture {
                        withAnimation(.easeInOut(duration: 0.3)) {
                            isPresented = false
                        }
                    }
            }
            
            // Menu content
            HStack {
                Spacer()
                
                VStack(spacing: 0) {
                    // Menu header
                    MenuHeader()
                    
                    // Search bar
                    SearchBar(text: $searchText)
                    
                    // Menu content
                    ScrollView {
                        LazyVStack(spacing: 0) {
                            // Profile Section
                            ProfileSection()
                            
                            Divider()
                                .padding(.horizontal, 16)
                            
                            // Core Navigation
                            CoreNavigationSection()
                            
                            Divider()
                                .padding(.horizontal, 16)
                            
                            // Creation & Discovery
                            CreationDiscoverySection()
                            
                            Divider()
                                .padding(.horizontal, 16)
                            
                            // Settings & Preferences
                            SettingsPreferencesSection()
                            
                            Divider()
                                .padding(.horizontal, 16)
                            
                            // Utility & Support
                            UtilitySupportSection()
                        }
                        .padding(.vertical, 8)
                    }
                }
                .frame(width: 320)
                .background(Color(.systemBackground))
                .clipShape(RoundedRectangle(cornerRadius: 16))
                .shadow(color: .black.opacity(0.2), radius: 20, x: -5, y: 10)
                .offset(x: isPresented ? 0 : 400)
                .animation(.easeInOut(duration: 0.3), value: isPresented)
        }
        }
    }
}

// MARK: - Menu Header
struct MenuHeader: View {
    var body: some View {
        HStack {
            Text("Menu")
                .font(.title2)
                .fontWeight(.bold)
                .foregroundColor(.primary)
            
            Spacer()
            
            Button("Done") {
                // This will be handled by parent view
            }
            .foregroundColor(.accentColor)
        }
        .padding(.horizontal, 20)
        .padding(.vertical, 16)
        .background(Color(.systemBackground))
    }
}

// MARK: - Search Bar
struct SearchBar: View {
    @Binding var text: String
    
    var body: some View {
        HStack {
            IconView(AppIcons.search)
                .foregroundColor(.secondary)
                .font(.system(size: 16))
            
            TextField("Search menu", text: $text)
                .textFieldStyle(PlainTextFieldStyle())
                .font(.system(size: 16))
            
            if !text.isEmpty {
                Button("Clear") {
                    text = ""
                }
                .foregroundColor(.accentColor)
                .font(.system(size: 14))
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
        .background(Color(.systemGray6))
        .cornerRadius(12)
        .padding(.horizontal, 20)
        .padding(.bottom, 16)
    }
}

// MARK: - Profile Section
struct ProfileSection: View {
    var body: some View {
        VStack(spacing: 16) {
            // Profile info
            HStack(spacing: 16) {
                // Profile picture
                AsyncImage(url: URL(string: "https://example.com/avatar.jpg")) { image in
                    image
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                } placeholder: {
                    Circle()
                        .fill(Color(.systemGray4))
                        .overlay(
                            IconView(AppIcons.person)
                                .foregroundColor(.secondary)
                        )
                }
                .frame(width: 60, height: 60)
                .clipShape(Circle())
                
                // Profile details
                VStack(alignment: .leading, spacing: 4) {
                    Text("John Doe")
                        .font(.title3)
                        .fontWeight(.semibold)
                        .foregroundColor(.primary)
                    
                    Text("@johndoe")
                        .font(.subheadline)
                        .foregroundColor(.secondary)
                }
                
                Spacer()
            }
            
            // Quick links
            HStack(spacing: 12) {
                Button("View Profile") {
                    // Navigate to profile
                }
                .foregroundColor(.white)
                .padding(.horizontal, 16)
                .padding(.vertical, 8)
                .background(
                    RoundedRectangle(cornerRadius: 20)
                        .fill(Color.blue)
                )
                
                Button("Edit Profile") {
                    // Navigate to edit profile
                }
                .foregroundColor(.blue)
                .padding(.horizontal, 16)
                .padding(.vertical, 8)
                .background(
                    RoundedRectangle(cornerRadius: 20)
                        .stroke(Color.blue, lineWidth: 1)
                )
            }
        }
        .padding(.horizontal, 20)
        .padding(.vertical, 16)
    }
}

// MARK: - Core Navigation Section
struct CoreNavigationSection: View {
    var body: some View {
        VStack(spacing: 0) {
            MenuItem(
                icon: "bell.fill",
                iconColor: .orange,
                title: "Notifications",
                subtitle: "Manage your notifications"
            ) {
                // Navigate to notifications
            }
            
            MenuItem(
                icon: "message.fill",
                iconColor: .blue,
                title: "Messages",
                subtitle: "View your conversations"
            ) {
                // Navigate to messages
            }
            
            MenuItem(
                icon: "person.2.fill",
                iconColor: .green,
                title: "Friends",
                subtitle: "Manage your connections"
            ) {
                // Navigate to friends
            }
            
            MenuItem(
                icon: "bookmark.fill",
                iconColor: .purple,
                title: "Saved Posts",
                subtitle: "Your bookmarked content"
            ) {
                // Navigate to saved posts
            }
            
            MenuItem(
                icon: "folder.fill",
                iconColor: .indigo,
                title: "Lists",
                subtitle: "Organize your content"
            ) {
                // Navigate to lists
            }
        }
    }
}

// MARK: - Creation & Discovery Section
struct CreationDiscoverySection: View {
    var body: some View {
        VStack(spacing: 0) {
            MenuItem(
                icon: "plus.circle.fill",
                iconColor: .green,
                title: "Create a Post",
                subtitle: "Share something new"
            ) {
                // Navigate to post creation
            }
            
            MenuItem(
                icon: "magnifyingglass",
                iconColor: .blue,
                title: "Explore",
                subtitle: "Discover new content"
            ) {
                // Navigate to explore
            }
            
            MenuItem(
                icon: "chart.line.uptrend.xyaxis",
                iconColor: .red,
                title: "Trending",
                subtitle: "See what's popular"
            ) {
                // Navigate to trending
            }
        }
    }
}

// MARK: - Settings & Preferences Section
struct SettingsPreferencesSection: View {
    @State private var isDarkMode = false
    
    var body: some View {
        VStack(spacing: 0) {
            MenuItem(
                icon: "gearshape.fill",
                iconColor: .gray,
                title: "Settings",
                subtitle: "Account, privacy, security"
            ) {
                // Navigate to settings
                // This would typically navigate to SettingsView
            }
            
            MenuItem(
                icon: "paintbrush.fill",
                iconColor: .purple,
                title: "Appearance",
                subtitle: "Dark/light mode toggle"
            ) {
                // Toggle appearance
            }
            
            MenuItem(
                icon: "lock.fill",
                iconColor: .blue,
                title: "Privacy & Safety",
                subtitle: "Manage your privacy"
            ) {
                // Navigate to privacy settings
            }
        }
    }
}

// MARK: - Utility & Support Section
struct UtilitySupportSection: View {
    var body: some View {
        VStack(spacing: 0) {
            MenuItem(
                icon: "questionmark.circle.fill",
                iconColor: .orange,
                title: "Help & Support",
                subtitle: "Get help and support"
            ) {
                // Navigate to help
            }
            
            MenuItem(
                icon: "doc.text.fill",
                iconColor: .gray,
                title: "Terms & Policies",
                subtitle: "Legal information"
            ) {
                // Navigate to terms
            }
            
            MenuItem(
                icon: "rectangle.portrait.and.arrow.right",
                iconColor: .red,
                title: "Log Out",
                subtitle: "Sign out of your account"
            ) {
                // Handle logout
            }
        }
    }
}

// MARK: - Menu Item
struct MenuItem: View {
    let icon: String
    let iconColor: Color
    let title: String
    let subtitle: String
    let action: () -> Void
    
    var body: some View {
        Button(action: action) {
            HStack(spacing: 16) {
                // Icon
                Image(systemName: icon)
                    .font(.system(size: 20))
                    .foregroundColor(iconColor)
                    .frame(width: 24, height: 24)
                
                // Content
                VStack(alignment: .leading, spacing: 2) {
                    Text(title)
                        .font(.system(size: 16, weight: .medium))
                        .foregroundColor(.primary)
                        .multilineTextAlignment(.leading)
                    
                    Text(subtitle)
                        .font(.system(size: 14))
                        .foregroundColor(.secondary)
                        .multilineTextAlignment(.leading)
                }
                
                Spacer()
                
                // Chevron
                Image(systemName: "chevron.right")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundColor(.secondary)
            }
            .padding(.horizontal, 20)
            .padding(.vertical, 16)
            .contentShape(Rectangle())
        }
        .buttonStyle(PlainButtonStyle())
        
        // Hover effect
        .background(
            RoundedRectangle(cornerRadius: 8)
                .fill(Color.clear)
                .background(
                    RoundedRectangle(cornerRadius: 8)
                        .fill(Color(.systemGray6))
                        .opacity(0.01)
                )
        )
        .onHover { isHovered in
            // Add hover effect if needed
        }
    }
}

// MARK: - Meatball Menu Button
struct MeatballMenuButton: View {
    let action: () -> Void
    
    var body: some View {
        Button(action: action) {
            HStack(spacing: 4) {
                ForEach(0..<3, id: \.self) { _ in
                    Circle()
                        .fill(Color(.systemGray))
                        .frame(width: 4, height: 4)
                }
            }
            .padding(.horizontal, 8)
            .padding(.vertical: 4)
            .background(
                RoundedRectangle(cornerRadius: 8)
                    .fill(Color(.systemGray6))
            )
        }
        .buttonStyle(PlainButtonStyle())
    }
}

// MARK: - Preview
struct MeatballMenu_Previews: PreviewProvider {
    static var previews: some View {
        ZStack {
            Color(.systemBackground)
                .ignoresSafeArea()
            
            VStack {
                Spacer()
                
                MeatballMenuButton {
                    // Show menu
                }
                
                Spacer()
            }
        }
        .sheet(isPresented: .constant(true)) {
            MeatballMenu(isPresented: .constant(true))
        }
    }
}