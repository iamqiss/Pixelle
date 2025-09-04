import SwiftUI

struct UserSearchRow: View {
    let user: UserProfile
    @State private var isPressed = false
    @State private var isFollowing = false
    
    var body: some View {
        HStack(spacing: 12) {
            // Avatar
            AsyncImage(url: URL(string: user.avatarUrl)) { image in
                image
                    .resizable()
                    .aspectRatio(contentMode: .fill)
            } placeholder: {
                Circle()
                    .fill(Color(.systemGray4))
                    .overlay(
                        IconView(AppIcons.person, size: 20, color: .secondary)
                    )
            }
            .frame(width: 48, height: 48)
            .clipShape(Circle())
            
            // User info
            VStack(alignment: .leading, spacing: 4) {
                HStack(spacing: 8) {
                    Text(user.displayName)
                        .font(.system(size: 16, weight: .semibold))
                        .foregroundColor(.primary)
                        .lineLimit(1)
                    
                    if user.isVerified {
                        IconView(AppIcons.verified, size: 14, color: .blue)
                    }
                }
                
                Text("@\(user.username)")
                    .font(.system(size: 14))
                    .foregroundColor(.secondary)
                    .lineLimit(1)
                
                if !user.bio.isEmpty {
                    Text(user.bio)
                        .font(.system(size: 14))
                        .foregroundColor(.secondary)
                        .lineLimit(2)
                }
                
                // Stats
                HStack(spacing: 16) {
                    Text("\(user.followerCount.formatted()) followers")
                        .font(.system(size: 12))
                        .foregroundColor(.secondary)
                    
                    Text("\(user.noteCount.formatted()) notes")
                        .font(.system(size: 12))
                        .foregroundColor(.secondary)
                }
            }
            
            Spacer()
            
            // Follow button
            Button(action: {
                withAnimation(.easeInOut(duration: 0.2)) {
                    isFollowing.toggle()
                }
            }) {
                Text(isFollowing ? "Following" : "Follow")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundColor(isFollowing ? .primary : .white)
                    .padding(.horizontal, 16)
                    .padding(.vertical, 8)
                    .background(
                        RoundedRectangle(cornerRadius: 20)
                            .fill(isFollowing ? Color(.systemGray5) : Color.accentColor)
                    )
            }
            .buttonStyle(PlainButtonStyle())
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
        .background(
            Rectangle()
                .fill(Color(.systemBackground))
        )
        .scaleEffect(isPressed ? 0.98 : 1.0)
        .animation(.easeInOut(duration: 0.1), value: isPressed)
        .onTapGesture {
            // Navigate to user profile
        }
        .onLongPressGesture(minimumDuration: 0, maximumDistance: .infinity, pressing: { pressing in
            withAnimation(.easeInOut(duration: 0.1)) {
                isPressed = pressing
            }
        }, perform: {})
    }
}

// MARK: - Preview
struct UserSearchRow_Previews: PreviewProvider {
    static var previews: some View {
        VStack(spacing: 0) {
            UserSearchRow(
                user: UserProfile(
                    userId: "1",
                    username: "techguru",
                    email: "tech@example.com",
                    displayName: "Tech Guru",
                    bio: "Passionate about technology and innovation. Building the future one line of code at a time.",
                    avatarUrl: "",
                    location: "San Francisco, CA",
                    website: "https://techguru.dev",
                    status: .USER_STATUS_ACTIVE,
                    isVerified: true,
                    isPrivate: false,
                    createdAt: Timestamp(seconds: 1640995200, nanos: 0),
                    updatedAt: Timestamp(seconds: 1640995200, nanos: 0),
                    lastLogin: Timestamp(seconds: 1640995200, nanos: 0),
                    followerCount: 15420,
                    followingCount: 890,
                    noteCount: 1234,
                    settings: [:],
                    privacySettings: [:]
                )
            )
            
            UserSearchRow(
                user: UserProfile(
                    userId: "2",
                    username: "artlover",
                    email: "art@example.com",
                    displayName: "Art Lover",
                    bio: "Exploring creativity through digital art and design.",
                    avatarUrl: "",
                    location: "New York, NY",
                    website: "",
                    status: .USER_STATUS_ACTIVE,
                    isVerified: false,
                    isPrivate: false,
                    createdAt: Timestamp(seconds: 1640995200, nanos: 0),
                    updatedAt: Timestamp(seconds: 1640995200, nanos: 0),
                    lastLogin: Timestamp(seconds: 1640995200, nanos: 0),
                    followerCount: 5430,
                    followingCount: 1200,
                    noteCount: 567,
                    settings: [:],
                    privacySettings: [:]
                )
            )
        }
        .background(Color(.systemBackground))
        .previewLayout(.sizeThatFits)
    }
}