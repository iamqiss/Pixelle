import SwiftUI

struct HomeTabView: View {
    @EnvironmentObject var navigationManager: NavigationManager
    @EnvironmentObject var themeManager: ThemeManager
    @StateObject private var homeViewModel = HomeViewModel()
    
    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                // Custom Navigation Bar
                HomeNavigationBar(
                    selectedFeed: $homeViewModel.selectedFeed,
                    onFeedChanged: { feed in
                        homeViewModel.selectFeed(feed)
                    }
                )
                
                // Feed Content
                if homeViewModel.isLoading {
                    LoadingView()
                } else if let error = homeViewModel.error {
                    ErrorView(error: error) {
                        homeViewModel.refreshFeed()
                    }
                } else {
                    FeedView(
                        feed: homeViewModel.selectedFeed,
                        notes: homeViewModel.notes,
                        onRefresh: {
                            homeViewModel.refreshFeed()
                        },
                        onLoadMore: {
                            homeViewModel.loadMoreNotes()
                        }
                    )
                }
            }
            .navigationBarHidden(true)
        }
        .navigationViewStyle(StackNavigationViewStyle())
        .onAppear {
            homeViewModel.loadFeed()
        }
    }
}

// MARK: - Home Navigation Bar
struct HomeNavigationBar: View {
    @Binding var selectedFeed: FeedType
    let onFeedChanged: (FeedType) -> Void
    
    var body: some View {
        VStack(spacing: 0) {
            // Main Header
            HStack {
                Text("Home")
                    .font(.largeTitle)
                    .fontWeight(.bold)
                
                Spacer()
                
                Button(action: { /* Open composer */ }) { IconView(AppIcons.add, size: 22, color: .blue) }
            }
            .padding(.horizontal, 16)
            .padding(.top, 8)
            
            // Feed Type Selector
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 16) {
                    ForEach(FeedType.allCases, id: \.self) { feedType in
                        FeedTypeButton(
                            feedType: feedType,
                            isSelected: selectedFeed == feedType
                        ) {
                            selectedFeed = feedType
                            onFeedChanged(feedType)
                        }
                    }
                }
                .padding(.horizontal, 16)
            }
            .padding(.vertical, 8)
            
            Divider()
        }
        .background(Color(.systemBackground))
    }
}

// MARK: - Feed Type Button
struct FeedTypeButton: View {
    let feedType: FeedType
    let isSelected: Bool
    let action: () -> Void
    
    var body: some View {
        Button(action: action) {
            Text(feedType.displayName)
                .font(.subheadline)
                .fontWeight(isSelected ? .semibold : .medium)
                .foregroundColor(isSelected ? .white : .primary)
                .padding(.horizontal, 16)
                .padding(.vertical, 8)
                .background(
                    RoundedRectangle(cornerRadius: 20)
                        .fill(isSelected ? Color.blue : Color.clear)
                )
                .overlay(
                    RoundedRectangle(cornerRadius: 20)
                        .stroke(isSelected ? Color.clear : Color.gray.opacity(0.3), lineWidth: 1)
                )
        }
        .buttonStyle(PlainButtonStyle())
    }
}

// MARK: - Feed View
struct FeedView: View {
    let feed: FeedType
    let notes: [SonetNote]
    let onRefresh: () -> Void
    let onLoadMore: () -> Void
    
    var body: some View {
        ScrollView {
            LazyVStack(spacing: 16) {
                ForEach(notes) { note in
                    NoteCard(note: note)
                        .padding(.horizontal, 16)
                }
                
                // Load More Button
                if !notes.isEmpty {
                    Button(action: onLoadMore) {
                        Text("Load More")
                            .font(.subheadline)
                            .foregroundColor(.blue)
                            .padding(.vertical, 12)
                    }
                    .padding(.horizontal, 16)
                }
            }
            .padding(.vertical, 16)
        }
        .refreshable {
            onRefresh()
        }
    }
}

// MARK: - Note Card
struct NoteCard: View {
    let note: SonetNote
    @State private var isLiked = false
    @State private var isReposted = false
    
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // User Header
            HStack {
                AsyncImage(url: note.author.avatarURL) { image in
                    image
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                } placeholder: {
                    IconView(AppIcons.personCircle, size: 16, color: .gray)
                }
                .frame(width: 40, height: 40)
                .clipShape(Circle())
                
                VStack(alignment: .leading, spacing: 2) {
                    HStack {
                        Text(note.author.displayName)
                            .font(.subheadline)
                            .fontWeight(.semibold)
                        
                        if note.author.isVerified { IconView(AppIcons.verified, size: 12, color: .blue) }
                    }
                    
                    Text(note.author.handle)
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                
                Spacer()
                
                Text(note.createdAt.timeAgoDisplay())
                    .font(.caption)
                    .foregroundColor(.secondary)
            }
            
            // Note Content
            if !note.text.isEmpty {
                Text(note.text)
                    .font(.body)
                    .multilineTextAlignment(.leading)
            }
            
            // Media Content
            if let media = note.media, !media.isEmpty {
                MediaGridView(media: media)
            }
            
            // Action Buttons
            HStack(spacing: 24) {
                // Reply Button
                Button(action: {
                    // Handle reply
                }) {
                    HStack(spacing: 4) {
                        Image(systemName: "bubble.left")
                        Text("\(note.replyCount)")
                    }
                    .font(.caption)
                    .foregroundColor(.secondary)
                }
                
                // Repost Button
                Button(action: {
                    isReposted.toggle()
                }) {
                    HStack(spacing: 4) {
                        Image(systemName: isReposted ? "arrow.2.squarepath.fill" : "arrow.2.squarepath")
                        Text("\(note.repostCount)")
                    }
                    .font(.caption)
                    .foregroundColor(isReposted ? .green : .secondary)
                }
                
                // Like Button
                Button(action: {
                    isLiked.toggle()
                }) {
                    HStack(spacing: 4) {
                        Image(systemName: isLiked ? "heart.fill" : "heart")
                        Text("\(note.likeCount)")
                    }
                    .font(.caption)
                    .foregroundColor(isLiked ? .red : .secondary)
                }
                
                // Share Button
                Button(action: {
                    // Handle share
                }) {
                    Image(systemName: "square.and.arrow.up")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
                
                Spacer()
            }
        }
        .padding(16)
        .background(Color(.systemBackground))
        .cornerRadius(12)
        .shadow(color: .black.opacity(0.05), radius: 2, x: 0, y: 1)
    }
}

// MARK: - Media Grid View
struct MediaGridView: View {
    let media: [MediaItem]
    
    var body: some View {
        let columns = media.count == 1 ? 1 : 2
        
        LazyVGrid(columns: Array(repeating: GridItem(.flexible(), spacing: 4), count: columns), spacing: 4) {
            ForEach(media) { item in
                AsyncImage(url: item.url) { image in
                    image
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                } placeholder: {
                    Rectangle()
                        .fill(Color.gray.opacity(0.3))
                        .overlay(
                            ProgressView()
                        )
                }
                .frame(height: columns == 1 ? 200 : 100)
                .clipped()
                .cornerRadius(8)
            }
        }
    }
}

// MARK: - Loading View
struct LoadingView: View {
    var body: some View {
        VStack(spacing: 16) {
            ProgressView()
                .scaleEffect(1.2)
            
            Text("Loading feed...")
                .font(.subheadline)
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Error View
struct ErrorView: View {
    let error: Error
    let retryAction: () -> Void
    
    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 48))
                .foregroundColor(.orange)
            
            Text("Something went wrong")
                .font(.headline)
            
            Text(error.localizedDescription)
                .font(.subheadline)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
            
            Button("Try Again") {
                retryAction()
            }
            .buttonStyle(.borderedProminent)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .padding()
    }
}

// MARK: - Preview
struct HomeTabView_Previews: PreviewProvider {
    static var previews: some View {
        HomeTabView()
            .environmentObject(NavigationManager())
            .environmentObject(ThemeManager())
    }
}