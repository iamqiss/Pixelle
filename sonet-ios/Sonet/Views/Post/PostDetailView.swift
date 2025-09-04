import SwiftUI
import Combine

struct PostDetailView: View {
    let noteId: String
    @StateObject private var viewModel = PostDetailViewModel()
    @Environment(\.dismiss) private var dismiss
    
    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                if viewModel.isLoading {
                    SpinnerView()
                } else if let error = viewModel.error {
                    ErrorView(error: error) {
                        viewModel.loadPost(noteId: noteId)
                    }
                } else if let post = viewModel.post {
                    ScrollView {
                        LazyVStack(spacing: 0) {
                            // Original post
                            PostDetailCard(
                                note: post,
                                onReply: { viewModel.setReplyToNote(post) },
                                onLike: { viewModel.toggleLike(noteId: post.noteId) },
                                onRepost: { viewModel.toggleRepost(noteId: post.noteId) },
                                onShare: { /* Handle share */ }
                            )
                            
                            // Thread separator
                            ThreadSeparator()
                            
                            // Comments/Replies
                            ForEach(viewModel.thread, id: \.noteId) { reply in
                                PostDetailCard(
                                    note: reply,
                                    onReply: { viewModel.setReplyToNote(reply) },
                                    onLike: { viewModel.toggleLike(noteId: reply.noteId) },
                                    onRepost: { viewModel.toggleRepost(noteId: reply.noteId) },
                                    onShare: { /* Handle share */ },
                                    isReply: true
                                )
                            }
                        }
                    }
                }
                
                // Reply input at bottom
                ReplyInput(
                    onReply: { content in
                        viewModel.createReply(content: content)
                    },
                    replyToNote: viewModel.replyToNote
                )
            }
            .navigationTitle("Thread")
            .navigationBarTitleDisplayMode(.inline)
            .navigationBarBackButtonHidden(true)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button(action: { dismiss() }) {
                        Image(systemName: "arrow.left")
                    }
                }
                
                ToolbarItem(placement: .navigationBarTrailing) {
                    HStack {
                        Button(action: { /* Notifications */ }) {
                            Image(systemName: "bell")
                        }
                        
                        Button(action: { /* More options */ }) {
                            Image(systemName: "ellipsis")
                        }
                    }
                }
            }
        }
        .onAppear {
            viewModel.loadPost(noteId: noteId)
            viewModel.loadThread(noteId: noteId)
        }
    }
}

struct ThreadSeparator: View {
    var body: some View {
        HStack {
            Divider()
                .padding(.leading, 40)
        }
        .padding(.horizontal, 16)
    }
}

struct MediaCarousel: View {
    let media: [Note.Attachment]
    
    var body: some View {
        if media.isEmpty { return EmptyView() }
        
        TabView {
            ForEach(Array(media.enumerated()), id: \.element.attachmentId) { index, mediaItem in
                AsyncImage(url: URL(string: mediaItem.url)) { image in
                    image
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                } placeholder: {
                    Rectangle()
                        .fill(Color(.systemGray4))
                        .overlay(
                            Image(systemName: "photo")
                                .font(.system(size: 20))
                                .foregroundColor(.secondary)
                        )
                }
                .frame(height: 200)
                .clipShape(RoundedRectangle(cornerRadius: 8))
            }
        }
        .frame(height: 200)
        .tabViewStyle(PageTabViewStyle())
    }
}

struct ReplyInput: View {
    let onReply: (String) -> Void
    let replyToNote: Note?
    
    @State private var replyText = ""
    @FocusState private var isFocused: Bool
    
    var body: some View {
        VStack(spacing: 8) {
            // Reply context if replying to someone
            if let note = replyToNote {
                HStack(spacing: 8) {
                    Image(systemName: "arrowshape.turn.up.left")
                        .font(.system(size: 14))
                        .foregroundColor(.secondary)
                    
                    Text("Replying to @\(note.author?.handle ?? "unknown")")
                        .font(.system(size: 12))
                        .foregroundColor(.secondary)
                }
            }
            
            HStack(spacing: 12) {
                // User avatar
                Circle()
                    .fill(Color(.systemGray4))
                    .frame(width: 32, height: 32)
                    .overlay(
                        Image(systemName: "person")
                            .font(.system(size: 16))
                            .foregroundColor(.secondary)
                    )
                
                // Reply input field
                TextField(
                    replyToNote != nil ? "Reply to @\(replyToNote?.author?.handle ?? "unknown")" : "Post your reply...",
                    text: $replyText,
                    axis: .vertical
                )
                .textFieldStyle(RoundedBorderTextFieldStyle())
                .lineLimit(1...4)
                .focused($isFocused)
                
                // Send button
                Button(action: {
                    if !replyText.isEmpty {
                        onReply(replyText)
                        replyText = ""
                        isFocused = false
                    }
                }) {
                    Image(systemName: "arrow.up.circle.fill")
                        .font(.system(size: 24))
                        .foregroundColor(replyText.isEmpty ? .secondary : .blue)
                }
                .disabled(replyText.isEmpty)
            }
        }
        .padding(16)
        .background(Color(.systemBackground))
        .overlay(
            Rectangle()
                .frame(height: 1)
                .foregroundColor(Color(.systemGray4)),
            alignment: .top
        )
    }
}

struct SpinnerView: View {
    var body: some View {
        VStack {
            ProgressView()
                .scaleEffect(1.5)
            Text("Loading...")
                .font(.caption)
                .foregroundColor(.secondary)
                .padding(.top, 8)
        }
    }
}

struct ErrorView: View {
    let error: String
    let onRetry: () -> Void
    
    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 48))
                .foregroundColor(.orange)
            
            Text("Error loading post")
                .font(.headline)
            
            Text(error)
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
            
            Button("Retry", action: onRetry)
                .buttonStyle(.borderedProminent)
        }
        .padding()
    }
}

private func formatTimeAgo(timestampSeconds: Int64) -> String {
    let now = Int64(Date().timeIntervalSince1970)
    let timeDiff = now - timestampSeconds
    
    switch timeDiff {
    case 0..<60:
        return "now"
    case 60..<3600:
        return "\(timeDiff / 60)m"
    case 3600..<86400:
        return "\(timeDiff / 3600)h"
    case 86400..<2592000:
        return "\(timeDiff / 86400)d"
    default:
        return "\(timeDiff / 2592000)mo"
    }
}

#Preview {
    PostDetailView(noteId: "preview")
}

struct PostDetailCard: View {
    let note: Note
    let onReply: () -> Void
    let onLike: () -> Void
    let onRepost: () -> Void
    let onShare: () -> Void
    let isReply: Bool
    
    init(
        note: Note,
        onReply: @escaping () -> Void,
        onLike: @escaping () -> Void,
        onRepost: @escaping () -> Void,
        onShare: @escaping () -> Void,
        isReply: Bool = false
    ) {
        self.note = note
        self.onReply = onReply
        self.onLike = onLike
        self.onRepost = onRepost
        self.onShare = onShare
        self.isReply = isReply
    }
    
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // User Header
            HStack(spacing: 12) {
                // Avatar
                AsyncImage(url: URL(string: note.author?.avatarUrl ?? "")) { image in
                    image
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                } placeholder: {
                    Circle()
                        .fill(Color(.systemGray4))
                        .overlay(
                            Image(systemName: "person")
                                .font(.system(size: 16))
                                .foregroundColor(.secondary)
                        )
                }
                .frame(width: 40, height: 40)
                .clipShape(Circle())
                
                // Author info
                VStack(alignment: .leading, spacing: 2) {
                    HStack(spacing: 4) {
                        Text(note.author?.displayName ?? "Unknown User")
                            .font(.system(size: 16, weight: .semibold))
                            .foregroundColor(.primary)
                        
                        if note.author?.isVerified == true {
                            Image(systemName: "checkmark.seal.fill")
                                .font(.system(size: 14))
                                .foregroundColor(.blue)
                        }
                    }
                    
                    Text("@\(note.author?.handle ?? "unknown")")
                        .font(.system(size: 14))
                        .foregroundColor(.secondary)
                }
                
                Spacer()
                
                // Time and menu
                HStack(spacing: 8) {
                    Text(formatTimeAgo(timestampSeconds: note.createdAt?.seconds ?? 0))
                        .font(.system(size: 12))
                        .foregroundColor(.secondary)
                    
                    Menu {
                        if note.author?.userId == SessionManager.shared.currentUserId {
                            Button("Edit") { /* Handle edit */ }
                            Button("Delete", role: .destructive) { /* Handle delete */ }
                        } else {
                            Button("Follow @\(note.author?.handle ?? "unknown")") { /* Handle follow */ }
                            Button("Mute @\(note.author?.handle ?? "unknown")") { /* Handle mute */ }
                            Button("Block @\(note.author?.handle ?? "unknown")", role: .destructive) { /* Handle block */ }
                            Button("Report", role: .destructive) { /* Handle report */ }
                        }
                    } label: {
                        Image(systemName: "ellipsis")
                            .font(.system(size: 14))
                            .foregroundColor(.secondary)
                    }
                }
            }
            
            // Note Content
            if !note.content.isEmpty {
                Text(note.content)
                    .font(.system(size: 16))
                    .foregroundColor(.primary)
                    .multilineTextAlignment(.leading)
            }
            
            // Media carousel
            if !note.attachmentsList.isEmpty {
                MediaCarousel(media: note.attachmentsList)
            }
            
            // Action Buttons
            HStack(spacing: 20) {
                // Reply Button
                Button(action: onReply) {
                    HStack(spacing: 6) {
                        Image(systemName: "bubble.left")
                            .font(.system(size: 14))
                            .foregroundColor(.secondary)
                        
                        Text("\(note.replyCount)")
                            .font(.system(size: 12))
                            .foregroundColor(.secondary)
                    }
                }
                .buttonStyle(PlainButtonStyle())
                
                // Repost Button
                Button(action: onRepost) {
                    HStack(spacing: 6) {
                        Image(systemName: note.userState?.isReposted == true ? "arrow.2.squarepath.fill" : "arrow.2.squarepath")
                            .font(.system(size: 14))
                            .foregroundColor(note.userState?.isReposted == true ? .green : .secondary)
                        
                        Text("\(note.renoteCount)")
                            .font(.system(size: 12))
                            .foregroundColor(.secondary)
                    }
                }
                .buttonStyle(PlainButtonStyle())
                
                // Like Button
                Button(action: onLike) {
                    HStack(spacing: 6) {
                        Image(systemName: note.userState?.isLiked == true ? "heart.fill" : "heart")
                            .font(.system(size: 14))
                            .foregroundColor(note.userState?.isLiked == true ? .red : .secondary)
                        
                        Text("\(note.likeCount)")
                            .font(.system(size: 12))
                            .foregroundColor(.secondary)
                    }
                }
                .buttonStyle(PlainButtonStyle())
                
                // Share Button
                Button(action: onShare) {
                    Image(systemName: "square.and.arrow.up")
                        .font(.system(size: 14))
                        .foregroundColor(.secondary)
                }
                .buttonStyle(PlainButtonStyle())
            }
        }
        .padding(.horizontal, isReply ? 32 : 16)
        .padding(.vertical, 12)
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(isReply ? Color(.systemGray6) : Color(.systemBackground))
        )
    }
}