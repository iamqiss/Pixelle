import SwiftUI

struct NoteSearchRow: View {
    let note: Note
    @State private var isPressed = false
    @State private var isLiked = false
    @State private var isReposted = false
    
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // Header
            HStack(spacing: 12) {
                // Author avatar
                AsyncImage(url: URL(string: "")) { image in
                    image
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                } placeholder: {
                    Circle()
                        .fill(Color(.systemGray4))
                        .overlay(
                            IconView(AppIcons.person, size: 16, color: .secondary)
                        )
                }
                .frame(width: 32, height: 32)
                .clipShape(Circle())
                
                // Author info
                VStack(alignment: .leading, spacing: 2) {
                    Text("User") // This would come from a separate user lookup
                        .font(.system(size: 14, weight: .semibold))
                        .foregroundColor(.primary)
                    
                    Text(timeAgoString)
                        .font(.system(size: 12))
                        .foregroundColor(.secondary)
                }
                
                Spacer()
                
                // More options
                Menu {
                    if note.authorId == "" /* inject session user id here when available */ {
                        Button("Edit") { /* Edit note */ }
                        Button("Delete", role: .destructive) { /* Delete note */ }
                    } else {
                        Button("Mute @user") { /* Mute */ }
                        Button("Block @user", role: .destructive) { /* Block */ }
                        Button("Report", role: .destructive) { /* Report */ }
                    }
                } label: {
                    IconView(AppIcons.more, size: 14, color: .secondary)
                }
                .buttonStyle(PlainButtonStyle())
            }
            
            // Content
            Text(note.content)
                .font(.system(size: 15))
                .foregroundColor(.primary)
                .lineLimit(3)
                .multilineTextAlignment(.leading)
            
            // Media preview
            if !note.media.isEmpty {
                mediaPreview
            }
            
            // Engagement
            HStack(spacing: 20) {
                // Like button
                Button(action: {
                    withAnimation(.easeInOut(duration: 0.2)) {
                        isLiked.toggle()
                    }
                }) {
                    HStack(spacing: 6) {
                        IconView(isLiked ? AppIcons.likeFilled : AppIcons.like, size: 14, color: isLiked ? .red : .secondary)
                        Text("\(note.engagement.likeCount)")
                            .font(.system(size: 12))
                            .foregroundColor(.secondary)
                    }
                }
                .buttonStyle(PlainButtonStyle())
                
                // Reply button
                Button(action: {
                    // Navigate to reply
                }) {
                    HStack(spacing: 6) {
                        IconView(AppIcons.reply, size: 14, color: .secondary)
                        Text("\(note.engagement.replyCount)")
                            .font(.system(size: 12))
                            .foregroundColor(.secondary)
                    }
                }
                .buttonStyle(PlainButtonStyle())
                
                // Repost button
                Button(action: {
                    withAnimation(.easeInOut(duration: 0.2)) {
                        isReposted.toggle()
                    }
                }) {
                    HStack(spacing: 6) {
                        IconView(AppIcons.repost, size: 14, color: isReposted ? .green : .secondary)
                        Text("\(note.engagement.repostCount)")
                            .font(.system(size: 12))
                            .foregroundColor(.secondary)
                    }
                }
                .buttonStyle(PlainButtonStyle())
                
                Spacer()
                
                // Share button
                Button(action: {
                    // Share note
                }) {
                    IconView(AppIcons.share, size: 14, color: .secondary)
                }
                .buttonStyle(PlainButtonStyle())
            }
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
            // Navigate to note detail
        }
        .onLongPressGesture(minimumDuration: 0, maximumDistance: .infinity, pressing: { pressing in
            withAnimation(.easeInOut(duration: 0.1)) {
                isPressed = pressing
            }
        }, perform: {})
    }
    
    // MARK: - Media Preview
    private var mediaPreview: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 8) {
                ForEach(note.media.prefix(3), id: \.mediaId) { media in
                    AsyncImage(url: URL(string: media.url)) { image in
                        image
                            .resizable()
                            .aspectRatio(contentMode: .fill)
                    } placeholder: {
                        Rectangle()
                            .fill(Color(.systemGray4))
                            .overlay(
                                IconView(mediaTypeIcon, size: 20, color: .secondary)
                            )
                    }
                    .frame(width: 80, height: 80)
                    .clipShape(RoundedRectangle(cornerRadius: 8))
                }
            }
            .padding(.horizontal, 16)
        }
    }
    
    // MARK: - Helper Properties
    private var timeAgoString: String {
        let now = Date()
        let noteDate = note.createdAt.date
        let timeInterval = now.timeIntervalSince(noteDate)
        
        if timeInterval < 60 {
            return "now"
        } else if timeInterval < 3600 {
            let minutes = Int(timeInterval / 60)
            return "\(minutes)m"
        } else if timeInterval < 86400 {
            let hours = Int(timeInterval / 3600)
            return "\(hours)h"
        } else if timeInterval < 2592000 {
            let days = Int(timeInterval / 86400)
            return "\(days)d"
        } else {
            let months = Int(timeInterval / 2592000)
            return "\(months)mo"
        }
    }
    
    private var mediaTypeIcon: String {
        guard let firstMedia = note.media.first else { return "photo" }
        
        switch firstMedia.type {
        case .MEDIA_TYPE_IMAGE:
            return "photo"
        case .MEDIA_TYPE_VIDEO:
            return "video"
        case .MEDIA_TYPE_GIF:
            return "play.rectangle"
        case .MEDIA_TYPE_AUDIO:
            return "waveform"
        default:
            return "photo"
        }
    }
}

// MARK: - Preview
struct NoteSearchRow_Previews: PreviewProvider {
    static var previews: some View {
        VStack(spacing: 0) {
            NoteSearchRow(
                note: Note(
                    noteId: "1",
                    authorId: "user1",
                    content: "Just discovered an amazing new technology that's going to revolutionize how we think about AI! The possibilities are endless and I can't wait to see what the future holds. #AI #Innovation #Future",
                    media: [],
                    replyToNoteId: nil,
                    renoteOfNoteId: nil,
                    quoteNoteId: nil,
                    createdAt: Timestamp(seconds: 1640995200, nanos: 0),
                    updatedAt: Timestamp(seconds: 1640995200, nanos: 0),
                    isDeleted: false,
                    isSensitive: false,
                    language: "en",
                    engagement: NoteEngagement(
                        likeCount: 154,
                        repostCount: 23,
                        replyCount: 12,
                        bookmarkCount: 45,
                        viewCount: 1234,
                        isLiked: false,
                        isReposted: false,
                        isBookmarked: false
                    ),
                    visibility: .NOTE_VISIBILITY_PUBLIC,
                    mentions: [],
                    hashtags: ["AI", "Innovation", "Future"],
                    urls: []
                )
            )
            
            NoteSearchRow(
                note: Note(
                    noteId: "2",
                    authorId: "user2",
                    content: "Beautiful sunset at the beach today! 🌅 Perfect way to end the weekend.",
                    media: [],
                    replyToNoteId: nil,
                    renoteOfNoteId: nil,
                    quoteNoteId: nil,
                    createdAt: Timestamp(seconds: 1640995200, nanos: 0),
                    updatedAt: Timestamp(seconds: 1640995200, nanos: 0),
                    isDeleted: false,
                    isSensitive: false,
                    language: "en",
                    engagement: NoteEngagement(
                        likeCount: 89,
                        repostCount: 5,
                        replyCount: 3,
                        bookmarkCount: 12,
                        viewCount: 567,
                        isLiked: false,
                        isReposted: false,
                        isBookmarked: false
                    ),
                    visibility: .NOTE_VISIBILITY_PUBLIC,
                    mentions: [],
                    hashtags: [],
                    urls: []
                )
            )
        }
        .background(Color(.systemBackground))
        .previewLayout(.sizeThatFits)
    }
}