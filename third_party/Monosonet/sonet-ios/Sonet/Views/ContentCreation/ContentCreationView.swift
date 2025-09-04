import SwiftUI
import PhotosUI

struct ContentCreationView: View {
    @StateObject private var viewModel: ContentCreationViewModel
    @Environment(\.dismiss) private var dismiss
    @Environment(\.colorScheme) var colorScheme
    
    init(grpcClient: SonetGRPCClient, replyToNote: Note? = nil, quoteNote: Note? = nil) {
        _viewModel = StateObject(wrappedValue: ContentCreationViewModel(
            grpcClient: grpcClient,
            replyToNote: replyToNote,
            quoteNote: quoteNote
        ))
    }
    
    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                // Header
                ContentCreationHeader(
                    canPost: viewModel.canPost,
                    isPosting: viewModel.isPosting,
                    onPost: {
                        Task {
                            await viewModel.postNote()
                        }
                    },
                    onCancel: { dismiss() }
                )
                
                // Content
                ScrollView {
                    VStack(spacing: 16) {
                        // Reply/Quote context
                        if let replyToNote = viewModel.replyToNote {
                            ReplyContextView(note: replyToNote)
                        }
                        
                        if let quoteNote = viewModel.quoteNote {
                            QuoteContextView(note: quoteNote)
                        }
                        
                        // Main content area
                        ContentArea(
                            noteContent: $viewModel.noteContent,
                            selectedMedia: $viewModel.selectedMedia,
                            characterCount: viewModel.characterCount,
                            remainingCharacters: viewModel.remainingCharacters,
                            characterCountColor: viewModel.characterCountColor,
                            onAddMedia: { viewModel.addMedia($0) },
                            onRemoveMedia: { viewModel.removeMedia(at: $0) }
                        )
                        
                        // Hashtags and mentions
                        if !viewModel.selectedHashtags.isEmpty || !viewModel.selectedMentions.isEmpty {
                            TagsAndMentionsView(
                                hashtags: viewModel.selectedHashtags,
                                mentions: viewModel.selectedMentions,
                                onRemoveHashtag: { viewModel.removeHashtag($0) },
                                onRemoveMention: { viewModel.removeMention($0) }
                            )
                        }
                        
                        // Scheduling info
                        if let scheduledDate = viewModel.scheduledDate {
                            SchedulingInfoView(
                                scheduledDate: scheduledDate,
                                onCancel: { viewModel.cancelScheduling() }
                            )
                        }
                        
                        // Error message
                        if let error = viewModel.postingError {
                            ErrorMessageView(error: error)
                        }
                    }
                    .padding(.horizontal, 16)
                    .padding(.vertical, 8)
                }
                
                // Bottom toolbar
                ContentCreationToolbar(
                    showHashtagSuggestions: viewModel.showHashtagSuggestions,
                    showMentionSuggestions: viewModel.showMentionSuggestions,
                    hashtagSuggestions: viewModel.hashtagSuggestions,
                    mentionSuggestions: viewModel.mentionSuggestions,
                    currentHashtagQuery: viewModel.currentHashtagQuery,
                    currentMentionQuery: viewModel.currentMentionQuery,
                    onAddHashtag: { viewModel.addHashtag($0) },
                    onAddMention: { viewModel.addMention($0) },
                    onSchedule: { viewModel.showScheduling = true },
                    onSaveDraft: { viewModel.saveDraft() }
                )
            }
            .navigationBarHidden(true)
            .sheet(isPresented: $viewModel.showScheduling) {
                SchedulingView(
                    scheduledDate: $viewModel.scheduledDate,
                    onSchedule: { viewModel.schedulePost(for: $0) },
                    onCancel: { viewModel.cancelScheduling() }
                )
            }
        }
    }
}

// MARK: - Content Creation Header
struct ContentCreationHeader: View {
    let canPost: Bool
    let isPosting: Bool
    let onPost: () -> Void
    let onCancel: () -> Void
    
    var body: some View {
        HStack {
            Button("Cancel", action: onCancel)
                .foregroundColor(.secondary)
            
            Spacer()
            
            Button(action: onPost) {
                if isPosting {
                    ProgressView()
                        .scaleEffect(0.8)
                        .foregroundColor(.accentColor)
                } else {
                    Text("Post")
                        .fontWeight(.semibold)
                        .foregroundColor(.white)
                }
            }
            .sensoryFeedback(.selection, trigger: isPosting)
            .disabled(!canPost)
            .padding(.horizontal, 20)
            .padding(.vertical, 8)
            .background(
                RoundedRectangle(cornerRadius: 20)
                    .fill(canPost ? Color.accentColor : Color.secondary.opacity(0.3))
            )
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
        .background(Color(UIColor.systemBackground))
        .overlay(
            Rectangle()
                .fill(Color(.separator))
                .frame(height: 0.5),
            alignment: .bottom
        )
    }
}

// MARK: - Reply Context View
struct ReplyContextView: View {
    let note: Note
    
    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            // Reply indicator
            VStack(spacing: 0) {
                Circle()
                    .fill(Color.secondary.opacity(0.3))
                    .frame(width: 8, height: 8)
                
                Rectangle()
                    .fill(Color.secondary.opacity(0.3))
                    .frame(width: 2)
                    .frame(maxHeight: .infinity)
            }
            
            // Note preview
            VStack(alignment: .leading, spacing: 4) {
                Text("Replying to")
                    .font(.caption)
                    .foregroundColor(.secondary)
                
                Text(note.content)
                    .font(.body)
                    .foregroundColor(.secondary)
                    .lineLimit(2)
            }
            
            Spacer()
        }
        .padding(.vertical, 8)
        .padding(.horizontal, 12)
        .background(Color(UIColor.systemGray6))
        .cornerRadius(12)
    }
}

// MARK: - Quote Context View
struct QuoteContextView: View {
    let note: Note
    
    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            // Quote indicator
            VStack(spacing: 0) {
                Circle()
                    .fill(Color.secondary.opacity(0.3))
                    .frame(width: 8, height: 8)
                
                Rectangle()
                    .fill(Color.secondary.opacity(0.3))
                    .frame(width: 2)
                    .frame(maxHeight: .infinity)
            }
            
            // Note preview
            VStack(alignment: .leading, spacing: 4) {
                Text("Quoting")
                    .font(.caption)
                    .foregroundColor(.secondary)
                
                Text(note.content)
                    .font(.body)
                    .foregroundColor(.secondary)
                    .lineLimit(2)
            }
            
            Spacer()
        }
        .padding(.vertical, 8)
        .padding(.horizontal, 12)
        .background(Color(UIColor.systemGray6))
        .cornerRadius(12)
    }
}

// MARK: - Content Area
struct ContentArea: View {
    @Binding var noteContent: String
    @Binding var selectedMedia: [MediaItem]
    let characterCount: Int
    let remainingCharacters: Int
    let characterCountColor: Color
    let onAddMedia: (MediaItem) -> Void
    let onRemoveMedia: (Int) -> Void
    
    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            // Text input
            TextField("What's happening?", text: $noteContent, axis: .vertical)
                .textFieldStyle(PlainTextFieldStyle())
                .font(.system(size: 20))
                .lineLimit(10...15)
                .frame(minHeight: 120)
            
            // Media grid with drag and drop reordering
            if !selectedMedia.isEmpty {
                MediaGridView(
                    media: selectedMedia,
                    onRemove: onRemoveMedia
                )
                .onDrag {
                    NSItemProvider(object: NSString(string: "media-drag"))
                }
            }
            
            // Character count
            HStack {
                Spacer()
                
                Text("\(characterCount)/280")
                    .font(.caption)
                    .foregroundColor(characterCountColor)
            }
        }
    }
}

// MARK: - Media Grid View
struct MediaGridView: View {
    let media: [MediaItem]
    let onRemove: (Int) -> Void
    
    var body: some View {
        LazyVGrid(columns: Array(repeating: GridItem(.flexible(), spacing: 8), count: 2), spacing: 8) {
            ForEach(Array(media.enumerated()), id: \.element.id) { index, mediaItem in
                MediaItemView(
                    media: mediaItem,
                    onRemove: { onRemove(index) }
                )
            }
        }
    }
}

// MARK: - Media Item View
struct MediaItemView: View {
    let media: MediaItem
    let onRemove: () -> Void
    
    var body: some View {
        ZStack(alignment: .topTrailing) {
            // Media content
            if let localImage = media.localImage {
                Image(uiImage: localImage)
                    .resizable()
                    .aspectRatio(contentMode: .fill)
                    .frame(height: 120)
                    .clipped()
                    .cornerRadius(8)
            } else {
                AsyncImage(url: URL(string: media.url)) { image in
                    image
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                } placeholder: {
                    Rectangle()
                        .fill(Color(.systemGray4))
                        .overlay(
                            Image(systemName: media.type.icon)
                                .font(.system(size: 24))
                                .foregroundColor(.secondary)
                        )
                }
                .frame(height: 120)
                .clipped()
                .cornerRadius(8)
            }
            
            // Remove button
            Button(action: onRemove) {
                Image(systemName: "xmark.circle.fill")
                    .font(.system(size: 20))
                    .foregroundColor(.white)
                    .background(Circle().fill(Color.primary.opacity(0.6)))
            }
            .padding(8)
        }
    }
}

// MARK: - Tags and Mentions View
struct TagsAndMentionsView: View {
    let hashtags: [String]
    let mentions: [String]
    let onRemoveHashtag: (String) -> Void
    let onRemoveMention: (String) -> Void
    
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // Hashtags
            if !hashtags.isEmpty {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Hashtags")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    
                    FlowLayout(spacing: 8) {
                        ForEach(hashtags, id: \.self) { hashtag in
                            HashtagChip(
                                hashtag: hashtag,
                                onRemove: { onRemoveHashtag(hashtag) }
                            )
                        }
                    }
                }
            }
            
            // Mentions
            if !mentions.isEmpty {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Mentions")
                        .font(.caption)
                        .foregroundColor(.secondary)
                    
                    FlowLayout(spacing: 8) {
                        ForEach(mentions, id: \.self) { mention in
                            MentionChip(
                                mention: mention,
                                onRemove: { onRemoveMention(mention) }
                            )
                        }
                    }
                }
            }
        }
    }
}

// MARK: - Hashtag Chip
struct HashtagChip: View {
    let hashtag: String
    let onRemove: () -> Void
    
    var body: some View {
        HStack(spacing: 6) {
            Text(hashtag)
                .font(.caption)
                .foregroundColor(.accentColor)
            
            Button(action: onRemove) {
                Image(systemName: "xmark.circle.fill")
                    .font(.system(size: 12))
                    .foregroundColor(.secondary)
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(Color.accentColor.opacity(0.1))
        )
    }
}

// MARK: - Mention Chip
struct MentionChip: View {
    let mention: String
    let onRemove: () -> Void
    
    var body: some View {
        HStack(spacing: 6) {
            Text(mention)
                .font(.caption)
                .foregroundColor(.accentColor)
            
            Button(action: onRemove) {
                Image(systemName: "xmark.circle.fill")
                    .font(.system(size: 12))
                    .foregroundColor(.secondary)
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(Color.accentColor.opacity(0.1))
        )
    }
}

// MARK: - Scheduling Info View
struct SchedulingInfoView: View {
    let scheduledDate: Date
    let onCancel: () -> Void
    
    var body: some View {
        HStack {
            Image(systemName: "clock")
                .foregroundColor(.orange)
            
            Text("Scheduled for \(scheduledDate.formatted(date: .abbreviated, time: .shortened))")
                .font(.caption)
                .foregroundColor(.secondary)
            
            Spacer()
            
            Button("Cancel", action: onCancel)
                .font(.caption)
                .foregroundColor(.red)
        }
        .padding(.vertical, 8)
        .padding(.horizontal, 12)
        .background(Color.secondary.opacity(0.1))
        .cornerRadius(8)
    }
}

// MARK: - Error Message View
struct ErrorMessageView: View {
    let error: String
    
    var body: some View {
        HStack {
            Image(systemName: "exclamationmark.triangle")
                .foregroundColor(.red)
            
            Text(error)
                .font(.caption)
                .foregroundColor(.red)
            
            Spacer()
        }
        .padding(.vertical, 8)
        .padding(.horizontal, 12)
        .background(Color.primary.opacity(0.08))
        .cornerRadius(8)
    }
}

// MARK: - Content Creation Toolbar
struct ContentCreationToolbar: View {
    let showHashtagSuggestions: Bool
    let showMentionSuggestions: Bool
    let hashtagSuggestions: [String]
    let mentionSuggestions: [UserProfile]
    let currentHashtagQuery: String
    let currentMentionQuery: String
    let onAddHashtag: (String) -> Void
    let onAddMention: (UserProfile) -> Void
    let onSchedule: () -> Void
    let onSaveDraft: () -> Void
    
    var body: some View {
        VStack(spacing: 0) {
            // Main toolbar
            HStack(spacing: 20) {
                // Media button
                Button(action: { /* Show media picker */ }) {
                    IconView(AppIcons.photo, size: 20, color: .accentColor)
                }
                
                // Hashtag button
                Button(action: { /* Show hashtag picker */ }) {
                    IconView(AppIcons.number, size: 20, color: .accentColor)
                }
                
                // Mention button
                Button(action: { /* Show mention picker */ }) {
                    IconView(AppIcons.at, size: 20, color: .accentColor)
                }
                
                // Schedule button
                Button(action: onSchedule) {
                    IconView(AppIcons.clock, size: 20, color: .accentColor)
                }
                
                Spacer()
                
                // Draft button
                Button(action: onSaveDraft) {
                    IconView(AppIcons.download, size: 20, color: .secondary)
                }
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 12)
            .background(Color(UIColor.systemBackground))
            .overlay(
                Rectangle()
                    .fill(Color(.separator))
                    .frame(height: 0.5),
                alignment: .top
            )
            
            // Suggestions overlay
            if showHashtagSuggestions || showMentionSuggestions {
                SuggestionsOverlay(
                    showHashtagSuggestions: showHashtagSuggestions,
                    showMentionSuggestions: showMentionSuggestions,
                    hashtagSuggestions: hashtagSuggestions,
                    mentionSuggestions: mentionSuggestions,
                    currentHashtagQuery: currentHashtagQuery,
                    currentMentionQuery: currentMentionQuery,
                    onAddHashtag: onAddHashtag,
                    onAddMention: onAddMention
                )
            }
        }
    }
}

// MARK: - Suggestions Overlay
struct SuggestionsOverlay: View {
    let showHashtagSuggestions: Bool
    let showMentionSuggestions: Bool
    let hashtagSuggestions: [String]
    let mentionSuggestions: [UserProfile]
    let currentHashtagQuery: String
    let currentMentionQuery: String
    let onAddHashtag: (String) -> Void
    let onAddMention: (UserProfile) -> Void
    
    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            if showHashtagSuggestions {
                HashtagSuggestionsView(
                    suggestions: hashtagSuggestions,
                    query: currentHashtagQuery,
                    onSelect: onAddHashtag
                )
            }
            
            if showMentionSuggestions {
                MentionSuggestionsView(
                    suggestions: mentionSuggestions,
                    query: currentMentionQuery,
                    onSelect: onAddMention
                )
            }
        }
        .background(Color(UIColor.systemBackground))
        .cornerRadius(12)
        .shadow(radius: 8)
        .padding(.horizontal, 16)
    }
}

// MARK: - Hashtag Suggestions View
struct HashtagSuggestionsView: View {
    let suggestions: [String]
    let query: String
    let onSelect: (String) -> Void
    
    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            Text("Hashtags")
                .font(.caption)
                .foregroundColor(.secondary)
                .padding(.horizontal, 12)
                .padding(.vertical, 8)
            
            ForEach(suggestions, id: \.self) { hashtag in
                Button(action: { onSelect(hashtag) }) {
                    HStack {
                        Text("#\(hashtag)")
                            .foregroundColor(.primary)
                        
                        Spacer()
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                }
                .buttonStyle(PlainButtonStyle())
                
                if hashtag != suggestions.last {
                    Divider()
                        .padding(.leading, 12)
                }
            }
        }
    }
}

// MARK: - Mention Suggestions View
struct MentionSuggestionsView: View {
    let suggestions: [UserProfile]
    let query: String
    let onSelect: (UserProfile) -> Void
    
    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            Text("Users")
                .font(.caption)
                .foregroundColor(.secondary)
                .padding(.horizontal, 12)
                .padding(.vertical, 8)
            
            ForEach(suggestions, id: \.userId) { user in
                Button(action: { onSelect(user) }) {
                    HStack(spacing: 12) {
                        AsyncImage(url: URL(string: user.avatarUrl)) { image in
                            image
                                .resizable()
                                .aspectRatio(contentMode: .fill)
                        } placeholder: {
                            Circle()
                                .fill(Color(.systemGray4))
                                .overlay(
                                    Image(systemName: "person.fill")
                                        .foregroundColor(.secondary)
                                )
                        }
                        .frame(width: 32, height: 32)
                        .clipShape(Circle())
                        
                        VStack(alignment: .leading, spacing: 2) {
                            Text(user.displayName)
                                .font(.body)
                                .foregroundColor(.primary)
                            
                            Text("@\(user.username)")
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                        
                        Spacer()
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                }
                .buttonStyle(PlainButtonStyle())
                
                if user.userId != suggestions.last?.userId {
                    Divider()
                        .padding(.leading, 56)
                }
            }
        }
    }
}

// MARK: - Scheduling View
struct SchedulingView: View {
    @Binding var scheduledDate: Date?
    let onSchedule: (Date) -> Void
    let onCancel: () -> Void
    
    @State private var selectedDate = Date().addingTimeInterval(3600) // 1 hour from now
    
    var body: some View {
        NavigationView {
            VStack(spacing: 20) {
                Text("Schedule Post")
                    .font(.title2)
                    .fontWeight(.bold)
                
                DatePicker(
                    "Schedule for",
                    selection: $selectedDate,
                    in: Date().addingTimeInterval(300)...Date().addingTimeInterval(2592000), // 5 min to 30 days
                    displayedComponents: [.date, .hourAndMinute]
                )
                .datePickerStyle(.wheel)
                
                HStack(spacing: 16) {
                    Button("Cancel", action: onCancel)
                        .buttonStyle(.bordered)
                    
                    Button("Schedule") {
                        onSchedule(selectedDate)
                    }
                    .buttonStyle(.borderedProminent)
                }
                
                Spacer()
            }
            .padding()
            .navigationBarHidden(true)
        }
    }
}

// MARK: - Flow Layout
struct FlowLayout: Layout {
    let spacing: CGFloat
    
    func sizeThatFits(proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) -> CGSize {
        let result = FlowResult(
            in: proposal.replacingUnspecifiedDimensions().width,
            subviews: subviews,
            spacing: spacing
        )
        return result.size
    }
    
    func placeSubviews(in bounds: CGRect, proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) {
        let result = FlowResult(
            in: bounds.width,
            subviews: subviews,
            spacing: spacing
        )
        
        for (index, subview) in subviews.enumerated() {
            subview.place(at: CGPoint(x: bounds.minX + result.positions[index].x, y: bounds.minY + result.positions[index].y), proposal: .unspecified)
        }
    }
    
    struct FlowResult {
        let positions: [CGPoint]
        let size: CGSize
        
        init(in maxWidth: CGFloat, subviews: Subviews, spacing: CGFloat) {
            var positions: [CGPoint] = []
            var currentX: CGFloat = 0
            var currentY: CGFloat = 0
            var lineHeight: CGFloat = 0
            var maxWidth = maxWidth
            
            for subview in subviews {
                let size = subview.sizeThatFits(.unspecified)
                
                if currentX + size.width > maxWidth && currentX > 0 {
                    currentX = 0
                    currentY += lineHeight + spacing
                    lineHeight = 0
                }
                
                positions.append(CGPoint(x: currentX, y: currentY))
                lineHeight = max(lineHeight, size.height)
                currentX += size.width + spacing
            }
            
            self.positions = positions
            self.size = CGSize(width: maxWidth, height: currentY + lineHeight)
        }
    }
}

// MARK: - Preview
struct ContentCreationView_Previews: PreviewProvider {
    static var previews: some View {
        ContentCreationView(
            grpcClient: SonetGRPCClient(configuration: .development)
        )
    }
}