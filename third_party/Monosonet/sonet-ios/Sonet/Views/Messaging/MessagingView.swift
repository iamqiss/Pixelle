import SwiftUI
import PhotosUI

struct MessagingView: View {
    @StateObject private var viewModel: MessagingViewModel
    @Environment(\.colorScheme) var colorScheme
    @Environment(\.dismiss) private var dismiss
    
    init(grpcClient: SonetGRPCClient, currentUserId: String) {
        _viewModel = StateObject(wrappedValue: MessagingViewModel(
            grpcClient: grpcClient,
            currentUserId: currentUserId
        ))
    }
    
    var body: some View {
        NavigationView {
            Group {
                if viewModel.showConversationList {
                    ConversationListView(
                        conversations: viewModel.filteredConversations,
                        isLoading: viewModel.isLoading,
                        error: viewModel.error,
                        searchQuery: $viewModel.searchQuery,
                        onConversationSelected: { viewModel.selectConversation($0) },
                        onSearch: { viewModel.searchConversations(query: $0) },
                        onRetry: { viewModel.loadConversations() }
                    )
                } else if let conversation = viewModel.currentConversation {
                    ChatView(
                        conversation: conversation,
                        messages: viewModel.messages,
                        messageText: $viewModel.messageText,
                        isTyping: viewModel.isTyping,
                        selectedMedia: $viewModel.selectedMedia,
                        isRecordingVoice: viewModel.isRecordingVoice,
                        voiceRecordingDuration: viewModel.voiceRecordingDuration,
                        onSendMessage: { viewModel.sendMessage() },
                        onSendMedia: { viewModel.sendMediaMessage() },
                        onStartVoiceRecording: { viewModel.startVoiceRecording() },
                        onStopVoiceRecording: { viewModel.stopVoiceRecording() },
                        onDeleteMessage: { viewModel.deleteMessage($0) },
                        onReportMessage: { viewModel.reportMessage($0) },
                        onBack: { viewModel.backToConversationList() }
                    )
                }
            }
            .navigationBarHidden(true)
        }
        .onAppear {
            viewModel.loadConversations()
        }
    }
}

// MARK: - Conversation List View
struct ConversationListView: View {
    let conversations: [Conversation]
    let isLoading: Bool
    let error: String?
    @Binding var searchQuery: String
    let onConversationSelected: (Conversation) -> Void
    let onSearch: (String) -> Void
    let onRetry: () -> Void
    
    var body: some View {
        VStack(spacing: 0) {
            // Header
            ConversationListHeader()
            
            // Search bar
            SearchBar(
                text: $searchQuery,
                placeholder: "Search conversations...",
                onSearch: onSearch
            )
            
            // Content
            if isLoading {
                LoadingView()
            } else if let error = error {
                ErrorView(error: error, onRetry: onRetry)
            } else if conversations.isEmpty {
                EmptyConversationsView()
            } else {
                LazyVStack(spacing: 0) {
                    ForEach(conversations) { conversation in
                        ConversationRow(
                            conversation: conversation,
                            onTap: { onConversationSelected(conversation) }
                        )
                        
                        if conversation.id != conversations.last?.id {
                            Divider()
                                .padding(.leading, 72)
                        }
                    }
                }
            }
        }
        .background(Color(.systemBackground))
    }
}

// MARK: - Conversation List Header
struct ConversationListHeader: View {
    var body: some View {
        HStack {
            Text("Messages")
                .font(.largeTitle)
                .fontWeight(.bold)
            
            Spacer()
            
            Button(action: { /* New conversation */ }) {
                Image(systemName: "square.and.pencil")
                    .font(.system(size: 20))
                    .foregroundColor(.accentColor)
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
        .background(Color(.systemBackground))
        .overlay(
            Rectangle()
                .fill(Color(.separator))
                .frame(height: 0.5),
            alignment: .bottom
        )
    }
}

// MARK: - Search Bar
struct SearchBar: View {
    @Binding var text: String
    let placeholder: String
    let onSearch: (String) -> Void
    
    var body: some View {
        HStack {
            Image(systemName: "magnifyingglass")
                .foregroundColor(.secondary)
            
            TextField(placeholder, text: $text)
                .textFieldStyle(PlainTextFieldStyle())
                .onChange(of: text) { newValue in
                    onSearch(newValue)
                }
            
            if !text.isEmpty {
                Button("Clear") {
                    text = ""
                    onSearch("")
                }
                .foregroundColor(.accentColor)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(Color(.systemGray6))
        .cornerRadius(10)
        .padding(.horizontal, 16)
        .padding(.vertical, 8)
    }
}

// MARK: - Conversation Row
struct ConversationRow: View {
    let conversation: Conversation
    let onTap: () -> Void
    
    var body: some View {
        Button(action: onTap) {
            HStack(spacing: 12) {
                // Avatar
                ConversationAvatar(conversation: conversation)
                
                // Content
                VStack(alignment: .leading, spacing: 4) {
                    HStack {
                        Text(conversation.name)
                            .font(.system(size: 16, weight: .semibold))
                            .foregroundColor(.primary)
                        
                        Spacer()
                        
                        Text(timeAgoString)
                            .font(.system(size: 12))
                            .foregroundColor(.secondary)
                    }
                    
                    HStack {
                        if let lastMessage = conversation.lastMessage {
                            Text(lastMessage.content)
                                .font(.system(size: 14))
                                .foregroundColor(.secondary)
                                .lineLimit(1)
                        } else {
                            Text("No messages yet")
                                .font(.system(size: 14))
                                .foregroundColor(.secondary)
                                .italic()
                        }
                        
                        Spacer()
                        
                        if conversation.unreadCount > 0 {
                            Text("\(conversation.unreadCount)")
                                .font(.system(size: 12, weight: .semibold))
                                .foregroundColor(.white)
                                .frame(width: 20, height: 20)
                                .background(Circle().fill(Color.red))
                        }
                    }
                }
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 12)
        }
        .buttonStyle(PlainButtonStyle())
    }
    
    private var timeAgoString: String {
        let now = Date()
        let timeInterval = now.timeIntervalSince(conversation.lastMessageTime)
        
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
}

// MARK: - Conversation Avatar
struct ConversationAvatar: View {
    let conversation: Conversation
    
    var body: some View {
        if conversation.isGroup {
            // Group avatar
            ZStack {
                Circle()
                    .fill(Color.accentColor.opacity(0.2))
                    .frame(width: 50, height: 50)
                
                Image(systemName: "person.3")
                    .font(.system(size: 20))
                    .foregroundColor(.accentColor)
            }
        } else {
            // Individual avatar
            if let participant = conversation.participants.first {
                AsyncImage(url: URL(string: participant.avatarUrl)) { image in
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
                .frame(width: 50, height: 50)
                .clipShape(Circle())
                .overlay(
                    Circle()
                        .stroke(conversation.isOnline ? Color.green : Color.clear, lineWidth: 2)
                )
            }
        }
    }
}

// MARK: - Chat View
struct ChatView: View {
    let conversation: Conversation
    let messages: [Message]
    @Binding var messageText: String
    let isTyping: Bool
    @Binding var selectedMedia: [MediaItem]
    let isRecordingVoice: Bool
    let voiceRecordingDuration: TimeInterval
    let onSendMessage: () -> Void
    let onSendMedia: () -> Void
    let onStartVoiceRecording: () -> Void
    let onStopVoiceRecording: () -> Void
    let onDeleteMessage: (Message) -> Void
    let onReportMessage: (Message) -> Void
    let onBack: () -> Void
    
    var body: some View {
        VStack(spacing: 0) {
            // Chat header
            ChatHeader(
                conversation: conversation,
                onBack: onBack
            )
            
            // Messages
            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(spacing: 8) {
                        ForEach(messages) { message in
                            MessageBubble(
                                message: message,
                                onDelete: { onDeleteMessage(message) },
                                onReport: { onReportMessage(message) }
                            )
                            .id(message.id)
                        }
                        
                        // Typing indicator
                        if isTyping {
                            TypingIndicator()
                        }
                    }
                    .padding(.horizontal, 16)
                    .padding(.vertical, 8)
                }
                .onChange(of: messages.count) { _ in
                    if let lastMessage = messages.last {
                        withAnimation(.easeInOut(duration: 0.3)) {
                            proxy.scrollTo(lastMessage.id, anchor: .bottom)
                        }
                    }
                }
            }
            
            // Input area
            ChatInputArea(
                messageText: $messageText,
                selectedMedia: $selectedMedia,
                isRecordingVoice: isRecordingVoice,
                voiceRecordingDuration: voiceRecordingDuration,
                onSendMessage: onSendMessage,
                onSendMedia: onSendMedia,
                onStartVoiceRecording: onStartVoiceRecording,
                onStopVoiceRecording: onStopVoiceRecording
            )
        }
        .background(Color(.systemBackground))
    }
}

// MARK: - Chat Header
struct ChatHeader: View {
    let conversation: Conversation
    let onBack: () -> Void
    
    var body: some View {
        HStack(spacing: 12) {
            Button(action: onBack) {
                Image(systemName: "chevron.left")
                    .font(.system(size: 18, weight: .semibold))
                    .foregroundColor(.accentColor)
            }
            
            // Avatar
            ConversationAvatar(conversation: conversation)
            
            // Info
            VStack(alignment: .leading, spacing: 2) {
                Text(conversation.name)
                    .font(.system(size: 18, weight: .semibold))
                    .foregroundColor(.primary)
                
                if conversation.isGroup {
                    Text("\(conversation.participants.count) members")
                        .font(.system(size: 12))
                        .foregroundColor(.secondary)
                } else {
                    Text(conversation.isOnline ? "Online" : "Offline")
                        .font(.system(size: 12))
                        .foregroundColor(conversation.isOnline ? .green : .secondary)
                }
            }
            
            Spacer()
            
            // Actions
            HStack(spacing: 16) {
                Button(action: { /* Video call */ }) {
                    Image(systemName: "video")
                        .font(.system(size: 18))
                        .foregroundColor(.accentColor)
                }
                
                Button(action: { /* Voice call */ }) {
                    Image(systemName: "phone")
                        .font(.system(size: 18))
                        .foregroundColor(.accentColor)
                }
                
                Button(action: { /* More options */ }) {
                    Image(systemName: "ellipsis")
                        .font(.system(size: 18))
                        .foregroundColor(.accentColor)
                }
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
        .background(Color(.systemBackground))
        .overlay(
            Rectangle()
                .fill(Color(.separator))
                .frame(height: 0.5),
            alignment: .bottom
        )
    }
}

// MARK: - Message Bubble
struct MessageBubble: View {
    let message: Message
    let onDelete: () -> Void
    let onReport: () -> Void
    
    @State private var showingDeleteAlert = false
    
    var body: some View {
        HStack {
            if message.isFromCurrentUser {
                Spacer(minLength: 60)
                
                VStack(alignment: .trailing, spacing: 4) {
                    // Message content
                    MessageContent(message: message)
                        .background(
                            RoundedRectangle(cornerRadius: 18)
                                .fill(Color.accentColor)
                        )
                        .foregroundColor(.white)
                    
                    // Message info
                    MessageInfo(message: message)
                }
            } else {
                VStack(alignment: .leading, spacing: 4) {
                    // Sender name (for group chats)
                    if message.sender.displayName != "You" {
                        Text(message.sender.displayName)
                            .font(.system(size: 12, weight: .medium))
                            .foregroundColor(.secondary)
                            .padding(.leading, 4)
                    }
                    
                    // Message content
                    MessageContent(message: message)
                        .background(
                            RoundedRectangle(cornerRadius: 18)
                                .fill(Color(.systemGray5))
                        )
                        .foregroundColor(.primary)
                    
                    // Message info
                    MessageInfo(message: message)
                }
                
                Spacer(minLength: 60)
            }
        }
        .contextMenu {
            if message.isFromCurrentUser {
                Button("Delete", role: .destructive, action: { showingDeleteAlert = true })
            } else {
                Button("Report", role: .destructive, action: onReport)
            }
        }
        .alert("Delete Message", isPresented: $showingDeleteAlert) {
            Button("Delete", role: .destructive, action: onDelete)
            Button("Cancel", role: .cancel) { }
        } message: {
            Text("Are you sure you want to delete this message?")
        }
    }
}

// MARK: - Message Content
struct MessageContent: View {
    let message: Message
    
    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            // Reply to message (if any)
            if let replyTo = message.replyTo {
                ReplyPreview(message: replyTo)
            }
            
            // Main content
            switch message.type {
            case .text:
                Text(message.content)
                    .font(.system(size: 16))
                    .padding(.horizontal, 16)
                    .padding(.vertical, 10)
                
            case .image:
                AsyncImage(url: URL(string: message.media.first?.url ?? "")) { image in
                    image
                        .resizable()
                        .aspectRatio(contentMode: .fit)
                        .frame(maxHeight: 200)
                        .cornerRadius(12)
                } placeholder: {
                    Rectangle()
                        .fill(Color(.systemGray4))
                        .frame(height: 200)
                        .overlay(
                            ProgressView()
                                .scaleEffect(1.5)
                        )
                        .cornerRadius(12)
                }
                .padding(.horizontal, 8)
                .padding(.vertical, 8)
                
            case .video:
                VideoMessageView(media: message.media.first)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 8)
                
            case .voice:
                VoiceMessageView(
                    duration: message.duration ?? 0,
                    isFromCurrentUser: message.isFromCurrentUser
                )
                .padding(.horizontal, 16)
                .padding(.vertical, 10)
                
            case .document:
                DocumentMessageView(media: message.media.first)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 8)
                
            case .location:
                LocationMessageView()
                    .padding(.horizontal, 8)
                    .padding(.vertical, 8)
                
            case .contact:
                ContactMessageView()
                    .padding(.horizontal, 8)
                    .padding(.vertical, 8)
            }
        }
    }
}

// MARK: - Reply Preview
struct ReplyPreview: View {
    let message: Message
    
    var body: some View {
        HStack(spacing: 8) {
            Rectangle()
                .fill(Color(.systemGray4))
                .frame(width: 3)
            
            VStack(alignment: .leading, spacing: 2) {
                Text(message.sender.displayName)
                    .font(.system(size: 12, weight: .semibold))
                    .foregroundColor(.secondary)
                
                Text(message.content)
                    .font(.system(size: 12))
                    .foregroundColor(.secondary)
                    .lineLimit(2)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(Color(.systemGray6))
        .cornerRadius(8)
        .padding(.horizontal, 4)
        .padding(.top, 4)
    }
}

// MARK: - Message Info
struct MessageInfo: View {
    let message: Message
    
    var body: some View {
        HStack(spacing: 4) {
            Text(timeString)
                .font(.system(size: 10))
                .foregroundColor(.secondary)
            
            if message.isFromCurrentUser {
                Image(systemName: message.isRead ? "checkmark.circle.fill" : "checkmark")
                    .font(.system(size: 10))
                    .foregroundColor(message.isRead ? .blue : .secondary)
            }
        }
        .padding(.horizontal, 4)
    }
    
    private var timeString: String {
        let formatter = DateFormatter()
        formatter.timeStyle = .short
        return formatter.string(from: message.timestamp)
    }
}

// MARK: - Video Message View
struct VideoMessageView: View {
    let media: MediaItem?
    
    var body: some View {
        VStack(spacing: 8) {
            AsyncImage(url: URL(string: media?.thumbnail ?? "")) { image in
                image
                    .resizable()
                    .aspectRatio(contentMode: .fit)
                    .frame(maxHeight: 150)
                    .cornerRadius(8)
            } placeholder: {
                Rectangle()
                    .fill(Color(.systemGray4))
                    .frame(height: 150)
                    .overlay(
                        Image(systemName: "play.circle.fill")
                            .font(.system(size: 40))
                            .foregroundColor(.white)
                    )
                    .cornerRadius(8)
            }
            
            HStack {
                Image(systemName: "video")
                    .font(.system(size: 12))
                    .foregroundColor(.secondary)
                
                Text(media?.fileName ?? "Video")
                    .font(.system(size: 12))
                    .foregroundColor(.secondary)
                
                Spacer()
                
                if let duration = media?.duration {
                    Text(formatDuration(duration))
                        .font(.system(size: 12))
                        .foregroundColor(.secondary)
                }
            }
        }
        .padding(12)
        .background(Color(.systemGray6))
        .cornerRadius(12)
    }
    
    private func formatDuration(_ duration: TimeInterval) -> String {
        let minutes = Int(duration) / 60
        let seconds = Int(duration) % 60
        return String(format: "%d:%02d", minutes, seconds)
    }
}

// MARK: - Voice Message View
struct VoiceMessageView: View {
    let duration: TimeInterval
    let isFromCurrentUser: Bool
    
    var body: some View {
        HStack(spacing: 12) {
            Button(action: { /* Play/pause */ }) {
                Image(systemName: "play.fill")
                    .font(.system(size: 16))
                    .foregroundColor(isFromCurrentUser ? .white : .accentColor)
            }
            
            // Waveform visualization
            HStack(spacing: 2) {
                ForEach(0..<Int(duration * 2), id: \.self) { _ in
                    Rectangle()
                        .fill(isFromCurrentUser ? .white : .accentColor)
                        .frame(width: 2, height: CGFloat.random(in: 8...20))
                        .cornerRadius(1)
                }
            }
            
            Text(formatDuration(duration))
                .font(.system(size: 12))
                .foregroundColor(isFromCurrentUser ? .white.opacity(0.8) : .secondary)
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 10)
    }
    
    private func formatDuration(_ duration: TimeInterval) -> String {
        let minutes = Int(duration) / 60
        let seconds = Int(duration) % 60
        return String(format: "%d:%02d", minutes, seconds)
    }
}

// MARK: - Document Message View
struct DocumentMessageView: View {
    let media: MediaItem?
    
    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: "doc.fill")
                .font(.system(size: 24))
                .foregroundColor(.accentColor)
            
            VStack(alignment: .leading, spacing: 2) {
                Text(media?.fileName ?? "Document")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundColor(.primary)
                
                if let size = media?.size {
                    Text(formatFileSize(size))
                        .font(.system(size: 12))
                        .foregroundColor(.secondary)
                }
            }
            
            Spacer()
            
            Button(action: { /* Download/open */ }) {
                Image(systemName: "arrow.down.circle")
                    .font(.system(size: 20))
                    .foregroundColor(.accentColor)
            }
        }
        .padding(12)
        .background(Color(.systemGray6))
        .cornerRadius(12)
    }
    
    private func formatFileSize(_ size: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB]
        formatter.countStyle = .file
        return formatter.string(fromByteCount: size)
    }
}

// MARK: - Location Message View
struct LocationMessageView: View {
    var body: some View {
        VStack(spacing: 8) {
            Image(systemName: "location.fill")
                .font(.system(size: 40))
                .foregroundColor(.accentColor)
            
            Text("Location shared")
                .font(.system(size: 14, weight: .medium))
                .foregroundColor(.primary)
            
            Button("View on Map") {
                // Open in Maps app
            }
            .font(.system(size: 12))
            .foregroundColor(.accentColor)
        }
        .padding(16)
        .background(Color(.systemGray6))
        .cornerRadius(12)
    }
}

// MARK: - Contact Message View
struct ContactMessageView: View {
    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: "person.circle.fill")
                .font(.system(size: 40))
                .foregroundColor(.accentColor)
            
            VStack(alignment: .leading, spacing: 4) {
                Text("Contact shared")
                    .font(.system(size: 14, weight: .medium))
                    .foregroundColor(.primary)
                
                Text("Tap to view contact")
                    .font(.system(size: 12))
                    .foregroundColor(.secondary)
            }
            
            Spacer()
        }
        .padding(12)
        .background(Color(.systemGray6))
        .cornerRadius(12)
    }
}

// MARK: - Typing Indicator
struct TypingIndicator: View {
    @State private var animationOffset: CGFloat = 0
    
    var body: some View {
        HStack {
            HStack(spacing: 4) {
                ForEach(0..<3, id: \.self) { index in
                    Circle()
                        .fill(Color.secondary.opacity(0.3))
                        .frame(width: 8, height: 8)
                        .scaleEffect(animationOffset == CGFloat(index) ? 1.2 : 1.0)
                        .animation(
                            Animation.easeInOut(duration: 0.6)
                                .repeatForever(autoreverses: true)
                                .delay(Double(index) * 0.2),
                            value: animationOffset
                        )
                }
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 10)
            .background(
                RoundedRectangle(cornerRadius: 18)
                    .fill(Color(UIColor.systemGray5))
            )
            
            Spacer()
        }
        .onAppear {
            animationOffset = 2
        }
    }
}

// MARK: - Chat Input Area
struct ChatInputArea: View {
    @Binding var messageText: String
    @Binding var selectedMedia: [MediaItem]
    let isRecordingVoice: Bool
    let voiceRecordingDuration: TimeInterval
    let onSendMessage: () -> Void
    let onSendMedia: () -> Void
    let onStartVoiceRecording: () -> Void
    let onStopVoiceRecording: () -> Void
    
    var body: some View {
        VStack(spacing: 0) {
            // Media preview
            if !selectedMedia.isEmpty {
                MediaPreviewGrid(
                    media: selectedMedia,
                    onRemove: { index in
                        selectedMedia.remove(at: index)
                    }
                )
            }
            
            // Input bar
            HStack(spacing: 12) {
                // Media button
                Button(action: { /* Show media picker */ }) {
                    Image(systemName: "plus")
                        .font(.system(size: 20))
                        .foregroundColor(.accentColor)
                }
                
                // Text input
                TextField("Message", text: $messageText, axis: .vertical)
                    .textFieldStyle(PlainTextFieldStyle())
                    .lineLimit(1...4)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                    .background(Color(UIColor.systemGray6))
                    .cornerRadius(20)
                
                // Voice recording or send button
                if messageText.isEmpty && selectedMedia.isEmpty {
                    Button(action: isRecordingVoice ? onStopVoiceRecording : onStartVoiceRecording) {
                        Image(systemName: isRecordingVoice ? "stop.fill" : "mic.fill")
                            .font(.system(size: 20))
                            .foregroundColor(isRecordingVoice ? .accentColor : .accentColor)
                            .scaleEffect(isRecordingVoice ? 1.2 : 1.0)
                            .animation(.easeInOut(duration: 0.2), value: isRecordingVoice)
                    }
                    .overlay(
                        Group {
                            if isRecordingVoice {
                                Text(formatDuration(voiceRecordingDuration))
                                    .font(.system(size: 10, weight: .semibold))
                                    .foregroundColor(.accentColor)
                                    .padding(.top, 24)
                            }
                        }
                    )
                } else {
                    Button(action: onSendMessage) {
                        Image(systemName: "arrow.up.circle.fill")
                            .font(.system(size: 24))
                            .foregroundColor(.accentColor)
                    }
                }
            }
            .padding(.horizontal, 16)
            .padding(.vertical, 12)
            .background(Color(.systemBackground))
            .overlay(
                Rectangle()
                    .fill(Color(.separator))
                    .frame(height: 0.5),
                alignment: .top
            )
        }
    }
    
    private func formatDuration(_ duration: TimeInterval) -> String {
        let seconds = Int(duration)
        return String(format: "%d", seconds)
    }
}

// MARK: - Media Preview Grid
struct MediaPreviewGrid: View {
    let media: [MediaItem]
    let onRemove: (Int) -> Void
    
    var body: some View {
        LazyVGrid(columns: Array(repeating: GridItem(.flexible(), spacing: 8), count: 4), spacing: 8) {
            ForEach(Array(media.enumerated()), id: \.element.id) { index, mediaItem in
                ZStack(alignment: .topTrailing) {
                    AsyncImage(url: URL(string: mediaItem.url)) { image in
                        image
                            .resizable()
                            .aspectRatio(contentMode: .fill)
                            .frame(height: 80)
                            .clipped()
                            .cornerRadius(8)
                    } placeholder: {
                        Rectangle()
                            .fill(Color(.systemGray4))
                            .frame(height: 80)
                            .overlay(
                                Image(systemName: mediaItem.type.icon)
                                    .font(.system(size: 20))
                                    .foregroundColor(.secondary)
                            )
                            .cornerRadius(8)
                    }
                    
                    Button(action: { onRemove(index) }) {
                        Image(systemName: "xmark.circle.fill")
                            .font(.system(size: 16))
                            .foregroundColor(.white)
                            .background(Circle().fill(Color.primary.opacity(0.6)))
                    }
                    .padding(4)
                }
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 8)
    }
}

// MARK: - Loading View
struct LoadingView: View {
    var body: some View {
        VStack(spacing: 16) {
            ProgressView()
                .scaleEffect(1.2)
            
            Text("Loading conversations...")
                .font(.system(size: 16, weight: .medium))
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(UIColor.systemBackground))
    }
}

// MARK: - Error View
struct ErrorView: View {
    let error: String
    let onRetry: () -> Void
    
    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 48))
                .foregroundColor(.primary)
            
            Text("Error")
                .font(.system(size: 20, weight: .bold))
                .foregroundColor(.primary)
            
            Text(error)
                .font(.system(size: 16))
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
            
            Button("Try Again", action: onRetry)
                .foregroundColor(.white)
                .padding(.horizontal, 24)
                .padding(.vertical, 12)
                .background(
                    RoundedRectangle(cornerRadius: 20)
                        .fill(Color.accentColor)
                )
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(.systemBackground))
    }
}

// MARK: - Empty Conversations View
struct EmptyConversationsView: View {
    var body: some View {
        VStack(spacing: 20) {
            Image(systemName: "message")
                .font(.system(size: 60))
                .foregroundColor(.secondary)
            
            Text("No conversations yet")
                .font(.title2)
                .fontWeight(.bold)
                .foregroundColor(.primary)
            
            Text("Start a conversation with someone to begin messaging")
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(.systemBackground))
    }
}

// MARK: - Preview
struct MessagingView_Previews: PreviewProvider {
    static var previews: some View {
        MessagingView(
            grpcClient: SonetGRPCClient(configuration: .development),
            currentUserId: "demo_user_123"
        )
    }
}