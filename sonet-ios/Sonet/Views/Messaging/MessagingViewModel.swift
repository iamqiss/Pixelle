import Foundation
import Combine
import SwiftUI
import PhotosUI

@MainActor
class MessagingViewModel: ObservableObject {
    
    // MARK: - Published Properties
    @Published var conversations: [Conversation] = []
    @Published var currentConversation: Conversation?
    @Published var messages: [Message] = []
    @Published var messageText = ""
    @Published var isTyping = false
    @Published var isLoading = false
    @Published var error: String?
    @Published var showMediaPicker = false
    @Published var showVoiceRecorder = false
    @Published var isRecordingVoice = false
    @Published var voiceRecordingDuration: TimeInterval = 0
    @Published var selectedMedia: [MediaItem] = []
    @Published var showConversationList = true
    @Published var searchQuery = ""
    @Published var filteredConversations: [Conversation] = []
    @Published var onlineUsers: Set<String> = []
    @Published var typingUsers: Set<String> = []
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private var cancellables = Set<AnyCancellable>()
    private var messageStream: AnyCancellable?
    private var typingTimer: Timer?
    private var voiceRecordingTimer: Timer?
    private var currentUserId: String
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient, currentUserId: String) {
        self.grpcClient = grpcClient
        self.currentUserId = currentUserId
        setupMessageStream()
        loadConversations()
    }
    
    // MARK: - Public Methods
    func loadConversations() {
        Task {
            isLoading = true
            error = nil
            
            do {
                let response = try await grpcClient.getConversations(page: 0, pageSize: 50)
                await MainActor.run {
                    self.conversations = response.conversations.map { Conversation(from: $0, currentUserId: self.currentUserId) }
                    self.applySearchFilter()
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to load conversations: \(error.localizedDescription)"
                }
            }
            
            await MainActor.run {
                self.isLoading = false
            }
        }
    }
    
    func selectConversation(_ conversation: Conversation) {
        currentConversation = conversation
        showConversationList = false
        loadMessages(for: conversation.id)
        markConversationAsRead(conversation.id)
    }
    
    func loadMessages(for conversationId: String) {
        Task {
            do {
                let response = try await grpcClient.getMessages(conversationId: conversationId, page: 0, pageSize: 100)
                await MainActor.run {
                    self.messages = response.messages.map { Message(from: $0, currentUserId: self.currentUserId) }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to load messages: \(error.localizedDescription)"
                }
            }
        }
    }
    
    func sendMessage() {
        guard !messageText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
              let conversation = currentConversation else { return }
        
        let messageContent = messageText.trimmingCharacters(in: .whitespacesAndNewlines)
        messageText = ""
        
        Task {
            do {
                var messageRequest = SendMessageRequest()
                messageRequest.conversationId = conversation.id
                messageRequest.content = messageContent
                messageRequest.type = .MESSAGE_TYPE_TEXT
                
                let response = try await grpcClient.sendMessage(request: messageRequest)
                if response.success {
                    // Message will be added via the stream
                    await sendTypingIndicator(false)
                } else {
                    await MainActor.run {
                        self.error = "Failed to send message: \(response.errorMessage)"
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to send message: \(error.localizedDescription)"
                }
            }
        }
    }
    
    func sendMediaMessage() {
        guard !selectedMedia.isEmpty,
              let conversation = currentConversation else { return }
        
        Task {
            for media in selectedMedia {
                do {
                    var messageRequest = SendMessageRequest()
                    messageRequest.conversationId = conversation.id
                    messageRequest.mediaList = [media.toGRPC()]
                    messageRequest.type = media.type.grpcType
                    
                    let response = try await grpcClient.sendMessage(request: messageRequest)
                    if !response.success {
                        await MainActor.run {
                            self.error = "Failed to send media: \(response.errorMessage)"
                        }
                    }
                } catch {
                    await MainActor.run {
                        self.error = "Failed to send media: \(error.localizedDescription)"
                    }
                }
            }
            
            await MainActor.run {
                self.selectedMedia.removeAll()
            }
        }
    }
    
    func startVoiceRecording() {
        isRecordingVoice = true
        voiceRecordingDuration = 0
        
        voiceRecordingTimer = Timer.scheduledTimer(withTimeInterval: 0.1, repeats: true) { _ in
            self.voiceRecordingDuration += 0.1
        }
        
        // Start actual voice recording here
        // This would integrate with AVAudioRecorder
    }
    
    func stopVoiceRecording() {
        isRecordingVoice = false
        voiceRecordingTimer?.invalidate()
        voiceRecordingTimer = nil
        
        // Stop recording and send voice message
        if voiceRecordingDuration > 1.0 {
            sendVoiceMessage(duration: voiceRecordingDuration)
        }
        
        voiceRecordingDuration = 0
    }
    
    func sendTypingIndicator(_ isTyping: Bool) async {
        guard let conversation = currentConversation else { return }
        
        do {
            var typingRequest = TypingIndicatorRequest()
            typingRequest.conversationId = conversation.id
            typingRequest.isTyping = isTyping
            
            _ = try await grpcClient.sendTypingIndicator(request: typingRequest)
        } catch {
            // Handle error silently
        }
    }
    
    func markConversationAsRead(_ conversationId: String) {
        Task {
            do {
                let response = try await grpcClient.markConversationAsRead(conversationId: conversationId)
                if response.success {
                    // Update local state
                    if let index = conversations.firstIndex(where: { $0.id == conversationId }) {
                        conversations[index].unreadCount = 0
                    }
                }
            } catch {
                // Handle error silently
            }
        }
    }
    
    func deleteMessage(_ message: Message) {
        Task {
            do {
                let response = try await grpcClient.deleteMessage(messageId: message.id)
                if response.success {
                    await MainActor.run {
                        self.messages.removeAll { $0.id == message.id }
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to delete message: \(error.localizedDescription)"
                }
            }
        }
    }
    
    func reportMessage(_ message: Message) {
        Task {
            do {
                // Replace with actual moderation/report gRPC when available
                _ = try await grpcClient.reportContent(contentId: message.id, contentType: .MESSAGE)
            } catch {
                // Silently ignore for now
            }
        }
    }
    
    func createGroupChat(name: String, participants: [String]) {
        Task {
            do {
                var groupRequest = CreateGroupRequest()
                groupRequest.name = name
                groupRequest.participantIds = participants
                
                let response = try await grpcClient.createGroup(request: groupRequest)
                if response.success {
                    await loadConversations()
                } else {
                    await MainActor.run {
                        self.error = "Failed to create group: \(response.errorMessage)"
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to create group: \(error.localizedDescription)"
                }
            }
        }
    }
    
    func searchConversations(query: String) {
        searchQuery = query
        applySearchFilter()
    }
    
    func backToConversationList() {
        showConversationList = true
        currentConversation = nil
        messages.removeAll()
    }
    
    // MARK: - Private Methods
    private func setupMessageStream() {
        messageStream = grpcClient.messageStream()
            .receive(on: DispatchQueue.main)
            .sink(
                receiveCompletion: { completion in
                    switch completion {
                    case .finished:
                        break
                    case .failure(let error):
                        self.error = "Message stream error: \(error.localizedDescription)"
                    }
                },
                receiveValue: { message in
                    self.handleNewMessage(message)
                }
            )
    }
    
    private func handleNewMessage(_ message: xyz.sonet.app.grpc.proto.Message) {
        let newMessage = Message(from: message, currentUserId: currentUserId)
        
        // Add to current conversation if it matches
        if let conversation = currentConversation,
           message.conversationId == conversation.id {
            messages.append(newMessage)
            
            // Mark as read if we're viewing the conversation
            if !message.isRead {
                markMessageAsRead(message.messageId)
            }
        }
        
        // Update conversation list
        if let index = conversations.firstIndex(where: { $0.id == message.conversationId }) {
            conversations[index].lastMessage = newMessage
            conversations[index].lastMessageTime = newMessage.timestamp
            conversations[index].unreadCount += message.isRead ? 0 : 1
            
            // Move to top
            let conversation = conversations.remove(at: index)
            conversations.insert(conversation, at: 0)
        }
        
        applySearchFilter()
    }
    
    private func markMessageAsRead(_ messageId: String) {
        Task {
            do {
                _ = try await grpcClient.markMessageAsRead(messageId: messageId)
            } catch {
                // Handle error silently
            }
        }
    }
    
    private func sendVoiceMessage(duration: TimeInterval) {
        guard let conversation = currentConversation else { return }
        
        Task {
            do {
                var messageRequest = SendMessageRequest()
                messageRequest.conversationId = conversation.id
                messageRequest.type = .MESSAGE_TYPE_VOICE
                messageRequest.duration = Int32(duration)
                
                // Add voice file data here
                // messageRequest.voiceData = voiceData
                
                let response = try await grpcClient.sendMessage(request: messageRequest)
                if !response.success {
                    await MainActor.run {
                        self.error = "Failed to send voice message: \(response.errorMessage)"
                    }
                }
            } catch {
                await MainActor.run {
                    self.error = "Failed to send voice message: \(error.localizedDescription)"
                }
            }
        }
    }
    
    private func applySearchFilter() {
        if searchQuery.isEmpty {
            filteredConversations = conversations
        } else {
            filteredConversations = conversations.filter { conversation in
                conversation.name.localizedCaseInsensitiveContains(searchQuery) ||
                conversation.lastMessage?.content.localizedCaseInsensitiveContains(searchQuery) == true
            }
        }
    }
    
    deinit {
        messageStream?.cancel()
        typingTimer?.invalidate()
        voiceRecordingTimer?.invalidate()
    }
}

// MARK: - Data Models
struct Conversation: Identifiable {
    let id: String
    let name: String
    let type: ConversationType
    let participants: [UserProfile]
    let lastMessage: Message?
    let lastMessageTime: Date
    let unreadCount: Int
    let isGroup: Bool
    let groupAvatar: String?
    let isOnline: Bool
    
    init(from grpcConversation: xyz.sonet.app.grpc.proto.Conversation, currentUserId: String) {
        self.id = grpcConversation.conversationId
        self.name = grpcConversation.name
        self.type = ConversationType(from: grpcConversation.type)
        self.participants = grpcConversation.participantsList.map { UserProfile(from: $0) }
        self.lastMessage = grpcConversation.hasLastMessage ? Message(from: grpcConversation.lastMessage, currentUserId: currentUserId) : nil
        self.lastMessageTime = Date(timeIntervalSince1970: TimeInterval(grpcConversation.lastMessageTime.seconds))
        self.unreadCount = Int(grpcConversation.unreadCount)
        self.isGroup = grpcConversation.type == .CONVERSATION_TYPE_GROUP
        self.groupAvatar = grpcConversation.hasGroupAvatar ? grpcConversation.groupAvatar : nil
        self.isOnline = false // This would be updated via presence stream
    }
}

enum ConversationType: String, CaseIterable {
    case direct = "direct"
    case group = "group"
    
    init(from grpcType: xyz.sonet.app.grpc.proto.ConversationType) {
        switch grpcType {
        case .CONVERSATION_TYPE_DIRECT: self = .direct
        case .CONVERSATION_TYPE_GROUP: self = .group
        default: self = .direct
        }
    }
}

struct Message: Identifiable {
    let id: String
    let content: String
    let type: MessageType
    let timestamp: Date
    let sender: UserProfile
    let isFromCurrentUser: Bool
    let isRead: Bool
    let media: [MediaItem]
    let replyTo: Message?
    let reactions: [MessageReaction]
    let duration: TimeInterval? // For voice messages
    
    init(from grpcMessage: xyz.sonet.app.grpc.proto.Message, currentUserId: String) {
        self.id = grpcMessage.messageId
        self.content = grpcMessage.content
        self.type = MessageType(from: grpcMessage.type)
        self.timestamp = Date(timeIntervalSince1970: TimeInterval(grpcMessage.timestamp.seconds))
        self.sender = UserProfile(from: grpcMessage.sender)
        self.isFromCurrentUser = grpcMessage.sender.userId == currentUserId
        self.isRead = grpcMessage.isRead
        self.media = grpcMessage.mediaList.map { MediaItem(from: $0) }
        self.replyTo = grpcMessage.hasReplyTo ? Message(from: grpcMessage.replyTo, currentUserId: currentUserId) : nil
        self.reactions = grpcMessage.reactionsList.map { MessageReaction(from: $0) }
        self.duration = grpcMessage.hasDuration ? TimeInterval(grpcMessage.duration) : nil
    }
}

enum MessageType: String, CaseIterable {
    case text = "text"
    case image = "image"
    case video = "video"
    case voice = "voice"
    case document = "document"
    case location = "location"
    case contact = "contact"
    
    init(from grpcType: xyz.sonet.app.grpc.proto.MessageType) {
        switch grpcType {
        case .MESSAGE_TYPE_TEXT: self = .text
        case .MESSAGE_TYPE_IMAGE: self = .image
        case .MESSAGE_TYPE_VIDEO: self = .video
        case .MESSAGE_TYPE_VOICE: self = .voice
        case .MESSAGE_TYPE_DOCUMENT: self = .document
        case .MESSAGE_TYPE_LOCATION: self = .location
        case .MESSAGE_TYPE_CONTACT: self = .contact
        default: self = .text
        }
    }
    
    var icon: String {
        switch self {
        case .text: return "text.bubble"
        case .image: return "photo"
        case .video: return "video"
        case .voice: return "waveform"
        case .document: return "doc"
        case .location: return "location"
        case .contact: return "person"
        }
    }
}

struct MessageReaction: Identifiable {
    let id: String
    let emoji: String
    let user: UserProfile
    let timestamp: Date
    
    init(from grpcReaction: xyz.sonet.app.grpc.proto.MessageReaction) {
        self.id = grpcReaction.reactionId
        self.emoji = grpcReaction.emoji
        self.user = UserProfile(from: grpcReaction.user)
        self.timestamp = Date(timeIntervalSince1970: TimeInterval(grpcReaction.timestamp.seconds))
    }
}

// MARK: - Media Types
enum MediaType: String, CaseIterable {
    case image = "image"
    case video = "video"
    case voice = "voice"
    case document = "document"
    
    var icon: String {
        switch self {
        case .image: return "photo"
        case .video: return "video"
        case .voice: return "waveform"
        case .document: return "doc"
        }
    }
    
    var grpcType: xyz.sonet.app.grpc.proto.MediaType {
        switch self {
        case .image: return .MEDIA_TYPE_IMAGE
        case .video: return .MEDIA_TYPE_VIDEO
        case .voice: return .MEDIA_TYPE_AUDIO
        case .document: return .MEDIA_TYPE_DOCUMENT
        }
    }
}

struct MediaItem: Identifiable {
    let id = UUID()
    let mediaId: String
    let url: String
    let type: MediaType
    let thumbnail: String?
    let duration: TimeInterval?
    let size: Int64?
    let fileName: String?
    
    init(from grpcMedia: xyz.sonet.app.grpc.proto.MediaItem) {
        self.mediaId = grpcMedia.mediaId
        self.url = grpcMedia.url
        self.type = MediaType(from: grpcMedia.type)
        self.thumbnail = grpcMedia.hasThumbnail ? grpcMedia.thumbnail : nil
        self.duration = grpcMedia.hasDuration ? TimeInterval(grpcMedia.duration) : nil
        self.size = grpcMedia.hasSize ? grpcMedia.size : nil
        self.fileName = grpcMedia.hasFileName ? grpcMedia.fileName : nil
    }
    
    func toGRPC() -> xyz.sonet.app.grpc.proto.MediaItem {
        var media = xyz.sonet.app.grpc.proto.MediaItem()
        media.mediaId = mediaId
        media.url = url
        media.type = type.grpcType
        if let thumbnail = thumbnail {
            media.thumbnail = thumbnail
        }
        if let duration = duration {
            media.duration = Int32(duration)
        }
        if let size = size {
            media.size = size
        }
        if let fileName = fileName {
            media.fileName = fileName
        }
        return media
    }
}

// MARK: - Chat Theme (Future Implementation)
struct ChatTheme {
    let backgroundColor: Color
    let bubbleColor: Color
    let textColor: Color
    let accentColor: Color
    let customBackground: String? // URL or asset name
    
    static let `default` = ChatTheme(
        backgroundColor: Color(.systemBackground),
        bubbleColor: Color.blue,
        textColor: Color.white,
        accentColor: Color.blue,
        customBackground: nil
    )
    
    static let dark = ChatTheme(
        backgroundColor: Color(.systemGray6),
        bubbleColor: Color.blue,
        textColor: Color.white,
        accentColor: Color.blue,
        customBackground: nil
    )
}