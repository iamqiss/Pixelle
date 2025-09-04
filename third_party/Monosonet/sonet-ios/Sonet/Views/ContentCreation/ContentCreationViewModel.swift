import Foundation
import Combine
import SwiftUI
import PhotosUI

@MainActor
class ContentCreationViewModel: ObservableObject {
    
    // MARK: - Published Properties
    @Published var noteContent = ""
    @Published var selectedMedia: [MediaItem] = []
    @Published var replyToNote: Note?
    @Published var quoteNote: Note?
    @Published var selectedHashtags: [String] = []
    @Published var selectedMentions: [String] = []
    @Published var isPosting = false
    @Published var postingError: String?
    @Published var showHashtagSuggestions = false
    @Published var showMentionSuggestions = false
    @Published var hashtagSuggestions: [String] = []
    @Published var mentionSuggestions: [UserProfile] = []
    @Published var currentHashtagQuery = ""
    @Published var currentMentionQuery = ""
    @Published var scheduledDate: Date?
    @Published var showScheduling = false
    @Published var isDraft = false
    @Published var draftId: String?
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private var cancellables = Set<AnyCancellable>()
    private var hashtagSearchTask: Task<Void, Never>?
    private var mentionSearchTask: Task<Void, Never>?
    
    // MARK: - Constants
    private let maxCharacterCount = 280
    private let maxMediaCount = 4
    
    // MARK: - Computed Properties
    var characterCount: Int {
        noteContent.count
    }
    
    var remainingCharacters: Int {
        maxCharacterCount - characterCount
    }
    
    var canPost: Bool {
        !noteContent.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty &&
        !isPosting &&
        remainingCharacters >= 0
    }
    
    var characterCountColor: Color {
        if remainingCharacters < 0 {
            return .red
        } else if remainingCharacters <= 20 {
            return .orange
        } else {
            return .secondary
        }
    }
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient, replyToNote: Note? = nil, quoteNote: Note? = nil) {
        self.grpcClient = grpcClient
        self.replyToNote = replyToNote
        self.quoteNote = quoteNote
        
        setupContentMonitoring()
        loadDraftIfNeeded()
    }
    
    // MARK: - Public Methods
    func addMedia(_ media: MediaItem) {
        guard selectedMedia.count < maxMediaCount else { return }
        selectedMedia.append(media)
    }
    
    func removeMedia(at index: Int) {
        guard index < selectedMedia.count else { return }
        selectedMedia.remove(at: index)
    }
    
    func addHashtag(_ hashtag: String) {
        let cleanHashtag = hashtag.hasPrefix("#") ? hashtag : "#\(hashtag)"
        if !selectedHashtags.contains(cleanHashtag) {
            selectedHashtags.append(cleanHashtag)
        }
        showHashtagSuggestions = false
        currentHashtagQuery = ""
    }
    
    func removeHashtag(_ hashtag: String) {
        selectedHashtags.removeAll { $0 == hashtag }
    }
    
    func addMention(_ user: UserProfile) {
        let mention = "@\(user.username)"
        if !selectedMentions.contains(mention) {
            selectedMentions.append(mention)
        }
        showMentionSuggestions = false
        currentMentionQuery = ""
    }
    
    func removeMention(_ mention: String) {
        selectedMentions.removeAll { $0 == mention }
    }
    
    func postNote() async {
        guard canPost else { return }
        
        isPosting = true
        postingError = nil
        var activity: Any? = nil
        if #available(iOS 16.1, *) {
            let firstFile = selectedMedia.first?.fileName ?? "Uploading"
            activity = UploadActivityManager.start(title: "Posting", filename: firstFile)
        }
        
        do {
            let content = noteContent.trimmingCharacters(in: .whitespacesAndNewlines)
            
            // Upload media first via MediaService.Upload and collect URLs
            var uploadedUrls: [String] = []
            for (index, media) in selectedMedia.enumerated() {
                if let local = media.localImage, let jpeg = local.jpegData(compressionQuality: 0.9) {
                    if #available(iOS 16.1, *), let act = activity as? Activity<UploadActivityAttributes> {
                        UploadActivityManager.update(act, progress: Double(index)/Double(max(selectedMedia.count,1)), filename: media.altText ?? "Image", isCompleted: false)
                    }
                    let upload = try await grpcClient.uploadMedia(ownerId: "current_user", filename: media.altText ?? "image.jpg", mimeType: "image/jpeg", data: jpeg) { p in
                        if #available(iOS 16.1, *), let act = activity as? Activity<UploadActivityAttributes> {
                            UploadActivityManager.update(act, progress: (Double(index) + p)/Double(max(selectedMedia.count,1)), filename: media.altText ?? "Image", isCompleted: false)
                        }
                    }
                    uploadedUrls.append(upload.url)
                } else if !media.url.isEmpty {
                    uploadedUrls.append(media.url)
                }
            }
            // Create note request
            var noteRequest = CreateNoteRequest()
            noteRequest.content = content
            noteRequest.mediaUrls = uploadedUrls
            
            // Add hashtags
            noteRequest.hashtags = selectedHashtags.map { $0.replacingOccurrences(of: "#", with: "") }
            
            // Add mentions
            noteRequest.mentions = selectedMentions.map { $0.replacingOccurrences(of: "@", with: "") }
            
            // Add reply info
            if let replyToNote = replyToNote {
                noteRequest.replyToNoteId = replyToNote.noteId
            }
            
            // Add quote info
            if let quoteNote = quoteNote {
                noteRequest.quoteNoteId = quoteNote.noteId
            }
            
            // Add scheduling
            if let scheduledDate = scheduledDate {
                var timestamp = Timestamp()
                timestamp.seconds = Int64(scheduledDate.timeIntervalSince1970)
                noteRequest.scheduledAt = timestamp
            }
            
            // Set visibility
            noteRequest.visibility = .NOTE_VISIBILITY_PUBLIC
            
            // Post the note
            // Simulate progressive updates if there are media items
            if #available(iOS 16.1, *), let act = activity as? Activity<UploadActivityAttributes> {
                for (index, media) in selectedMedia.enumerated() {
                    let progress = Double(index + 1) / Double(max(selectedMedia.count, 1))
                    UploadActivityManager.update(act, progress: progress, filename: media.fileName ?? "Media", isCompleted: false)
                    try await Task.sleep(nanoseconds: 200_000_000)
                }
            }
            let response = try await grpcClient.createNote(request: noteRequest)
            
            if response.success {
                // Clear form
                clearForm()
                
                // Navigate back or show success
                // This would be handled by the view
                if #available(iOS 16.1, *), let act = activity as? Activity<UploadActivityAttributes> {
                    UploadActivityManager.update(act, progress: 1.0, filename: "Done", isCompleted: true)
                }
            } else {
                postingError = response.errorMessage
            }
            
        } catch {
            postingError = "Failed to post note: \(error.localizedDescription)"
        }
        
        isPosting = false
    }
    
    func saveDraft() {
        let draft = NoteDraft(
            id: draftId ?? UUID().uuidString,
            content: noteContent,
            media: selectedMedia,
            replyToNote: replyToNote,
            quoteNote: quoteNote,
            hashtags: selectedHashtags,
            mentions: selectedMentions,
            scheduledDate: scheduledDate,
            createdAt: Date()
        )
        
        saveDraftToStorage(draft)
        draftId = draft.id
        isDraft = true
    }
    
    func loadDraft(_ draft: NoteDraft) {
        noteContent = draft.content
        selectedMedia = draft.media
        replyToNote = draft.replyToNote
        quoteNote = draft.quoteNote
        selectedHashtags = draft.hashtags
        selectedMentions = draft.mentions
        scheduledDate = draft.scheduledDate
        draftId = draft.id
        isDraft = true
    }
    
    func clearForm() {
        noteContent = ""
        selectedMedia = []
        selectedHashtags = []
        selectedMentions = []
        scheduledDate = nil
        draftId = nil
        isDraft = false
        postingError = nil
    }
    
    func schedulePost(for date: Date) {
        scheduledDate = date
        showScheduling = false
    }
    
    func cancelScheduling() {
        scheduledDate = nil
        showScheduling = false
    }
    
    // MARK: - Private Methods
    private func setupContentMonitoring() {
        // Monitor content changes for hashtag and mention detection
        $noteContent
            .debounce(for: .milliseconds(300), scheduler: DispatchQueue.main)
            .sink { [weak self] content in
                self?.detectHashtagsAndMentions(in: content)
            }
            .store(in: &cancellables)
    }
    
    private func detectHashtagsAndMentions(in content: String) {
        // Detect hashtags
        let hashtagPattern = #"#(\w+)"#
        let hashtagMatches = content.matches(of: hashtagPattern)
        
        if let lastMatch = hashtagMatches.last {
            let query = String(lastMatch.1)
            if query.count >= 2 {
                currentHashtagQuery = query
                searchHashtags(query: query)
            } else {
                showHashtagSuggestions = false
            }
        } else {
            showHashtagSuggestions = false
        }
        
        // Detect mentions
        let mentionPattern = #"@(\w+)"#
        let mentionMatches = content.matches(of: mentionPattern)
        
        if let lastMatch = mentionMatches.last {
            let query = String(lastMatch.1)
            if query.count >= 2 {
                currentMentionQuery = query
                searchUsers(query: query)
            } else {
                showMentionSuggestions = false
            }
        } else {
            showMentionSuggestions = false
        }
    }
    
    private func searchHashtags(query: String) {
        hashtagSearchTask?.cancel()
        
        hashtagSearchTask = Task {
            do {
                let hashtags = try await grpcClient.searchHashtags(query: query, page: 0, pageSize: 10)
                
                if !Task.isCancelled {
                    await MainActor.run {
                        hashtagSuggestions = hashtags
                        showHashtagSuggestions = true
                    }
                }
            } catch {
                // Handle error silently for hashtag search
            }
        }
    }
    
    private func searchUsers(query: String) {
        mentionSearchTask?.cancel()
        
        mentionSearchTask = Task {
            do {
                let users = try await grpcClient.searchUsers(query: query, page: 0, pageSize: 10)
                
                if !Task.isCancelled {
                    await MainActor.run {
                        mentionSuggestions = users
                        showMentionSuggestions = true
                    }
                }
            } catch {
                // Handle error silently for user search
            }
        }
    }
    
    private func loadDraftIfNeeded() {
        // Load draft if editing an existing draft
        // This would be implemented based on navigation context
    }
    
    private func saveDraftToStorage(_ draft: NoteDraft) {
        // Save draft to UserDefaults or Core Data
        // This would be implemented for draft persistence
    }
}

// MARK: - Data Models
struct MediaItem: Identifiable {
    let id = UUID()
    let mediaId: String
    let url: String
    let type: MediaType
    let altText: String?
    let localImage: UIImage?
    
    init(mediaId: String = "", url: String = "", type: MediaType = .MEDIA_TYPE_IMAGE, altText: String? = nil, localImage: UIImage? = nil) {
        self.mediaId = mediaId
        self.url = url
        self.type = type
        self.altText = altText
        self.localImage = localImage
    }
}

struct NoteDraft: Identifiable, Codable {
    let id: String
    let content: String
    let media: [MediaItem]
    let replyToNote: Note?
    let quoteNote: Note?
    let hashtags: [String]
    let mentions: [String]
    let scheduledDate: Date?
    let createdAt: Date
}

// MARK: - Media Types
enum MediaType: String, CaseIterable {
    case image = "image"
    case video = "video"
    case gif = "gif"
    case audio = "audio"
    
    var icon: String {
        switch self {
        case .image: return "photo"
        case .video: return "video"
        case .gif: return "play.rectangle"
        case .audio: return "waveform"
        }
    }
    
    var grpcType: xyz.sonet.app.grpc.proto.MediaType {
        switch self {
        case .image: return .MEDIA_TYPE_IMAGE
        case .video: return .MEDIA_TYPE_VIDEO
        case .gif: return .MEDIA_TYPE_GIF
        case .audio: return .MEDIA_TYPE_AUDIO
        }
    }
}