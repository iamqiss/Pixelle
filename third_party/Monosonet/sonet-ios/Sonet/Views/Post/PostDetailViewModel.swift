import Foundation
import Combine

@MainActor
class PostDetailViewModel: ObservableObject {
    
    private let grpcClient = SonetGRPCClient.shared
    
    // State
    @Published var post: Note?
    @Published var thread: [Note] = []
    @Published var isLoading = false
    @Published var error: String?
    @Published var replyToNote: Note?
    @Published var isCreatingReply = false
    
    // Actions
    func loadPost(noteId: String) {
        Task {
            do {
                isLoading = true
                error = nil
                
                let note = try await grpcClient.getNote(noteId: noteId)
                post = note
            } catch {
                self.error = "Failed to load post: \(error.localizedDescription)"
            }
            isLoading = false
        }
    }
    
    func loadThread(noteId: String) {
        Task {
            do {
                error = nil
                
                let request = GetThreadRequest(
                    noteId: noteId,
                    userId: "", // Will be set by the server based on auth
                    pagination: PaginationRequest(page: 0, pageSize: 50),
                    sortOrder: .chronological
                )
                
                let response = try await grpcClient.getThread(request: request)
                if response.success {
                    thread = response.threadNotes
                } else {
                    error = response.errorMessage
                }
            } catch {
                self.error = "Failed to load thread: \(error.localizedDescription)"
            }
        }
    }
    
    func setReplyToNote(_ note: Note?) {
        replyToNote = note
    }
    
    func createReply(content: String) {
        let replyTo = replyToNote ?? post
        guard let replyTo = replyTo else { return }
        
        Task {
            do {
                isCreatingReply = true
                error = nil
                
                let request = CreateReplyRequest(
                    parentNoteId: replyTo.noteId,
                    userId: "", // Will be set by the server based on auth
                    content: content,
                    attachments: [],
                    isSensitive: false
                )
                
                let response = try await grpcClient.createReply(request: request)
                if response.success {
                    // Clear reply input and refresh thread
                    replyToNote = nil
                    await loadThread(noteId: replyTo.noteId)
                } else {
                    error = response.errorMessage
                }
            } catch {
                self.error = "Failed to create reply: \(error.localizedDescription)"
            }
            isCreatingReply = false
        }
    }
    
    func toggleLike(noteId: String) {
        Task {
            do {
                let request = LikeNoteRequest(
                    noteId: noteId,
                    userId: "" // Will be set by the server based on auth
                )
                
                let response = try await grpcClient.likeNote(request: request)
                if response.success {
                    // Update local state
                    if let currentPost = post, currentPost.noteId == noteId {
                        var updatedPost = currentPost
                        updatedPost.likeCount += (currentPost.userState?.isLiked == true ? -1 : 1)
                        post = updatedPost
                    }
                    
                    thread = thread.map { note in
                        if note.noteId == noteId {
                            var updatedNote = note
                            updatedNote.likeCount += (note.userState?.isLiked == true ? -1 : 1)
                            return updatedNote
                        }
                        return note
                    }
                }
            } catch {
                self.error = "Failed to toggle like: \(error.localizedDescription)"
            }
        }
    }
    
    func toggleRepost(noteId: String) {
        Task {
            do {
                let request = RenoteNoteRequest(
                    noteId: noteId,
                    userId: "" // Will be set by the server based on auth
                )
                
                let response = try await grpcClient.renoteNote(request: request)
                if response.success {
                    // Update local state
                    if let currentPost = post, currentPost.noteId == noteId {
                        var updatedPost = currentPost
                        updatedPost.renoteCount += (currentPost.userState?.isReposted == true ? -1 : 1)
                        post = updatedPost
                    }
                    
                    thread = thread.map { note in
                        if note.noteId == noteId {
                            var updatedNote = note
                            updatedNote.renoteCount += (note.userState?.isReposted == true ? -1 : 1)
                            return updatedNote
                        }
                        return note
                    }
                }
            } catch {
                self.error = "Failed to toggle repost: \(error.localizedDescription)"
            }
        }
    }
    
    func clearError() {
        error = nil
    }
}