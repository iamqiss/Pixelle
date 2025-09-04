import Foundation
import Combine
import SwiftUI

@MainActor
class SearchViewModel: ObservableObject {
    
    // MARK: - Published Properties
    @Published var searchQuery = ""
    @Published var searchResults: SearchResults = SearchResults()
    @Published var trendingTopics: [TrendingTopic] = []
    @Published var recentSearches: [String] = []
    @Published var isSearching = false
    @Published var searchError: String?
    @Published var selectedSearchType: SearchType = .all
    
    // MARK: - Private Properties
    private let grpcClient: SonetGRPCClient
    private var cancellables = Set<AnyCancellable>()
    private var searchTask: Task<Void, Never>?
    
    // MARK: - Initialization
    init(grpcClient: SonetGRPCClient) {
        self.grpcClient = grpcClient
        loadRecentSearches()
        loadTrendingTopics()
        setupSearchDebouncing()
    }
    
    // MARK: - Search Types
    enum SearchType: String, CaseIterable {
        case all = "All"
        case users = "Users"
        case notes = "Notes"
        case hashtags = "Hashtags"
        
        var icon: String {
            switch self {
            case .all: return "magnifyingglass"
            case .users: return "person.2"
            case .notes: return "text.bubble"
            case .hashtags: return "number"
            }
        }
    }
    
    // MARK: - Search Results
    struct SearchResults {
        var users: [UserProfile] = []
        var notes: [Note] = []
        var hashtags: [String] = []
        var hasMoreResults = false
    }
    
    // MARK: - Trending Topic
    struct TrendingTopic: Identifiable {
        let id = UUID()
        let hashtag: String
        let postCount: Int
        let isTrending: Bool
        let category: String?
    }
    
    // MARK: - Public Methods
    func performSearch() {
        guard !searchQuery.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            clearSearchResults()
            return
        }
        
        // Cancel previous search
        searchTask?.cancel()
        
        searchTask = Task {
            await performSearchTask()
        }
    }
    
    func clearSearchResults() {
        searchResults = SearchResults()
        searchError = nil
    }
    
    func addToRecentSearches(_ query: String) {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedQuery.isEmpty else { return }
        
        // Remove if already exists
        recentSearches.removeAll { $0 == trimmedQuery }
        
        // Add to beginning
        recentSearches.insert(trimmedQuery, at: 0)
        
        // Keep only last 10 searches
        if recentSearches.count > 10 {
            recentSearches = Array(recentSearches.prefix(10))
        }
        
        saveRecentSearches()
    }
    
    func removeFromRecentSearches(_ query: String) {
        recentSearches.removeAll { $0 == trimmedQuery }
        saveRecentSearches()
    }
    
    func clearRecentSearches() {
        recentSearches.removeAll()
        saveRecentSearches()
    }
    
    // MARK: - Private Methods
    private func setupSearchDebouncing() {
        $searchQuery
            .debounce(for: .milliseconds(300), scheduler: DispatchQueue.main)
            .removeDuplicates()
            .sink { [weak self] query in
                if !query.isEmpty {
                    self?.performSearch()
                } else {
                    self?.clearSearchResults()
                }
            }
            .store(in: &cancellables)
    }
    
    private func performSearchTask() async {
        guard !searchQuery.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        
        await MainActor.run {
            isSearching = true
            searchError = nil
        }
        
        do {
            let query = searchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
            
            // Add to recent searches
            addToRecentSearches(query)
            
            // Perform search based on selected type
            switch selectedSearchType {
            case .all:
                await performComprehensiveSearch(query: query)
            case .users:
                await performUserSearch(query: query)
            case .notes:
                await performNoteSearch(query: query)
            case .hashtags:
                await performHashtagSearch(query: query)
            }
            
        } catch {
            await MainActor.run {
                searchError = error.localizedDescription
            }
        }
        
        await MainActor.run {
            isSearching = false
        }
    }
    
    private func performComprehensiveSearch(query: String) async {
        do {
            async let usersTask = grpcClient.searchUsers(query: query, page: 0, pageSize: 10)
            async let notesTask = grpcClient.searchNotes(query: query, page: 0, pageSize: 10)
            
            let (users, notes) = try await (usersTask, notesTask)
            
            await MainActor.run {
                searchResults.users = users
                searchResults.notes = notes
                searchResults.hashtags = extractHashtags(from: query)
                searchResults.hasMoreResults = users.count >= 10 || notes.count >= 10
            }
            
        } catch {
            await MainActor.run {
                searchError = "Search failed: \(error.localizedDescription)"
            }
        }
    }
    
    private func performUserSearch(query: String) async {
        do {
            let users = try await grpcClient.searchUsers(query: query, page: 0, pageSize: 20)
            
            await MainActor.run {
                searchResults.users = users
                searchResults.notes = []
                searchResults.hashtags = []
                searchResults.hasMoreResults = users.count >= 20
            }
            
        } catch {
            await MainActor.run {
                searchError = "User search failed: \(error.localizedDescription)"
            }
        }
    }
    
    private func performNoteSearch(query: String) async {
        do {
            let notes = try await grpcClient.searchNotes(query: query, page: 0, pageSize: 20)
            
            await MainActor.run {
                searchResults.users = []
                searchResults.notes = notes
                searchResults.hashtags = extractHashtags(from: query)
                searchResults.hasMoreResults = notes.count >= 20
            }
            
        } catch {
            await MainActor.run {
                searchError = "Note search failed: \(error.localizedDescription)"
            }
        }
    }
    
    private func performHashtagSearch(query: String) async {
        // For hashtag search, we'll search notes and extract hashtags
        do {
            let notes = try await grpcClient.searchNotes(query: query, page: 0, pageSize: 50)
            let hashtags = extractHashtagsFromNotes(notes)
            
            await MainActor.run {
                searchResults.users = []
                searchResults.notes = []
                searchResults.hashtags = hashtags
                searchResults.hasMoreResults = false
            }
            
        } catch {
            await MainActor.run {
                searchError = "Hashtag search failed: \(error.localizedDescription)"
            }
        }
    }
    
    private func extractHashtags(from query: String) -> [String] {
        let hashtagPattern = #"#(\w+)"#
        let regex = try? NSRegularExpression(pattern: hashtagPattern)
        let range = NSRange(query.startIndex..<query.endIndex, in: query)
        
        guard let matches = regex?.matches(in: query, range: range) else { return [] }
        
        return matches.compactMap { match in
            guard let range = Range(match.range(at: 1), in: query) else { return nil }
            return String(query[range])
        }
    }
    
    private func extractHashtagsFromNotes(_ notes: [Note]) -> [String] {
        var hashtags: Set<String> = []
        
        for note in notes {
            hashtags.formUnion(note.hashtags)
        }
        
        return Array(hashtags).sorted()
    }
    
    private func loadTrendingTopics() {
        // Load trending topics from gRPC or generate mock data
        trendingTopics = [
            TrendingTopic(hashtag: "#TechNews", postCount: 15420, isTrending: true, category: "Technology"),
            TrendingTopic(hashtag: "#AI", postCount: 12850, isTrending: true, category: "Technology"),
            TrendingTopic(hashtag: "#ClimateAction", postCount: 9870, isTrending: false, category: "Environment"),
            TrendingTopic(hashtag: "#SpaceX", postCount: 7650, isTrending: true, category: "Space"),
            TrendingTopic(hashtag: "#Gaming", postCount: 6540, isTrending: false, category: "Entertainment"),
            TrendingTopic(hashtag: "#Crypto", postCount: 5430, isTrending: false, category: "Finance"),
            TrendingTopic(hashtag: "#Health", postCount: 4320, isTrending: false, category: "Health"),
            TrendingTopic(hashtag: "#Travel", postCount: 3210, isTrending: false, category: "Lifestyle")
        ]
    }
    
    private func loadRecentSearches() {
        recentSearches = UserDefaults.standard.stringArray(forKey: "recentSearches") ?? []
    }
    
    private func saveRecentSearches() {
        UserDefaults.standard.set(recentSearches, forKey: "recentSearches")
    }
}