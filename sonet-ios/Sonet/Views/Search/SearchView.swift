import SwiftUI

struct SearchView: View {
    @StateObject private var viewModel: SearchViewModel
    @Environment(\.colorScheme) var colorScheme
    @FocusState private var isSearchFocused: Bool
    
    init(grpcClient: SonetGRPCClient) {
        _viewModel = StateObject(wrappedValue: SearchViewModel(grpcClient: grpcClient))
    }
    
    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                // Search Header
                searchHeader
                
                // Search Type Filter
                searchTypeFilter
                
                // Content
                if viewModel.searchQuery.isEmpty {
                    discoveryContent
                } else {
                    searchResultsContent
                }
            }
            .navigationBarHidden(true)
            .background(Color(.systemBackground))
        }
    }
    
    // MARK: - Search Header
    private var searchHeader: some View {
        VStack(spacing: 16) {
            HStack {
                // Search Bar
                HStack(spacing: 12) {
                    IconView(AppIcons.search, size: 16, color: .secondary)
                    
                    TextField("Search Sonet", text: $viewModel.searchQuery)
                        .focused($isSearchFocused)
                        .textFieldStyle(PlainTextFieldStyle())
                        .font(.system(size: 17))
                    
                    if !viewModel.searchQuery.isEmpty {
                        Button(action: {
                            viewModel.searchQuery = ""
                            isSearchFocused = false
                        }) {
                            IconView(AppIcons.closeCircle, size: 16, color: .secondary)
                        }
                    }
                }
                .padding(.horizontal, 16)
                .padding(.vertical, 12)
                .background(
                    RoundedRectangle(cornerRadius: 25)
                        .fill(Color(.systemGray6))
                )
                .overlay(
                    RoundedRectangle(cornerRadius: 25)
                        .stroke(isSearchFocused ? Color.accentColor : Color.clear, lineWidth: 2)
                )
                
                // Cancel Button
                if isSearchFocused {
                    Button("Cancel") {
                        viewModel.searchQuery = ""
                        isSearchFocused = false
                    }
                    .foregroundColor(.accentColor)
                    .font(.system(size: 17, weight: .medium))
                    .transition(.move(edge: .trailing).combined(with: .opacity))
                }
            }
            .padding(.horizontal, 16)
            .animation(.easeInOut(duration: 0.3), value: isSearchFocused)
        }
        .padding(.top, 8)
        .background(Color(.systemBackground))
    }
    
    // MARK: - Search Type Filter
    private var searchTypeFilter: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 16) {
                ForEach(SearchViewModel.SearchType.allCases, id: \.self) { searchType in
                    Button(action: {
                        withAnimation(.easeInOut(duration: 0.3)) {
                            viewModel.selectedSearchType = searchType
                        }
                    }) {
                        HStack(spacing: 8) {
                            IconView(searchType.icon, size: 14)
                            Text(searchType.rawValue)
                                .font(.system(size: 15, weight: .medium))
                        }
                        .foregroundColor(viewModel.selectedSearchType == searchType ? .white : .primary)
                        .padding(.horizontal, 16)
                        .padding(.vertical, 8)
                        .background(
                            RoundedRectangle(cornerRadius: 20)
                                .fill(viewModel.selectedSearchType == searchType ? Color.accentColor : Color(.systemGray6))
                        )
                    }
                    .buttonStyle(PlainButtonStyle())
                }
            }
            .padding(.horizontal, 16)
        }
        .padding(.vertical, 8)
        .background(Color(.systemBackground))
    }
    
    // MARK: - Discovery Content
    private var discoveryContent: some View {
        ScrollView {
            LazyVStack(spacing: 24) {
                // Trending Topics
                trendingTopicsSection
                
                // Recent Searches
                if !viewModel.recentSearches.isEmpty {
                    recentSearchesSection
                }
                
                // Suggested Searches
                suggestedSearchesSection
            }
            .padding(.top, 16)
        }
        .background(Color(.systemBackground))
    }
    
    // MARK: - Trending Topics Section
    private var trendingTopicsSection: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Text("Trending")
                    .font(.system(size: 22, weight: .bold))
                    .foregroundColor(.primary)
                
                Spacer()
                
                Button("See all") {
                    // Navigate to trending page
                }
                .foregroundColor(.accentColor)
                .font(.system(size: 15, weight: .medium))
            }
            .padding(.horizontal, 16)
            
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 16) {
                    ForEach(viewModel.trendingTopics) { topic in
                        TrendingTopicCard(topic: topic)
                    }
                }
                .padding(.horizontal, 16)
            }
        }
    }
    
    // MARK: - Recent Searches Section
    private var recentSearchesSection: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Text("Recent Searches")
                    .font(.system(size: 22, weight: .bold))
                    .foregroundColor(.primary)
                
                Spacer()
                
                Button("Clear") {
                    viewModel.clearRecentSearches()
                }
                .foregroundColor(.red)
                .font(.system(size: 15, weight: .medium))
            }
            .padding(.horizontal, 16)
            
            LazyVStack(spacing: 0) {
                ForEach(viewModel.recentSearches, id: \.self) { search in
                    RecentSearchRow(
                        search: search,
                        onTap: {
                            viewModel.searchQuery = search
                        },
                        onRemove: {
                            viewModel.removeFromRecentSearches(search)
                        }
                    )
                }
            }
        }
    }
    
    // MARK: - Suggested Searches Section
    private var suggestedSearchesSection: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Suggested")
                .font(.system(size: 22, weight: .bold))
                .foregroundColor(.primary)
                .padding(.horizontal, 16)
            
            LazyVGrid(columns: [
                GridItem(.flexible()),
                GridItem(.flexible())
            ], spacing: 12) {
                ForEach(suggestedSearches, id: \.self) { suggestion in
                    SuggestedSearchCard(suggestion: suggestion) {
                        viewModel.searchQuery = suggestion
                    }
                }
            }
            .padding(.horizontal, 16)
        }
    }
    
    // MARK: - Search Results Content
    private var searchResultsContent: some View {
        Group {
            if viewModel.isSearching {
                loadingView
            } else if let error = viewModel.searchError {
                errorView(error: error)
            } else if viewModel.searchResults.users.isEmpty && 
                      viewModel.searchResults.notes.isEmpty && 
                      viewModel.searchResults.hashtags.isEmpty {
                noResultsView
            } else {
                searchResultsList
            }
        }
    }
    
    // MARK: - Loading View
    private var loadingView: some View {
        VStack(spacing: 16) {
            ProgressView()
                .scaleEffect(1.2)
            
            Text("Searching...")
                .font(.system(size: 16, weight: .medium))
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(.systemBackground))
    }
    
    // MARK: - Error View
    private func errorView(error: String) -> some View {
        VStack(spacing: 16) {
            IconView(AppIcons.warning, size: 48, color: .orange)
            
            Text("Search Error")
                .font(.system(size: 20, weight: .bold))
                .foregroundColor(.primary)
            
            Text(error)
                .font(.system(size: 16))
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
            
            Button("Try Again") {
                viewModel.performSearch()
            }
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
    
    // MARK: - No Results View
    private var noResultsView: some View {
        VStack(spacing: 16) {
            IconView(AppIcons.search, size: 48, color: .secondary)
            
            Text("No Results Found")
                .font(.system(size: 20, weight: .bold))
                .foregroundColor(.primary)
            
            Text("Try adjusting your search terms or browse trending topics")
                .font(.system(size: 16))
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, 32)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(.systemBackground))
    }
    
    // MARK: - Search Results List
    private var searchResultsList: some View {
        ScrollView {
            LazyVStack(spacing: 16) {
                // Users
                if !viewModel.searchResults.users.isEmpty {
                    usersSection
                }
                
                // Notes
                if !viewModel.searchResults.notes.isEmpty {
                    notesSection
                }
                
                // Hashtags
                if !viewModel.searchResults.hashtags.isEmpty {
                    hashtagsSection
                }
                
                // Load More
                if viewModel.searchResults.hasMoreResults {
                    loadMoreButton
                }
            }
            .padding(.top, 16)
        }
        .background(Color(.systemBackground))
    }
    
    // MARK: - Users Section
    private var usersSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Users")
                .font(.system(size: 20, weight: .bold))
                .foregroundColor(.primary)
                .padding(.horizontal, 16)
            
            LazyVStack(spacing: 0) {
                ForEach(viewModel.searchResults.users, id: \.userId) { user in
                    UserSearchRow(user: user)
                }
            }
        }
    }
    
    // MARK: - Notes Section
    private var notesSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Notes")
                .font(.system(size: 20, weight: .bold))
                .foregroundColor(.primary)
                .padding(.horizontal, 16)
            
            LazyVStack(spacing: 0) {
                ForEach(viewModel.searchResults.notes, id: \.noteId) { note in
                    NoteSearchRow(note: note)
                }
            }
        }
    }
    
    // MARK: - Hashtags Section
    private var hashtagsSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Hashtags")
                .font(.system(size: 20, weight: .bold))
                .foregroundColor(.primary)
                .padding(.horizontal, 16)
            
            LazyVStack(spacing: 0) {
                ForEach(viewModel.searchResults.hashtags, id: \.self) { hashtag in
                    HashtagSearchRow(hashtag: hashtag)
                }
            }
        }
    }
    
    // MARK: - Load More Button
    private var loadMoreButton: some View {
        Button(action: {
            // Load more results
        }) {
            HStack {
                Text("Load More Results")
                    .font(.system(size: 16, weight: .medium))
                    .foregroundColor(.accentColor)
                
                IconView(AppIcons.chevronDown, size: 14, color: .accentColor)
            }
            .padding(.vertical, 16)
            .frame(maxWidth: .infinity)
        }
        .buttonStyle(PlainButtonStyle())
        .background(Color(.systemBackground))
    }
    
    // MARK: - Suggested Searches
    private var suggestedSearches: [String] {
        [
            "Technology",
            "Science",
            "Art",
            "Music",
            "Sports",
            "Travel",
            "Food",
            "Fashion"
        ]
    }
}

// MARK: - Preview
struct SearchView_Previews: PreviewProvider {
    static var previews: some View {
        SearchView(grpcClient: SonetGRPCClient(configuration: .development))
    }
}