package xyz.sonet.app.viewmodels

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import xyz.sonet.app.grpc.SonetGRPCClient
import xyz.sonet.app.grpc.SonetConfiguration
import xyz.sonet.app.grpc.proto.*
import java.util.*

class SearchViewModel(application: Application) : AndroidViewModel(application) {
    
    // MARK: - State Flows
    private val _searchQuery = MutableStateFlow("")
    val searchQuery: StateFlow<String> = _searchQuery.asStateFlow()
    
    private val _searchResults = MutableStateFlow(SearchResults())
    val searchResults: StateFlow<SearchResults> = _searchResults.asStateFlow()
    
    private val _trendingTopics = MutableStateFlow<List<TrendingTopic>>(emptyList())
    val trendingTopics: StateFlow<List<TrendingTopic>> = _trendingTopics.asStateFlow()
    
    private val _recentSearches = MutableStateFlow<List<String>>(emptyList())
    val recentSearches: StateFlow<List<String>> = _recentSearches.asStateFlow()
    
    private val _isSearching = MutableStateFlow(false)
    val isSearching: StateFlow<Boolean> = _isSearching.asStateFlow()
    
    private val _searchError = MutableStateFlow<String?>(null)
    val searchError: StateFlow<String?> = _searchError.asStateFlow()
    
    private val _selectedSearchType = MutableStateFlow(SearchType.ALL)
    val selectedSearchType: StateFlow<SearchType> = _selectedSearchType.asStateFlow()
    
    // MARK: - Private Properties
    private val grpcClient = SonetGRPCClient(application, SonetConfiguration.development)
    private var searchJob: kotlinx.coroutines.Job? = null
    
    // MARK: - Initialization
    init {
        loadRecentSearches()
        loadTrendingTopics()
    }
    
    // MARK: - Search Types
    enum class SearchType(val title: String, val icon: String) {
        ALL("All", "magnifyingglass"),
        USERS("Users", "person.2"),
        NOTES("Notes", "text.bubble"),
        HASHTAGS("Hashtags", "number")
    }
    
    // MARK: - Data Classes
    data class SearchResults(
        val users: List<UserProfile> = emptyList(),
        val notes: List<Note> = emptyList(),
        val hashtags: List<String> = emptyList(),
        val hasMoreResults: Boolean = false
    )
    
    data class TrendingTopic(
        val id: String = UUID.randomUUID().toString(),
        val hashtag: String,
        val postCount: Int,
        val isTrending: Boolean,
        val category: String?
    )
    
    // MARK: - Public Methods
    fun updateSearchQuery(query: String) {
        _searchQuery.value = query
        performSearch()
    }
    
    fun setSearchType(searchType: SearchType) {
        _selectedSearchType.value = searchType
        if (_searchQuery.value.isNotEmpty()) {
            performSearch()
        }
    }
    
    fun performSearch() {
        val query = _searchQuery.value.trim()
        if (query.isEmpty) {
            clearSearchResults()
            return
        }
        
        // Cancel previous search
        searchJob?.cancel()
        
        searchJob = viewModelScope.launch {
            performSearchTask(query)
        }
    }
    
    fun clearSearchResults() {
        _searchResults.value = SearchResults()
        _searchError.value = null
    }
    
    fun addToRecentSearches(query: String) {
        val trimmedQuery = query.trim()
        if (trimmedQuery.isEmpty) return
        
        val currentSearches = _recentSearches.value.toMutableList()
        
        // Remove if already exists
        currentSearches.remove(trimmedQuery)
        
        // Add to beginning
        currentSearches.add(0, trimmedQuery)
        
        // Keep only last 10 searches
        if (currentSearches.size > 10) {
            currentSearches.subList(10, currentSearches.size).clear()
        }
        
        _recentSearches.value = currentSearches
        saveRecentSearches()
    }
    
    fun removeFromRecentSearches(query: String) {
        val currentSearches = _recentSearches.value.toMutableList()
        currentSearches.remove(query)
        _recentSearches.value = currentSearches
        saveRecentSearches()
    }
    
    fun clearRecentSearches() {
        _recentSearches.value = emptyList()
        saveRecentSearches()
    }
    
    // MARK: - Private Methods
    private suspend fun performSearchTask(query: String) {
        _isSearching.value = true
        _searchError.value = null
        
        try {
            // Add to recent searches
            addToRecentSearches(query)
            
            // Perform search based on selected type
            when (_selectedSearchType.value) {
                SearchType.ALL -> performComprehensiveSearch(query)
                SearchType.USERS -> performUserSearch(query)
                SearchType.NOTES -> performNoteSearch(query)
                SearchType.HASHTAGS -> performHashtagSearch(query)
            }
            
        } catch (error: Exception) {
            _searchError.value = error.message ?: "Search failed"
        } finally {
            _isSearching.value = false
        }
    }
    
    private suspend fun performComprehensiveSearch(query: String) {
        try {
            val users = grpcClient.searchUsers(query, 0, 10)
            val notes = grpcClient.searchNotes(query, 0, 10)
            val hashtags = extractHashtags(query)
            
            _searchResults.value = SearchResults(
                users = users,
                notes = notes,
                hashtags = hashtags,
                hasMoreResults = users.size >= 10 || notes.size >= 10
            )
            
        } catch (error: Exception) {
            _searchError.value = "Search failed: ${error.message}"
        }
    }
    
    private suspend fun performUserSearch(query: String) {
        try {
            val users = grpcClient.searchUsers(query, 0, 20)
            
            _searchResults.value = SearchResults(
                users = users,
                notes = emptyList(),
                hashtags = emptyList(),
                hasMoreResults = users.size >= 20
            )
            
        } catch (error: Exception) {
            _searchError.value = "User search failed: ${error.message}"
        }
    }
    
    private suspend fun performNoteSearch(query: String) {
        try {
            val notes = grpcClient.searchNotes(query, 0, 20)
            val hashtags = extractHashtags(query)
            
            _searchResults.value = SearchResults(
                users = emptyList(),
                notes = notes,
                hashtags = hashtags,
                hasMoreResults = notes.size >= 20
            )
            
        } catch (error: Exception) {
            _searchError.value = "Note search failed: ${error.message}"
        }
    }
    
    private suspend fun performHashtagSearch(query: String) {
        try {
            val notes = grpcClient.searchNotes(query, 0, 50)
            val hashtags = extractHashtagsFromNotes(notes)
            
            _searchResults.value = SearchResults(
                users = emptyList(),
                notes = emptyList(),
                hashtags = hashtags,
                hasMoreResults = false
            )
            
        } catch (error: Exception) {
            _searchError.value = "Hashtag search failed: ${error.message}"
        }
    }
    
    private fun extractHashtags(query: String): List<String> {
        val hashtagPattern = Regex("#(\\w+)")
        return hashtagPattern.findAll(query)
            .map { it.groupValues[1] }
            .toList()
    }
    
    private fun extractHashtagsFromNotes(notes: List<Note>): List<String> {
        val hashtags = mutableSetOf<String>()
        notes.forEach { note ->
            hashtags.addAll(note.hashtagsList)
        }
        return hashtags.sorted()
    }
    
    private fun loadTrendingTopics() {
        // Load trending topics from gRPC or generate mock data
        _trendingTopics.value = listOf(
            TrendingTopic(
                hashtag = "#TechNews",
                postCount = 15420,
                isTrending = true,
                category = "Technology"
            ),
            TrendingTopic(
                hashtag = "#AI",
                postCount = 12850,
                isTrending = true,
                category = "Technology"
            ),
            TrendingTopic(
                hashtag = "#ClimateAction",
                postCount = 9870,
                isTrending = false,
                category = "Environment"
            ),
            TrendingTopic(
                hashtag = "#SpaceX",
                postCount = 7650,
                isTrending = true,
                category = "Space"
            ),
            TrendingTopic(
                hashtag = "#Gaming",
                postCount = 6540,
                isTrending = false,
                category = "Entertainment"
            ),
            TrendingTopic(
                hashtag = "#Crypto",
                postCount = 5430,
                isTrending = false,
                category = "Finance"
            ),
            TrendingTopic(
                hashtag = "#Health",
                postCount = 4320,
                isTrending = false,
                category = "Health"
            ),
            TrendingTopic(
                hashtag = "#Travel",
                postCount = 3210,
                isTrending = false,
                category = "Lifestyle"
            )
        )
    }
    
    private fun loadRecentSearches() {
        // Load from SharedPreferences
        val sharedPrefs = getApplication<Application>().getSharedPreferences("search_prefs", 0)
        val searches = sharedPrefs.getStringSet("recent_searches", emptySet())?.toList() ?: emptyList()
        _recentSearches.value = searches
    }
    
    private fun saveRecentSearches() {
        val sharedPrefs = getApplication<Application>().getSharedPreferences("search_prefs", 0)
        val searches = _recentSearches.value.toSet()
        sharedPrefs.edit().putStringSet("recent_searches", searches).apply()
    }
}