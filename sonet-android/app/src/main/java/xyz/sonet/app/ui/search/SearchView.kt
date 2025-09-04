package xyz.sonet.app.ui.search

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.animateFloatAsState
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.animation.slideInVertically
import androidx.compose.animation.slideOutVertically
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.BasicTextField
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.scale
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.focus.onFocusChanged
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.viewmodel.compose.viewModel
import xyz.sonet.app.grpc.proto.*
import xyz.sonet.app.viewmodels.SearchViewModel

@Composable
fun SearchView(
    modifier: Modifier = Modifier,
    viewModel: SearchViewModel = viewModel()
) {
    val context = LocalContext.current
    val searchQuery by viewModel.searchQuery.collectAsState()
    val searchResults by viewModel.searchResults.collectAsState()
    val trendingTopics by viewModel.trendingTopics.collectAsState()
    val recentSearches by viewModel.recentSearches.collectAsState()
    val isSearching by viewModel.isSearching.collectAsState()
    val searchError by viewModel.searchError.collectAsState()
    val selectedSearchType by viewModel.selectedSearchType.collectAsState()
    
    var isSearchFocused by remember { mutableStateOf(false) }
    val focusRequester = remember { FocusRequester() }
    
    Column(
        modifier = modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.surface)
    ) {
        // Search Header
        SearchHeader(
            searchQuery = searchQuery,
            onQueryChange = { viewModel.updateSearchQuery(it) },
            isFocused = isSearchFocused,
            onFocusChange = { isSearchFocused = it },
            focusRequester = focusRequester,
            onCancel = {
                viewModel.updateSearchQuery("")
                isSearchFocused = false
            }
        )
        
        // Search Type Filter
        SearchTypeFilter(
            selectedType = selectedSearchType,
            onTypeSelected = { viewModel.setSearchType(it) }
        )
        
        // Content
        if (searchQuery.isEmpty()) {
            DiscoveryContent(
                trendingTopics = trendingTopics,
                recentSearches = recentSearches,
                onSearchSuggestion = { viewModel.updateSearchQuery(it) },
                onClearRecentSearches = { viewModel.clearRecentSearches() },
                onRemoveRecentSearch = { viewModel.removeFromRecentSearches(it) }
            )
        } else {
            SearchResultsContent(
                searchResults = searchResults,
                isSearching = isSearching,
                error = searchError,
                onRetry = { viewModel.performSearch() }
            )
        }
    }
}

@Composable
private fun SearchHeader(
    searchQuery: String,
    onQueryChange: (String) -> Unit,
    isFocused: Boolean,
    onFocusChange: (Boolean) -> Unit,
    focusRequester: FocusRequester,
    onCancel: () -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .padding(16.dp)
    ) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier.fillMaxWidth()
        ) {
            // Search Bar
            Box(
                modifier = Modifier
                    .weight(1f)
                    .clip(RoundedCornerShape(25.dp))
                    .background(
                        if (isFocused) MaterialTheme.colorScheme.surface
                        else MaterialTheme.colorScheme.surfaceVariant
                    )
                    .border(
                        width = if (isFocused) 2.dp else 0.dp,
                        color = if (isFocused) MaterialTheme.colorScheme.primary else Color.Transparent,
                        shape = RoundedCornerShape(25.dp)
                    )
            ) {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    modifier = Modifier.padding(horizontal = 16.dp, vertical = 12.dp)
                ) {
                    Icon(
                        imageVector = xyz.sonet.app.ui.AppIcons.Search,
                        contentDescription = "Search",
                        tint = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.size(20.dp)
                    )
                    
                    Spacer(modifier = Modifier.width(12.dp))
                    
                    BasicTextField(
                        value = searchQuery,
                        onValueChange = onQueryChange,
                        textStyle = MaterialTheme.typography.bodyLarge.copy(
                            color = MaterialTheme.colorScheme.onSurface
                        ),
                        modifier = Modifier
                            .weight(1f)
                            .focusRequester(focusRequester)
                            .onFocusChanged { onFocusChange(it.isFocused) },
                        keyboardOptions = KeyboardOptions(imeAction = ImeAction.Search),
                        keyboardActions = KeyboardActions(onSearch = { /* Perform search */ }),
                        singleLine = true
                    )
                    
                    if (searchQuery.isNotEmpty()) {
                        IconButton(
                            onClick = { onQueryChange("") }
                        ) {
                            Icon(
                                imageVector = xyz.sonet.app.ui.AppIcons.Close,
                                contentDescription = "Clear",
                                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                                modifier = Modifier.size(18.dp)
                            )
                        }
                    }
                }
            }
            
            // Cancel Button
            AnimatedVisibility(
                visible = isFocused,
                enter = slideInVertically(
                    animationSpec = tween(300),
                    initialOffsetY = { -it }
                ) + fadeIn(animationSpec = tween(300)),
                exit = slideOutVertically(
                    animationSpec = tween(300),
                    targetOffsetY = { -it }
                ) + fadeOut(animationSpec = tween(300))
            ) {
                TextButton(
                    onClick = onCancel,
                    modifier = Modifier.padding(start = 8.dp)
                ) {
                    Text(
                        text = "Cancel",
                        color = MaterialTheme.colorScheme.primary,
                        fontWeight = FontWeight.Medium
                    )
                }
            }
        }
    }
}

@Composable
private fun SearchTypeFilter(
    selectedType: SearchViewModel.SearchType,
    onTypeSelected: (SearchViewModel.SearchType) -> Unit
) {
    LazyRow(
        contentPadding = PaddingValues(horizontal = 16.dp),
        horizontalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        items(SearchViewModel.SearchType.values()) { searchType ->
            SearchTypeChip(
                searchType = searchType,
                isSelected = selectedType == searchType,
                onClick = { onTypeSelected(searchType) }
            )
        }
    }
}

@Composable
private fun SearchTypeChip(
    searchType: SearchViewModel.SearchType,
    isSelected: Boolean,
    onClick: () -> Unit
) {
    val backgroundColor = if (isSelected) {
        MaterialTheme.colorScheme.primary
    } else {
        MaterialTheme.colorScheme.surfaceVariant
    }
    
    val textColor = if (isSelected) {
        MaterialTheme.colorScheme.onPrimary
    } else {
        MaterialTheme.colorScheme.onSurfaceVariant
    }
    
    Surface(
        onClick = onClick,
        color = backgroundColor,
        shape = RoundedCornerShape(20.dp),
        modifier = Modifier.padding(vertical = 4.dp)
    ) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier.padding(horizontal = 16.dp, vertical = 8.dp)
        ) {
            Icon(
                imageVector = getIconForSearchType(searchType.icon),
                contentDescription = null,
                tint = textColor,
                modifier = Modifier.size(16.dp)
            )
            
            Spacer(modifier = Modifier.width(8.dp))
            
            Text(
                text = searchType.title,
                color = textColor,
                fontSize = 14.sp,
                fontWeight = FontWeight.Medium
            )
        }
    }
}

@Composable
private fun DiscoveryContent(
    trendingTopics: List<SearchViewModel.TrendingTopic>,
    recentSearches: List<String>,
    onSearchSuggestion: (String) -> Unit,
    onClearRecentSearches: () -> Unit,
    onRemoveRecentSearch: (String) -> Unit
) {
    LazyColumn(
        contentPadding = PaddingValues(16.dp),
        verticalArrangement = Arrangement.spacedBy(24.dp)
    ) {
        // Trending Topics
        item {
            TrendingTopicsSection(
                trendingTopics = trendingTopics,
                onTopicClick = { onSearchSuggestion(it.hashtag) }
            )
        }
        
        // Recent Searches
        if (recentSearches.isNotEmpty()) {
            item {
                RecentSearchesSection(
                    recentSearches = recentSearches,
                    onSearchClick = onSearchSuggestion,
                    onRemoveSearch = onRemoveRecentSearch,
                    onClearAll = onClearRecentSearches
                )
            }
        }
        
        // Suggested Searches
        item {
            SuggestedSearchesSection(
                onSuggestionClick = onSearchSuggestion
            )
        }
    }
}

@Composable
private fun TrendingTopicsSection(
    trendingTopics: List<SearchViewModel.TrendingTopic>,
    onTopicClick: (SearchViewModel.TrendingTopic) -> Unit
) {
    Column(verticalArrangement = Arrangement.spacedBy(16.dp)) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Trending",
                fontSize = 22.sp,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.onSurface
            )
            
            TextButton(onClick = { /* Navigate to trending page */ }) {
                Text(
                    text = "See all",
                    color = MaterialTheme.colorScheme.primary,
                    fontWeight = FontWeight.Medium
                )
            }
        }
        
        LazyRow(
            horizontalArrangement = Arrangement.spacedBy(16.dp)
        ) {
            items(trendingTopics) { topic ->
                TrendingTopicCard(
                    topic = topic,
                    onClick = { onTopicClick(topic) }
                )
            }
        }
    }
}

@Composable
private fun TrendingTopicCard(
    topic: SearchViewModel.TrendingTopic,
    onClick: () -> Unit
) {
    var isPressed by remember { mutableStateOf(false) }
    val scale by animateFloatAsState(
        targetValue = if (isPressed) 0.95f else 1f,
        animationSpec = tween(100)
    )
    
    Surface(
        onClick = onClick,
        modifier = Modifier
            .width(200.dp)
            .scale(scale),
        color = MaterialTheme.colorScheme.surfaceVariant,
        shape = RoundedCornerShape(16.dp)
    ) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            // Header
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.Top
            ) {
                Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
                    Text(
                        text = topic.hashtag,
                        fontSize = 16.sp,
                        fontWeight = FontWeight.Bold,
                        color = MaterialTheme.colorScheme.onSurface,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis
                    )
                    
                    topic.category?.let { category ->
                        Text(
                            text = category,
                            fontSize = 12.sp,
                            fontWeight = FontWeight.Medium,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis
                        )
                    }
                }
                
                if (topic.isTrending) {
                    Surface(
                        color = MaterialTheme.colorScheme.tertiary.copy(alpha = 0.1f),
                        shape = RoundedCornerShape(12.dp)
                    ) {
                        Row(
                            verticalAlignment = Alignment.CenterVertically,
                            modifier = Modifier.padding(horizontal = 8.dp, vertical = 4.dp)
                        ) {
                            Icon(
                                imageVector = Icons.Default.LocalFireDepartment,
                                contentDescription = "Trending",
                                tint = MaterialTheme.colorScheme.tertiary,
                                modifier = Modifier.size(12.dp)
                            )
                            
                            Spacer(modifier = Modifier.width(4.dp))
                            
                            Text(
                                text = "Trending",
                                fontSize = 10.sp,
                                fontWeight = FontWeight.Bold,
                                color = MaterialTheme.colorScheme.tertiary
                            )
                        }
                    }
                }
            }
            
            // Post count
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    text = "${topic.postCount.formatNumber()} posts",
                    fontSize = 14.sp,
                    fontWeight = FontWeight.Medium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                
                Icon(
                    imageVector = Icons.Default.ArrowUpward,
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.size(16.dp)
                )
            }
        }
    }
}

@Composable
private fun RecentSearchesSection(
    recentSearches: List<String>,
    onSearchClick: (String) -> Unit,
    onRemoveSearch: (String) -> Unit,
    onClearAll: () -> Unit
) {
    Column(verticalArrangement = Arrangement.spacedBy(16.dp)) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(
                text = "Recent Searches",
                fontSize = 22.sp,
                fontWeight = FontWeight.Bold,
                color = MaterialTheme.colorScheme.onSurface
            )
            
            TextButton(onClick = onClearAll) {
                Text(
                    text = "Clear",
                    color = MaterialTheme.colorScheme.error,
                    fontWeight = FontWeight.Medium
                )
            }
        }
        
        Column {
            recentSearches.forEach { search ->
                RecentSearchRow(
                    search = search,
                    onClick = { onSearchClick(search) },
                    onRemove = { onRemoveSearch(search) }
                )
            }
        }
    }
}

@Composable
private fun RecentSearchRow(
    search: String,
    onClick: () -> Unit,
    onRemove: () -> Unit
) {
    var isPressed by remember { mutableStateOf(false) }
    var showRemoveButton by remember { mutableStateOf(false) }
    val scale by animateFloatAsState(
        targetValue = if (isPressed) 0.98f else 1f,
        animationSpec = tween(100)
    )
    
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .scale(scale)
            .clickable { onClick() }
            .padding(horizontal = 16.dp, vertical = 12.dp)
    ) {
        Icon(
            imageVector = Icons.Default.Schedule,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(20.dp)
        )
        
        Spacer(modifier = Modifier.width(12.dp))
        
        Text(
            text = search,
            fontSize = 17.sp,
            color = MaterialTheme.colorScheme.onSurface,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
            modifier = Modifier.weight(1f)
        )
        
        AnimatedVisibility(
            visible = showRemoveButton,
            enter = fadeIn() + slideInVertically(),
            exit = fadeOut() + slideOutVertically()
        ) {
            IconButton(onClick = onRemove) {
                Icon(
                    imageVector = xyz.sonet.app.ui.AppIcons.Close,
                    contentDescription = "Remove",
                    tint = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
    }
}

@Composable
private fun SuggestedSearchesSection(
    onSuggestionClick: (String) -> Unit
) {
    val suggestions = listOf(
        "Technology", "Science", "Art", "Music",
        "Sports", "Travel", "Food", "Fashion"
    )
    
    Column(verticalArrangement = Arrangement.spacedBy(16.dp)) {
        Text(
            text = "Suggested",
            fontSize = 22.sp,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colorScheme.onSurface
        )
        
        LazyVerticalGrid(
            columns = GridCells.Fixed(2),
            horizontalArrangement = Arrangement.spacedBy(12.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            items(suggestions) { suggestion ->
                SuggestedSearchCard(
                    suggestion = suggestion,
                    onClick = { onSuggestionClick(suggestion) }
                )
            }
        }
    }
}

@Composable
private fun SuggestedSearchCard(
    suggestion: String,
    onClick: () -> Unit
) {
    var isPressed by remember { mutableStateOf(false) }
    val scale by animateFloatAsState(
        targetValue = if (isPressed) 0.95f else 1f,
        animationSpec = tween(100)
    )
    
    Surface(
        onClick = onClick,
        modifier = Modifier.scale(scale),
        color = MaterialTheme.colorScheme.surfaceVariant,
        shape = RoundedCornerShape(12.dp)
    ) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier.padding(16.dp)
        ) {
            Icon(
                imageVector = getIconForSuggestion(suggestion),
                contentDescription = null,
                tint = MaterialTheme.colorScheme.primary,
                modifier = Modifier.size(20.dp)
            )
            
            Spacer(modifier = Modifier.width(12.dp))
            
            Text(
                text = suggestion,
                fontSize = 16.sp,
                fontWeight = FontWeight.Medium,
                color = MaterialTheme.colorScheme.onSurface,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis,
                modifier = Modifier.weight(1f)
            )
            
            Icon(
                imageVector = Icons.Default.ArrowUpward,
                contentDescription = null,
                tint = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.size(16.dp)
            )
        }
    }
}

@Composable
private fun SearchResultsContent(
    searchResults: SearchViewModel.SearchResults,
    isSearching: Boolean,
    error: String?,
    onRetry: () -> Unit
) {
    when {
        isSearching -> LoadingView()
        error != null -> ErrorView(error = error, onRetry = onRetry)
        searchResults.users.isEmpty() && 
        searchResults.notes.isEmpty() && 
        searchResults.hashtags.isEmpty() -> NoResultsView()
        else -> SearchResultsList(searchResults = searchResults)
    }
}

@Composable
private fun LoadingView() {
    Column(
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
        modifier = Modifier.fillMaxSize()
    ) {
        CircularProgressIndicator(
            modifier = Modifier.size(48.dp),
            color = MaterialTheme.colorScheme.primary
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Text(
            text = "Searching...",
            fontSize = 16.sp,
            fontWeight = FontWeight.Medium,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
    }
}

@Composable
private fun ErrorView(
    error: String,
    onRetry: () -> Unit
) {
    Column(
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
        modifier = Modifier.fillMaxSize()
    ) {
        Icon(
            imageVector = Icons.Default.Warning,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.tertiary,
            modifier = Modifier.size(48.dp)
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Text(
            text = "Search Error",
            fontSize = 20.sp,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colorScheme.onSurface
        )
        
        Spacer(modifier = Modifier.height(8.dp))
        
        Text(
            text = error,
            fontSize = 16.sp,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            textAlign = androidx.compose.ui.text.style.TextAlign.Center,
            modifier = Modifier.padding(horizontal = 32.dp)
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Button(onClick = onRetry) {
            Text("Try Again")
        }
    }
}

@Composable
private fun NoResultsView() {
    Column(
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
        modifier = Modifier.fillMaxSize()
    ) {
        Icon(
            imageVector = xyz.sonet.app.ui.AppIcons.Search,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(48.dp)
        )
        
        Spacer(modifier = Modifier.height(16.dp))
        
        Text(
            text = "No Results Found",
            fontSize = 20.sp,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colorScheme.onSurface
        )
        
        Spacer(modifier = Modifier.height(8.dp))
        
        Text(
            text = "Try adjusting your search terms or browse trending topics",
            fontSize = 16.sp,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            textAlign = androidx.compose.ui.text.style.TextAlign.Center,
            modifier = Modifier.padding(horizontal = 32.dp)
        )
    }
}

@Composable
private fun SearchResultsList(
    searchResults: SearchViewModel.SearchResults
) {
    LazyColumn(
        contentPadding = PaddingValues(16.dp),
        verticalArrangement = Arrangement.spacedBy(16.dp)
    ) {
        // Users
        if (searchResults.users.isNotEmpty()) {
            item {
                Text(
                    text = "Users",
                    fontSize = 20.sp,
                    fontWeight = FontWeight.Bold,
                    color = MaterialTheme.colorScheme.onSurface
                )
            }
            
            items(searchResults.users) { user ->
                UserSearchRow(user = user)
            }
        }
        
        // Notes
        if (searchResults.notes.isNotEmpty()) {
            item {
                Text(
                    text = "Notes",
                    fontSize = 20.sp,
                    fontWeight = FontWeight.Bold,
                    color = MaterialTheme.colorScheme.onSurface
                )
            }
            
            items(searchResults.notes) { note ->
                NoteSearchRow(note = note)
            }
        }
        
        // Hashtags
        if (searchResults.hashtags.isNotEmpty()) {
            item {
                Text(
                    text = "Hashtags",
                    fontSize = 20.sp,
                    fontWeight = FontWeight.Bold,
                    color = MaterialTheme.colorScheme.onSurface
                )
            }
            
            items(searchResults.hashtags) { hashtag ->
                HashtagSearchRow(hashtag = hashtag)
            }
        }
        
        // Load More
        if (searchResults.hasMoreResults) {
            item {
                LoadMoreButton()
            }
        }
    }
}

@Composable
private fun UserSearchRow(user: UserProfile) {
    var isPressed by remember { mutableStateOf(false) }
    var isFollowing by remember { mutableStateOf(false) }
    val scale by animateFloatAsState(
        targetValue = if (isPressed) 0.98f else 1f,
        animationSpec = tween(100)
    )
    
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .scale(scale)
            .clickable { /* Navigate to user profile */ }
            .padding(horizontal = 16.dp, vertical = 12.dp)
    ) {
        // Avatar
        Surface(
            shape = CircleShape,
            color = MaterialTheme.colorScheme.surfaceVariant,
            modifier = Modifier.size(48.dp)
        ) {
            Box(
                contentAlignment = Alignment.Center
            ) {
                Icon(
                    imageVector = xyz.sonet.app.ui.AppIcons.Profile,
                    contentDescription = null,
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(24.dp)
                )
            }
        }
        
        Spacer(modifier = Modifier.width(12.dp))
        
        // User info
        Column(
            modifier = Modifier.weight(1f),
            verticalArrangement = Arrangement.spacedBy(4.dp)
        ) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(
                    text = user.displayName,
                    fontSize = 16.sp,
                    fontWeight = FontWeight.SemiBold,
                    color = MaterialTheme.colorScheme.onSurface,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis
                )
                
                if (user.isVerified) {
                    Spacer(modifier = Modifier.width(8.dp))
                    Icon(
                        imageVector = xyz.sonet.app.ui.AppIcons.Verified,
                        contentDescription = "Verified",
                        tint = MaterialTheme.colorScheme.primary,
                        modifier = Modifier.size(16.dp)
                    )
                }
            }
            
            Text(
                text = "@${user.username}",
                fontSize = 14.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
            
            if (user.bio.isNotEmpty()) {
                Text(
                    text = user.bio,
                    fontSize = 14.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis
                )
            }
            
            Row(horizontalArrangement = Arrangement.spacedBy(16.dp)) {
                Text(
                    text = "${user.followerCount.formatNumber()} followers",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                
                Text(
                    text = "${user.noteCount.formatNumber()} notes",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
        
        Spacer(modifier = Modifier.width(12.dp))
        
        // Follow button
        Button(
            onClick = {
                isFollowing = !isFollowing
            },
            colors = ButtonDefaults.buttonColors(
                containerColor = if (isFollowing) {
                    MaterialTheme.colorScheme.surfaceVariant
                } else {
                    MaterialTheme.colorScheme.primary
                }
            ),
            shape = RoundedCornerShape(20.dp)
        ) {
            Text(
                text = if (isFollowing) "Following" else "Follow",
                fontSize = 14.sp,
                fontWeight = FontWeight.SemiBold,
                color = if (isFollowing) {
                    MaterialTheme.colorScheme.onSurface
                } else {
                    MaterialTheme.colorScheme.onPrimary
                }
            )
        }
    }
}

@Composable
private fun NoteSearchRow(note: Note) {
    var isPressed by remember { mutableStateOf(false) }
    var isLiked by remember { mutableStateOf(false) }
    var isReposted by remember { mutableStateOf(false) }
    val scale by animateFloatAsState(
        targetValue = if (isPressed) 0.98f else 1f,
        animationSpec = tween(100)
    )
    
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .scale(scale)
            .clickable { /* Navigate to note detail */ }
            .padding(horizontal = 16.dp, vertical = 12.dp)
    ) {
        // Header
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier.fillMaxWidth()
        ) {
            Surface(
                shape = CircleShape,
                color = MaterialTheme.colorScheme.surfaceVariant,
                modifier = Modifier.size(32.dp)
            ) {
                Box(
                    contentAlignment = Alignment.Center
                ) {
                    Icon(
                        imageVector = xyz.sonet.app.ui.AppIcons.Profile,
                        contentDescription = null,
                        tint = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.size(16.dp)
                    )
                }
            }
            
            Spacer(modifier = Modifier.width(12.dp))
            
            Column {
                Text(
                    text = "User", // This would come from a separate user lookup
                    fontSize = 14.sp,
                    fontWeight = FontWeight.SemiBold,
                    color = MaterialTheme.colorScheme.onSurface
                )
                
                Text(
                    text = "2h", // This would be calculated from note.createdAt
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            Spacer(modifier = Modifier.weight(1f))
            
            var showMenu by remember { mutableStateOf(false) }
            Box {
                IconButton(onClick = { showMenu = true }) {
                    Icon(
                        imageVector = xyz.sonet.app.ui.AppIcons.More,
                        contentDescription = "More options",
                        tint = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                DropdownMenu(expanded = showMenu, onDismissRequest = { showMenu = false }) {
                    val isOwn = false // TODO: compare against session user id when available here
                    if (isOwn) {
                        DropdownMenuItem(text = { Text("Edit") }, onClick = { showMenu = false })
                        DropdownMenuItem(text = { Text("Delete", color = MaterialTheme.colorScheme.error) }, onClick = { showMenu = false })
                    } else {
                        DropdownMenuItem(text = { Text("Mute @user") }, onClick = { showMenu = false })
                        DropdownMenuItem(text = { Text("Block @user", color = MaterialTheme.colorScheme.error) }, onClick = { showMenu = false })
                        DropdownMenuItem(text = { Text("Report", color = MaterialTheme.colorScheme.error) }, onClick = { showMenu = false })
                    }
                }
            }
        }
        
        // Content
        Text(
            text = note.content,
            fontSize = 15.sp,
            color = MaterialTheme.colorScheme.onSurface,
            maxLines = 3,
            overflow = TextOverflow.Ellipsis,
            modifier = Modifier.padding(top = 12.dp)
        )
        
        // Engagement
        Row(
            horizontalArrangement = Arrangement.spacedBy(20.dp),
            modifier = Modifier.padding(top = 12.dp)
        ) {
            // Like button
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier.clickable { isLiked = !isLiked }
            ) {
                Icon(
                    imageVector = if (isLiked) xyz.sonet.app.ui.AppIcons.Like else xyz.sonet.app.ui.AppIcons.LikeBorder,
                    contentDescription = "Like",
                    tint = if (isLiked) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(18.dp)
                )
                
                Spacer(modifier = Modifier.width(6.dp))
                
                Text(
                    text = note.engagement.likeCount.toString(),
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            // Reply button
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier.clickable { /* Navigate to reply */ }
            ) {
                Icon(
                    imageVector = xyz.sonet.app.ui.AppIcons.Comment,
                    contentDescription = "Reply",
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(18.dp)
                )
                
                Spacer(modifier = Modifier.width(6.dp))
                
                Text(
                    text = note.engagement.replyCount.toString(),
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            // Repost button
            Row(
                verticalAlignment = Alignment.CenterVertically,
                modifier = Modifier.clickable { isReposted = !isReposted }
            ) {
                Icon(
                    imageVector = xyz.sonet.app.ui.AppIcons.Reply,
                    contentDescription = "Repost",
                    tint = if (isReposted) MaterialTheme.colorScheme.tertiary else MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(18.dp)
                )
                
                Spacer(modifier = Modifier.width(6.dp))
                
                Text(
                    text = note.engagement.repostCount.toString(),
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
            
            Spacer(modifier = Modifier.weight(1f))
            
            // Share button
            IconButton(onClick = { /* Share note */ }) {
                Icon(
                    imageVector = xyz.sonet.app.ui.AppIcons.Share,
                    contentDescription = "Share",
                    tint = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.size(18.dp)
                )
            }
        }
    }
}

@Composable
private fun HashtagSearchRow(hashtag: String) {
    var isPressed by remember { mutableStateOf(false) }
    val scale by animateFloatAsState(
        targetValue = if (isPressed) 0.98f else 1f,
        animationSpec = tween(100)
    )
    
    Row(
        verticalAlignment = Alignment.CenterVertically,
        modifier = Modifier
            .fillMaxWidth()
            .scale(scale)
            .clickable { /* Navigate to hashtag page */ }
            .padding(horizontal = 16.dp, vertical = 12.dp)
    ) {
        Icon(
            imageVector = Icons.Default.Tag,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.primary,
            modifier = Modifier.size(20.dp)
        )
        
        Spacer(modifier = Modifier.width(12.dp))
        
        Column {
            Text(
                text = "#$hashtag",
                fontSize = 17.sp,
                fontWeight = FontWeight.SemiBold,
                color = MaterialTheme.colorScheme.onSurface
            )
            
            Text(
                text = "Explore hashtag",
                fontSize = 14.sp,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
        
        Spacer(modifier = Modifier.weight(1f))
        
        Icon(
            imageVector = Icons.Default.ArrowUpward,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.size(16.dp)
        )
    }
}

@Composable
private fun LoadMoreButton() {
    Button(
        onClick = { /* Load more results */ },
        modifier = Modifier.fillMaxWidth(),
        colors = ButtonDefaults.buttonColors(
            containerColor = Color.Transparent
        )
    ) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.Center
        ) {
            Text(
                text = "Load More Results",
                fontSize = 16.sp,
                fontWeight = FontWeight.Medium,
                color = MaterialTheme.colorScheme.primary
            )
            
            Spacer(modifier = Modifier.width(8.dp))
            
            Icon(
                imageVector = Icons.Default.KeyboardArrowDown,
                contentDescription = null,
                tint = MaterialTheme.colorScheme.primary,
                modifier = Modifier.size(20.dp)
            )
        }
    }
}

// MARK: - Helper Functions
private fun getIconForSearchType(iconName: String): ImageVector {
    return when (iconName) {
        "magnifyingglass" -> Icons.Default.Search
        "person.2" -> Icons.Default.Group
        "text.bubble" -> Icons.Default.ChatBubbleOutline
        "number" -> Icons.Default.Tag
        else -> Icons.Default.Search
    }
}

private fun getIconForSuggestion(suggestion: String): ImageVector {
    return when {
        suggestion.contains("Tech", ignoreCase = true) -> Icons.Default.Computer
        suggestion.contains("Science", ignoreCase = true) -> Icons.Default.Science
        suggestion.contains("Art", ignoreCase = true) -> Icons.Default.Brush
        suggestion.contains("Music", ignoreCase = true) -> Icons.Default.MusicNote
        suggestion.contains("Sport", ignoreCase = true) -> Icons.Default.SportsSoccer
        suggestion.contains("Travel", ignoreCase = true) -> Icons.Default.Flight
        suggestion.contains("Food", ignoreCase = true) -> Icons.Default.Restaurant
        suggestion.contains("Fashion", ignoreCase = true) -> Icons.Default.Checkroom
        else -> Icons.Default.Search
    }
}

private fun Int.formatNumber(): String {
    return when {
        this >= 1_000_000 -> "${this / 1_000_000}M"
        this >= 1_000 -> "${this / 1_000}K"
        else -> this.toString()
    }
}