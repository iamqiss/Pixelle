package xyz.sonet.app.views.post

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.lifecycle.viewmodel.compose.viewModel
import xyz.sonet.app.grpc.proto.Note
import xyz.sonet.app.viewmodels.SessionViewModel
import xyz.sonet.app.viewmodels.ThemeViewModel
import xyz.sonet.app.viewmodels.PostDetailViewModel

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun PostDetailView(
    noteId: String,
    sessionViewModel: SessionViewModel,
    themeViewModel: ThemeViewModel,
    onBackPressed: () -> Unit
) {
    val postDetailViewModel: PostDetailViewModel = viewModel()
    val currentUser by sessionViewModel.currentUser.collectAsState()
    
    LaunchedEffect(noteId) {
        postDetailViewModel.loadPost(noteId)
        postDetailViewModel.loadThread(noteId)
    }
    
    val post by postDetailViewModel.post.collectAsState()
    val thread by postDetailViewModel.thread.collectAsState()
    val isLoading by postDetailViewModel.isLoading.collectAsState()
    val error by postDetailViewModel.error.collectAsState()
    
    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("Thread") },
                navigationIcon = {
                    IconButton(onClick = onBackPressed) {
                        Icon(Icons.Default.ArrowBack, contentDescription = "Back")
                    }
                },
                actions = {
                    IconButton(onClick = { /* Notifications */ }) {
                        Icon(Icons.Default.Notifications, contentDescription = "Notifications")
                    }
                    IconButton(onClick = { /* More options */ }) {
                        Icon(Icons.Default.MoreVert, contentDescription = "More options")
                    }
                }
            )
        }
    ) { paddingValues ->
        when {
            isLoading -> {
                Box(
                    modifier = Modifier.fillMaxSize(),
                    contentAlignment = Alignment.Center
                ) {
                    CircularProgressIndicator()
                }
            }
            error != null -> {
                Box(
                    modifier = Modifier.fillMaxSize(),
                    contentAlignment = Alignment.Center
                ) {
                    Column(
                        horizontalAlignment = Alignment.CenterHorizontally,
                        verticalArrangement = Arrangement.spacedBy(16.dp)
                    ) {
                        Text("Error loading post", style = MaterialTheme.typography.headlineSmall)
                        Text(error!!, style = MaterialTheme.typography.bodyMedium)
                        Button(onClick = { postDetailViewModel.loadPost(noteId) }) {
                            Text("Retry")
                        }
                    }
                }
            }
            post != null -> {
                LazyColumn(
                    modifier = Modifier
                        .fillMaxSize()
                        .padding(paddingValues),
                    verticalArrangement = Arrangement.spacedBy(0.dp)
                ) {
                    // Original post
                    item {
                        PostDetailCard(
                            note = post!!,
                            currentUserId = currentUser?.id,
                            onReply = { postDetailViewModel.setReplyToNote(post!!) },
                            onLike = { postDetailViewModel.toggleLike(post!!.noteId) },
                            onRepost = { postDetailViewModel.toggleRepost(post!!.noteId) },
                            onShare = { /* Handle share */ }
                        )
                    }
                    
                    // Thread separator
                    item {
                        ThreadSeparator()
                    }
                    
                    // Comments/Replies
                    items(thread) { reply ->
                        PostDetailCard(
                            note = reply,
                            currentUserId = currentUserId,
                            onReply = { postDetailViewModel.setReplyToNote(reply) },
                            onLike = { postDetailViewModel.toggleLike(reply.noteId) },
                            onRepost = { postDetailViewModel.toggleRepost(reply.noteId) },
                            onShare = { /* Handle share */ },
                            isReply = true
                        )
                    }
                }
            }
        }
    }
    
    // Reply input at bottom
    ReplyInput(
        onReply = { content ->
            postDetailViewModel.createReply(content)
        },
        replyToNote = postDetailViewModel.replyToNote.collectAsState().value
    )
}