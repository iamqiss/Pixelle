package xyz.sonet.app.widgets

import android.content.Context
import android.content.Intent
import android.widget.RemoteViews
import android.widget.RemoteViewsService
import xyz.sonet.app.R
import xyz.sonet.app.models.Post

class SonetWidgetRemoteViewsFactory(
    private val context: Context,
    private val intent: Intent
) : RemoteViewsService.RemoteViewsFactory {
    
    private var posts: List<Post> = emptyList()
    
    override fun onCreate() {
        // Initialize data source
        loadPosts()
    }
    
    override fun onDataSetChanged() {
        // Refresh data
        loadPosts()
    }
    
    override fun onDestroy() {
        posts = emptyList()
    }
    
    override fun getCount(): Int = posts.size
    
    override fun getViewAt(position: Int): RemoteViews? {
        if (position >= posts.size) return null
        
        val post = posts[position]
        val views = RemoteViews(context.packageName, R.layout.widget_post_item)
        
        // Set post content
        views.setTextViewText(R.id.widget_post_text, post.text)
        views.setTextViewText(R.id.widget_post_author, post.author.displayName)
        
        // Set click intent
        val fillInIntent = Intent()
        fillInIntent.putExtra("post_id", post.id)
        fillInIntent.putExtra("post_uri", post.uri)
        views.setOnClickFillInIntent(R.id.widget_post_container, fillInIntent)
        
        return views
    }
    
    override fun getLoadingView(): RemoteViews? {
        return RemoteViews(context.packageName, R.layout.widget_loading)
    }
    
    override fun getViewTypeCount(): Int = 1
    
    override fun getItemId(position: Int): Long = position.toLong()
    
    override fun hasStableIds(): Boolean = true
    
    private fun loadPosts() {
        // TODO: Load posts from local database or cache
        // This would typically fetch from a local database or cached data
        posts = listOf(
            Post(
                id = "1",
                uri = "at://did:plc:example/app.bsky.feed.post/1",
                text = "Sample post for widget",
                author = Post.Author(
                    did = "did:plc:example",
                    displayName = "Sample User",
                    handle = "sample.bsky.app"
                ),
                createdAt = System.currentTimeMillis()
            )
        )
    }
}

// MARK: - Post Model
data class Post(
    val id: String,
    val uri: String,
    val text: String,
    val author: Author,
    val createdAt: Long
) {
    data class Author(
        val did: String,
        val displayName: String,
        val handle: String
    )
}