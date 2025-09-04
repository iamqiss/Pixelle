package xyz.sonet.app.services

import android.app.Activity
import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.provider.MediaStore
import androidx.core.content.FileProvider
import xyz.sonet.app.MainActivity
import xyz.sonet.app.models.ShareContent
import java.io.File
import java.io.FileOutputStream
import java.io.InputStream

class CustomShareTargetService : Activity() {
    
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        
        handleIntent(intent)
        finish()
    }
    
    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        handleIntent(intent)
        finish()
    }
    
    private fun handleIntent(intent: Intent) {
        when (intent.action) {
            Intent.ACTION_SEND -> handleSingleShare(intent)
            Intent.ACTION_SEND_MULTIPLE -> handleMultipleShare(intent)
            "xyz.sonet.app.SHARE_TO_STORY" -> handleShareToStory(intent)
            "xyz.sonet.app.SHARE_TO_FEED" -> handleShareToFeed(intent)
            "xyz.sonet.app.SAVE_DRAFT" -> handleSaveDraft(intent)
            else -> handleGenericShare(intent)
        }
    }
    
    private fun handleSingleShare(intent: Intent) {
        val type = intent.type ?: return
        
        when {
            type.startsWith("image/") -> handleImageShare(intent)
            type.startsWith("video/") -> handleVideoShare(intent)
            type == "text/plain" -> handleTextShare(intent)
            else -> handleGenericShare(intent)
        }
    }
    
    private fun handleMultipleShare(intent: Intent) {
        val uris = intent.getParcelableArrayListExtra<Uri>(Intent.EXTRA_STREAM)
        val type = intent.type
        
        if (uris != null && type != null) {
            when {
                type.startsWith("image/") -> handleMultipleImageShare(uris)
                type.startsWith("video/") -> handleMultipleVideoShare(uris)
                else -> handleGenericShare(intent)
            }
        }
    }
    
    private fun handleImageShare(intent: Intent) {
        val imageUri = intent.getParcelableExtra<Uri>(Intent.EXTRA_STREAM)
        val text = intent.getStringExtra(Intent.EXTRA_TEXT) ?: ""
        
        if (imageUri != null) {
            val shareContent = ShareContent(
                type = ShareContent.Type.IMAGE,
                uris = listOf(imageUri),
                text = text,
                source = "share_target"
            )
            
            openSonetWithContent(shareContent)
        }
    }
    
    private fun handleVideoShare(intent: Intent) {
        val videoUri = intent.getParcelableExtra<Uri>(Intent.EXTRA_STREAM)
        val text = intent.getStringExtra(Intent.EXTRA_TEXT) ?: ""
        
        if (videoUri != null) {
            val shareContent = ShareContent(
                type = ShareContent.Type.VIDEO,
                uris = listOf(videoUri),
                text = text,
                source = "share_target"
            )
            
            openSonetWithContent(shareContent)
        }
    }
    
    private fun handleTextShare(intent: Intent) {
        val text = intent.getStringExtra(Intent.EXTRA_TEXT) ?: ""
        val subject = intent.getStringExtra(Intent.EXTRA_SUBJECT) ?: ""
        
        val fullText = if (subject.isNotEmpty()) "$subject\n\n$text" else text
        
        val shareContent = ShareContent(
            type = ShareContent.Type.TEXT,
            uris = emptyList(),
            text = fullText,
                source = "share_target"
        )
        
        openSonetWithContent(shareContent)
    }
    
    private fun handleMultipleImageShare(uris: ArrayList<Uri>) {
        val shareContent = ShareContent(
            type = ShareContent.Type.MULTIPLE_IMAGES,
            uris = uris,
            text = "",
            source = "share_target"
        )
        
        openSonetWithContent(shareContent)
    }
    
    private fun handleMultipleVideoShare(uris: ArrayList<Uri>) {
        // For multiple videos, we'll take the first one as primary
        val shareContent = ShareContent(
            type = ShareContent.Type.VIDEO,
            uris = listOf(uris.first()),
            text = "",
            source = "share_target"
        )
        
        openSonetWithContent(shareContent)
    }
    
    private fun handleShareToStory(intent: Intent) {
        val type = intent.type
        val uri = intent.getParcelableExtra<Uri>(Intent.EXTRA_STREAM)
        val text = intent.getStringExtra(Intent.EXTRA_TEXT) ?: ""
        
        if (uri != null) {
            val shareContent = ShareContent(
                type = if (type?.startsWith("image/") == true) ShareContent.Type.STORY_IMAGE else ShareContent.Type.STORY_VIDEO,
                uris = listOf(uri),
                text = text,
                source = "story_share_target"
            )
            
            openSonetWithContent(shareContent, "story")
        }
    }
    
    private fun handleShareToFeed(intent: Intent) {
        val type = intent.type
        val uri = intent.getParcelableExtra<Uri>(Intent.EXTRA_STREAM)
        val text = intent.getStringExtra(Intent.EXTRA_TEXT) ?: ""
        
        if (uri != null) {
            val shareContent = ShareContent(
                type = ShareContent.Type.VIDEO,
                uris = listOf(uri),
                text = text,
                source = "feed_share_target"
            )
            
            openSonetWithContent(shareContent, "feed")
        }
    }
    
    private fun handleSaveDraft(intent: Intent) {
        val type = intent.type
        val uri = intent.getParcelableExtra<Uri>(Intent.EXTRA_STREAM)
        val text = intent.getStringExtra(Intent.EXTRA_TEXT) ?: ""
        
        if (uri != null) {
            val shareContent = ShareContent(
                type = if (type?.startsWith("image/") == true) ShareContent.Type.DRAFT_IMAGE else ShareContent.Type.DRAFT_VIDEO,
                uris = listOf(uri),
                text = text,
                source = "draft_share_target"
            )
            
            openSonetWithContent(shareContent, "draft")
        }
    }
    
    private fun handleGenericShare(intent: Intent) {
        val text = intent.getStringExtra(Intent.EXTRA_TEXT) ?: ""
        val subject = intent.getStringExtra(Intent.EXTRA_SUBJECT) ?: ""
        
        val fullText = if (subject.isNotEmpty()) "$subject\n\n$text" else text
        
        val shareContent = ShareContent(
            type = ShareContent.Type.TEXT,
            uris = emptyList(),
            text = fullText,
            source = "generic_share_target"
        )
        
        openSonetWithContent(shareContent)
    }
    
    private fun openSonetWithContent(shareContent: ShareContent, mode: String = "compose") {
        // Save content to shared preferences or database for Sonet to access
        saveShareContent(shareContent)
        
        // Open Sonet app with the shared content
        val intent = Intent(this, MainActivity::class.java).apply {
            action = "OPEN_SHARED_CONTENT"
            putExtra("share_mode", mode)
            putExtra("content_id", shareContent.id)
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_ACTIVITY_CLEAR_TOP
        }
        
        startActivity(intent)
    }
    
    private fun saveShareContent(shareContent: ShareContent) {
        // Save to shared preferences or local database
        // This would typically use a database helper or shared preferences
        val prefs = getSharedPreferences("sonet_share", MODE_PRIVATE)
        prefs.edit().apply {
            putString("last_shared_content", shareContent.toJson())
            putLong("share_timestamp", System.currentTimeMillis())
        }.apply()
    }
    
    private fun copyUriToLocalFile(uri: Uri): Uri? {
        return try {
            val inputStream = contentResolver.openInputStream(uri)
            val file = File(cacheDir, "shared_${System.currentTimeMillis()}.tmp")
            val outputStream = FileOutputStream(file)
            
            inputStream?.use { input ->
                outputStream.use { output ->
                    input.copyTo(output)
                }
            }
            
            FileProvider.getUriForFile(this, "${packageName}.fileprovider", file)
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }
}

// MARK: - Share Content Model
data class ShareContent(
    val id: String = System.currentTimeMillis().toString(),
    val type: Type,
    val uris: List<Uri>,
    val text: String,
    val source: String,
    val timestamp: Long = System.currentTimeMillis()
) {
    enum class Type {
        IMAGE,
        VIDEO,
        TEXT,
        MULTIPLE_IMAGES,
        STORY_IMAGE,
        STORY_VIDEO,
        FEED_IMAGE,
        FEED_VIDEO,
        DRAFT_IMAGE,
        DRAFT_VIDEO
    }
    
    fun toJson(): String {
        // Simple JSON representation for shared preferences
        return """
            {
                "id": "$id",
                "type": "$type",
                "uris": [${uris.joinToString(",") { "\"$it\"" }}],
                "text": "$text",
                "source": "$source",
                "timestamp": $timestamp
            }
        """.trimIndent()
    }
}