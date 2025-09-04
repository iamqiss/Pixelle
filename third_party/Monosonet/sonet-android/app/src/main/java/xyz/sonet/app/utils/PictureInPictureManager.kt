package xyz.sonet.app.utils

import android.app.PictureInPictureParams
import android.content.Context
import android.content.pm.PackageManager
import android.graphics.Rect
import android.os.Build
import android.util.Rational
import androidx.annotation.RequiresApi

@RequiresApi(Build.VERSION_CODES.O)
class PictureInPictureManager(private val context: Context) {
    
    private var pipParams: PictureInPictureParams? = null
    private var isPipSupported = false
    
    init {
        checkPipSupport()
    }
    
    private fun checkPipSupport() {
        isPipSupported = context.packageManager.hasSystemFeature(PackageManager.FEATURE_PICTURE_IN_PICTURE)
    }
    
    fun isPipSupported(): Boolean = isPipSupported
    
    fun enterPipMode(
        sourceRectHint: Rect? = null,
        aspectRatio: Rational? = null,
        autoEnterEnabled: Boolean = true
    ): Boolean {
        if (!isPipSupported) return false
        
        val builder = PictureInPictureParams.Builder()
        
        // Set source rect hint for smooth transition
        sourceRectHint?.let { builder.setSourceRectHint(it) }
        
        // Set aspect ratio (16:9 for videos)
        aspectRatio?.let { builder.setAspectRatio(it) }
        
        // Enable auto-enter PIP when user navigates away
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            builder.setAutoEnterEnabled(autoEnterEnabled)
        }
        
        // Set expanded aspect ratio for better viewing
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            builder.setExpandedAspectRatio(Rational(16, 9))
        }
        
        // Set seamlessness for smooth transitions
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            builder.setSeamlessResizeEnabled(true)
        }
        
        pipParams = builder.build()
        
        // Note: The actual enterPipMode() call should be made from the activity
        // This method just prepares the parameters
        return true
    }
    
    fun getPipParams(): PictureInPictureParams? = pipParams
    
    fun updatePipParams(
        sourceRectHint: Rect? = null,
        aspectRatio: Rational? = null
    ) {
        if (!isPipSupported) return
        
        val currentParams = pipParams ?: PictureInPictureParams.Builder().build()
        val builder = PictureInPictureParams.Builder(currentParams)
        
        sourceRectHint?.let { builder.setSourceRectHint(it) }
        aspectRatio?.let { builder.setAspectRatio(it) }
        
        pipParams = builder.build()
    }
    
    fun clearPipParams() {
        pipParams = null
    }
    
    companion object {
        fun createVideoPipParams(
            sourceRect: Rect,
            videoAspectRatio: Rational = Rational(16, 9)
        ): PictureInPictureParams {
            return PictureInPictureParams.Builder()
                .setSourceRectHint(sourceRect)
                .setAspectRatio(videoAspectRatio)
                .build()
        }
        
        fun createCustomPipParams(
            sourceRect: Rect,
            aspectRatio: Rational,
            autoEnter: Boolean = true
        ): PictureInPictureParams {
            val builder = PictureInPictureParams.Builder()
                .setSourceRectHint(sourceRect)
                .setAspectRatio(aspectRatio)
            
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                builder.setAutoEnterEnabled(autoEnter)
            }
            
            return builder.build()
        }
    }
}