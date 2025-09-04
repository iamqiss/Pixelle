package xyz.sonet.app.services

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.work.*
import kotlinx.coroutines.*
import xyz.sonet.app.MainActivity
import xyz.sonet.app.R
import xyz.sonet.app.models.UploadTask
import java.io.File
import java.util.concurrent.TimeUnit

class BackgroundUploadManager(private val context: Context) {
    
    private val workManager = WorkManager.getInstance(context)
    private val notificationManager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
    private val connectivityManager = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
    
    companion object {
        const val UPLOAD_WORK_NAME = "sonet_upload_work"
        const val DOWNLOAD_WORK_NAME = "sonet_download_work"
        const val UPLOAD_CHANNEL_ID = "sonet_upload_channel"
        const val DOWNLOAD_CHANNEL_ID = "sonet_download_channel"
        const val NOTIFICATION_ID_UPLOAD = 1001
        const val NOTIFICATION_ID_DOWNLOAD = 1002
    }
    
    init {
        createNotificationChannels()
    }
    
    private fun createNotificationChannels() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val uploadChannel = NotificationChannel(
                UPLOAD_CHANNEL_ID,
                "Sonet Uploads",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Background upload notifications"
                setShowBadge(false)
            }
            
            val downloadChannel = NotificationChannel(
                DOWNLOAD_CHANNEL_ID,
                "Sonet Downloads",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Background download notifications"
                setShowBadge(false)
            }
            
            notificationManager.createNotificationChannels(listOf(uploadChannel, downloadChannel))
        }
    }
    
    // MARK: - Upload Management
    fun queueUpload(uploadTask: UploadTask) {
        val constraints = createUploadConstraints(uploadTask)
        
        val uploadWork = OneTimeWorkRequestBuilder<UploadWorker>()
            .setConstraints(constraints)
            .setInputData(workDataOf(
                "file_path" to uploadTask.filePath,
                "caption" to uploadTask.caption,
                "tags" to uploadTask.tags.joinToString(","),
                "location" to uploadTask.location,
                "is_video" to uploadTask.isVideo
            ))
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
            .setRetryPolicy(RetryPolicy.DEFAULT)
            .build()
        
        workManager.enqueueUniqueWork(
            "${UPLOAD_WORK_NAME}_${uploadTask.id}",
            ExistingWorkPolicy.REPLACE,
            uploadWork
        )
        
        showUploadNotification(uploadTask)
    }
    
    private fun createUploadConstraints(uploadTask: UploadTask): Constraints {
        val networkType = if (uploadTask.requireWifi) {
            NetworkType.UNMETERED
        } else {
            NetworkType.CONNECTED
        }
        
        return Constraints.Builder()
            .setRequiredNetworkType(networkType)
            .setRequiresBatteryNotLow(true)
            .setRequiresCharging(false)
            .build()
    }
    
    fun cancelUpload(uploadId: String) {
        workManager.cancelUniqueWork("${UPLOAD_WORK_NAME}_$uploadId")
        hideUploadNotification()
    }
    
    fun pauseAllUploads() {
        workManager.cancelAllWorkByTag(UPLOAD_WORK_NAME)
    }
    
    fun resumeAllUploads() {
        // Re-queue paused uploads
        // This would typically reload from a local database
    }
    
    // MARK: - Download Management
    fun queueSmartDownload(postIds: List<String>) {
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.UNMETERED)
            .setRequiresBatteryNotLow(false)
            .setRequiresCharging(false)
            .build()
        
        val downloadWork = OneTimeWorkRequestBuilder<DownloadWorker>()
            .setConstraints(constraints)
            .setInputData(workDataOf("post_ids" to postIds.toTypedArray()))
            .setBackoffCriteria(BackoffPolicy.LINEAR, 1, TimeUnit.MINUTES)
            .build()
        
        workManager.enqueueUniqueWork(
            DOWNLOAD_WORK_NAME,
            ExistingWorkPolicy.APPEND_OR_REPLACE,
            downloadWork
        )
        
        showDownloadNotification(postIds.size)
    }
    
    fun cancelDownloads() {
        workManager.cancelUniqueWork(DOWNLOAD_WORK_NAME)
        hideDownloadNotification()
    }
    
    // MARK: - Network Monitoring
    fun startNetworkMonitoring() {
        val networkRequest = NetworkRequest.Builder()
            .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
            .addTransportType(NetworkCapabilities.TRANSPORT_WIFI)
            .build()
        
        connectivityManager.registerNetworkCallback(networkRequest, networkCallback)
    }
    
    fun stopNetworkMonitoring() {
        connectivityManager.unregisterNetworkCallback(networkCallback)
    }
    
    private val networkCallback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) {
            super.onAvailable(network)
            // Resume uploads when network becomes available
            resumeAllUploads()
        }
        
        override fun onLost(network: Network) {
            super.onLost(network)
            // Pause uploads when network is lost
            pauseAllUploads()
        }
    }
    
    // MARK: - Notifications
    private fun showUploadNotification(uploadTask: UploadTask) {
        val intent = Intent(context, MainActivity::class.java)
        val pendingIntent = PendingIntent.getActivity(
            context,
            0,
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        
        val notification = NotificationCompat.Builder(context, UPLOAD_CHANNEL_ID)
            .setContentTitle("Uploading to Sonet")
            .setContentText("Uploading ${uploadTask.filePath.substringAfterLast("/")}")
            .setSmallIcon(R.drawable.ic_upload)
            .setContentIntent(pendingIntent)
            .setOngoing(true)
            .setProgress(0, 0, true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
        
        notificationManager.notify(NOTIFICATION_ID_UPLOAD, notification)
    }
    
    private fun hideUploadNotification() {
        notificationManager.cancel(NOTIFICATION_ID_UPLOAD)
    }
    
    private fun showDownloadNotification(count: Int) {
        val intent = Intent(context, MainActivity::class.java)
        val pendingIntent = PendingIntent.getActivity(
            context,
            0,
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        
        val notification = NotificationCompat.Builder(context, DOWNLOAD_CHANNEL_ID)
            .setContentTitle("Downloading Content")
            .setContentText("Preloading $count posts for offline viewing")
            .setSmallIcon(R.drawable.ic_download)
            .setContentIntent(pendingIntent)
            .setOngoing(true)
            .setProgress(0, 0, true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
        
        notificationManager.notify(NOTIFICATION_ID_DOWNLOAD, notification)
    }
    
    private fun hideDownloadNotification() {
        notificationManager.cancel(NOTIFICATION_ID_DOWNLOAD)
    }
    
    // MARK: - Utility Methods
    fun isWifiConnected(): Boolean {
        val network = connectivityManager.activeNetwork ?: return false
        val capabilities = connectivityManager.getNetworkCapabilities(network) ?: return false
        return capabilities.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)
    }
    
    fun getUploadQueueSize(): Int {
        // This would typically query a local database
        return 0
    }
    
    fun getDownloadQueueSize(): Int {
        // This would typically query a local database
        return 0
    }
}

// MARK: - Upload Task Model
data class UploadTask(
    val id: String,
    val filePath: String,
    val caption: String,
    val tags: List<String>,
    val location: String?,
    val isVideo: Boolean,
    val requireWifi: Boolean = true,
    val priority: Int = 0
)