package xyz.sonet.app.notifications

import android.app.Notification
import android.app.Service
import android.content.Intent
import android.os.IBinder
import android.os.Handler
import android.os.Looper

class UploadForegroundService : Service() {
    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val handler = Handler(Looper.getMainLooper())
        var progress = 0
        val max = 100
        val update = object : Runnable {
            override fun run() {
                val notif = MessagingNotificationHelper.buildUploadProgressNotification(this@UploadForegroundService, "Uploading…", progress, max)
                startForeground(1001, notif)
                if (progress < max) {
                    progress += 5
                    handler.postDelayed(this, 200)
                } else {
                    stopForeground(true)
                    stopSelf()
                }
            }
        }
        handler.post(update)
        return START_NOT_STICKY
    }
}

