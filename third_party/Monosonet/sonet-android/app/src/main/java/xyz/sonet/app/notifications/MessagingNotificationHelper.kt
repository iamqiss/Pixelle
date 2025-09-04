package xyz.sonet.app.notifications

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.graphics.Color
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.app.Person
import androidx.core.app.RemoteInput
import androidx.core.app.NotificationCompat.MessagingStyle
import xyz.sonet.app.MainActivity
import xyz.sonet.app.R

object MessagingNotificationHelper {
    private const val CHANNEL_ID = "messages"
    private const val UPLOAD_CHANNEL_ID = "uploads"
    const val ACTION_REPLY = "xyz.sonet.app.ACTION_REPLY"
    const val EXTRA_CONVERSATION_ID = "conversation_id"
    const val EXTRA_REMOTE_INPUT = "remote_input"

    fun ensureChannel(context: Context) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(CHANNEL_ID, "Messages", NotificationManager.IMPORTANCE_HIGH)
            channel.description = "Direct messages"
            channel.enableLights(true)
            channel.lightColor = Color.WHITE
            val nm = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            nm.createNotificationChannel(channel)
            val upload = NotificationChannel(UPLOAD_CHANNEL_ID, "Uploads", NotificationManager.IMPORTANCE_LOW)
            upload.description = "Upload progress"
            nm.createNotificationChannel(upload)
        }
    }

    fun buildMessageNotification(
        context: Context,
        conversationId: String,
        conversationTitle: String,
        selfName: String,
        messages: List<Pair<String, String>> // (senderName, text)
    ): Notification {
        ensureChannel(context)

        val me = Person.Builder().setName(selfName).build()
        val style = MessagingStyle(me).setConversationTitle(conversationTitle)
        messages.forEach { (sender, text) ->
            style.addMessage(text, System.currentTimeMillis(), Person.Builder().setName(sender).build())
        }

        val remoteInput = RemoteInput.Builder(EXTRA_REMOTE_INPUT)
            .setLabel("Reply")
            .build()

        val replyIntent = Intent(context, MessagingReplyReceiver::class.java).apply {
            action = ACTION_REPLY
            putExtra(EXTRA_CONVERSATION_ID, conversationId)
        }
        val replyPending = PendingIntent.getBroadcast(
            context,
            conversationId.hashCode(),
            replyIntent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        val replyAction = NotificationCompat.Action.Builder(
            R.drawable.notification_icon,
            "Reply",
            replyPending
        ).addRemoteInput(remoteInput).build()

        val contentIntent = PendingIntent.getActivity(
            context,
            0,
            Intent(context, MainActivity::class.java),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        var builder = NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(R.drawable.notification_icon)
            .setStyle(style)
            .setContentTitle(conversationTitle)
            .setContentIntent(contentIntent)
            .setAutoCancel(true)
            .addAction(replyAction)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            builder = BubblesSupport.applyBubble(builder, context, conversationId)
        }
        return builder.build()
    }

    fun buildUploadProgressNotification(context: Context, title: String, progress: Int, max: Int, content: String? = null): Notification {
        ensureChannel(context)
        val builder = NotificationCompat.Builder(context, UPLOAD_CHANNEL_ID)
            .setSmallIcon(R.drawable.notification_icon)
            .setContentTitle(title)
            .setOnlyAlertOnce(true)
            .setOngoing(true)
            .setProgress(max, progress, false)
        if (content != null) builder.setContentText(content)
        return builder.build()
    }
}

