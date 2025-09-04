package xyz.sonet.app.notifications

import android.app.Person
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.graphics.drawable.Icon
import android.os.Build
import androidx.annotation.RequiresApi
import androidx.core.app.NotificationCompat
import xyz.sonet.app.MainActivity
import xyz.sonet.app.R

@RequiresApi(Build.VERSION_CODES.Q)
object BubblesSupport {
    fun applyBubble(builder: NotificationCompat.Builder, context: Context, conversationId: String): NotificationCompat.Builder {
        val target = PendingIntent.getActivity(
            context,
            conversationId.hashCode(),
            Intent(context, MainActivity::class.java),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
        val person = Person.Builder().setName("Conversation").setIcon(Icon.createWithResource(context, R.drawable.notification_icon)).build()
        val bubble = NotificationCompat.BubbleMetadata.Builder(target, Icon.createWithResource(context, R.drawable.notification_icon))
            .setDesiredHeight(600)
            .build()
        return builder.setBubbleMetadata(bubble).addPerson(person)
    }
}

